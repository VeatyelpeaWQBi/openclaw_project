"""
游牧型T+1策略引擎
从 src/strategy.py 迁移，重构为 NomadT1Strategy 类
"""

import sys
import os
import pandas as pd
import sqlite3
import logging
from datetime import datetime, timedelta

from strategies.base import BaseStrategy
from core.data_access import (
    get_sector_ranking, get_sector_stocks,
    get_stock_daily_kline_range, get_etf_daily_kline,
)
from core.indicators import is_supertrend_bullish, calculate_volume_ratio
from core.storage import merge_and_save_kline, get_daily_data_from_sqlite, INITIAL_FETCH_DAYS
from core.paths import DB_PATH

# config 目录需要加入 sys.path 以导入 sectors
_config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'config')
if _config_dir not in sys.path:
    sys.path.insert(0, _config_dir)

from sectors import is_attack_sector
from strategies.nomad_t1.filters import filter_etf_candidates

logger = logging.getLogger(__name__)


def get_last_trading_date():
    """从SQLite交易日历获取上一个交易日"""
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT trade_date FROM trade_calendar WHERE trade_status=1 AND trade_date < ? ORDER BY trade_date DESC LIMIT 1",
            (today,)
        ).fetchone()
        conn.close()
        if row:
            return datetime.strptime(row[0], '%Y-%m-%d')
    except Exception:
        pass
    # fallback: 简单跳过周末
    last_day = datetime.now() - timedelta(days=1)
    if last_day.weekday() == 6:
        last_day -= timedelta(days=2)
    elif last_day.weekday() == 5:
        last_day -= timedelta(days=1)
    return last_day


def load_or_fetch_kline(stock_code, market='sh', stock_name='', sector_name=''):
    """
    加载已有日K数据，不足时增量获取
    自动识别ETF代码并使用对应的ETF日K接口

    参数:
        stock_code: 股票/ETF代码
        market: 市场（ETF会自动识别）
        stock_name: 名称
        sector_name: 所属板块

    返回:
        DataFrame: 日K数据（SuperTrend最低需要15条）
    """
    existing = get_daily_data_from_sqlite(stock_code)

    if existing.empty:
        start_date = (datetime.now() - timedelta(days=INITIAL_FETCH_DAYS)).strftime('%Y%m%d')
    else:
        last_date = pd.to_datetime(existing['date']).max()
        start_date = (last_date - timedelta(days=1)).strftime('%Y%m%d')

    end_date = datetime.now().strftime('%Y%m%d')

    # 判断是否为ETF代码
    is_etf = stock_code.startswith(('51', '159', '56', '58'))

    if is_etf:
        new_df = get_etf_daily_kline(stock_code, start_date=start_date, end_date=end_date)
    else:
        new_df = get_stock_daily_kline_range(stock_code, market=market,
                                              start_date=start_date, end_date=end_date)

    if new_df.empty:
        logger.warning(f"[{stock_code}] 日K数据获取为空 (start={start_date}, end={end_date})")
    else:
        logger.debug(f"[{stock_code}] 获取日K {len(new_df)} 条 (start={start_date}, end={end_date}), 最新: {new_df.iloc[-1].to_dict()}")

    if not new_df.empty:
        return merge_and_save_kline(stock_code, new_df, stock_name=stock_name, sector_name=sector_name)
    else:
        return existing


class NomadT1Strategy(BaseStrategy):
    """游牧型T+1策略"""

    @property
    def name(self) -> str:
        return 'nomad_t1'

    def _fetch_hot_sectors(self):
        """
        获取热门板块排名（行业+概念板块合并）

        返回:
            list: 排名后的板块列表
        """
        logger.info("获取热门板块排名...")
        industry_sectors = get_sector_ranking(sector_type=1, limit=20)
        logger.debug(f"行业板块: 获取 {len(industry_sectors)} 个, TOP5: {[s['name']+'('+str(s['change_percent'])+'%)' for s in industry_sectors[:5]]}")

        concept_sectors = get_sector_ranking(sector_type=2, limit=20)
        logger.debug(f"概念板块: 获取 {len(concept_sectors)} 个, TOP5: {[s['name']+'('+str(s['change_percent'])+'%)' for s in concept_sectors[:5]]}")

        # 过滤掉纯统计性的概念板块
        STATS_KEYWORDS = ['昨日', '连板', '涨停', '跌停', '炸板', '首板', '二板', '三板',
                          '四板', '五板', '龙头', '妖股', '强势', '弱势', 'ST板块']
        concept_sectors = [s for s in concept_sectors
                           if not any(kw in s['name'] for kw in STATS_KEYWORDS)]

        # 合并去重，按涨幅排序
        all_sectors = []
        seen_names = set()
        for s in industry_sectors + concept_sectors:
            if s['name'] not in seen_names:
                all_sectors.append(s)
                seen_names.add(s['name'])
        all_sectors.sort(key=lambda x: x.get('change_percent', 0), reverse=True)

        return all_sectors

    def _filter_stock(self, stock, sector_name):
        """
        两阶段筛选单只股票
        阶段一：用成分股自带数据轻量筛选（涨幅/换手率/ST/北交所），不请求日K
        阶段二：通过阶段一的股票才获取日K，计算量比+SuperTrend

        返回:
            (is_candidate: bool, stats: dict)
        """
        code = stock['code']
        name = stock['name']
        change_pct = stock.get('change_pct', 0) or stock.get('change_percent', 0)
        turnover = stock.get('turnover', 0)
        is_etf = code.startswith(('51', '159', '56', '58'))

        _skip_stats = lambda reason: {
            'code': code, 'name': name, 'sector': sector_name,
            'change_pct': change_pct, 'turnover': turnover,
            'volume_ratio': 0, 'daily_supertrend': '-',
            'is_candidate': False, 'reason': reason
        }

        # ========== 阶段一：轻量筛选（不请求日K） ==========

        # 跳过ST股
        if 'ST' in name or '*ST' in name:
            return False, _skip_stats('ST股')

        # 跳过北交所股票（代码83/87/430/92开头，新浪无数据）
        if code.startswith(('83', '87', '430', '92')):
            return False, _skip_stats('北交所股票')

        # 涨幅筛选（3%-7%），ETF放宽到1%-10%
        if is_etf:
            if change_pct < 1 or change_pct > 10:
                return False, _skip_stats(f'涨幅{change_pct}%不在1-10%范围')
        else:
            if change_pct < 3 or change_pct > 7:
                return False, _skip_stats(f'涨幅{change_pct}%不在3-7%范围')

        # 换手率筛选（5%-15%），ETF跳过
        if not is_etf:
            if turnover < 5 or turnover > 15:
                return False, _skip_stats(f'换手率{turnover}%不在5-15%范围')

        # ========== 阶段二：获取日K计算量比+SuperTrend ==========

        market = 'sh' if code.startswith(('5', '6', '9')) else 'sz'

        try:
            df = load_or_fetch_kline(code, market=market, stock_name=name, sector_name=sector_name)
        except Exception as e:
            logger.warning(f"[{code}] {name}: 日K获取异常: {type(e).__name__}: {e}")
            return False, _skip_stats(f'日K异常: {type(e).__name__}')

        if df.empty or len(df) < 30:
            return False, _skip_stats('数据不足(日K<30条)')

        # 量比
        volume_ratio = calculate_volume_ratio(df)
        if volume_ratio < 1.2:
            return False, _skip_stats(f'量比{volume_ratio:.2f}不足1.2')

        # SuperTrend
        daily_bullish = is_supertrend_bullish(df)
        if not daily_bullish:
            return False, _skip_stats('SuperTrend日线非多头')

        # ========== 全部通过 ==========

        logger.info(f"[{code}] {name} 候选! 涨幅={change_pct}%, 换手={turnover}%, 量比={volume_ratio:.2f}, SuperTrend=多头")

        return True, {
            'code': code, 'name': name, 'sector': sector_name,
            'change_pct': change_pct, 'turnover': turnover,
            'volume_ratio': round(volume_ratio, 2),
            'daily_supertrend': '多头',
            'price': stock.get('price', 0),
            'target_profit': '5-10%', 'stop_loss': '-3%',
            'risk_level': '较低' if is_etf else '中等',
            'is_etf': is_etf
        }

    def run(self) -> dict:
        """
        执行尾盘T+1选股策略

        返回:
            dict: {
                'date_str': str,
                'top_sectors': list,   # 前10板块
                'top10_attack': list,  # 前10中的进攻型板块
                'candidates': list,    # 筛选出的候选股
                'all_analyzed': list,  # 所有分析过的股票统计
                'has_signal': bool,
                'skip_reason': str,    # 跳过原因
                'metadata': dict,      # 额外元数据
            }
        """
        date_str = datetime.now().strftime('%Y-%m-%d')

        # 1. 获取热门板块排名
        all_sectors = self._fetch_hot_sectors()

        if not all_sectors or len(all_sectors) < 5:
            return {
                'date_str': date_str,
                'candidates': [],
                'signals': [],
                'has_signal': False,
                'skip_reason': '热门板块不足5个',
                'metadata': {
                    'top_sectors': [],
                    'top10_attack': [],
                    'all_analyzed': [],
                },
            }

        # 2. 检查进攻型板块
        top10_sectors = all_sectors[:10]
        top10_attack = [s for s in top10_sectors if is_attack_sector(s['name'])]

        if not top10_attack:
            return {
                'date_str': date_str,
                'candidates': [],
                'signals': [],
                'has_signal': False,
                'skip_reason': '前10无进攻型题材',
                'metadata': {
                    'top_sectors': all_sectors[:10],
                    'top10_attack': [],
                    'all_analyzed': [],
                },
            }

        top5_sectors = all_sectors[:5]
        top5_attack = [s for s in top5_sectors if is_attack_sector(s['name'])]

        if not top5_attack:
            return {
                'date_str': date_str,
                'candidates': [],
                'signals': [],
                'has_signal': False,
                'skip_reason': '前5无进攻型题材',
                'metadata': {
                    'top_sectors': top10_sectors,
                    'top10_attack': top10_attack,
                    'all_analyzed': [],
                },
            }

        logger.info(f"前10进攻型板块: {', '.join(s['name'] for s in top10_attack)}")
        logger.info(f"前5进攻型板块: {', '.join(s['name'] for s in top5_attack)}")

        # 3. 从前5全部板块中筛选个股
        logger.info("从前5板块中筛选个股...")
        all_candidates = []
        all_analyzed_stocks = []

        for sector in top5_sectors:
            logger.info(f"  处理板块: {sector['name']}")

            try:
                stocks = get_sector_stocks(sector['code'], limit=30)
            except Exception as e:
                logger.error(f"  板块[{sector['name']}] 获取成分股失败: {e}")
                continue

            logger.debug(f"  板块[{sector['name']}] 获取成分股 {len(stocks) if stocks else 0} 只")

            if stocks:
                for stock in stocks[:15]:
                    try:
                        is_cand, stats = self._filter_stock(stock, sector['name'])
                    except Exception as e:
                        logger.warning(f"  [{stock.get('code','?')}] {stock.get('name','?')} 筛选异常: {e}")
                        stats = {
                            'code': stock.get('code', '?'), 'name': stock.get('name', '?'),
                            'sector': sector['name'],
                            'change_pct': 0, 'turnover': 0, 'volume_ratio': 0,
                            'daily_supertrend': '-',
                            'is_candidate': False, 'reason': f'筛选异常: {e}'
                        }
                        is_cand = False
                    all_analyzed_stocks.append(stats)
                    if is_cand:
                        all_candidates.append(stats)

            # 如果个股没有符合条件的，尝试ETF
            if not any(c.get('sector') == sector['name'] for c in all_candidates):
                etf_candidates = filter_etf_candidates(sector['name'])
                for etf in etf_candidates:
                    code = etf['code']
                    market = 'sh' if code.startswith('5') else 'sz'
                    try:
                        df = load_or_fetch_kline(code, market=market, stock_name=etf['name'], sector_name=sector['name'])
                        if not df.empty:
                            etf_change = etf.get('change_pct', 0)
                            etf_vol_ratio = etf.get('volume_ratio', 0)
                            if etf_change >= 1 and etf_vol_ratio >= 1.0:
                                etf['is_candidate'] = True
                                all_candidates.append(etf)
                    except Exception as e:
                        logger.warning(f"  ETF [{code}] {etf.get('name','')} 异常: {e}")

        # 4. 去重和排序
        if all_candidates:
            all_candidates = sorted(all_candidates, key=lambda x: x.get('volume_ratio', 0), reverse=True)
            all_candidates = all_candidates[:10]

        return {
            'date_str': date_str,
            'candidates': all_candidates,
            'signals': all_candidates,  # 信号即候选
            'has_signal': len(all_candidates) > 0,
            'skip_reason': '',
            'metadata': {
                'top_sectors': top10_sectors,
                'top10_attack': top10_attack,
                'all_analyzed': all_analyzed_stocks,
            },
        }

    def generate_report(self, result: dict) -> str:
        """生成报告（委托 report.py）"""
        from strategies.nomad_t1.report import generate_report
        # 转换为旧格式以兼容 report.py
        legacy_result = {
            'date_str': result['date_str'],
            'top_sectors': result.get('metadata', {}).get('top_sectors', []),
            'top10_attack': result.get('metadata', {}).get('top10_attack', []),
            'candidates': result['candidates'],
            'all_analyzed': result.get('metadata', {}).get('all_analyzed', []),
            'has_signal': result['has_signal'],
            'skip_reason': result.get('skip_reason', ''),
        }
        return generate_report(legacy_result)
