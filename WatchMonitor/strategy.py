"""
盯盘助手 - 大盘和题材数据汇总引擎
"""

import sqlite3
import logging

import pandas as pd

from datetime import datetime, timedelta

from core.data_access import (
    get_sector_ranking, get_sector_stocks,
    get_etf_daily_kline,
)
from core.indicators import is_supertrend_bullish, calculate_volume_ratio
from core.storage import merge_and_save_kline, get_daily_data_from_sqlite, INITIAL_FETCH_DAYS
from core.paths import DB_PATH

from config.sectors import is_attack_sector
from filters import filter_etf_candidates

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
    加载日K数据
    - 非ETF：直接从SQLite读取（日K Job已批量更新）
    - ETF：调API获取（暂无ETF日K Job）

    参数:
        stock_code: 股票/ETF代码
        market: 市场（ETF会自动识别）
        stock_name: 名称
        sector_name: 所属板块

    返回:
        DataFrame: 日K数据
    """
    # 判断是否为ETF代码
    is_etf = stock_code.startswith(('51', '159', '56', '58'))

    if is_etf:
        # ETF暂无批量更新Job，走API
        existing = get_daily_data_from_sqlite(stock_code)
        if existing.empty:
            start_date = (datetime.now() - timedelta(days=INITIAL_FETCH_DAYS)).strftime('%Y%m%d')
        else:
            last_date = pd.to_datetime(existing['date']).max()
            start_date = (last_date - timedelta(days=1)).strftime('%Y%m%d')
        end_date = datetime.now().strftime('%Y%m%d')
        new_df = get_etf_daily_kline(stock_code, start_date=start_date, end_date=end_date)
        if not new_df.empty:
            return merge_and_save_kline(stock_code, new_df, stock_name=stock_name, sector_name=sector_name)
        return existing
    else:
        # 个股：优先从SQLite读取
        df = get_daily_data_from_sqlite(stock_code)
        if df.empty:
            logger.warning(f"[{stock_code}] SQLite无数据，尝试API获取")
            start_date = (datetime.now() - timedelta(days=INITIAL_FETCH_DAYS)).strftime('%Y%m%d')
            end_date = datetime.now().strftime('%Y%m%d')
            from core.data_access import get_stock_daily_kline_range
            new_df = get_stock_daily_kline_range(stock_code, market=market,
                                                  start_date=start_date, end_date=end_date)
            if not new_df.empty:
                return merge_and_save_kline(stock_code, new_df, stock_name=stock_name, sector_name=sector_name)
            return pd.DataFrame()
        return df


class WatchMonitorStrategy:
    """盯盘助手策略 - 大盘和题材数据汇总"""

    @property
    def name(self) -> str:
        return 'watch_monitor'

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

    def run(self) -> dict:
        """
        执行盯盘助手策略 - 大盘和题材数据汇总

        返回:
            dict: {
                'date_str': str,
                'top_sectors': list,   # 前10板块
                'top10_attack': list,  # 前10中的进攻型板块
                'top5_attack': list,   # 前5中的进攻型板块
                'sector_details': list, # 板块详细数据（成分股数量、领涨股等）
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
                'top_sectors': [],
                'top10_attack': [],
                'top5_attack': [],
                'sector_details': [],
                'has_signal': False,
                'skip_reason': '热门板块不足5个',
                'metadata': {
                    'all_sectors': [],
                },
            }

        # 2. 检查进攻型板块
        top10_sectors = all_sectors[:10]
        top10_attack = [s for s in top10_sectors if is_attack_sector(s['name'])]

        top5_sectors = all_sectors[:5]
        top5_attack = [s for s in top5_sectors if is_attack_sector(s['name'])]

        logger.info(f"前10进攻型板块: {', '.join(s['name'] for s in top10_attack)}")
        logger.info(f"前5进攻型板块: {', '.join(s['name'] for s in top5_attack)}")

        # 3. 获取板块详细数据（成分股数量、领涨股等）
        sector_details = []
        for sector in top5_sectors:
            logger.info(f"  处理板块: {sector['name']}")

            try:
                stocks = get_sector_stocks(sector['code'], limit=30)
            except Exception as e:
                logger.error(f"  板块[{sector['name']}] 获取成分股失败: {e}")
                stocks = []

            # 统计板块数据
            stock_count = len(stocks) if stocks else 0
            
            # 获取领涨股（涨幅最大的前3只）
            lead_stocks = []
            if stocks:
                sorted_stocks = sorted(stocks, key=lambda x: x.get('change_pct', 0) or x.get('change_percent', 0), reverse=True)
                lead_stocks = sorted_stocks[:3]
            
            sector_details.append({
                'name': sector['name'],
                'code': sector['code'],
                'change_percent': sector.get('change_percent', 0),
                'stock_count': stock_count,
                'lead_stocks': [{'code': s['code'], 'name': s['name'], 'change_pct': s.get('change_pct', 0) or s.get('change_percent', 0)} for s in lead_stocks],
                'is_attack': is_attack_sector(sector['name']),
            })

        return {
            'date_str': date_str,
            'top_sectors': top10_sectors,
            'top10_attack': top10_attack,
            'top5_attack': top5_attack,
            'sector_details': sector_details,
            'has_signal': len(top10_attack) > 0,
            'skip_reason': '' if len(top10_attack) > 0 else '前10无进攻型题材',
            'metadata': {
                'all_sectors': all_sectors,
            },
        }

    def generate_report(self, result: dict) -> str:
        """生成报告（委托 report.py）"""
        from report import generate_report

        # 转换为旧格式以兼容 report.py
        legacy_result = {
            'date_str': result['date_str'],
            'top_sectors': result['top_sectors'],
            'top10_attack': result['top10_attack'],
            'sector_details': result.get('sector_details', []),
            'candidates': [],
            'all_analyzed': [],
            'has_signal': result['has_signal'],
            'skip_reason': result.get('skip_reason', ''),
        }
        return generate_report(legacy_result)


def main():
    """主入口：运行盯盘助手策略并输出报告（4部分，扫雷可选）"""
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    start_time = datetime.now()
    logger.info("=" * 50)
    logger.info("盯盘助手启动")
    logger.info("=" * 50)

    # 执行策略
    strategy = WatchMonitorStrategy()
    result = strategy.run()
    date_str = result['date_str']

    # 导入报告生成模块
    from report import generate_market_report, generate_position_report, generate_position_mine_report, generate_candidate_report, save_report_parts

    # 生成4部分报告（扫雷部分可选）
    part1 = generate_market_report(result)
    part2_upper = generate_position_report()
    part2_mine = generate_position_mine_report()
    part3 = generate_candidate_report()

    # 输出报告
    print("\n===== 部分1：大盘分析 =====")
    print(part1)
    print("\n===== 部分2上半：持仓池风险 =====")
    print(part2_upper)
    if part2_mine:
        print("\n===== 部分2下半：持仓池扫雷风险 =====")
        print(part2_mine)
    print("\n===== 部分3：候选池抄底 =====")
    print(part3)

    # 保存报告文件
    paths = save_report_parts(date_str, part1, part2_upper, part2_mine, part3)
    path_strs = [paths[0], paths[1]]
    if paths[2]:
        path_strs.append(paths[2])
    path_strs.append(paths[3])
    logger.info(f"报告已保存: {', '.join(path_strs)}")

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"运行完成，耗时 {elapsed:.1f} 秒")

    return result


if __name__ == '__main__':
    main()