#!/usr/bin/env python3
"""
尾盘T+1信号生成系统
每日14:45运行，生成买入信号并通过QQ通知主人
"""

import sys
import os
# 将本模块所在目录和 config 目录加入 path（支持直接运行和 import）
_src_dir = os.path.dirname(os.path.abspath(__file__))
_config_dir = os.path.join(os.path.dirname(_src_dir), 'config')
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)
if _config_dir not in sys.path:
    sys.path.insert(0, _config_dir)

from paths import DATA_DIR, REPORTS_DIR as REPORT_DIR
from sector_data import get_sector_ranking, get_sector_stocks
from stock_filter import filter_etf_candidates, calculate_volume_ratio
from sectors import is_attack_sector, filter_attack_sectors
from stock_data import get_stock_daily_kline_range
from supertrend import is_supertrend_bullish
from data_storage import (merge_and_save_kline, load_kline, save_signal,
                          save_report, get_month_str, get_kline_filepath)
import pandas as pd
from datetime import datetime, timedelta
import json


def get_last_trading_date():
    """
    获取上一个交易日日期（简单逻辑：跳过周末）
    """
    today = datetime.now()
    last_day = today - timedelta(days=1)
    # 跳过周日(6)和周六(5)
    if last_day.weekday() == 6:  # 周日
        last_day -= timedelta(days=2)
    elif last_day.weekday() == 5:  # 周六
        last_day -= timedelta(days=1)
    return last_day


def load_or_fetch_kline(stock_code, market='sh', stock_name='', sector_name=''):
    """
    加载已有日K数据，不足时增量获取

    参数:
        stock_code: 股票代码
        market: 市场
        stock_name: 股票名称（用于文件命名）
        sector_name: 所属板块（写入CSV）

    返回:
        DataFrame: 日K数据（SuperTrend最低需要15条）
    """
    from data_storage import INITIAL_FETCH_DAYS

    now = datetime.now()
    month_str = get_month_str(now)
    last_month_str = get_month_str(now - timedelta(days=31))

    existing = load_kline(stock_code, month_str)

    # 上个月的数据也加载（用于跨月）
    if last_month_str != month_str:
        prev_month = load_kline(stock_code, last_month_str)
        if not prev_month.empty:
            existing = pd.concat([prev_month, existing], ignore_index=True)
            existing['date'] = pd.to_datetime(existing['date'])
            existing = existing.drop_duplicates(subset='date', keep='last')
            existing = existing.sort_values('date').reset_index(drop=True)

    # 判断需要获取的日期范围
    if existing.empty:
        # 完全没有数据，获取近60天（SuperTrend需要足够多的历史数据）
        start_date = (now - timedelta(days=INITIAL_FETCH_DAYS)).strftime('%Y%m%d')
    else:
        # 从已有数据最后一天的前一天开始（刷新上一个交易日）
        last_date = existing['date'].max()
        fetch_start = last_date - timedelta(days=1)
        start_date = fetch_start.strftime('%Y%m%d')

    end_date = now.strftime('%Y%m%d')

    # 获取数据
    new_df = get_stock_daily_kline_range(stock_code, market=market,
                                         start_date=start_date, end_date=end_date)

    if not new_df.empty:
        # 合并并保存（当前月和上月）
        for m_str in [month_str, last_month_str]:
            month_data = new_df[new_df['date'].dt.strftime('%Y-%m') == m_str]
            if not month_data.empty:
                merge_and_save_kline(stock_code, month_data, m_str,
                                     stock_name=stock_name, sector_name=sector_name)

        # 重新加载合并后的数据
        combined = load_kline(stock_code, month_str)
        if last_month_str != month_str:
            prev = load_kline(stock_code, last_month_str)
            if not prev.empty:
                combined = pd.concat([prev, combined], ignore_index=True)
                combined['date'] = pd.to_datetime(combined['date'])
                combined = combined.drop_duplicates(subset='date', keep='last')
                combined = combined.sort_values('date').reset_index(drop=True)
        return combined
    else:
        return existing


def generate_report(date_str, top_sectors, candidates, has_signal):
    """
    生成通知报告

    返回:
        str: 报告文本
    """
    lines = []
    lines.append(f"📊 尾盘T+1信号 — {date_str}")
    lines.append("")

    if not top_sectors:
        lines.append("今日热门板块获取失败，今日跳过。")
        return '\n'.join(lines)

    # 热门板块
    lines.append("🔥 今日热门板块前5：")
    for s in top_sectors[:5]:
        pct = s.get('change_percent', 0)
        lines.append(f"  • {s['name']}: {'+' if pct > 0 else ''}{pct}%")
    lines.append("")

    if not has_signal:
        lines.append("今日有进攻型板块，但未筛选到符合条件的个股/ETF，今日跳过。")
        return '\n'.join(lines)

    # 有信号
    lines.append(f"⚠️ 发现买入信号（{len(candidates)}只）：")
    lines.append("")

    for i, c in enumerate(candidates, 1):
        emoji = "📈" if c.get('is_etf') else "🔺"
        lines.append(f"{emoji} {i}. {c['name']} ({c['code']})")
        lines.append(f"   板块：{c['sector']}")
        lines.append(f"   当日涨幅：{'+' if c['change_pct'] > 0 else ''}{c['change_pct']}%")
        lines.append(f"   量比：{c['volume_ratio']}")
        lines.append(f"   SuperTrend：日线{c['daily_supertrend']}")
        lines.append(f"   目标盈利：{c['target_profit']}")
        lines.append(f"   建议止损参考：{c['stop_loss']}")
        lines.append(f"   风险等级：{c['risk_level']}")
        lines.append("")

    report_path = os.path.join(REPORT_DIR, f'report_{date_str}.txt')
    lines.append(f"📝 报告已保存至：{report_path}")

    return '\n'.join(lines)


def run():
    """主运行函数"""
    date_str = datetime.now().strftime('%Y-%m-%d')
    print(f"=== 尾盘T+1信号生成 — {date_str} ===")

    # 1. 获取热门板块排名（行业+概念板块合并）
    print("获取热门板块排名...")
    industry_sectors = get_sector_ranking(sector_type=1, limit=20)  # 行业板块
    concept_sectors = get_sector_ranking(sector_type=2, limit=20)   # 概念板块

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

    if not all_sectors or len(all_sectors) < 5:
        report = f"📊 尾盘T+1信号 — {date_str}\n\n⚠️ 获取板块数据失败，请检查网络连接。"
        print(report)
        save_signal(date_str, [])
        save_report(date_str, report)
        return report

    # 2. 检查进攻型板块
    top10_sectors = all_sectors[:10]
    top10_attack = [s for s in top10_sectors if is_attack_sector(s['name'])]

    if not top10_attack:
        report = f"📊 尾盘T+1信号 — {date_str}\n\n今日热门板块前10中无进攻型题材，今日跳过。"
        print(report)
        save_signal(date_str, [])
        save_report(date_str, report)
        return report

    top5_sectors = all_sectors[:5]
    top5_attack = [s for s in top5_sectors if is_attack_sector(s['name'])]

    if not top5_attack:
        report = f"📊 尾盘T+1信号 — {date_str}\n\n今日热门板块前5中无进攻型题材，今日跳过。"
        print(report)
        save_signal(date_str, [])
        save_report(date_str, report)
        return report

    print(f"前10进攻型板块: {', '.join(s['name'] for s in top10_attack)}")
    print(f"前5进攻型板块: {', '.join(s['name'] for s in top5_attack)}")

    # 3. 从前5全部板块中筛选个股（不是只从进攻型）
    print("从前5板块中筛选个股...")
    all_candidates = []
    all_analyzed_stocks = []

    for sector in top5_sectors:  # 前5全部板块都分析
        print(f"  处理板块: {sector['name']}")

        # 获取板块内个股
        stocks = get_sector_stocks(sector['code'], sector_type=2, limit=30)

        if stocks:
            for stock in stocks[:15]:  # 每个板块最多分析15只
                code = stock['code']
                name = stock['name']
                change_pct = stock.get('change_pct', 0) or stock.get('change_percent', 0)
                turnover = stock.get('turnover', 0)

                # 跳过ST股
                if 'ST' in name or '*ST' in name:
                    continue

                # ===== 增量获取日K数据 =====
                market = 'sh' if code.startswith('6') else 'sz'
                df = load_or_fetch_kline(code, market=market, stock_name=name, sector_name=sector['name'])

                if df.empty or len(df) < 30:
                    # 数据不足，跳过
                    all_analyzed_stocks.append({
                        'code': code, 'name': name, 'sector': sector['name'],
                        'change_pct': change_pct, 'turnover': turnover,
                        'volume_ratio': 0, 'daily_supertrend': '-',
                        'is_candidate': False, 'reason': '数据不足'
                    })
                    continue

                # 检查各项条件
                volume_ratio = calculate_volume_ratio(df)
                daily_bullish = is_supertrend_bullish(df)

                # 判断是否符合条件
                reasons = []
                if change_pct < 3 or change_pct > 7:
                    reasons.append(f'涨幅{change_pct}%不在3-7%范围')
                if turnover < 5 or turnover > 15:
                    reasons.append(f'换手率{turnover}%不在5-15%范围')
                if volume_ratio < 1.2:
                    reasons.append(f'量比{volume_ratio:.2f}不足1.2')
                if not daily_bullish:
                    reasons.append('SuperTrend日线非多头')

                is_candidate = len(reasons) == 0
                filter_reason = '; '.join(reasons) if reasons else '符合条件'

                all_analyzed_stocks.append({
                    'code': code, 'name': name, 'sector': sector['name'],
                    'change_pct': change_pct, 'turnover': turnover,
                    'volume_ratio': round(volume_ratio, 2),
                    'daily_supertrend': '多头' if daily_bullish else '空头',
                    'is_candidate': is_candidate, 'reason': filter_reason
                })

                if is_candidate:
                    all_candidates.append({
                        'code': code, 'name': name, 'sector': sector['name'],
                        'change_pct': change_pct, 'turnover': turnover,
                        'volume_ratio': round(volume_ratio, 2),
                        'price': stock.get('price', 0),
                        'daily_supertrend': '多头',
                        'target_profit': '5-10%', 'stop_loss': '-3%',
                        'risk_level': '中等'
                    })

        # 如果个股没有符合条件的，尝试ETF（也需要满足涨幅+量比条件）
        if not any(c['sector'] == sector['name'] for c in all_candidates):
            etf_candidates = filter_etf_candidates(sector['name'])
            for etf in etf_candidates:
                code = etf['code']
                market = 'sh' if code.startswith('5') else 'sz'
                df = load_or_fetch_kline(code, market=market, stock_name=etf['name'], sector_name=sector['name'])
                if not df.empty:
                    # ETF也需要满足涨幅和量比条件
                    etf_change = etf.get('change_pct', 0)
                    etf_vol_ratio = etf.get('volume_ratio', 0)
                    if etf_change >= 1 and etf_vol_ratio >= 1.0:
                        all_candidates.append(etf)

    # 4. 去重和排序
    if all_candidates:
        all_candidates = sorted(all_candidates, key=lambda x: x.get('volume_ratio', 0), reverse=True)
        all_candidates = all_candidates[:5]

    # 5. 生成报告
    has_signal = len(all_candidates) > 0
    report = generate_report(date_str, top5_sectors, all_candidates, has_signal)

    # 6. 保存信号和报告到共享文件夹
    signal_file = save_signal(date_str, all_candidates)
    report_file = save_report(date_str, report)

    # 7. 生成分析统计
    candidate_count = sum(1 for s in all_analyzed_stocks if s['is_candidate'])
    analyzed_count = len(all_analyzed_stocks)

    print(f"\n📊 分析统计:")
    print(f"  分析股票总数: {analyzed_count}")
    print(f"  符合条件: {candidate_count}")
    print(f"  不符合: {analyzed_count - candidate_count}")

    reason_stats = {}
    for s in all_analyzed_stocks:
        if not s['is_candidate']:
            reasons = s['reason'].split('; ')
            for r in reasons:
                reason_stats[r] = reason_stats.get(r, 0) + 1

    if reason_stats:
        print(f"\n  不符合原因统计:")
        for reason, count in sorted(reason_stats.items(), key=lambda x: -x[1])[:5]:
            print(f"    • {reason}: {count}只")

    print(f"\n日K数据目录: {DATA_DIR}/kline/")
    print(f"信号文件: {signal_file}")
    print(f"报告文件: {report_file}")

    print("\n运行完成！")
    print(report)

    return report


if __name__ == '__main__':
    report = run()
    print("\n=== 最终报告 ===")
    print(report)
