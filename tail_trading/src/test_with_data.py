#!/usr/bin/env python3
"""
用指定板块数据跑一遍，测试新的存储逻辑
"""

import sys
import os
import logging
sys.path.append('***REMOVED***/tail_trading/src')
sys.path.append('***REMOVED***/tail_trading/config')

from sector_data import get_sector_stocks
from stock_filter import filter_etf_candidates, calculate_volume_ratio
from stock_data import get_stock_daily_kline_range
from supertrend import is_supertrend_bullish
from data_storage import (merge_and_save_kline, save_signal, save_report,
                          get_month_str)
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)

REPORT_DIR = '***REMOVED***/reports'

# 上周五的进攻型板块数据
ATTACK_SECTORS = [
    {'code': '', 'name': 'CRO', 'change_percent': 4.73},
    {'code': '', 'name': '创新药', 'change_percent': 4.63},
    {'code': '', 'name': 'CAR-T细胞疗法', 'change_percent': 4.03},
]

def load_or_fetch_kline(stock_code, market='sh'):
    """增量加载日K"""
    from data_storage import load_kline, merge_and_save_kline
    now = datetime.now()
    month_str = get_month_str(now)
    last_month_str = get_month_str(pd.Timestamp(now) - pd.Timedelta(days=31))

    existing = load_kline(stock_code, month_str)
    if last_month_str != month_str:
        prev = load_kline(stock_code, last_month_str)
        if not prev.empty:
            existing = pd.concat([prev, existing], ignore_index=True)
            existing = existing.drop_duplicates(subset='date', keep='last')
            existing = existing.sort_values('date').reset_index(drop=True)

    if existing.empty:
        start_date = (now - pd.Timedelta(days=30)).strftime('%Y%m%d')
    else:
        last_date = existing['date'].max()
        fetch_start = last_date - pd.Timedelta(days=1)
        start_date = fetch_start.strftime('%Y%m%d')

    end_date = now.strftime('%Y%m%d')
    new_df = get_stock_daily_kline_range(stock_code, market=market,
                                          start_date=start_date, end_date=end_date)

    if not new_df.empty:
        for m_str in [month_str, last_month_str]:
            month_data = new_df[new_df['date'].dt.strftime('%Y-%m') == m_str]
            if not month_data.empty:
                merge_and_save_kline(stock_code, month_data, m_str)

        combined = load_kline(stock_code, month_str)
        if last_month_str != month_str:
            prev = load_kline(stock_code, last_month_str)
            if not prev.empty:
                combined = pd.concat([prev, combined], ignore_index=True)
                combined['date'] = pd.to_datetime(combined['date'])
                combined = combined.drop_duplicates(subset='date', keep='last')
                combined = combined.sort_values('date').reset_index(drop=True)
        return combined
    return existing


def generate_report(date_str, attack_sectors, candidates, has_signal):
    lines = []
    lines.append(f"📊 尾盘T+1信号 — {date_str}")
    lines.append("")
    lines.append("🔥 今日进攻型板块：")
    for s in attack_sectors[:5]:
        lines.append(f"  • {s['name']}: +{s['change_percent']}%")
    lines.append("")

    if not has_signal:
        lines.append("今日有进攻型板块，但未筛选到符合条件的个股/ETF，今日跳过。")
        return '\n'.join(lines)

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
    date_str = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"=== 基于指定板块数据跑一次 — {date_str} ===")

    # 直接从搜索API找板块内的个股（通过板块名称搜索）
    # 因为没有板块code，需要先找到板块code
    from sector_data import get_sector_ranking

    all_candidates = []
    all_analyzed = []

    # 先获取所有板块排名，找到匹配的板块code
    logger.info("获取板块数据...")
    all_sectors = get_sector_ranking(sector_type=2, limit=50)

    matched_sectors = []
    for target in ATTACK_SECTORS:
        for s in all_sectors:
            if s['name'] == target['name']:
                matched_sectors.append({**s, 'change_percent': target['change_percent']})
                break

    if not matched_sectors:
        logger.warning("未找到匹配的板块数据，尝试用ETF方式...")
        for target in ATTACK_SECTORS:
            etf_candidates = filter_etf_candidates(target['name'])
            for etf in etf_candidates:
                all_candidates.append(etf)
    else:
        for sector in matched_sectors[:3]:
            logger.info(f"处理板块: {sector['name']} (+{sector['change_percent']}%)")
            stocks = get_sector_stocks(sector['code'], sector_type=2, limit=30)

            if not stocks:
                logger.warning(f"未获取到 {sector['name']} 内个股")
                continue

            for stock in stocks[:15]:
                code = stock['code']
                name = stock['name']
                change_pct = stock.get('change_pct', 0) or stock.get('change_percent', 0)
                turnover = stock.get('turnover', 0)

                market = 'sh' if code.startswith('6') else 'sz'
                df = load_or_fetch_kline(code, market=market)

                if df.empty or len(df) < 30:
                    all_analyzed.append({
                        'code': code, 'name': name, 'sector': sector['name'],
                        'change_pct': change_pct, 'turnover': turnover,
                        'volume_ratio': 0, 'daily_supertrend': '-',
                        'is_candidate': False, 'reason': '数据不足'
                    })
                    continue

                volume_ratio = calculate_volume_ratio(df)
                daily_bullish = is_supertrend_bullish(df)

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

                all_analyzed.append({
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
                    logger.info(f"候选! {name}({code})")

            # ETF fallback
            if not any(c['sector'] == sector['name'] for c in all_candidates):
                etf_candidates = filter_etf_candidates(sector['name'])
                for etf in etf_candidates:
                    code = etf['code']
                    market = 'sh' if code.startswith('5') else 'sz'
                    df = load_or_fetch_kline(code, market=market)
                    if not df.empty:
                        all_candidates.append(etf)

    # 排序
    if all_candidates:
        all_candidates = sorted(all_candidates, key=lambda x: x.get('volume_ratio', 0), reverse=True)
        all_candidates = all_candidates[:5]

    has_signal = len(all_candidates) > 0
    report = generate_report(date_str, ATTACK_SECTORS, all_candidates, has_signal)

    signal_file = save_signal(date_str, all_candidates)
    report_file = save_report(date_str, report)

    candidate_count = sum(1 for s in all_analyzed if s['is_candidate'])
    analyzed_count = len(all_analyzed)

    logger.info(f"分析统计: 总数{analyzed_count}, 符合{candidate_count}, 不符合{analyzed_count - candidate_count}")
    logger.info(f"信号文件: {signal_file}")
    logger.info(f"报告文件: {report_file}")
    logger.info(report)

    return report


if __name__ == '__main__':
    run()
