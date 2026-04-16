"""
趋势交易回测报告生成器
生成概要报告和详细报告

用法:
    from backtest_engine.trend.trend_backtest_report import generate_backtest_report
    report = generate_backtest_report(...)
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def generate_backtest_report(account_id, start_date, end_date,
                             trade_dates, daily_results, monthly_records):
    """
    生成完整回测报告

    返回:
        str: Markdown格式的完整报告
    """
    parts = []

    # 概要报告
    parts.append(_generate_summary(
        account_id, start_date, end_date, trade_dates,
        daily_results, monthly_records,
    ))

    parts.append("")

    # 月度收益表
    parts.append(_generate_monthly_table(monthly_records))

    parts.append("")

    # 详细交易明细
    parts.append(_generate_detail_log(daily_results))

    return '\n'.join(parts)


def _generate_summary(account_id, start_date, end_date,
                      trade_dates, daily_results, monthly_records):
    """概要报告"""
    # 统计总动作
    total_open = sum(r.get('open_count', 0) for r in monthly_records)
    total_add = sum(r.get('add_count', 0) for r in monthly_records)
    total_reduce = sum(r.get('reduce_count', 0) for r in monthly_records)
    total_close = sum(r.get('close_count', 0) for r in monthly_records)

    # 起始/期末资金
    start_capital = monthly_records[0].get('start_capital', 0) if monthly_records else 0
    end_capital = monthly_records[-1].get('end_capital', 0) if monthly_records else 0
    total_profit = end_capital - start_capital
    total_profit_pct = (total_profit / start_capital * 100) if start_capital > 0 else 0

    # 账户昵称
    nickname = ''
    for dr in daily_results:
        nickname = dr.get('nickname', '')
        if nickname:
            break

    lines = [
        f"📊 趋势交易回测报告 — {nickname or account_id}",
        f"",
        f"**账户ID：** {account_id}",
        f"**回测区间：** {start_date} ~ {end_date}",
        f"**交易日数：** {len(trade_dates)} 天",
        f"**初始资金：** {start_capital:,.2f}",
        f"**期末资金：** {end_capital:,.2f}",
        f"**总盈亏：** {total_profit:+,.2f} ({total_profit_pct:+.2f}%)",
        f"",
        f"**交易统计：** 开仓{total_open}次 | 加仓{total_add}次 | 减仓{total_reduce}次 | 平仓{total_close}次",
    ]

    # 按年汇总（如果跨年）
    years = set(r['year_month'][:4] for r in monthly_records)
    if len(years) > 1:
        lines.append("")
        lines.append("---")
        lines.append("**年度汇总：**")
        lines.append("```")
        lines.append(f"{'年份':<8} {'收益':>12} {'收益率':>8} {'交易日':>6} {'开':>4} {'加':>4} {'减':>4} {'平':>4}")
        for year in sorted(years):
            year_records = [r for r in monthly_records if r['year_month'].startswith(year)]
            y_profit = sum(r['profit'] for r in year_records)
            y_start = year_records[0].get('start_capital', 0) if year_records else 0
            y_pct = (y_profit / y_start * 100) if y_start > 0 else 0
            y_days = sum(r['trade_days'] for r in year_records)
            y_open = sum(r['open_count'] for r in year_records)
            y_add = sum(r['add_count'] for r in year_records)
            y_reduce = sum(r['reduce_count'] for r in year_records)
            y_close = sum(r['close_count'] for r in year_records)
            lines.append(f"{year:<8} {y_profit:>+12,.2f} {y_pct:>+7.2f}% {y_days:>6} {y_open:>4} {y_add:>4} {y_reduce:>4} {y_close:>4}")
        lines.append("```")

    return '\n'.join(lines)


def _generate_monthly_table(monthly_records):
    """月度收益表"""
    if not monthly_records:
        return "**月度收益：** 无数据"

    lines = [
        "---",
        "**月度收益：**",
        "```",
        f"{'月份':<10} {'交易日':>5} {'月初资金':>12} {'月末资金':>12} {'收益':>12} {'收益率':>8} {'开':>3} {'加':>3} {'减':>3} {'平':>3}",
    ]

    for r in monthly_records:
        lines.append(
            f"{r['year_month']:<10} {r['trade_days']:>5} "
            f"{r['start_capital']:>12,.2f} {r['end_capital']:>12,.2f} "
            f"{r['profit']:>+12,.2f} {r['profit_pct']:>+7.2f}% "
            f"{r['open_count']:>3} {r['add_count']:>3} {r['reduce_count']:>3} {r['close_count']:>3}"
        )

    lines.append("```")
    return '\n'.join(lines)


def _generate_detail_log(daily_results):
    """详细交易明细（时序排列）"""
    lines = [
        "---",
        "**交易明细：**",
        "```",
    ]

    trade_count = 0
    for dr in daily_results:
        date_str = dr.get('date_str', '')
        robot_result = dr.get('robot_result')
        if not robot_result:
            continue

        results = robot_result.get('results', [])
        for r in results:
            if not r.get('success'):
                continue

            action = r.get('action', '')
            code = r.get('code', '')
            name = r.get('name', '')
            shares = r.get('executed_shares', 0)
            price = r.get('executed_price', 0)
            amount = r.get('executed_amount', 0)
            profit = r.get('profit', 0)
            units_before = r.get('units_before', 0)
            units_after = r.get('units_after', 0)

            # 动作中文映射
            action_cn = {
                'OPEN': '开仓',
                'ADD': '加仓',
                'REDUCE': '减仓',
                'CLOSE': '平仓',
                'CLOSE_STOP_LOSS': '平仓(止损)',
                'CLOSE_TAKE_PROFIT': '平仓(止盈)',
            }.get(action, action)

            line = f"{date_str}  {action_cn:<10} {code} {name:<8} {shares:>5}股 @ {price:.2f}  成交{amount:>10,.2f}"

            if action in ('REDUCE', 'CLOSE', 'CLOSE_STOP_LOSS', 'CLOSE_TAKE_PROFIT'):
                line += f"  盈亏{profit:>+10,.2f}"

            if units_after != units_before:
                line += f"  单位{units_before}→{units_after}"

            lines.append(line)
            trade_count += 1

    if trade_count == 0:
        lines.append("  (无成交记录)")

    lines.append("```")
    return '\n'.join(lines)
