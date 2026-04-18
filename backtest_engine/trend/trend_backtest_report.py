"""
趋势交易回测报告生成器
生成概要报告和详细报告

用法:
    from backtest_engine.trend.trend_backtest_report import generate_backtest_report
    report = generate_backtest_report(...)
"""

import logging
from datetime import datetime

from core.storage import get_db_connection

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

    # 详细交易明细（从position_flow表读取）
    parts.append(_generate_detail_from_flow(account_id))

    return '\n'.join(parts)


def _generate_summary(account_id, start_date, end_date,
                      trade_dates, daily_results, monthly_records):
    """概要报告"""
    # 统计总动作
    total_open = sum(r.get('open_count', 0) for r in monthly_records)
    total_add = sum(r.get('add_count', 0) for r in monthly_records)
    # total_reduce 已禁用（原版海龟无减仓规则）
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
        f"本程序作者：博德之门的巨龙杀手——神驱一梦",
        f"投资有风险，本程序输出一切报告仅用于量化程序学习验证用",
        f"不对任何投资/投机行为作为参考，不对任何人的买卖行为负责",
        f"请谨慎阅读",
        f"",
        f"**账户ID：** {account_id}",
        f"**回测区间：** {start_date} ~ {end_date}",
        f"**交易日数：** {len(trade_dates)} 天",
        f"**初始资金：** {start_capital:,.2f}",
        f"**期末资金：** {end_capital:,.2f}",
        f"**总盈亏：** {total_profit:+,.2f} ({total_profit_pct:+.2f}%)",
        f"",
        f"**交易统计：** 开仓{total_open}次 | 加仓{total_add}次 | 平仓{total_close}次",
    ]

    # 按年汇总（如果跨年）
    years = set(r['year_month'][:4] for r in monthly_records)
    if len(years) > 1:
        # 收集年度数据
        year_data = []
        for year in sorted(years):
            year_records = [r for r in monthly_records if r['year_month'].startswith(year)]
            y_profit = sum(r['profit'] for r in year_records)
            y_start = year_records[0].get('start_capital', 0) if year_records else 0
            y_end = year_records[-1].get('end_capital', 0) if year_records else 0
            y_pct = (y_profit / y_start * 100) if y_start > 0 else 0
            y_days = sum(r['trade_days'] for r in year_records)
            y_open = sum(r['open_count'] for r in year_records)
            y_add = sum(r['add_count'] for r in year_records)
            # y_reduce 已禁用（原版海龟无减仓规则）
            y_close = sum(r['close_count'] for r in year_records)
            year_data.append({
                'year': year,
                'trade_days': y_days,
                'start_capital': y_start,
                'end_capital': y_end,
                'profit': y_profit,
                'profit_pct': y_pct,
                'open_count': y_open,
                'add_count': y_add,
                'close_count': y_close,
            })
        
        # 年度汇总表格
        lines.append("")
        lines.append("---")
        lines.append("**年度汇总：**")
        lines.append("")
        lines.append("| 年份 | 交易日 | 年初资金 | 年末资金 | 收益 | 收益率 | 开 | 加 | 平 |")
        lines.append("|:------|:------:|----------:|----------:|----------:|:------:|:---:|:---:|:---:|")
        for y in year_data:
            lines.append(
                f"| {y['year']} | {y['trade_days']} | "
                f"{y['start_capital']:,.2f} | {y['end_capital']:,.2f} | "
                f"{y['profit']:>+,.2f} | {y['profit_pct']:>+,.2f}% | "
                f"{y['open_count']} | {y['add_count']} | {y['close_count']} |"
            )

    return '\n'.join(lines)


def _generate_monthly_table(monthly_records):
    """月度收益表（Markdown表格格式）"""
    if not monthly_records:
        return "**月度收益：** 无数据"

    lines = [
        "---",
        "**月度收益：**",
        "",
        "| 月份 | 交易日 | 月初资金 | 月末资金 | 收益 | 收益率 | 开 | 加 | 平 |",
        "|:------|:------:|----------:|----------:|----------:|:------:|:---:|:---:|:---:|",
    ]

    for r in monthly_records:
        lines.append(
            f"| {r['year_month']} | {r['trade_days']} | "
            f"{r['start_capital']:,.2f} | {r['end_capital']:,.2f} | "
            f"{r['profit']:>+,.2f} | {r['profit_pct']:>+,.2f}% | "
            f"{r['open_count']} | {r['add_count']} | {r['close_count']} |"
        )

    return '\n'.join(lines)


def _generate_detail_from_flow(account_id):
    """从position_flow表读取交易明细"""
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT operate_date, code, name, action, shares, price, amount, profit,
                   units_before, units_after
            FROM position_flow
            WHERE account_id = ?
            ORDER BY operate_date
        """, (account_id,)).fetchall()
    finally:
        conn.close()

    lines = [
        "---",
        "**交易明细：**",
        "",
        "| 日期 | 动作 | 股票 | 名称 | 数量 | 价格 | 成交额 | 盈亏 | 单位变化 |",
        "|:------|:------|:------|:------|------:|------:|----------:|----------:|:----------|",
    ]

    trade_count = 0
    for r in rows:
        date_str = r['operate_date'] or ''
        action = r['action'] or ''
        code = r['code'] or ''
        name = r['name'] or ''
        shares = r['shares'] or 0
        price = r['price'] or 0
        amount = r['amount'] or 0
        profit = r['profit'] or 0
        units_before = r['units_before'] or 0
        units_after = r['units_after'] or 0

        units_str = f"{units_before}→{units_after}" if units_after != units_before else "-"
        # 减仓和平仓类显示盈亏
        profit_str = f"{profit:>+,.2f}" if action in ('减仓', '清仓止损', '清仓止盈', '部分平仓') else "-"

        lines.append(
            f"| {date_str} | {action} | {code} | {name} | "
            f"{shares} | {price:.2f} | {amount:,.2f} | {profit_str} | {units_str} |"
        )
        trade_count += 1

    if trade_count == 0:
        lines.append("| (无成交记录) | | | | | | | | |")

    return '\n'.join(lines)
