"""
趋势交易 — 日报生成模块
生成持仓监控、账户概况、信号提醒等报告
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def generate_report(signals, positions, account, candidates, today_opens=None):
    """
    生成趋势信号日报

    参数:
        signals: 信号列表 [{type, code, name, detail, urgency}]
        positions: 持仓列表 [{code, name, units, total_shares, avg_cost, current_stop, ...}]
        account: 账户摘要 {total, available, realized_profit}
        candidates: 候选池列表 [{code, name, source}]

    返回:
        str: 日报文本
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    lines = []

    lines.append(f"🐢 趋势信号日报 — {now}")
    lines.append("=" * 40)

    # === 账户概况 ===
    lines.append("")
    lines.append("💰 账户概况")
    total = account.get('total', 0)
    available = account.get('available', 0)
    realized = account.get('realized_profit', 0)
    used_pct = ((total - available) / total * 100) if total > 0 else 0

    lines.append(f"  总资金: {total:,.0f} 元")
    lines.append(f"  可用资金: {available:,.0f} 元 ({100 - used_pct:.1f}%)")
    lines.append(f"  已实现盈亏: {realized:+,.0f} 元")

    # === 仓位控制状态 ===
    unit_pct = account.get('unit_pct', 5.0)
    max_holdings = account.get('max_holdings', 5)
    if positions:
        holding_count = len(positions)
        lines.append(f"")
        lines.append(f"📐 仓位控制: 单位{unit_pct}% | 最多{max_holdings}个标的")
        lines.append(f"  当前持仓: {holding_count}个标的")

        # 标的数超限警告
        if holding_count > max_holdings:
            lines.append(f"  🚨 警告: 持仓标的数({holding_count})超出计划上限({max_holdings})！")

        # 单日开仓信息
        max_daily_open = account.get('max_daily_open', 2)
        # 需要从外部传入今日开仓数（这里先显示配置）
        lines.append(f"  单日开仓上限: {max_daily_open}个")

        # 各标的仓位预警 + 整除检查
        for pos in positions:
            units = pos.get('turtle_units', 0)
            code = pos.get('code', '')
            name = pos.get('name', '')
            total_shares = pos.get('total_shares', 0)
            avg_cost = pos.get('avg_cost', 0)

            # 实际持仓金额占比（用成本价估算）
            position_value = total_shares * avg_cost
            actual_pct = (position_value / total * 100) if total > 0 else 0
            # 计划占比
            planned_pct = units * unit_pct
            # 超出
            over_pct = actual_pct - planned_pct

            # 仓位预警
            if units > 4:
                lines.append(f"  🚨 [{code}]{name} {units}单位{total_shares}股，{actual_pct:.1f}%超出上限")
            elif abs(over_pct) > 1:  # 实际与计划偏差>1%
                sign = "+" if over_pct > 0 else ""
                lines.append(f"  ⚠️ [{code}]{name} {units}单位{total_shares}股，{actual_pct:.1f}%({sign}{over_pct:.1f}%)")
            elif units >= 3:
                lines.append(f"  ⚠️ [{code}]{name} {units}单位{total_shares}股，{actual_pct:.1f}%")

            # 整除检查
            spu = pos.get('shares_per_unit', 0)
            if not spu and units > 0:
                spu = total_shares // units
            if spu > 0 and total_shares > 0:
                remainder = total_shares % spu
                if remainder != 0:
                    lines.append(f"  🚨 [{code}]{name} 持仓{total_shares}股，{spu}股/单位，余{remainder}股不整除！")

    # === 持仓监控 ===
    lines.append("")
    lines.append("📊 持仓监控")

    if not positions:
        lines.append("  当前无持仓")
    else:
        for i, pos in enumerate(positions, 1):
            code = pos.get('code', '')
            name = pos.get('name', '')
            units = pos.get('turtle_units', 0)
            shares = pos.get('total_shares', 0)
            avg_cost = pos.get('avg_cost', 0)
            stop = pos.get('current_stop', 0)
            next_add = pos.get('next_add_price', 0)

            lines.append(f"  {i}. {code} {name}")
            lines.append(f"     {units}单位/{shares}股 | 成本{avg_cost:.2f}")
            lines.append(f"     止损{stop:.2f} | 加仓价{next_add:.2f}")

    # === 信号提醒 ===
    lines.append("")
    lines.append("🔔 信号提醒")

    # 紧急信号单独突出
    critical_signals = [s for s in signals if s.get('urgency') == 'critical']
    high_signals = [s for s in signals if s.get('urgency') == 'high']
    other_signals = [s for s in signals if s.get('urgency') not in ('critical', 'high')]

    if critical_signals:
        lines.append("")
        lines.append("🚨 紧急信号（需立即操作）:")
        for sig in critical_signals:
            lines.append(f"  ⛔ [{sig['code']}] {sig['name']}: {sig['detail']}")

    if high_signals:
        lines.append("")
        lines.append("⚠️ 重要信号:")
        for sig in high_signals:
            icon = '📤' if sig['type'] == 'exit' else '⚡'
            lines.append(f"  {icon} [{sig['code']}] {sig['name']}: {sig['detail']}")

    if other_signals:
        lines.append("")
        lines.append("📋 普通信号:")
        for sig in other_signals:
            icon = '➕' if sig['type'] == 'add' else '🆕' if sig['type'] == 'entry' else 'ℹ️'
            lines.append(f"  {icon} [{sig['code']}] {sig['name']}: {sig['detail']}")

    if not signals:
        lines.append("  暂无新信号")

    # === 候选池 ===
    # 按来源分组
    sources = {}
    for c in candidates:
        src = c.get('source', 'unknown')
        sources.setdefault(src, []).append(c)

    source_names = {'candidate': '候选池', 'hotspot': '热点池', 'manual': '自选池'}
    for src, stocks in sources.items():
        src_name = source_names.get(src, src)
        lines.append(f"  {src_name}: {len(stocks)} 只")

    # === 操作提示 ===
    if critical_signals:
        lines.append("")
        lines.append("💡 操作建议:")
        for sig in critical_signals:
            lines.append(f"  请立即卖出 [{sig['code']}] {sig['name']}")

    lines.append("")
    lines.append("=" * 40)
    lines.append("🤖 趋势交易 v1.0 | 仅供参考，投资有风险")

    logger.info(f'[日报生成] 信号{len(signals)}个, 持仓{len(positions)}只, 候选{len(candidates)}只')
    return '\n'.join(lines)


def generate_urgent_notice(signals):
    """
    生成紧急通知（仅止损信号）

    参数:
        signals: 信号列表

    返回:
        str or None: 紧急通知文本
    """
    critical = [s for s in signals if s.get('urgency') == 'critical']
    if not critical:
        return None

    lines = ["🚨 趋势交易 — 紧急止损通知 🚨", ""]
    for sig in critical:
        lines.append(f"⛔ {sig['code']} {sig['name']}")
        lines.append(f"   {sig['detail']}")
        lines.append("")
    lines.append("请立即执行卖出操作！")
    logger.info(f'[紧急通知] 止损信号{len(critical)}个')
    return '\n'.join(lines)
