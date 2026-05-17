"""
盯盘助手报告生成模块
拆分成4个独立部分：大盘分析 + 持仓池风险上半 + 持仓池扫雷下半(可选) + 候选池抄底
扫雷部分仅在检测到扫雷风险时生成
"""

import logging
import os
import sys

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # 添加根目录路径

from core.data_access import get_index_realtime, get_market_sentiment, get_market_volume_compare
from core.paths import REPORTS_DIR
from core.storage import get_daily_data_from_sqlite
from indicators import IndicatorManager
from signal_detector import detect_all_signals_with_trend
from fetch_fear_index import get_fear_index

logger = logging.getLogger(__name__)


def generate_market_report(result):
    """
    生成大盘分析报告（部分1）

    参数:
        result: WatchMonitorStrategy.run() 的返回值

    返回:
        str: 大盘分析报告文本
    """
    date_str = result['date_str']
    top_sectors = result['top_sectors']
    has_signal = result['has_signal']
    skip_reason = result.get('skip_reason', '')
    sector_details = result.get('sector_details', [])

    lines = []
    lines.append(f"📊 盯盘助手 — {date_str}")
    lines.append("")
    lines.append("***")
    lines.append("")

    # ========== 市场概况 ==========
    try:
        indices = get_index_realtime()
        sentiment = get_market_sentiment()
        volume = get_market_volume_compare()

        # 主要指数
        if indices:
            lines.append("## 📈 主要指数")
            lines.append("")
            show_indices = ['上证指数', '深证成指', '创业板指', '沪深300', '中证500', '中证1000']
            for name in show_indices:
                if name in indices:
                    idx = indices[name]
                    sign = '+' if idx['change_pct'] >= 0 else ''
                    if idx['change_pct'] > 0:
                        lines.append(f"**<font color=\"red\">{name}</font>**: {idx['price']:.2f} **<font color=\"red\">({sign}{idx['change_pct']:.2f}%)</font>**")
                    elif idx['change_pct'] < 0:
                        lines.append(f"**<font color=\"green\">{name}</font>**: {idx['price']:.2f} **<font color=\"green\">({sign}{idx['change_pct']:.2f}%)</font>**")
                    else:
                        lines.append(f"**{name}**: {idx['price']:.2f} **({sign}{idx['change_pct']:.2f}%)**")
            lines.append("")

        # 市场分化
        if indices and '上证指数' in indices and '中证全指' in indices:
            lines.append("## 🔀 市场分化")
            lines.append("")
            sh_change = indices['上证指数']['change_pct']
            zz_change = indices['中证全指']['change_pct']
            if sh_change != 0:
                ratio = zz_change / sh_change
                # 根据涨跌趋势和分化系数判断市场状态
                if ratio < 0:  # 分化系数为负：上证和中证全指方向相反
                    if sh_change > 0 and zz_change < 0:
                        desc = '🐊 鳄鱼张嘴（指数红个股跌，主力拉指数掩护出货，危险！）'
                    else:
                        desc = '🦅 题材先行（权重跌，题材涨，风格可能在切换）'
                elif sh_change > 0.3:  # 上涨趋势
                    if ratio > 1.2:
                        desc = '🟢 权重搭台，题材唱戏（大盘稳，题材涨，赚钱效应明显）'
                    elif ratio < 0.8:
                        desc = '🟡 只赚指数不赚钱（涨指数不涨题材，赚钱效应弱）'
                    else:
                        desc = '🟢 普涨行情'
                elif sh_change < -0.3:  # 下跌趋势
                    if ratio > 1.2:
                        desc = '🔴 题材踩踏（散户恐慌抛售题材股，亏钱效应炸裂）'
                    elif ratio < 0.8:
                        desc = '🟡 权重补跌（大盘跌，题材稳，可能见底）'
                    else:
                        desc = '🟠 市场弱势，单边下跌，小心系统性风险'
                else:  # 震荡
                    if abs(ratio - 1) > 0.3:
                        desc = '🜲 二八分化（板块轮动快，板块选对吃肉选错吃面）'
                    else:
                        desc = '⚪ 横盘磨叽（多空僵持，观望为主）'
                lines.append(f"- 上证(加权): {sh_change:+.2f}% | 中证全指(等权): {zz_change:+.2f}%")
                lines.append(f"- 分化系数: **{ratio:.2f}**")
                lines.append(f"- {desc}")
            lines.append("")

        # 市场情绪
        if sentiment:
            lines.append("## 💭 市场情绪")
            lines.append("")
            up = sentiment.get('up', 0)
            down = sentiment.get('down', 0)
            limit_up = sentiment.get('limit_up', 0)
            limit_down = sentiment.get('limit_down', 0)
            if up + down > 0:
                ratio_str = f"1:{down/up:.1f}" if up > 0 else "-"
                if down > up * 2:
                    mood = '🔴偏空'
                elif up > down:
                    mood = '🟢偏多'
                else:
                    mood = '🟡震荡'
                lines.append(f"- 上涨: **{up}**只({limit_up}涨停) | 下跌: **{down}**只({limit_down}跌停)")
                lines.append(f"- 涨跌比: **{ratio_str}** → {mood}")
                activity_rate = sentiment.get('activity_rate', 0)
                if activity_rate:
                    lines.append(f"- 活跃度: **{activity_rate:.2f}%**")
            lines.append("")

        # 恐贪指数
        try:
            fear_data = get_fear_index(timeout=30)
            if fear_data:
                lines.append("## 😨 恐贪指数")
                lines.append("")
                score = fear_data.get('score')
                score_decimal = fear_data.get('score_decimal')
                status = fear_data.get('status', '')
                update_time = fear_data.get('update_time', '')

                # 根据状态选择颜色和图标
                status_icons = {
                    '极度恐惧': ('🔴🔴', 'green'),
                    '恐惧': ('🔴', 'green'),
                    '中立': ('⚪', ''),
                    '贪婪': ('🟢', 'red'),
                    '极度贪婪': ('🟢🟢', 'red'),
                }
                icon, color = status_icons.get(status, ('⚪', ''))

                # 日期显示
                date_part = f"（{update_time}）" if update_time else ""

                if color:
                    lines.append(f"- {icon} 恐贪指数: **<font color=\"{color}\">{score}</font>** ({score_decimal}){date_part} → **<font color=\"{color}\">{status}</font>**")
                else:
                    lines.append(f"- {icon} 恐贪指数: **{score}** ({score_decimal}){date_part} → **{status}**")
                lines.append("")
        except Exception as e:
            logger.warning(f"获取恐贪指数失败: {e}")

        # 成交量
        if volume:
            today = volume.get('today_amount', 0)
            yesterday = volume.get('yesterday_amount', 0)
            change_pct = volume.get('change_pct', 0)
            is_fang = volume.get('is_fangliang', False)
            is_estimated = volume.get('yesterday_estimated', False)
            traded_minutes = volume.get('traded_minutes', 240)
            direction = '放量' if is_fang else '缩量'

            # 显示文案：估算时标注"≈昨日同期"，收盘后显示"昨日全天"
            if is_estimated and traded_minutes < 240:
                yesterday_label = f"≈昨日同期{yesterday:.0f}亿(估算)"
            else:
                yesterday_label = f"昨日{yesterday:.0f}亿"

            lines.append(f"大盘成交额: **{today:.0f}亿** vs {yesterday_label} — {direction}{abs(change_pct):.1f}%")
            lines.append("")

    except Exception as e:
        logger.warning(f"获取市场概况失败: {e}")
        lines.append("")

    # ========== ADX市场情绪 ==========
    adx_distribution = result.get('adx_distribution', {})
    if adx_distribution and adx_distribution.get('distribution'):
        lines.append("## 📈 ADX市场情绪")
        lines.append("")
        lines.append("**趋势强度+方向分布:**")
        distribution = adx_distribution['distribution']
        for item in distribution:
            type_name = item['type']
            count = item['count']
            pct = item['pct']
            color = item['color']
            lines.append(f"- {type_name}: {count}只 ({pct}%) {color}")
        lines.append("")
        summary = adx_distribution.get('summary', '')
        if summary:
            lines.append(f"**市场评价:** {summary}")
            lines.append("")

    # ========== 热门板块 ==========
    if not top_sectors:
        lines.append("今日热门板块获取失败。")
        lines.append("")
    else:
        lines.append("## 🔥 今日热门板块TOP10")
        lines.append("")
        top10_attack_names = set()
        for atk in result.get('top10_attack', []):
            top10_attack_names.add(atk['name'])
        for i, s in enumerate(top_sectors[:10], 1):
            name = s['name']
            pct = round(s.get('change_percent', 0), 2)
            sign = '+' if pct > 0 else ''
            if name in top10_attack_names:
                lines.append(f"- {i}. **{name}**: **{sign}{pct}%**")
            else:
                lines.append(f"- {i}. {name}: **{sign}{pct}%**")
        lines.append("")

        # 进攻型板块
        top10_attack = result.get('top10_attack', [])
        if top10_attack:
            attack_names = [s['name'] for s in top10_attack]
            lines.append(f"- 🎯 进攻型板块: {', '.join(attack_names)}")
            lines.append("")

        # 板块详细数据
        if sector_details:
            lines.append("## 板块详细数据")
            lines.append("")
            for detail in sector_details:
                name = detail['name']
                change_pct = detail['change_percent']
                stock_count = detail['stock_count']
                is_attack = detail['is_attack']
                lead_stocks = detail['lead_stocks']

                sign = '+' if change_pct > 0 else ''
                attack_flag = '进攻型' if is_attack else ''

                lines.append(f"- **{name}** ({sign}{change_pct:.2f}%) {attack_flag}")
                lines.append(f"     成分股: **{stock_count}**只")

                if lead_stocks:
                    lead_str = ', '.join([f"{s['name']}({s['change_pct']:+.2f}%)" for s in lead_stocks])
                    lines.append(f"     领涨股: {lead_str}")
                lines.append("")

        # 信号状态
        if not has_signal:
            reason_text = f"（{skip_reason}）" if skip_reason else ""
            lines.append(f"今日{reason_text}，暂无进攻型板块信号。")
        else:
            lines.append(f"今日有进攻型板块信号。")

    return '\n'.join(lines)


def generate_single_position_report(pr):
    """
    生成单只持仓的报告（内部函数）

    参数:
        pr: 单只持仓数据

    返回:
        list: 报告行列表
    """
    lines = []
    code = pr['code']
    name = pr['name']
    position_type = pr['position_type']
    entry_price = pr['entry_price']
    current_price = pr['current_price']
    profit_pct = pr['profit_pct']
    signals = pr['signals']
    mine_result = pr['mine_result']
    indicators = pr.get('indicators', {})

    # 持仓基本信息
    if profit_pct is None:
        profit_pct = 0
        profit_sign = ''
        profit_color = ''
    else:
        profit_sign = '+' if profit_pct >= 0 else ''
        profit_color = 'red' if profit_pct > 0 else 'green' if profit_pct < 0 else ''

    if current_price is None:
        lines.append(f"- **{name} ({code})** — {position_type}持仓")
        lines.append(f"  - 成本{entry_price:.2f} | 现价未知 (无日K数据)")
    elif profit_color:
        lines.append(f"- **{name} ({code})** — {position_type}持仓")
        lines.append(f"  - 成本{entry_price:.2f} | 现价<font color=\"{profit_color}\">{current_price:.2f} ({profit_sign}{profit_pct:.1f}%)</font>")
    else:
        lines.append(f"- **{name} ({code})** — {position_type}持仓")
        lines.append(f"  - 成本{entry_price:.2f} | 现价{current_price:.2f} ({profit_sign}{profit_pct:.1f}%)")

    # ========== 趋势状态显示 ==========
    trend = pr.get('trend', {})
    if trend:
        trend_type = trend.get('trend_type', '')

        # 趋势状态文本
        if trend_type == 'uptrend':
            trend_text = '📈上涨'
        elif trend_type == 'downtrend':
            trend_text = '📉下跌'
        elif trend_type == 'sideways':
            # 震荡时显示区间
            sideways_range = pr.get('sideways_range', '')
            if sideways_range:
                trend_text = f'🜲震荡 {sideways_range}'
            else:
                trend_text = '🜲震荡'
        else:
            trend_text = '🜲未知'

        lines.append(f"  - 趋势状态: {trend_text}")
    lines.append("")

    # 筛选真实风险信号（排除扫雷）
    risk_signals = [s for s in signals if s['type'] != 'mine_warning']

    # 先显示风险信号（如果有）
    if risk_signals:
        # 按严重度排序显示
        severity_order = {'fatal': 0, 'critical': 1, 'high': 2, 'medium': 3, 'warning': 4, 'info': 5, 'positive': 6}
        sorted_signals = sorted(risk_signals, key=lambda x: severity_order.get(x.get('severity', 'info'), 99))

        for sig in sorted_signals:
            severity = sig.get('severity', 'info')
            message = sig.get('message', '')

            # 根据严重度选择图标
            if severity == 'fatal':
                icon = '🔴🔴'
            elif severity == 'critical':
                icon = '🔴'
            elif severity == 'high':
                icon = '🔴'
            elif severity == 'medium':
                icon = '⚠️'
            elif severity == 'warning':
                icon = '⚠️'
            elif severity == 'positive':
                icon = '✅'
            else:
                icon = '💡'

            lines.append(f"  - {icon} {message}")
    else:
        lines.append("  - ✅ 暂无风险信号")
    lines.append("")

    # ========== 技术概要分析（通过IndicatorManager一站式调用） ==========
    # 获取日K数据用于指标分析
    df = get_daily_data_from_sqlite(code, days=340)
    if df is not None and not df.empty:
        lines.append("  - 📊 技术概要:")

        # 创建IndicatorManager实例
        manager = IndicatorManager()
        context = {
            'current_price': current_price,
            'is_position': True,
            'position_type': position_type,
            'entry_price': entry_price,
        }

        # 一站式分析（计算、信号、报告、评分全部在内部完成）
        result = manager.analyze_stock(code, df, context)

        # 直接获取报告内容（黑盒生成，无需二次加工）
        report_lines = result.get('report_lines', [])
        lines.extend(report_lines)

        # 获取综合评分
        total_score = result.get('total_score', 0)

        # 综合评价（评分结果已在指标对象内部计算）
        if total_score >= 6:
            trend_judge = '📈强势向上'
        elif total_score >= 3:
            trend_judge = '🟢偏强向上'
        elif total_score >= 1:
            trend_judge = '📊偏多震荡'
        elif total_score <= -6:
            trend_judge = '📉强势向下'
        elif total_score <= -3:
            trend_judge = '🔴偏弱向下'
        elif total_score <= -1:
            trend_judge = '📊偏空震荡'
        else:
            trend_judge = '⚪中性震荡'

        lines.append(f"    - **综合判断**: {trend_judge} (总分{total_score:.1f})")

    lines.append("")
    return lines


def generate_position_reports(group_size: int = 3):
    """
    生成持仓池风险信号报告（按组生成，避免消息过长）

    参数:
        group_size: 每组的个股数量（默认3）

    返回:
        list: 报告文本列表
    """
    try:
        signals_result = detect_all_signals_with_trend()
        position_risks = signals_result['position_risks']

        if not position_risks:
            return ["## 🚨 持仓池风险信号\n\n持仓池为空"]

        # 计算持仓总市值
        total_market_value = 0
        for pr in position_risks:
            shares = pr.get('shares', 0)
            current_price = pr.get('current_price', 0)
            if shares and current_price:
                total_market_value += shares * current_price

        # 分组
        groups = [position_risks[i:i + group_size] for i in range(0, len(position_risks), group_size)]
        reports = []

        # 格式化总市值（万元）
        if total_market_value >= 10000:
            total_value_str = f"{total_market_value/10000:.2f}万"
        else:
            total_value_str = f"{total_market_value:.0f}"

        for idx, group in enumerate(groups, 1):
            lines = []
            lines.append(f"## 🚨 持仓池风险信号 ({idx}/{len(groups)}) | 总市值: {total_value_str}")
            lines.append("")

            for pr in group:
                lines.extend(generate_single_position_report(pr))

            reports.append('\n'.join(lines))

        return reports

    except Exception as e:
        logger.warning(f"检测持仓池风险信号失败: {e}")
        return ["## 🚨 持仓池风险信号\n\n检测失败，请查看日志"]


def generate_position_report():
    """
    生成持仓池风险信号报告上半部分（部分2）
    - 显示持仓基本信息、风险信号、技术概要分析
    - 扫雷风险单独作为下半部分报告

    返回:
        str: 持仓池风险信号报告文本（不含扫雷）
    """
    reports = generate_position_reports(group_size=999)  # 不分组，返回完整报告
    return reports[0] if reports else ""


def generate_position_mine_report():
    """
    生成持仓池扫雷风险报告（部分2下半部分）
    - 仅显示扫雷风险项
    - 如果全部无扫雷项则返回空字符串

    返回:
        str: 扫雷风险报告文本，无扫雷项时返回空字符串
    """
    reports = generate_position_mine_reports(group_size=999)  # 不分组，返回完整报告
    return reports[0] if reports else ""


def generate_position_mine_reports(group_size: int = 3):
    """
    生成持仓池扫雷风险报告（按组生成，避免消息过长）

    参数:
        group_size: 每组的个股数量（默认3）

    返回:
        list: 扫雷报告文本列表，无扫雷项时返回空列表
    """
    try:
        signals_result = detect_all_signals_with_trend()
        position_risks = signals_result['position_risks']

        if not position_risks:
            return []  # 持仓池为空，无扫雷报告

        # 筛选有扫雷风险的持仓
        mined_positions = []
        for pr in position_risks:
            signals = pr.get('signals', [])
            mine_signals = [s for s in signals if s['type'] == 'mine_warning']
            if mine_signals:
                mined_positions.append(pr)

        if not mined_positions:
            return []  # 无扫雷项

        # 分组
        groups = [mined_positions[i:i + group_size] for i in range(0, len(mined_positions), group_size)]
        reports = []

        for idx, group in enumerate(groups, 1):
            lines = []
            lines.append(f"## 💣 持仓池扫雷风险 ({idx}/{len(groups)})")
            lines.append("")

            for pr in group:
                code = pr['code']
                name = pr['name']
                current_price = pr['current_price']
                signals = pr['signals']
                mine_result = pr['mine_result']

                # 扫雷警告
                mine_signals = [s for s in signals if s['type'] == 'mine_warning']

                lines.append(f"- **{name} ({code})** — 现价{current_price:.2f}")

                for ms in mine_signals:
                    message = ms.get('message', '')
                    lines.append(f"  - {message}")

                # 如果有mine_result详细信息，也显示
                if mine_result:
                    mine_items = mine_result.get('items', [])
                    if mine_items:
                        for item in mine_items:
                            item_desc = item.get('description', '')
                            if item_desc:
                                lines.append(f"    - {item_desc}")

                lines.append("")

            reports.append('\n'.join(lines))

        return reports

    except Exception as e:
        logger.warning(f"检测持仓池扫雷风险失败: {e}")
        return ["## 💣 持仓池扫雷风险\n\n扫雷检测失败，请查看日志"]


def generate_single_candidate_report(cs):
    """
    生成单只候选的报告（内部函数）

    参数:
        cs: 单只候选数据

    返回:
        list: 报告行列表
    """
    lines = []
    code = cs['code']
    name = cs['name']
    watch_type = cs['watch_type']
    watch_price = cs['watch_price']
    current_price = cs['current_price']
    drop_pct = cs['drop_pct']
    signals = cs['signals']
    mine_result = cs['mine_result']

    # 基本信息行
    drop_sign = '+' if drop_pct >= 0 else ''
    drop_color = 'red' if drop_pct > 0 else 'green' if drop_pct < 0 else ''

    lines.append(f"- **{name} ({code})** — {watch_type}")
    if drop_color:
        lines.append(f"  关注价{watch_price:.2f} | 现价<font color=\"{drop_color}\">{current_price:.2f} ({drop_sign}{drop_pct:.1f}%)</font>")
    else:
        lines.append(f"  关注价{watch_price:.2f} | 现价{current_price:.2f} ({drop_sign}{drop_pct:.1f}%)")

    # ========== 趋势状态显示 ==========
    trend = cs.get('trend', {})
    if trend:
        trend_type = trend.get('trend_type', '')

        # 趋势状态文本
        if trend_type == 'uptrend':
            trend_text = '📈上涨'
        elif trend_type == 'downtrend':
            trend_text = '📉下跌'
        elif trend_type == 'sideways':
            # 震荡时显示区间
            sideways_range = cs.get('sideways_range', '')
            if sideways_range:
                trend_text = f'🜲震荡 {sideways_range}'
            else:
                trend_text = '🜲震荡'
        else:
            trend_text = '🜲未知'

        lines.append(f"  - 趋势状态: {trend_text}")

    # 信号列表（只显示关键信号）
    if signals:
        for sig in signals:
            if sig['type'] in ['no_data', 'mine_warning']:
                continue
            message = sig.get('message', '')
            lines.append(f"  - {message}")

    # ========== 技术概览（候选池，通过IndicatorManager一站式调用） ==========
    # 获取日K数据用于指标分析
    df_candidate = get_daily_data_from_sqlite(code, days=340)
    if df_candidate is not None and not df_candidate.empty:
        lines.append("  - 📊 技术概览:")

        # 创建IndicatorManager实例
        manager = IndicatorManager()
        context = {
            'current_price': current_price,
            'is_candidate': True,
            'watch_price': watch_price,
        }

        # 一站式分析
        result = manager.analyze_stock(code, df_candidate, context)

        # 直接获取报告内容
        report_lines = result.get('report_lines', [])
        lines.extend(report_lines)

        # ========== 抄底机会综合判断 ==========
        total_score = result.get('total_score', 0)

        # 跌幅评分（候选池特有，跌幅越大抄底机会越好）
        drop_score = 0
        if drop_pct <= -10:
            drop_score = 4
        elif drop_pct <= -5:
            drop_score = 2
        elif drop_pct <= -2:
            drop_score = 1

        # 抄底机会评价（技术指标评分 + 跌幅评分）
        combined_score = total_score + drop_score

        if combined_score >= 5:
            judge = '🟢绝佳'
        elif combined_score >= 3:
            judge = '🟢较好'
        elif combined_score >= 1:
            judge = '🟡可关注'
        elif combined_score >= -1:
            judge = '⚪观望'
        else:
            judge = '🔴不宜'

        # 显示抄底机会
        lines.append(f"    - **抄底机会**: {judge}")

    lines.append("")
    return lines


def generate_candidate_reports(group_size: int = 3):
    """
    生成候选池抄底信号报告（按组生成，避免消息过长）

    参数:
        group_size: 每组的个股数量（默认3）

    返回:
        list: 报告文本列表
    """
    try:
        signals_result = detect_all_signals_with_trend()
        candidate_signals = signals_result['candidate_signals']

        if not candidate_signals:
            return ["## 🎯 候选池抄底信号\n\n候选池为空"]

        # 分组
        groups = [candidate_signals[i:i + group_size] for i in range(0, len(candidate_signals), group_size)]
        reports = []

        for idx, group in enumerate(groups, 1):
            lines = []
            lines.append(f"## 🎯 候选池抄底信号 ({idx}/{len(groups)})")
            lines.append("")

            for cs in group:
                lines.extend(generate_single_candidate_report(cs))

            reports.append('\n'.join(lines))

        return reports

    except Exception as e:
        logger.warning(f"检测候选池抄底信号失败: {e}")
        return ["## 🎯 候选池抄底信号\n\n检测失败，请查看日志"]


def generate_candidate_report():
    """
    生成候选池抄底信号报告（部分3）

    返回:
        str: 候选池抄底信号报告文本
    """
    reports = generate_candidate_reports(group_size=999)  # 不分组，返回完整报告
    return reports[0] if reports else ""


def generate_report(result):
    """
    生成完整报告（备用，合并4个部分）
    - 扫雷部分为空时不包含

    参数:
        result: WatchMonitorStrategy.run() 的返回值

    返回:
        str: 完整报告文本
    """
    lines = []

    # 部分1：大盘分析
    market_report = generate_market_report(result)
    lines.append(market_report)
    lines.append("")
    lines.append("***")
    lines.append("")

    # 部分2上半：持仓池风险（不含扫雷）
    position_report = generate_position_report()
    lines.append(position_report)
    lines.append("")
    lines.append("***")
    lines.append("")

    # 部分2下半：持仓池扫雷风险（仅在有扫雷项时显示）
    mine_report = generate_position_mine_report()
    if mine_report:
        lines.append(mine_report)
        lines.append("")
        lines.append("***")
        lines.append("")

    # 部分3：候选池抄底
    candidate_report = generate_candidate_report()
    lines.append(candidate_report)
    lines.append("")
    lines.append("***")
    lines.append(f"报告已保存")

    return '\n'.join(lines)


def save_report_parts(date_str, part1, part2_reports, part2_mine_reports, part3_reports):
    """
    保存报告文件（支持分组）

    参数:
        date_str: 日期字符串
        part1: 大盘分析报告
        part2_reports: 持仓池报告列表
        part2_mine_reports: 扫雷报告列表
        part3_reports: 候选池报告列表

    返回:
        dict: {
            'part1': str,
            'part2_upper': list,
            'part2_mine': list or None,
            'part3': list,
        }
    """
    from core.storage import ensure_dirs
    ensure_dirs()

    path1 = os.path.join(REPORTS_DIR, f'report_{date_str}_part1.md')

    with open(path1, 'w', encoding='utf-8') as f:
        f.write(part1)

    # 保存部分2分组报告
    part2_upper_paths = []
    for idx, report in enumerate(part2_reports, 1):
        path = os.path.join(REPORTS_DIR, f'report_{date_str}_part2_upper_{idx}.md')
        with open(path, 'w', encoding='utf-8') as f:
            f.write(report)
        part2_upper_paths.append(path)

    # 保存部分2下半（扫雷）- 支持分组
    part2_mine_paths = []
    if part2_mine_reports:
        for idx, report in enumerate(part2_mine_reports, 1):
            path = os.path.join(REPORTS_DIR, f'report_{date_str}_part2_mine_{idx}.md')
            with open(path, 'w', encoding='utf-8') as f:
                f.write(report)
            part2_mine_paths.append(path)

    # 保存部分3分组报告
    part3_paths = []
    for idx, report in enumerate(part3_reports, 1):
        path = os.path.join(REPORTS_DIR, f'report_{date_str}_part3_{idx}.md')
        with open(path, 'w', encoding='utf-8') as f:
            f.write(report)
        part3_paths.append(path)

    return {
        'part1': path1,
        'part2_upper': part2_upper_paths,
        'part2_mine': part2_mine_paths,
        'part3': part3_paths,
    }

