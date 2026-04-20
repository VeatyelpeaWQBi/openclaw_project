"""
盯盘助手报告生成模块
从游牧型T+1报告模块迁移，删除筛选股票逻辑，仅保留大盘和题材数据汇总
"""

import logging
import os
from core.data_access import get_index_realtime, get_market_sentiment, get_market_volume_compare
from core.paths import REPORTS_DIR

logger = logging.getLogger(__name__)


def generate_report(result):
    """
    根据策略结果生成通知报告（仅大盘和题材数据汇总）

    参数:
        result: WatchMonitorStrategy.run() 的返回值

    返回:
        str: 报告文本
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
                        lines.append(f"> **<font color=\"red\">{name}</font>**: {idx['price']:.2f} **<font color=\"red\">({sign}{idx['change_pct']:.2f}%)</font>**")
                    elif idx['change_pct'] < 0:
                        lines.append(f"> **<font color=\"green\">{name}</font>**: {idx['price']:.2f} **<font color=\"green\">({sign}{idx['change_pct']:.2f}%)</font>**")
                    else:
                        lines.append(f"> **{name}**: {idx['price']:.2f} **({sign}{idx['change_pct']:.2f}%)**")
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
                lines.append(f"> 上证(加权): {sh_change:+.2f}% | 中证全指(等权): {zz_change:+.2f}%")
                lines.append(f"> 分化系数: **{ratio:.2f}**")
                lines.append(f"> {desc}")
            lines.append("")

        # 市场情绪
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
                lines.append(f"> 上涨: **{up}**只({limit_up}涨停) | 下跌: **{down}**只({limit_down}跌停)")
                lines.append(f"> 涨跌比: **{ratio_str}** → {mood}")
                activity_rate = sentiment.get('activity_rate', 0)
                if activity_rate:
                    lines.append(f"> 活跃度: **{activity_rate:.2f}%**")

        # 成交量
        if volume:
            today = volume.get('today_amount', 0)
            yesterday = volume.get('yesterday_amount', 0)
            change_pct = volume.get('change_pct', 0)
            is_fang = volume.get('is_fangliang', False)
            direction = '放量' if is_fang else '缩量'
            lines.append(f"> 大盘成交额: **{today:.0f}亿** vs 昨日{yesterday:.0f}亿 — {direction}{abs(change_pct):.1f}%")
            lines.append("")

    except Exception as e:
        logger.warning(f"获取市场概况失败: {e}")
        lines.append("")

    if not top_sectors:
        lines.append("今日热门板块获取失败，今日跳过。")
        return '\n'.join(lines)

    # 热门板块（进攻型板块加粗）
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
            lines.append(f"{i}. **{name}**: **{sign}{pct}%**")
        else:
            lines.append(f"{i}. {name}: **{sign}{pct}%**")
    lines.append("")

    # 进攻型板块信息
    top10_attack = result.get('top10_attack', [])
    if top10_attack:
        attack_names = [s['name'] for s in top10_attack]
        lines.append(f"> 🎯 进攻型板块: {', '.join(attack_names)}")
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
            
            lines.append(f"> **{name}** ({sign}{change_pct:.2f}%) {attack_flag}")
            lines.append(f">   成分股: **{stock_count}**只")
            
            if lead_stocks:
                lead_str = ', '.join([f"{s['name']}({s['change_pct']:+.2f}%)" for s in lead_stocks])
                lines.append(f">   领涨股: {lead_str}")
            lines.append("")

    # 信号状态
    if not has_signal:
        reason_text = f"（{skip_reason}）" if skip_reason else ""
        lines.append(f"> 今日{reason_text}，暂无进攻型板块信号。")
    else:
        lines.append(f"> 今日有进攻型板块信号，建议关注。")

    report_path = os.path.join(REPORTS_DIR, f'report_{date_str}.md')
    lines.append("***")
    lines.append(f"报告已保存 | 运行时长见下方")

    return '\n'.join(lines)