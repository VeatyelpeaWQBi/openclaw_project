"""
游牧型T+1报告生成模块
从 src/main.py 的 generate_report() 抽取
"""

import logging
import os
from core.data_access import get_index_realtime, get_market_sentiment, get_market_volume_compare
from core.paths import REPORTS_DIR

try:
    import adata
    HAS_ADATA = True
except ImportError:
    HAS_ADATA = False

logger = logging.getLogger(__name__)


def generate_report(result):
    """
    根据策略结果生成通知报告

    参数:
        result: run_tail_t1_strategy() 的返回值（旧格式兼容）

    返回:
        str: 报告文本
    """
    date_str = result['date_str']
    top_sectors = result['top_sectors']
    candidates = result['candidates']
    has_signal = result['has_signal']
    skip_reason = result.get('skip_reason', '')

    lines = []
    lines.append(f"📊 尾盘T+1信号 — {date_str}")
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

    if not has_signal:
        reason_text = f"（{skip_reason}）" if skip_reason else ""
        lines.append(f"> 今日有进攻型板块{reason_text}，但未筛选到符合条件的个股/ETF，今日跳过。")
        return '\n'.join(lines)

    # 有信号
    lines.append(f"## ⚠️ 买入信号({len(candidates)}只)")
    lines.append("")

    for i, c in enumerate(candidates, 1):
        emoji = "📈" if c.get('is_etf') else "🔺"
        lines.append(f"**{emoji} {c['name']} ({c['code']})**")
        lines.append("")
        lines.append(f"- 板块：{c['sector']}")
        lines.append(f"- 当日涨幅：**{'+' if c['change_pct'] > 0 else ''}{c['change_pct']}%**")
        lines.append(f"- 量比：**{c['volume_ratio']}**")
        lines.append(f"- SuperTrend：日线**{c['daily_supertrend']}**")
        lines.append(f"- 目标盈利：{c['target_profit']}")
        lines.append(f"- 建议止损：{c['stop_loss']}")
        lines.append(f"- 风险等级：{c['risk_level']}")

        # 扫雷风险提示
        if HAS_ADATA:
            try:
                mine_df = adata.sentiment.mine.mine_clearance_tdx(c['code'])
                if mine_df is not None and not mine_df.empty:
                    lines.append("")
                    lines.append("> 🔴 **<font color=\"red\">该标的潜在风险如下：</font>**")
                    for j, (_, mrow) in enumerate(mine_df.iterrows(), 1):
                        f_type = mrow.get('f_type', '')
                        reason = mrow.get('reason', '')
                        score = mrow.get('score', '')
                        lines.append(f"> {j}. **[{f_type}]** {reason}（评分{score}）")
            except Exception as e:
                logger.debug(f"扫雷查询失败 [{c['code']}]: {e}")

        lines.append("")

    report_path = os.path.join(REPORTS_DIR, f'report_{date_str}.md')
    lines.append("***")
    lines.append(f"📝 报告已保存 | ⏱️ 运行时长见下方")

    # 异常/跳过统计
    all_analyzed = result.get('all_analyzed', [])
    if all_analyzed:
        skip_reasons = {}
        for s in all_analyzed:
            if not s.get('is_candidate', False):
                reason = s.get('reason', '未知')
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
        # 过滤掉常见原因，只显示异常
        exception_reasons = {k: v for k, v in skip_reasons.items()
                            if '异常' in k or '数据' in k}
        if exception_reasons:
            lines.append("")
            lines.append("⚠️ 异常情况：")
            for reason, count in exception_reasons.items():
                lines.append(f"  • {reason}: {count}只")

    return '\n'.join(lines)
