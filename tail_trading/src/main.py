#!/usr/bin/env python3
"""
尾盘T+1信号生成系统
每日14:45运行，生成买入信号并通过QQ通知主人
职责：环境初始化 → 调用策略引擎 → 生成报告 → 保存信号/报告
"""

import sys
import os
_src_dir = os.path.dirname(os.path.abspath(__file__))
_config_dir = os.path.join(os.path.dirname(_src_dir), 'config')
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)
if _config_dir not in sys.path:
    sys.path.insert(0, _config_dir)

from paths import DATA_DIR, REPORTS_DIR as REPORT_DIR
from log_setup import setup_logging
from strategy import run_tail_t1_strategy
from data_storage import save_signal, save_report
from data_source import get_index_realtime, get_market_sentiment, get_market_volume_compare

import logging

# 初始化模块级日志器
logger = logging.getLogger(__name__)


def generate_report(result):
    """
    根据策略结果生成通知报告

    参数:
        result: run_tail_t1_strategy() 的返回值

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

    # ========== 市场概况 ==========
    try:
        indices = get_index_realtime()
        sentiment = get_market_sentiment()
        volume = get_market_volume_compare()

        # 主要指数
        if indices:
            lines.append("【主要指数】")
            show_indices = ['上证指数', '深证成指', '创业板指', '沪深300', '中证500', '中证1000']
            for name in show_indices:
                if name in indices:
                    idx = indices[name]
                    sign = '+' if idx['change_pct'] >= 0 else ''
                    lines.append(f"  {name}: {idx['price']:.2f} ({sign}{idx['change_pct']:.2f}%)")
            lines.append("")

        # 市场分化
        if indices and '上证指数' in indices and '中证全指' in indices:
            lines.append("【市场分化】")
            sh_change = indices['上证指数']['change_pct']
            zz_change = indices['中证全指']['change_pct']
            if sh_change != 0:
                ratio = zz_change / sh_change
                if ratio > 1.2:
                    desc = '中小盘股跌更多，市场偏空'
                elif ratio < 0.8:
                    desc = '权重股跌更多，市场偏空'
                else:
                    desc = '市场分化不明显'
                lines.append(f"  上证(加权): {sh_change:+.2f}% | 中证全指(等权参考): {zz_change:+.2f}%")
                lines.append(f"  分化系数: {ratio:.2f} → {desc}")
            lines.append("")

        # 市场情绪
        if sentiment:
            lines.append("【市场情绪】")
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
                lines.append(f"  上涨: {up}只({limit_up}涨停) | 下跌: {down}只({limit_down}跌停)")
                lines.append(f"  涨跌比: {ratio_str}  情绪: {mood}")

        # 成交量
        if volume:
            today = volume.get('today_amount', 0)
            yesterday = volume.get('yesterday_amount', 0)
            change_pct = volume.get('change_pct', 0)
            is_fang = volume.get('is_fangliang', False)
            direction = '放量' if is_fang else '缩量'
            lines.append(f"  大盘成交额: {today:.0f}亿 vs 昨日{yesterday:.0f}亿 {direction}{abs(change_pct):.1f}%")
            lines.append("")

    except Exception as e:
        logger.warning(f"获取市场概况失败: {e}")
        lines.append("")

    if not top_sectors:
        lines.append("今日热门板块获取失败，今日跳过。")
        return '\n'.join(lines)

    # 热门板块
    lines.append("🔥 今日热门板块前10：")
    for s in top_sectors[:10]:
        pct = s.get('change_percent', 0)
        lines.append(f"  • {s['name']}: {'+' if pct > 0 else ''}{pct}%")
    lines.append("")
    
    # 进攻型板块信息
    top10_attack = result.get('top10_attack', [])
    if top10_attack:
        attack_names = [s['name'] for s in top10_attack]
        lines.append(f"🎯 前10中包含的进攻型板块：{', '.join(attack_names)}")
        lines.append("")

    if not has_signal:
        reason_text = f"（{skip_reason}）" if skip_reason else ""
        lines.append(f"今日有进攻型板块{reason_text}，但未筛选到符合条件的个股/ETF，今日跳过。")
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


def run():
    """主运行函数"""
    # 初始化日志系统
    setup_logging()

    import time
    start_time = time.time()
    date_str = __import__('datetime').datetime.now().strftime('%Y-%m-%d')
    logger.info(f"=== 尾盘T+1信号生成 — {date_str} ===")

    # 1. 调用策略引擎
    result = run_tail_t1_strategy()

    # 2. 生成报告
    report = generate_report(result)

    # 3. 保存信号和报告
    signal_file = save_signal(date_str, result['candidates'])
    report_file = save_report(date_str, report)

    # 4. 输出分析统计
    all_analyzed = result.get('all_analyzed', [])
    if all_analyzed:
        candidate_count = sum(1 for s in all_analyzed if s.get('is_candidate', False))
        analyzed_count = len(all_analyzed)
        exception_count = sum(1 for s in all_analyzed if '异常' in s.get('reason', ''))

        logger.info(f"📊 分析统计: 总数{analyzed_count}, 符合{candidate_count}, 不符合{analyzed_count - candidate_count - exception_count}, 异常{exception_count}")

        reason_stats = {}
        for s in all_analyzed:
            if not s.get('is_candidate', False):
                reason = s.get('reason', '未知')
                reasons = reason.split('; ')
                for r in reasons:
                    reason_stats[r] = reason_stats.get(r, 0) + 1

        if reason_stats:
            top_reasons = ', '.join(f"{r}({c}只)" for r, c in sorted(reason_stats.items(), key=lambda x: -x[1])[:5])
            logger.info(f"不符合原因TOP5: {top_reasons}")

    logger.info(f"信号文件: {signal_file}")
    logger.info(f"报告文件: {report_file}")

    # 计算运行时长
    elapsed = time.time() - start_time
    if elapsed < 1:
        elapsed_str = f"{int(elapsed*1000)}毫秒"
    else:
        elapsed_str = f"{int(elapsed//60)}分{int(elapsed%60)}秒"
    report += f"\n⏱️ 运行时长：{elapsed_str}"
    logger.info(f"⏱️ 总运行时长：{elapsed_str}")
    logger.info("运行完成！")

    return report


if __name__ == '__main__':
    report = run()
    logger.info("=== 最终报告 ===")
    logger.info(report)
