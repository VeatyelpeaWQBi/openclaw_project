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

import logging


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

    return '\n'.join(lines)


def run():
    """主运行函数"""
    # 初始化日志系统
    setup_logging()
    logger = logging.getLogger(__name__)

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
        candidate_count = sum(1 for s in all_analyzed if s['is_candidate'])
        analyzed_count = len(all_analyzed)

        logger.info(f"📊 分析统计: 总数{analyzed_count}, 符合{candidate_count}, 不符合{analyzed_count - candidate_count}")

        reason_stats = {}
        for s in all_analyzed:
            if not s['is_candidate']:
                reasons = s['reason'].split('; ')
                for r in reasons:
                    reason_stats[r] = reason_stats.get(r, 0) + 1

        if reason_stats:
            top_reasons = ', '.join(f"{r}({c}只)" for r, c in sorted(reason_stats.items(), key=lambda x: -x[1])[:5])
            logger.info(f"不符合原因TOP5: {top_reasons}")

    logger.info(f"信号文件: {signal_file}")
    logger.info(f"报告文件: {report_file}")

    # 计算运行时长
    elapsed = time.time() - start_time
    elapsed_str = f"{int(elapsed//60)}分{int(elapsed%60)}秒"
    report += f"\n⏱️ 运行时长：{elapsed_str}"
    logger.info(f"⏱️ 总运行时长：{elapsed_str}")
    logger.info("运行完成！")

    return report


if __name__ == '__main__':
    report = run()
    logger.info("=== 最终报告 ===")
    logger.info(report)
