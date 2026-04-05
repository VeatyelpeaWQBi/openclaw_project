#!/usr/bin/env python3
"""
尾盘T+1信号生成系统 — 策略调度器
每日14:45运行，生成买入信号并通过QQ通知主人

职责：环境初始化 → 调用策略 → 生成报告 → 保存信号/报告
向后兼容：run() 默认使用 nomad_t1 策略
"""

import sys
import os
import time
import logging
from datetime import datetime

# 确保项目根目录和 config 在 sys.path 中
_project_root = os.path.dirname(os.path.abspath(__file__))
_config_dir = os.path.join(_project_root, 'config')
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _config_dir not in sys.path:
    sys.path.insert(0, _config_dir)

from core.log_setup import setup_logging
from core.storage import save_signal, save_report
from strategies import get_strategy, STRATEGY_MAP

logger = logging.getLogger(__name__)


def run(strategy_name='nomad_t1'):
    """
    主运行函数（向后兼容，默认 nomad_t1）

    参数:
        strategy_name: 策略名称，默认 'nomad_t1'

    返回:
        str: 报告文本
    """
    # 初始化日志系统
    setup_logging()

    start_time = time.time()
    date_str = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"=== 尾盘T+1信号生成 — {date_str} (策略: {strategy_name}) ===")

    # 1. 获取策略实例
    strategy = get_strategy(strategy_name)

    # 2. 执行策略
    result = strategy.run()

    # 3. 生成报告
    report = strategy.generate_report(result)

    # 4. 保存信号和报告
    # 兼容：nomad_t1返回candidates，turtle返回accounts
    if 'candidates' in result:
        signal_file = save_signal(date_str, result['candidates'])
    elif 'accounts' in result:
        all_candidates = []
        for acc in result.get('accounts', []):
            all_candidates.extend(acc.get('candidates', []))
        signal_file = save_signal(date_str, all_candidates)
    else:
        signal_file = ''
    report_file = save_report(date_str, report)

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
    # 支持命令行指定策略名，默认 nomad_t1
    strat = sys.argv[1] if len(sys.argv) > 1 else 'nomad_t1'
    report = run(strat)
    logger.info("=== 最终报告 ===")
    logger.info(report)
