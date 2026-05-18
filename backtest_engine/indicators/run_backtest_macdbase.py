#!/usr/bin/env python3
"""
回测引擎入口脚本

执行MACD/OBV/EMA技术指标回测

用法:
    python run_backtest_macdbase.py
    python run_backtest_macdbase.py --start-date 2020-01-01 --end-date 2023-12-31
    python run_backtest_macdbase.py --signal-window 7
"""

import argparse
import logging
import sys
import os

# 添加项目根目录到路径
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from backtest_engine.indicators.config import BacktestConfig
from backtest_engine.indicators.backtest_engine import BacktestEngine

# 配置日志
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('logs/backtest.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='MACD/OBV/EMA 技术指标回测引擎')
    parser.add_argument('--start-date', type=str, help='回测起始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='回测结束日期 (YYYY-MM-DD)')
    parser.add_argument('--initial-capital', type=float, help='初始资金')
    parser.add_argument('--signal-window', type=int, choices=[3, 5, 7], help='信号检测窗口天数')
    parser.add_argument('--min-position', type=float, help='最小仓位比例 (0-1)')
    parser.add_argument('--max-position', type=float, help='最大仓位比例 (0-1)')

    args = parser.parse_args()

    # 创建配置
    config = BacktestConfig()

    # 覆盖命令行参数
    if args.start_date:
        config.START_DATE = args.start_date
    if args.end_date:
        config.END_DATE = args.end_date
    if args.initial_capital:
        config.INITIAL_CAPITAL = args.initial_capital
    if args.signal_window:
        config.SIGNAL_WINDOW = args.signal_window
    if args.min_position:
        config.MIN_POSITION_PCT = args.min_position
    if args.max_position:
        config.MAX_POSITION_PCT = args.max_position

    logger.info("回测配置:")
    logger.info(f"  时间范围: {config.START_DATE} ~ {config.END_DATE}")
    logger.info(f"  初始资金: {config.INITIAL_CAPITAL:,.0f}")
    logger.info(f"  信号窗口: {config.SIGNAL_WINDOW}天")
    logger.info(f"  仓位范围: {config.MIN_POSITION_PCT*100:.0f}% ~ {config.MAX_POSITION_PCT*100:.0f}%")
    logger.info(f"  单日最大建仓: {config.MAX_DAILY_OPEN}个")

    # 运行回测
    engine = BacktestEngine(config)
    result = engine.run()

    # 打印汇总
    result.print_summary()

    # 保存报告
    result.save_to_file()

    logger.info("回测完成！")


if __name__ == '__main__':
    main()