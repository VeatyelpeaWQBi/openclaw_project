#!/usr/bin/env python3
"""
趋势交易回测入口脚本

用法:
  # 先创建回测账户（首次）
  python backtest_engine/trend/init_backtest_account.py --capital 100000 --name "趋势回测1号"

  # 运行回测
  python backtest_engine/trend/run_backtest.py --account-id 12345 --start 2026-01-01 --end 2026-04-16
  python backtest_engine/trend/run_backtest.py --account-id 12345 --start 2026-01-01 --end 2026-04-16 --codes 002261 300750 600519
"""

import sys
import os
import argparse
import logging
from datetime import datetime

# 路径设置
_BE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # backtest_engine
_PROJECT_ROOT = os.path.dirname(_BE_ROOT)  # openclaw_project
_QT_ROOT = os.path.join(_PROJECT_ROOT, 'quant_trading')
for p in [_BE_ROOT, _QT_ROOT]:
    if p not in sys.path:
        sys.path.insert(0, p)

from trend.trend_backtest_engine import TrendBacktestEngine


def _setup_logging():
    """配置日志：控制台 + shares/Log文件"""
    # 外部shares目录（VMware共享目录）
    # Linux: /mnt/hgfs/shares, Windows: D:\VMware\shares
    _linux_shares = '/mnt/hgfs/shares'
    _win_shares = os.path.join(os.path.dirname(_PROJECT_ROOT), 'shares')
    _shares_dir = _linux_shares if os.path.exists(_linux_shares) else _win_shares
    _log_dir = os.path.join(_shares_dir, 'Log')
    os.makedirs(_log_dir, exist_ok=True)

    today = datetime.now().strftime('%Y-%m-%d')
    log_file = os.path.join(_log_dir, f'趋势交易回测_{today}.log')

    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # 控制台
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    root_logger.addHandler(console)

    # 文件
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    return log_file


_log_file = _setup_logging()
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='趋势交易回测')
    parser.add_argument('--account-id', type=int, required=True, help='回测账户ID（simulator=1）')
    parser.add_argument('--start', type=str, required=True, help='回测起始日期 YYYY-MM-DD')
    parser.add_argument('--end', type=str, required=True, help='回测结束日期 YYYY-MM-DD')
    parser.add_argument('--codes', type=str, nargs='*', default=None,
                        help='指定股票代码列表（不填则从watchlist获取）')
    args = parser.parse_args()

    logger.info(f"日志文件: {_log_file}")

    engine = TrendBacktestEngine()
    result = engine.run_backtest(
        account_id=args.account_id,
        start_date=args.start,
        end_date=args.end,
        stock_codes=args.codes,
    )

    if 'error' in result:
        logger.error(f"回测失败: {result['error']}")
        sys.exit(1)

    # 输出报告到控制台
    print("\n" + "=" * 60)
    print(result['report'])
    print("=" * 60)
    print(f"\n耗时: {result['elapsed']}")
    print(f"月度记录已写入 backtest_monthly 表")

    # 保存报告到shares目录
    report_file = os.path.join(os.path.dirname(_log_file), f'趋势交易回测_{args.start}_{args.end}.md')
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(result['report'])
    print(f"报告已保存: {report_file}")


if __name__ == '__main__':
    main()
