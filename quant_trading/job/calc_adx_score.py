#!/usr/bin/env python3
"""
Job: 计算ADX评分

支持模式：
  python3 job/calc_adx_score.py                  # 全量刷新（默认）
  python3 job/calc_adx_score.py --full           # 全量刷新
  python3 job/calc_adx_score.py --days 30        # 近日增量（最近30天）

自动从交易日历获取预热数据（ADX需要2*period-1天预热）。
"""

import sys
import os
import logging
import argparse
from datetime import datetime

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.storage import get_trading_day_offset
from strategies.trend_trading.score.adx_core import calc_adx_batch, calc_adx_recent

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_PERIOD = 14


def run(days=None, end_date=None, period=DEFAULT_PERIOD):
    """主入口"""
    if end_date is None:
        end_date = get_trading_day_offset(0)
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

    if days is not None:
        logger.info(f"=== ADX 近日刷新: 最近{days}天到 {end_date} ===")
        count = calc_adx_recent(end_date, days, period)
    else:
        logger.info(f"=== ADX 全量刷新 ===")
        count = calc_adx_batch(period)

    logger.info(f"完成: {count} 条")
    return count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ADX评分计算')
    parser.add_argument('--full', action='store_true', help='全量刷新')
    parser.add_argument('--days', type=int, default=None, help='近日刷新天数（如 30）')
    parser.add_argument('--end-date', type=str, default=None, help='结束日期 YYYY-MM-DD')
    parser.add_argument('--period', type=int, default=DEFAULT_PERIOD, help='ADX计算周期')
    args = parser.parse_args()

    if args.full:
        days = None
    elif args.days is not None:
        if args.days <= 0:
            parser.error('--days 必须为正整数')
        days = args.days
    else:
        days = None

    run(days, args.end_date, args.period)
