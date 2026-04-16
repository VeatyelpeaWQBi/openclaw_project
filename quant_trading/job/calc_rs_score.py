#!/usr/bin/env python3
"""
Job: 计算RS Score

支持模式：
  python3 job/calc_rs_score.py 000510              # 全量刷新（默认）
  python3 job/calc_rs_score.py 000510 --full        # 全量刷新
  python3 job/calc_rs_score.py 000510 --days 30     # 近日增量（最近30天）

自动从交易日历获取预热数据。
"""

import sys
import os
import logging
import argparse
from datetime import datetime

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.storage import get_trading_day_offset_from
from strategies.trend_trading.score.rs_core import calc_rs_scores_full, calc_rs_scores_recent

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def run(index_code, days=None, end_date=None):
    """主入口"""
    if end_date is None:
        today = datetime.now().strftime('%Y-%m-%d')
        end_date = get_trading_day_offset_from(today, 0)  # 最近交易日
        if end_date is None:
            end_date = today

    if days is not None:
        logger.info(f"=== RS Score 近日刷新: {index_code}, 最近{days}天到 {end_date} ===")
        count = calc_rs_scores_recent(index_code, end_date, days)
    else:
        logger.info(f"=== RS Score 全量刷新: {index_code} ===")
        count = calc_rs_scores_full(index_code)

    logger.info(f"完成: {count} 条")
    return count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='RS Score 计算')
    parser.add_argument('index_code', help='基准指数代码（如 000510）')
    parser.add_argument('--full', action='store_true', help='全量刷新')
    parser.add_argument('--days', type=int, default=None, help='近日刷新天数（如 30）')
    parser.add_argument('--end-date', type=str, default=None, help='结束日期 YYYY-MM-DD（默认最近交易日）')
    args = parser.parse_args()

    if args.full:
        days = None
    elif args.days:
        days = args.days
    else:
        days = None  # 默认全量

    run(args.index_code, days, args.end_date)
