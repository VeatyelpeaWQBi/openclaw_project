#!/usr/bin/env python3
"""
Job: 统一刷新全部评分（RS + VCP + ADX）

场景：日K数据更新后，一键刷新近N天的所有评分。
支持全量和增量两种模式。

用法：
  python3 job/calc_scores.py --days 30            # 近日增量刷新（推荐日常使用）
  python3 job/calc_scores.py --full               # 全量刷新
  python3 job/calc_scores.py --days 30 --index 000510  # 指定基准指数

流程：
  1. 自动从交易日历获取最近交易日
  2. RS评分（需要250天预热）
  3. VCP评分（需要104天窗口）
  4. ADX评分（需要27天预热）
  5. 输出汇总
"""

import sys
import os
import logging
import argparse
import time
from datetime import datetime

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.storage import get_trading_day_offset, init_adx_table
from strategies.trend_trading.score.rs_core import calc_rs_scores_full, calc_rs_scores_recent
from strategies.trend_trading.score.vcp_core import calc_vcp_batch, calc_vcp_recent
from strategies.trend_trading.score.adx_core import calc_adx_batch, calc_adx_recent

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


DEFAULT_INDEX = "000510"
DEFAULT_DAYS = 30


def run(days=None, end_date=None, index_code=DEFAULT_INDEX, adx_period=14):
    """主入口"""
    if end_date is None:
        end_date = get_trading_day_offset(0)
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

    mode = f"近日{days}天" if days else "全量"
    logger.info(f"{'='*60}")
    logger.info(f"  评分统一刷新: {mode} | 结束日期: {end_date}")
    logger.info(f"  基准指数: {index_code} | ADX周期: {adx_period}")
    logger.info(f"{'='*60}")

    start = time.time()

    # 1. ADX表初始化
    init_adx_table()

    # 2. RS评分
    rs_start = time.time()
    if days:
        rs_count = calc_rs_scores_recent(index_code, end_date, days)
    else:
        rs_count = calc_rs_scores_full(index_code)
    rs_time = time.time() - rs_start
    logger.info(f"[1/3] RS完成: {rs_count}条, 耗时{rs_time:.0f}秒")

    # 3. VCP评分
    vcp_start = time.time()
    if days:
        vcp_count = calc_vcp_recent(end_date, days)
    else:
        vcp_count = calc_vcp_batch()
    vcp_time = time.time() - vcp_start
    logger.info(f"[2/3] VCP完成: {vcp_count}条, 耗时{vcp_time:.0f}秒")

    # 4. ADX评分
    adx_start = time.time()
    if days:
        adx_count = calc_adx_recent(end_date, days, adx_period)
    else:
        adx_count = calc_adx_batch(adx_period)
    adx_time = time.time() - adx_start
    logger.info(f"[3/3] ADX完成: {adx_count}条, 耗时{adx_time:.0f}秒")

    # 汇总
    total_time = time.time() - start
    logger.info(f"{'='*60}")
    logger.info(f"  全部完成!")
    logger.info(f"  RS: {rs_count}条 ({rs_time:.0f}秒)")
    logger.info(f"  VCP: {vcp_count}条 ({vcp_time:.0f}秒)")
    logger.info(f"  ADX: {adx_count}条 ({adx_time:.0f}秒)")
    logger.info(f"  总耗时: {total_time:.0f}秒 ({total_time/60:.1f}分钟)")
    logger.info(f"{'='*60}")

    return rs_count + vcp_count + adx_count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='统一刷新全部评分（RS+VCP+ADX）')
    parser.add_argument('--full', action='store_true', help='全量刷新')
    parser.add_argument('--days', type=int, default=DEFAULT_DAYS, help=f'近日刷新天数（默认{DEFAULT_DAYS}）')
    parser.add_argument('--end-date', type=str, default=None, help='结束日期 YYYY-MM-DD')
    parser.add_argument('--index', type=str, default=DEFAULT_INDEX, help='RS基准指数代码')
    parser.add_argument('--adx-period', type=int, default=14, help='ADX计算周期')
    args = parser.parse_args()

    if args.full:
        run(days=None, end_date=args.end_date, index_code=args.index, adx_period=args.adx_period)
    else:
        run(days=args.days, end_date=args.end_date, index_code=args.index, adx_period=args.adx_period)
