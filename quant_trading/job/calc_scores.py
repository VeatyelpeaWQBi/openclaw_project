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
  1. 一次性预加载全部股票日K + 指数收盘价到内存
  2. VCP + ADX 评分（不依赖基准指数，执行一次）
  3. RS 评分（依赖基准指数，按 watchlist 中每个 index 循环）
  4. 输出汇总
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

from core.storage import (
    get_trading_day_offset, get_trading_day_offset_from,
    get_all_stocks_daily_data, get_index_daily_closes,
)
from strategies.trend_trading.score._base import (
    get_all_stock_codes, get_index_members,
)
from strategies.trend_trading.score.rs_core import calc_rs_scores_from_data, calc_rs_scores_full
from strategies.trend_trading.score.vcp_core import calc_vcp_from_data, calc_vcp_batch
from strategies.trend_trading.score.adx_core import calc_adx_from_data, calc_adx_batch

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_INDEX = "000510"
DEFAULT_DAYS = 30


def preload_data(codes, index_code, end_date, lookback_days):
    """
    统一预加载：通过 storage 层加载全部股票日K + 指数收盘价到内存

    参数:
        codes: 全部股票代码列表
        index_code: 基准指数代码
        end_date: 结束日期 'YYYY-MM-DD'
        lookback_days: 往前取的天数（含end_date）

    返回:
        stock_data: {code: DataFrame} 每只股票的日K（date升序）
        index_closes: {date: close} 指数收盘价
        all_dates: list[str] 指数的日期列表（升序），作为全局时间轴
    """
    start_date = get_trading_day_offset_from(end_date, -lookback_days)
    if not start_date:
        logger.error("无法获取预加载起始日期")
        return {}, {}, []

    logger.info(f"预加载: {len(codes)}只股票, {start_date} ~ {end_date}")

    # 1. 批量加载全部股票完整日K（OHLCV）
    stock_data = get_all_stocks_daily_data(codes, start_date, end_date)

    # 2. 加载指数收盘价
    index_closes = get_index_daily_closes(index_code, start_date, end_date)

    # 3. 从指数提取日期列表作为全局时间轴
    all_dates = sorted(index_closes.keys())

    logger.info(f"预加载完成: 股票{len(stock_data)}只, 指数{len(all_dates)}天")
    return stock_data, index_closes, all_dates


def run_scores_without_index(stock_data, all_dates, days, adx_period=14):
    """
    运行 VCP + ADX 评分（不依赖基准指数）

    参数:
        stock_data: {code: DataFrame} 预加载的日K数据
        all_dates: list[str] 指数日期列表（升序）
        days: 计算天数
        adx_period: ADX计算周期

    返回:
        (vcp_count, adx_count)
    """
    logger.info(f"{'='*60}")
    logger.info(f"  VCP + ADX 评分: {days}天")
    logger.info(f"{'='*60}")

    # VCP
    vcp_start = time.time()
    try:
        vcp_count = calc_vcp_from_data(stock_data, all_dates, days)
    except Exception as e:
        logger.error(f"VCP评分异常: {e}")
        vcp_count = 0
    vcp_time = time.time() - vcp_start
    logger.info(f"VCP完成: {vcp_count}条, 耗时{vcp_time:.0f}秒")

    # ADX
    adx_start = time.time()
    try:
        adx_count = calc_adx_from_data(stock_data, all_dates, days, period=adx_period)
    except Exception as e:
        logger.error(f"ADX评分异常: {e}")
        adx_count = 0
    adx_time = time.time() - adx_start
    logger.info(f"ADX完成: {adx_count}条, 耗时{adx_time:.0f}秒")

    return vcp_count or 0, adx_count or 0


def run_rs(index_code, stock_data, all_dates, days):
    """
    运行单个基准指数的 RS 评分

    参数:
        index_code: 基准指数代码
        stock_data: {code: DataFrame} 预加载的日K数据
        all_dates: list[str] 指数日期列表（升序）
        days: 计算天数

    返回:
        int: 写入条数
    """
    logger.info(f"{'='*60}")
    logger.info(f"  RS评分: {index_code}, {days}天")
    logger.info(f"{'='*60}")

    rs_start = time.time()
    try:
        # 获取成分股子集
        rs_stock_codes = get_index_members(index_code)
        rs_closes = {code: stock_data[code] for code in rs_stock_codes if code in stock_data}
        # RS 需要 {code: {date: close}} 格式
        rs_closes_dict = {}
        for code, df in rs_closes.items():
            rs_closes_dict[code] = {
                row['date']: float(row['close'])
                for _, row in df.iterrows()
                if row['close'] is not None
            }

        # 需要重新加载该指数的收盘价
        start_date = all_dates[0]
        end_date = all_dates[-1]
        index_closes = get_index_daily_closes(index_code, start_date, end_date)

        rs_count = calc_rs_scores_from_data(
            index_code, rs_closes_dict, index_closes, all_dates, days
        )
    except Exception as e:
        logger.error(f"RS评分异常 [{index_code}]: {e}")
        rs_count = 0
    rs_time = time.time() - rs_start
    logger.info(f"RS完成 [{index_code}]: {rs_count}条, 耗时{rs_time:.0f}秒")

    return rs_count or 0


def run(days=None, end_date=None, index_code=DEFAULT_INDEX, adx_period=14):
    """CLI 主入口（VCP+ADX 一次 + RS 单指数）"""
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

    if days:
        # 增量模式：统一预加载
        codes = get_all_stock_codes()
        if not codes:
            logger.error("未找到股票代码")
            return 0

        max_lookback = 250 + days + 5  # 多取5天余量，应对指数数据缺失
        stock_data, index_closes, all_dates = preload_data(
            codes, index_code, end_date, max_lookback
        )
        if not all_dates:
            logger.error("预加载失败，无法继续")
            return 0

        vcp_count, adx_count = run_scores_without_index(stock_data, all_dates, days, adx_period)
        rs_count = run_rs(index_code, stock_data, all_dates, days)

    else:
        # 全量模式：各模块自行加载全部数据
        rs_start = time.time()
        try:
            rs_count = calc_rs_scores_full(index_code)
        except Exception as e:
            logger.error(f"[RS] 评分异常: {e}")
            rs_count = 0
        rs_time = time.time() - rs_start
        logger.info(f"[RS] 完成: {rs_count}条, 耗时{rs_time:.0f}秒")

        vcp_start = time.time()
        try:
            vcp_count = calc_vcp_batch()
        except Exception as e:
            logger.error(f"[VCP] 评分异常: {e}")
            vcp_count = 0
        vcp_time = time.time() - vcp_start
        logger.info(f"[VCP] 完成: {vcp_count}条, 耗时{vcp_time:.0f}秒")

        adx_start = time.time()
        try:
            adx_count = calc_adx_batch(period=adx_period)
        except Exception as e:
            logger.error(f"[ADX] 评分异常: {e}")
            adx_count = 0
        adx_time = time.time() - adx_start
        logger.info(f"[ADX] 完成: {adx_count}条, 耗时{adx_time:.0f}秒")

    # 汇总
    total_time = time.time() - start
    logger.info(f"{'='*60}")
    logger.info(f"  全部完成!")
    logger.info(f"  RS: {rs_count or 0}条")
    logger.info(f"  VCP: {vcp_count or 0}条")
    logger.info(f"  ADX: {adx_count or 0}条")
    logger.info(f"  总耗时: {total_time:.0f}秒 ({total_time/60:.1f}分钟)")
    logger.info(f"{'='*60}")

    return (rs_count or 0) + (vcp_count or 0) + (adx_count or 0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='统一刷新全部评分（RS+VCP+ADX）')
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--full', action='store_true', help='全量刷新')
    mode_group.add_argument('--days', type=int, default=DEFAULT_DAYS, help=f'近日刷新天数（默认{DEFAULT_DAYS}）')
    parser.add_argument('--end-date', type=str, default=None, help='结束日期 YYYY-MM-DD')
    parser.add_argument('--index', type=str, default=DEFAULT_INDEX, help='RS基准指数代码')
    parser.add_argument('--adx-period', type=int, default=14, help='ADX计算周期')
    args = parser.parse_args()

    if args.full:
        run(days=None, end_date=args.end_date, index_code=args.index, adx_period=args.adx_period)
    else:
        run(days=args.days, end_date=args.end_date, index_code=args.index, adx_period=args.adx_period)
