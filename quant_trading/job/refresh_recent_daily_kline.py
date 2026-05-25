#!/usr/bin/env python3
"""
Job: 刷新个股日K数据

两种模式：
  1. 回补最近N个交易日个股日K（默认模式）
  2. 指定日期区间和股票列表刷新

用法：
  # 模式1: 回补最近N个交易日
  python job/refresh_recent_daily_kline.py [天数] [--calc-scores]

  # 模式2: 指定日期区间和股票列表
  python job/refresh_recent_daily_kline.py --start <yyyyMMdd> --end <yyyyMMdd> --codes <code1,code2,...> [--qfq] [--calc-scores]

示例：
  python job/refresh_recent_daily_kline.py 30
  python job/refresh_recent_daily_kline.py 30 --calc-scores
  python job/refresh_recent_daily_kline.py --start 20240101 --end 20240131 --codes 600028,002409 --qfq
  python job/refresh_recent_daily_kline.py --start 20240101 --end 20240131 --codes 600028,002409 --qfq --calc-scores
"""

import sys
import os
import sqlite3
import logging
import time
import random
import pandas as pd
from datetime import datetime

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.paths import DB_PATH
from core.storage import batch_upsert_daily_kline, get_watchlist_index_codes
from core.data_access import _sina_daily_kline
from job.calc_scores import preload_data, run_scores_without_index, run_rs
# VCP已屏蔽（计算开销大且效果不佳）
# from strategies.trend_trading.score.vcp_core import WINDOW

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

TRADE_DAYS = 30  # 回补天数


def get_recent_trade_days(n=TRADE_DAYS):
    """从 trade_calendar 获取最近N个交易日（从近到远，不含未来）"""
    today = datetime.now().strftime('%Y-%m-%d')
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT trade_date FROM trade_calendar
        WHERE trade_status = 1 AND trade_date <= ?
        ORDER BY trade_date DESC LIMIT ?
    """, (today, n)).fetchall()
    conn.close()
    days = [r[0] for r in rows]
    logger.info(f"最近{len(days)}个交易日: {days[-1]} ~ {days[0]}")
    return days

def get_stock_codes():
    """从 stock_info 获取全部股票代码"""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT code, name FROM stock_info ORDER BY code").fetchall()
    conn.close()
    logger.info(f"共 {len(rows)} 只股票待回补")
    return [(r[0], r[1]) for r in rows]


def get_stock_names(codes):
    """获取指定股票代码的名称"""
    if not codes:
        return []
    conn = sqlite3.connect(DB_PATH)
    placeholders = ','.join(['?'] * len(codes))
    rows = conn.execute(f"SELECT code, name FROM stock_info WHERE code IN ({placeholders})", codes).fetchall()
    conn.close()
    return [(r[0], r[1]) for r in rows]


def code_to_market(code):
    """根据代码判断市场前缀"""
    if code.startswith('6'):
        return 'sh'
    elif code.startswith(('0', '3')):
        return 'sz'
    return 'sh'  # 默认沪市


def fetch_and_save(stock_list, start_date, end_date):
    """
    逐只获取日K并批量写入

    参数:
        stock_list: [(code, name), ...]
        start_date: 'YYYY-MM-DD'
        end_date: 'YYYY-MM-DD'
        qfq: 是否前复权
    """
    # 转换为新浪接口要求的 YYYYMMDD 格式
    start_sina = start_date.replace('-', '')
    end_sina = end_date.replace('-', '')

    total_success = 0
    total_skip = 0
    total_error = 0

    logger.info(f"获取日K: {len(stock_list)}只股票 {start_date}~{end_date} 前复权")

    for idx, (code, name) in enumerate(stock_list):
        try:
            market = code_to_market(code)
            df = _sina_daily_kline(code, market=market, start_date=start_sina, end_date=end_sina)

            if df.empty:
                total_skip += 1
                time.sleep(random.uniform(0.5, 1.5))
                continue

            rows = []
            for _, row in df.iterrows():
                date_val = row.get('date', '')
                if hasattr(date_val, 'strftime'):
                    date_str = date_val.strftime('%Y-%m-%d')
                else:
                    date_str = str(date_val)[:10]

                rows.append((
                    code, name, date_str,
                    _safe_float(row.get('open')),
                    _safe_float(row.get('high')),
                    _safe_float(row.get('low')),
                    _safe_float(row.get('close')),
                    _safe_int(row.get('volume')),
                    _safe_float(row.get('amount')),
                    _safe_float(row.get('change_pct')),
                    _safe_float(row.get('turnover')),
                    _safe_float(row.get('pe_ratio')),
                    _safe_float(row.get('pb_ratio')),
                    _safe_float(row.get('mktcap')),
                    _safe_float(row.get('nmc')),
                    _safe_float(row.get('outstanding_share')),
                    None,  # volume_ratio 需要单独计算
                ))

            success, error = batch_upsert_daily_kline(rows)
            total_success += success
            total_error += error

        except Exception as e:
            logger.warning(f"[{code}] {name}: 获取失败 - {e}")
            total_error += 1

        # 每只股票间隔 0.5~1.5 秒
        time.sleep(random.uniform(0.5, 1.5))

        # 每100只输出进度
        if (idx + 1) % 100 == 0 or idx == len(stock_list) - 1:
            logger.info(f"  进度: {idx + 1}/{len(stock_list)} 只, "
                        f"成功{total_success}条, 跳过{total_skip}只, 失败{total_error}只")

    logger.info(f"回补完成: 成功{total_success}条, 跳过{total_skip}只, 失败{total_error}只")
    return total_success


def _safe_float(val):
    if val is None or (isinstance(val, str) and val.strip() in ('', '-', 'nan', 'None')):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val):
    if val is None or (isinstance(val, str) and val.strip() in ('', '-', 'nan', 'None')):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def run(days=None, calc_scores=False):
    n = days or TRADE_DAYS
    logger.info(f"=== 回补最近{n}个交易日个股日K ===")

    # 1. 获取最近N个交易日
    trade_days = get_recent_trade_days(n)
    if not trade_days:
        logger.error("trade_calendar 无数据，无法继续")
        return

    start_date = trade_days[-1]  # 最早的那天
    end_date = trade_days[0]     # 最新的那天

    # 3. 获取全部股票代码（或手动指定测试列表）
    # stock_list = get_stock_codes()
    stock_list = [("600028", "中国石化"), ("600735", "新华锦")]

    # 4. 逐只获取并写入
    total = fetch_and_save(stock_list, start_date, end_date)

    # 5. 计算评分（如果需要）
    if calc_scores:
        stock_codes = [code for code, _ in stock_list]
        calc_scores_for_stock_list(stock_codes, start_date, end_date)

    logger.info(f"=== 完成! {len(stock_list)}只股票, {total}条日K ===")


def run_custom(start_date, end_date, stock_codes, calc_scores=False):
    """
    指定日期区间和股票列表刷新日K

    参数:
        start_date: 'YYYY-MM-DD'
        end_date: 'YYYY-MM-DD'
        stock_codes: 股票代码列表 ['600028', '002409', ...]
        qfq: 是否前复权
        calc_scores: 是否计算评分
    """
    logger.info(f"=== 刷新日K: {start_date}~{end_date}, {len(stock_codes)}只股票 ===")

    # 获取股票名称
    stock_list = get_stock_names(stock_codes)
    if not stock_list:
        logger.warning("未找到指定的股票代码")
        return

    # 逐只获取并写入
    total = fetch_and_save(stock_list, start_date, end_date)

    # 计算评分（如果需要）
    if calc_scores:
        calc_scores_for_stock_list(stock_codes, start_date, end_date)

    logger.info(f"=== 完成! {len(stock_list)}只股票, {total}条日K ===")


def calc_scores_for_stock_list(stock_codes, start_date, end_date):
    """
    为指定股票列表计算历史日期范围的评分（ADX + RS，VCP已屏蔽）

    与 update_daily_kline.py 的区别：
    - update_daily_kline.py 只计算当天（days=1）
    - 本函数计算指定历史日期范围内的所有天数

    参数:
        stock_codes: 股票代码列表 ['600028', '002409', ...]
        start_date: 'YYYY-MM-DD' 计算起始日
        end_date: 'YYYY-MM-DD' 计算结束日
    """
    logger.info(f"=== 开始计算评分: {len(stock_codes)}只股票 ===")

    # 获取基准指数
    index_codes = get_watchlist_index_codes()
    index_code = index_codes[0] if index_codes else '000510'

    # 计算日期范围天数（日历天数，非交易日）
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    range_days = (end_dt - start_dt).days + 1

    # 计算各指标需要的预热期（VCP已屏蔽）
    # vcp_warmup = WINDOW - 1  # VCP: 90天窗口 + 14天ATR = 104，预热103天
    adx_warmup = 2 * 14 - 1  # ADX: 27天预热
    rs_warmup = 250  # RS: 250天历史数据

    # 取最大预热期 + 日历范围作为预加载天数
    max_warmup = max(adx_warmup, rs_warmup)
    lookback_days = max_warmup + range_days

    logger.info(f"计算范围: {start_date}~{end_date} ({range_days}天)")
    logger.info(f"预热期: ADX={adx_warmup}, RS={rs_warmup}, 最大={max_warmup} (VCP已屏蔽)")

    # 预加载数据（包含股票和指数）
    stock_data, index_closes, all_dates = preload_data(
        stock_codes, index_code, end_date, lookback_days
    )

    if not all_dates:
        logger.warning("预加载失败，跳过评分计算")
        return 0

    logger.info(f"预加载数据: 股票{len(stock_data)}只, 指数{len(all_dates)}天")

    # 计算各股票实际可计算的天数（基于股票K线数据，而不是指数）
    stock_calc_days_list = []
    for code, df in stock_data.items():
        df_len = len(df)
        if df_len >= max_warmup:
            calc_days_for_stock = df_len - max_warmup
        else:
            calc_days_for_stock = 0
        stock_calc_days_list.append(calc_days_for_stock)

        if df_len < max_warmup:
            logger.warning(f"股票 {code} 数据不足: 需要{max_warmup}天，实际{df_len}天")

    # 取所有股票的最小可计算天数，确保计算范围一致
    if not stock_calc_days_list:
        logger.error("没有足够数据计算评分")
        return 0

    calc_days = min(stock_calc_days_list)

    # 限制不超过用户指定的范围
    if calc_days > range_days:
        calc_days = range_days

    logger.info(f"实际可计算: {calc_days}个交易日 (基于股票K线数据)")
    if calc_days == 0:
        logger.error("无足够数据计算评分")
        return 0

    # 计算 ADX（VCP已屏蔽，不需要基准指数）
    vcp_count, adx_count = run_scores_without_index(stock_data, all_dates, calc_days)

    # 计算 RS（需要基准指数，按 watchlist 中每个指数循环）
    rs_count = run_rs(index_code, stock_data, all_dates, calc_days)

    total = adx_count + rs_count
    logger.info(f"=== 评分完成: ADX={adx_count}, RS={rs_count}, 总计={total} (VCP已屏蔽) ===")
    return total


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='刷新个股日K数据')
    parser.add_argument('days', nargs='?', type=int, help='回补最近N个交易日')
    parser.add_argument('--start', type=str, help='起始日期 yyyyMMdd')
    parser.add_argument('--end', type=str, help='截止日期 yyyyMMdd')
    parser.add_argument('--codes', type=str, help='股票代码列表，逗号分隔，如: 600028,002409')
    parser.add_argument('--calc-scores', action='store_true', help='日K刷新后自动计算评分')

    args = parser.parse_args()

    # 指定日期区间模式
    if args.start and args.end:
        if not args.codes:
            print("错误: 指定日期区间时必须提供股票代码列表 --codes")
            sys.exit(1)

        # 格式转换 yyyyMMdd -> yyyy-MM-dd
        def format_date(date_str):
            if len(date_str) == 8:
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            return date_str

        start_date = format_date(args.start)
        end_date = format_date(args.end)
        stock_codes = [c.strip() for c in args.codes.split(',')]

        run_custom(start_date, end_date, stock_codes, calc_scores=args.calc_scores)
    # 默认模式：回补最近N天
    else:
        run(days=args.days, calc_scores=args.calc_scores)
