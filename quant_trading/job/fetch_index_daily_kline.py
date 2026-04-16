#!/usr/bin/env python3
"""
Job: 获取指数日K数据
通过中证指数官网API获取指数的日K线数据，写入 index_daily_kline 表

用法：
  python3 job/fetch_index_daily_kline.py                      # 全部指数，最近30天
  python3 job/fetch_index_daily_kline.py --days 5             # 全部指数，最近5天
  python3 job/fetch_index_daily_kline.py 000510               # 指定指数，最近30天
  python3 job/fetch_index_daily_kline.py 000510 --days 5      # 指定指数，最近5天
  python3 job/fetch_index_daily_kline.py 000510 20240923 20260408  # 指定日期区间

数据源：ak.stock_zh_index_hist_csindex（中证指数有限公司）
导入表：index_daily_kline
"""

import sys
import os
import time
import random
import logging
import sqlite3
import argparse

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.paths import DB_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_DAYS = 30


def get_all_index_codes():
    """从 index_info 获取全部指数代码"""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT code, short_name FROM index_info ORDER BY code").fetchall()
    conn.close()
    return [(r[0], r[1] or '') for r in rows]


def fetch_index_daily_kline(code, start_date=None, end_date=None):
    """
    从中证指数官网获取日K数据

    参数:
        code: 指数代码
        start_date: 起始日期 YYYYMMDD（None则从发布日起）
        end_date: 截止日期 YYYYMMDD（None则到今天）

    返回:
        DataFrame 或 None
    """
    import akshare as ak
    from datetime import datetime

    # 获取发布日期作为默认起始日
    if start_date is None:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT publish_date FROM index_info WHERE code = ?", (code,)
        ).fetchone()
        conn.close()
        if row and row[0]:
            start_date = row[0].replace('-', '')
        else:
            start_date = '20200101'

    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')

    logger.info(f"[{code}] 获取日K: {start_date} ~ {end_date}")

    try:
        df = ak.stock_zh_index_hist_csindex(
            symbol=code,
            start_date=start_date,
            end_date=end_date,
        )
        logger.info(f"[{code}] 获取完成: {len(df)} 条")
        return df
    except Exception as e:
        logger.error(f"[{code}] 获取失败: {e}")
        return None


def save_to_db(code, df):
    """
    写入 index_daily_kline 表

    参数:
        code: 指数代码
        df: CSIndex返回的DataFrame

    返回:
        int: 写入条数
    """
    if df is None or df.empty:
        return 0

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 获取指数名称
    index_name = str(df.iloc[0].get('指数中文简称', ''))

    rows = []
    for _, row in df.iterrows():
        date = str(row.get('日期', ''))[:10]
        rows.append((
            code, index_name, date,
            _safe_float(row.get('开盘')),
            _safe_float(row.get('最高')),
            _safe_float(row.get('最低')),
            _safe_float(row.get('收盘')),
            _safe_float(row.get('成交量')),
            _safe_float(row.get('成交金额')),
            _safe_float(row.get('涨跌')),
            _safe_float(row.get('涨跌幅')),
            _safe_int(row.get('样本数量')),
            _safe_float(row.get('滚动市盈率')),
        ))

    cursor.executemany("""
        INSERT OR REPLACE INTO index_daily_kline
        (index_code, index_name, date, open, high, low, close,
         volume, amount, change, change_pct, constituent_count, pe_ttm)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    success = len(rows)

    # 更新 index_info
    cursor.execute("""
        UPDATE index_info SET daily_kline_done = 1, last_update_at = datetime('now')
        WHERE code = ?
    """, (code,))

    conn.commit()
    conn.close()

    logger.info(f"[{code}] {index_name}: {success}条日K已写入")
    return success


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


def calc_start_date_from_days(days):
    """从最近N个交易日推算起始日期"""
    from datetime import datetime, timedelta
    # 用日历天数 * 1.5 留余量
    start = (datetime.now() - timedelta(days=int(days * 1.5))).strftime('%Y%m%d')
    return start


def run_single(code, start_date=None, end_date=None):
    """单个指数完整流程"""
    df = fetch_index_daily_kline(code, start_date, end_date)
    if df is None or df.empty:
        logger.warning(f"[{code}] 无数据")
        return 0
    return save_to_db(code, df)


def run_all(days=None):
    """
    批量获取全部指数的日K

    参数:
        days: 最近N天（None则从发布日起全量）

    返回:
        int: 总写入条数
    """
    from datetime import datetime

    indexes = get_all_index_codes()
    if not indexes:
        logger.error("index_info 表中无指数数据")
        return 0

    logger.info(f"=== 批量获取指数日K: {len(indexes)} 个指数 ===")

    if days:
        start_date = calc_start_date_from_days(days)
        end_date = datetime.now().strftime('%Y%m%d')
        logger.info(f"日期范围: {start_date} ~ {end_date} (最近{days}天)")
    else:
        start_date = None
        end_date = None
        logger.info("日期范围: 全量（从发布日起）")

    total = 0
    success_count = 0
    fail_count = 0

    for idx, (code, name) in enumerate(indexes):
        label = f"{code} {name}" if name else code
        logger.info(f"[{idx + 1}/{len(indexes)}] {label}")

        try:
            count = run_single(code, start_date, end_date)
            total += count
            if count > 0:
                success_count += 1
            else:
                fail_count += 1
        except Exception as e:
            logger.error(f"[{code}] 异常: {e}")
            fail_count += 1

        # 随机间隔 2~5 秒防 ban
        if idx < len(indexes) - 1:
            delay = random.uniform(0.5, 1)
            logger.info(f"  等待 {delay:.1f}秒...")
            time.sleep(delay)

    logger.info(f"=== 完成: {success_count} 成功, {fail_count} 失败, 共 {total} 条 ===")
    return total


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='获取指数日K数据')
    parser.add_argument('codes', nargs='*', help='指数代码（不填则全部指数）')
    parser.add_argument('--days', type=int, default=DEFAULT_DAYS, help=f'最近N天（默认{DEFAULT_DAYS}）')
    parser.add_argument('--full', action='store_true', help='全量获取（从发布日起）')
    args = parser.parse_args()

    if args.codes:
        # 指定了指数代码
        start = None
        end = None
        if not args.full:
            start = calc_start_date_from_days(args.days)
            from datetime import datetime
            end = datetime.now().strftime('%Y%m%d')

        for code in args.codes:
            run_single(code, start, end)
    else:
        # 未指定，全部指数
        run_all(days=None if args.full else args.days)
