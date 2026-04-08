#!/usr/bin/env python3
"""
Job: 获取指数日K数据
通过中证指数官网API获取指定指数的日K线数据，写入 index_daily_kline 表

用法：
  python3 job/fetch_index_daily_kline.py 000510                          # 全量（从发布日起）
  python3 job/fetch_index_daily_kline.py 000510 20240923 20260408        # 指定日期区间
  python3 job/fetch_index_daily_kline.py 000510 000300                   # 多个指数

数据源：ak.stock_zh_index_hist_csindex（中证指数有限公司）
导入表：index_daily_kline
"""

import sys
import os
import time
import logging
import sqlite3

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.paths import DB_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


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

    success = 0
    for _, row in df.iterrows():
        date = str(row.get('日期', ''))[:10]

        cursor.execute("""
            INSERT OR REPLACE INTO index_daily_kline
            (index_code, index_name, date, open, high, low, close,
             volume, amount, change, change_pct, constituent_count, pe_ttm)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
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
        success += 1

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


def run(code, start_date=None, end_date=None):
    """单个指数完整流程"""
    logger.info(f"=== 获取 {code} 日K数据 ===")

    df = fetch_index_daily_kline(code, start_date, end_date)
    if df is None or df.empty:
        logger.warning(f"[{code}] 无数据")
        return 0

    count = save_to_db(code, df)
    return count


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python3 fetch_index_daily_kline.py <指数代码> [start_date] [end_date]")
        print("示例: python3 fetch_index_daily_kline.py 000510")
        print("示例: python3 fetch_index_daily_kline.py 000510 20240923 20260408")
        sys.exit(1)

    code = sys.argv[1]
    start = sys.argv[2] if len(sys.argv) > 2 else None
    end = sys.argv[3] if len(sys.argv) > 3 else None

    total = run(code, start, end)
    logger.info(f"=== 完成: {total} 条 ===")
