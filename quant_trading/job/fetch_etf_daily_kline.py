#!/usr/bin/env python3
"""
Job: 回补ETF历史日K数据

流程：
  1. 从 trade_calendar 查最近N个交易日
  2. 删除 etf_daily_kline 中这N天的全部数据
  3. 从 etf_info 获取全部ETF代码
  4. 逐只通过新浪接口获取日K
  5. 批量写入 etf_daily_kline

用法：
  python job/fetch_etf_daily_kline.py                # 默认回补最近30天全部ETF
  python job/fetch_etf_daily_kline.py 60             # 回补最近60天全部ETF
  python job/fetch_etf_daily_kline.py --code 159915  # 单独获取某只ETF全量历史
  python job/fetch_etf_daily_kline.py --code 159915 --start 2024-01-01  # 指定起始日期
  python job/fetch_etf_daily_kline.py --code 159915 --start 2024-01-01 --end 2025-12-31  # 指定日期范围
"""

import sys
import os
import sqlite3
import logging
import time
import random
import argparse
import pandas as pd
from datetime import datetime

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.paths import DB_PATH
from core.data_access import _sina_etf_daily_kline

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

TRADE_DAYS = 30  # 默认回补天数


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


def delete_etf_daily_kline(trade_days):
    """删除 etf_daily_kline 中指定日期范围的全部数据"""
    if not trade_days:
        return 0
    conn = sqlite3.connect(DB_PATH)
    placeholders = ','.join(['?'] * len(trade_days))
    cur = conn.execute(f"DELETE FROM etf_daily_kline WHERE date IN ({placeholders})", trade_days)
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    logger.info(f"已删除 {deleted} 条ETF日K记录 ({len(trade_days)}天)")
    return deleted


def get_etf_codes():
    """从 etf_info 获取全部ETF代码"""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT code, name FROM etf_info ORDER BY code").fetchall()
    conn.close()
    logger.info(f"共 {len(rows)} 只ETF待回补")
    return [(r[0], r[1]) for r in rows]


def batch_upsert_etf_daily_kline(rows):
    """批量写入 etf_daily_kline 表"""
    if not rows:
        return 0, 0

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    success = 0
    error = 0

    for row in rows:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO etf_daily_kline
                (code, name, date, open, high, low, close, volume, amount, change_pct, nav, nav_change)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row)
            success += 1
        except Exception as e:
            error += 1
            logger.warning(f"写入失败: {row[0]} {row[2]} - {e}")

    conn.commit()
    conn.close()
    return success, error


def fetch_and_save(etf_list, start_date, end_date):
    """
    逐只获取ETF日K并批量写入

    参数:
        etf_list: [(code, name), ...]
        start_date: 'YYYY-MM-DD'
        end_date: 'YYYY-MM-DD'
    """
    # 转换为新浪接口要求的 YYYYMMDD 格式
    start_sina = start_date.replace('-', '')
    end_sina = end_date.replace('-', '')

    total_success = 0
    total_skip = 0
    total_error = 0

    for idx, (code, name) in enumerate(etf_list):
        try:
            df = _sina_etf_daily_kline(code, start_date=start_sina, end_date=end_sina)

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
                    None,  # nav 净值字段新浪接口不提供
                    None,  # nav_change
                ))

            success, error = batch_upsert_etf_daily_kline(rows)
            total_success += success
            total_error += error

        except Exception as e:
            logger.warning(f"[{code}] {name}: 获取失败 - {e}")
            total_error += 1

        # 每只ETF间隔 0.5~1.5 秒防限流
        time.sleep(random.uniform(0.5, 1.5))

        # 每50只输出进度
        if (idx + 1) % 50 == 0 or idx == len(etf_list) - 1:
            logger.info(f"  进度: {idx + 1}/{len(etf_list)} 只, "
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


def run(days=None, code=None, start_date=None, end_date=None):
    """主入口"""
    if code:
        # 单只ETF模式
        run_single_etf(code, start_date, end_date)
    else:
        # 批量回补模式
        run_batch(days or TRADE_DAYS)


def run_single_etf(code, start_date=None, end_date=None):
    """单独获取某只ETF的日K数据"""
    logger.info(f"=== 单只ETF日K获取: {code} ===")

    # 获取ETF名称（从数据库或默认）
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT name FROM etf_info WHERE code = ?", (code,)).fetchone()
    conn.close()
    name = row[0] if row else code

    # end_date默认为今天
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')

    logger.info(f"日期范围: {start_date or '全部历史'} ~ {end_date}")

    # 转换为新浪接口格式
    start_sina = start_date.replace('-', '') if start_date else None
    end_sina = end_date.replace('-', '')

    try:
        df = _sina_etf_daily_kline(code, start_date=start_sina, end_date=end_sina)

        if df.empty:
            logger.warning(f"ETF {code} 日K数据获取为空")
            return

        # 删除该ETF在日期范围内的旧数据
        conn = sqlite3.connect(DB_PATH)
        if start_date:
            conn.execute("DELETE FROM etf_daily_kline WHERE code = ? AND date >= ? AND date <= ?",
                         (code, start_date, end_date))
        else:
            conn.execute("DELETE FROM etf_daily_kline WHERE code = ? AND date <= ?", (code, end_date))
        conn.commit()
        conn.close()

        # 写入新数据
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
                None,  # nav
                None,  # nav_change
            ))

        success, error = batch_upsert_etf_daily_kline(rows)
        logger.info(f"=== 完成! {code} 写入 {success} 条日K ===")

    except Exception as e:
        logger.error(f"获取ETF {code} 日K失败: {e}")


def run_batch(n):
    """批量回补模式"""
    logger.info(f"=== 回补最近{n}个交易日ETF日K ===")

    # 1. 获取最近N个交易日
    trade_days = get_recent_trade_days(n)
    if not trade_days:
        logger.error("trade_calendar 无数据，无法继续")
        return

    start_date = trade_days[-1]  # 最早的那天
    end_date = trade_days[0]     # 最新的那天

    # 2. 删除旧数据
    delete_etf_daily_kline(trade_days)

    # 3. 获取全部ETF代码
    etf_list = get_etf_codes()

    if not etf_list:
        logger.warning("etf_info 无数据，请先运行 fetch_etf_info.py")
        return

    # 4. 逐只获取并写入
    total = fetch_and_save(etf_list, start_date, end_date)

    logger.info(f"=== 完成! {len(etf_list)}只ETF, {total}条日K ===")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='回补ETF历史日K数据')
    parser.add_argument('days', nargs='?', type=int, default=None,
                        help='回补天数（默认30天，批量模式）')
    parser.add_argument('--code', '-c', type=str, default=None,
                        help='单独获取某只ETF（如 159915）')
    parser.add_argument('--start', '-s', type=str, default=None,
                        help='起始日期（如 2024-01-01）')
    parser.add_argument('--end', '-e', type=str, default=None,
                        help='截止日期（如 2025-12-31，默认为今天）')

    args = parser.parse_args()

    run(days=args.days, code=args.code, start_date=args.start, end_date=args.end)