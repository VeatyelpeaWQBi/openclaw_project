#!/usr/bin/env python3
"""
Job: 回补最近30个交易日个股日K

流程：
  1. 从 trade_calendar 查最近30个交易日
  2. 删除 daily_kline 中这30天的全部个股数据
  3. 从 stock_info 获取全部股票代码
  4. 逐只通过新浪接口重新获取这30天日K
  5. 批量写入 daily_kline

用法：
  python job/refresh_recent_daily_kline.py
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
from core.storage import batch_upsert_daily_kline
from core.data_access import _sina_daily_kline

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


def delete_daily_kline(trade_days):
    """删除 daily_kline 中指定日期范围的全部数据"""
    if not trade_days:
        return 0
    conn = sqlite3.connect(DB_PATH)
    placeholders = ','.join(['?'] * len(trade_days))
    cur = conn.execute(f"DELETE FROM daily_kline WHERE date IN ({placeholders})", trade_days)
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    logger.info(f"已删除 {deleted} 条日K记录 ({len(trade_days)}天)")
    return deleted


def get_stock_codes():
    """从 stock_info 获取全部股票代码"""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT code, name FROM stock_info ORDER BY code").fetchall()
    conn.close()
    logger.info(f"共 {len(rows)} 只股票待回补")
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
    """
    # 转换为新浪接口要求的 YYYYMMDD 格式
    start_sina = start_date.replace('-', '')
    end_sina = end_date.replace('-', '')

    total_success = 0
    total_skip = 0
    total_error = 0

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


def run(days=None):
    n = days or TRADE_DAYS
    logger.info(f"=== 回补最近{n}个交易日个股日K ===")

    # 1. 获取最近N个交易日
    trade_days = get_recent_trade_days(n)
    if not trade_days:
        logger.error("trade_calendar 无数据，无法继续")
        return

    start_date = trade_days[-1]  # 最早的那天
    end_date = trade_days[0]     # 最新的那天

    # 2. 删除旧数据
    delete_daily_kline(trade_days)

    # 3. 获取全部股票代码（或手动指定测试列表）
    stock_list = get_stock_codes()
    # stock_list = [("600028", "中国石化"), ("600735", "新华锦")]

    # 4. 逐只获取并写入
    total = fetch_and_save(stock_list, start_date, end_date)

    logger.info(f"=== 完成! {len(stock_list)}只股票, {total}条日K ===")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        try:
            days = int(sys.argv[1])
        except ValueError:
            print("用法: python job/refresh_recent_daily_kline.py [天数]")
            print("示例: python job/refresh_recent_daily_kline.py 30")
            sys.exit(1)
        run(days)
    else:
        run()
