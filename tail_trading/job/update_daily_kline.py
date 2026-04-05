#!/usr/bin/env python3
"""
Job: 更新全市场最新日K数据
通过 ak.stock_zh_a_spot() 获取全A股最新一天的行情数据
写入 daily_kline 表

定时：每个交易日 14:48 和 19:00
"""

import sys
import os
import time
import logging
import sqlite3
import pandas as pd
from datetime import datetime

# 确保项目根目录在 sys.path 中
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.paths import DB_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def fetch_market_spot():
    """通过ak.stock_zh_a_spot()获取全市场最新行情"""
    import akshare as ak

    logger.info("开始获取全市场行情...")
    start = time.time()

    df = ak.stock_zh_a_spot()

    elapsed = time.time() - start
    logger.info(f"获取完成: {len(df)} 只股票, 耗时 {elapsed:.1f}秒")

    return df


def save_to_db(df, trade_date):
    """
    将spot数据写入daily_kline表

    参数:
        df: ak.stock_zh_a_spot() 返回的DataFrame
        trade_date: 交易日期 'YYYY-MM-DD'
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    success = 0
    skip = 0
    error = 0

    for _, row in df.iterrows():
        code = str(row.get('代码', '')).strip()
        name = str(row.get('名称', '')).strip()

        if not code:
            skip += 1
            continue

        # 提取字段
        open_price = _safe_float(row.get('今开'))
        high_price = _safe_float(row.get('最高'))
        low_price = _safe_float(row.get('最低'))
        close_price = _safe_float(row.get('最新价'))
        volume = _safe_int(row.get('成交量'))
        amount = _safe_float(row.get('成交额'))

        # 成交额从元转为元（与已有数据保持一致）
        # ak.stock_zh_a_spot() 返回的成交额单位是元

        if close_price is None or close_price <= 0:
            skip += 1
            continue

        try:
            cursor.execute("""
                INSERT OR REPLACE INTO daily_kline
                (code, name, date, open, high, low, close, volume, amount, turnover, volume_ratio)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
            """, (code, name, trade_date, open_price, high_price, low_price, close_price, volume, amount))
            success += 1
        except Exception as e:
            logger.debug(f"[{code}] 写入失败: {e}")
            error += 1

    conn.commit()
    conn.close()

    logger.info(f"写入完成: 成功{success}, 跳过{skip}, 失败{error}")
    return success


def _safe_float(val):
    """安全转float"""
    if val is None or (isinstance(val, str) and val.strip() in ('', '-', 'nan')):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val):
    """安全转int"""
    if val is None or (isinstance(val, str) and val.strip() in ('', '-', 'nan')):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def run():
    """主入口"""
    today = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"=== 更新全市场日K — {today} ===")

    # 1. 获取行情
    df = fetch_market_spot()

    if df is None or df.empty:
        logger.warning("获取行情数据为空，跳过")
        return

    # 2. 写入DB
    count = save_to_db(df, today)

    logger.info(f"=== 完成: {count} 只股票更新到 {today} ===")


if __name__ == '__main__':
    run()
