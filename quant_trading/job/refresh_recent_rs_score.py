#!/usr/bin/env python3
"""
Job: 回补最近N个交易日的RS Score

通过 index_members 获取成分股全量日K，复用 update_daily_kline 中的
calc_rs_batch 核心计算逻辑，刷新最近N个交易日的RS数据。

用法：
  python job/refresh_recent_rs_score.py          # 默认30天
  python job/refresh_recent_rs_score.py 5        # 最近5天
"""

import sys
import os
import sqlite3
import logging
import time
from datetime import datetime

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.paths import DB_PATH
from core.storage import get_stocks_daily_closes, get_index_daily_closes, get_trade_days_range
from job.update_daily_kline import calc_rs_batch

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

LOOKBACK = 250
DEFAULT_DAYS = 30


def get_recent_trade_days(n):
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
    if days:
        logger.info(f"最近{len(days)}个交易日: {days[-1]} ~ {days[0]}")
    return days


def delete_rs_scores(trade_days, benchmark_code):
    """删除 rs_score 中指定日期范围的数据"""
    if not trade_days:
        return 0
    conn = sqlite3.connect(DB_PATH)
    placeholders = ','.join(['?'] * len(trade_days))
    cur = conn.execute(
        f"DELETE FROM rs_score WHERE benchmark_code = ? AND calc_date IN ({placeholders})",
        [benchmark_code] + trade_days
    )
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    logger.info(f"[{benchmark_code}] 已删除 {deleted} 条旧RS Score")
    return deleted


def run(days=None):
    n = days or DEFAULT_DAYS
    logger.info(f"=== 回补最近{n}个交易日RS Score ===")
    start_time = time.time()

    # 1. 获取最近N个交易日
    trade_days = get_recent_trade_days(n)
    if not trade_days:
        logger.error("trade_calendar 无数据，无法继续")
        return

    # 2. 固定指数列表
    index_codes = ["000510", "000985"]
    logger.info(f"关联指数: {index_codes}")

    total_rows = 0

    for index_code in index_codes:
        # 3. 从 index_members 获取成分股
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT DISTINCT stock_code FROM index_members WHERE index_code = ?",
            (index_code,)
        ).fetchall()
        conn.close()
        stock_codes = [r[0] for r in rows]
        if not stock_codes:
            logger.info(f"[{index_code}] 无成分股，跳过")
            continue

        # 4. 删除旧数据
        delete_rs_scores(trade_days, index_code)

        # 5. 加载数据（需回溯到最早一天的前 LOOKBACK 天）
        all_trade_days = get_trade_days_range('2014-01-01', trade_days[0])
        date_to_idx = {d: i for i, d in enumerate(all_trade_days)}
        first_idx = date_to_idx.get(trade_days[-1])
        if first_idx is None or first_idx < LOOKBACK:
            logger.warning(f"[{index_code}] 交易日数据不足{LOOKBACK}天，跳过")
            continue

        actual_data_start = all_trade_days[first_idx - LOOKBACK]

        logger.info(f"[{index_code}] 加载数据: {actual_data_start} ~ {trade_days[0]}, {len(stock_codes)}只成分股")
        stock_closes = get_stocks_daily_closes(stock_codes, actual_data_start, trade_days[0])
        index_closes = get_index_daily_closes(index_code, actual_data_start, trade_days[0])

        # 6. 复用核心计算逻辑
        rows = calc_rs_batch(trade_days, stock_codes, stock_closes, index_closes, all_trade_days, index_code, LOOKBACK)
        total_rows += rows
        logger.info(f"[{index_code}] 写入{rows}条RS Score")

    elapsed = time.time() - start_time
    logger.info(f"=== 完成! {total_rows}条RS Score, 耗时{elapsed:.1f}秒 ===")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        try:
            days = int(sys.argv[1])
        except ValueError:
            print("用法: python job/refresh_recent_rs_score.py [天数]")
            print("示例: python job/refresh_recent_rs_score.py 30")
            sys.exit(1)
        run(days)
    else:
        run()
