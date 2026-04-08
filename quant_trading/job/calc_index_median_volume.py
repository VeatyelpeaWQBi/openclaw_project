#!/usr/bin/env python3
"""
Job: 计算指数成分股日均成交量/成交额中位数
纯本地SQLite运算，不调用任何外部API

用法：
  python3 job/calc_index_median_volume.py 000510
  python3 job/calc_index_median_volume.py 000510 000300

工作流程：
  1. 从 index_info 获取指数发布日期
  2. 从 index_members 获取成分股列表
  3. 一条SQL批量计算每只成分股的日均成交量、日均成交额（从发布日期起）
  4. 取中位数
  5. 更新 index_info.median_daily_volume / median_daily_amount
"""

import sys
import os
import sqlite3
import logging

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.paths import DB_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def get_index_publish_date(code):
    """获取指数发布日期"""
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT short_name, publish_date FROM index_info WHERE code = ?", (code,)
    ).fetchone()
    conn.close()
    if row:
        return row[0], row[1]
    return None, None


def calc_median(code):
    """
    纯SQL计算成分股日均成交量/成交额中位数

    返回: (median_volume, median_amount) 或 (None, None)
    """
    name, publish_date = get_index_publish_date(code)
    if not name:
        logger.error(f"[{code}] 指数不存在")
        return None, None

    logger.info(f"[{code}] {name}, 发布日期: {publish_date}")

    conn = sqlite3.connect(DB_PATH)

    # 一条SQL：JOIN index_members + daily_kline，按成分股分组求平均
    rows = conn.execute("""
        SELECT m.stock_code, m.stock_name,
               AVG(d.volume) as avg_vol,
               AVG(d.amount) as avg_amt
        FROM index_members m
        JOIN daily_kline d ON m.stock_code = d.code
        WHERE m.index_code = ?
          AND d.date >= ?
          AND d.volume > 0
        GROUP BY m.stock_code
    """, (code, publish_date)).fetchall()

    conn.close()

    if not rows:
        logger.error(f"[{code}] 无有效数据")
        return None, None

    # 提取日均值列表
    vols = sorted([r[2] for r in rows])
    amts = sorted([r[3] for r in rows])

    median_vol = vols[len(vols) // 2]
    median_amt = amts[len(amts) // 2]

    logger.info(f"[{code}] 有效成分股: {len(rows)}")
    median_vol = int(median_vol)
    median_amt = int(median_amt)

    logger.info(f"  日均成交量中位数: {median_vol/1e8:.2f} 亿股 ({median_vol/1e4:.0f} 万手)")
    logger.info(f"  日均成交额中位数: {median_amt/1e8:.2f} 亿元")

    return median_vol, median_amt


def update_index_info(code, median_vol, median_amt):
    """更新 index_info 表"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        UPDATE index_info SET
            median_daily_volume = ?,
            median_daily_amount = ?,
            last_update_at = datetime('now')
        WHERE code = ?
    """, (median_vol, median_amt, code))
    conn.commit()
    conn.close()
    logger.info(f"[{code}] index_info 已更新")


def run(code):
    """单个指数完整流程"""
    logger.info(f"=== 计算 {code} 成分股日均成交量/额中位数 ===")

    median_vol, median_amt = calc_median(code)

    if median_vol is not None:
        update_index_info(code, int(median_vol), int(median_amt))
        return True
    return False


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python3 calc_index_median_volume.py <指数代码> [指数代码2] ...")
        sys.exit(1)

    success = 0
    for code in sys.argv[1:]:
        if run(code):
            success += 1

    logger.info(f"=== 全部完成: {success}/{len(sys.argv)-1} 个指数 ===")
