"""
补全交易日历数据（2014年至今）
从 akshare 的 tool_trade_date_hist_sina() 获取完整交易日列表，
生成全量日历（含非交易日）并写入 SQLite trade_calendar 表。
"""

import akshare as ak
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.paths import DB_PATH


def extend_trade_calendar():
    print("=" * 50)
    print("补全交易日历数据（2014年至今）")
    print("=" * 50)

    # 1. 从 akshare 获取完整交易日列表
    print("\n[1/4] 从 akshare 获取交易日数据...")
    df = ak.tool_trade_date_hist_sina()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    all_trading_days = set(df['trade_date'].dt.strftime('%Y-%m-%d').tolist())
    print(f"  akshare 共 {len(all_trading_days)} 个交易日")

    # 2. 连接数据库，获取已有数据
    print("\n[2/4] 连接数据库...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT MIN(trade_date), MAX(trade_date), COUNT(*) FROM trade_calendar")
    result = cursor.fetchone()
    if result and result[0]:
        print(f"  已有数据: {result[0]} ~ {result[1]}，共 {result[2]} 条")
    else:
        print("  表为空")

    # 3. 生成 2014-01-01 至今天的全量日历
    print("\n[3/4] 生成全量日历...")
    start_date = datetime(2014, 1, 1)
    end_date = datetime.now()

    # 先删除2014年之前的数据（如果有的话保留，但我们要从2014开始）
    cursor.execute("DELETE FROM trade_calendar WHERE trade_date < '2014-01-01'")
    deleted_old = cursor.rowcount
    if deleted_old:
        print(f"  删除2014年之前的数据: {deleted_old} 条")

    # 获取已有的2014年之后的数据
    cursor.execute("SELECT trade_date FROM trade_calendar WHERE trade_date >= '2014-01-01'")
    existing_dates = set(row[0] for row in cursor.fetchall())
    print(f"  2014年之后已有: {len(existing_dates)} 条")

    # 生成需要插入的数据
    rows_to_insert = []
    current = start_date
    while current <= end_date:
        date_str = current.strftime('%Y-%m-%d')
        if date_str not in existing_dates:
            is_trading = 1 if date_str in all_trading_days else 0
            day_week = current.isoweekday()  # 1=周一, 7=周日
            rows_to_insert.append((date_str, is_trading, day_week))
        current += timedelta(days=1)

    print(f"  需要新增: {len(rows_to_insert)} 条")

    # 4. 批量插入
    print("\n[4/4] 写入数据库...")
    if rows_to_insert:
        cursor.executemany(
            "INSERT OR IGNORE INTO trade_calendar (trade_date, trade_status, day_week) VALUES (?, ?, ?)",
            rows_to_insert
        )
        conn.commit()
        print(f"  成功插入: {cursor.rowcount} 条")
    else:
        print("  无需插入，数据已完整")

    # 验证结果
    cursor.execute("SELECT MIN(trade_date), MAX(trade_date), COUNT(*) FROM trade_calendar")
    result = cursor.fetchone()
    cursor.execute("SELECT COUNT(*) FROM trade_calendar WHERE trade_status=1 AND trade_date >= '2014-01-01'")
    trading_count = cursor.fetchone()[0]
    print(f"\n最终结果:")
    print(f"  总日历天数: {result[2]} ({result[0]} ~ {result[1]})")
    print(f"  2014年至今交易日: {trading_count}")

    conn.close()
    print("\n完成！")


if __name__ == "__main__":
    extend_trade_calendar()
