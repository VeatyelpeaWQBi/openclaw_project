"""
数据存储模块
负责保存原始日K数据和筛选信号
日K数据存储在SQLite中
信号/报告输出到共享文件夹: ***REMOVED***/reports/
"""

import pandas as pd
import os
import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

from paths import DATA_DIR, REPORTS_DIR, DB_PATH

# 报告输出到共享文件夹
REPORT_SIGNAL_DIR = os.path.join(REPORTS_DIR, 'signals')

# 初始获取天数（最近两个月约44个自然日，约30+交易日）
INITIAL_FETCH_DAYS = 50


# ==================== SQLite辅助方法 ====================

def get_db_connection():
    """获取SQLite连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def save_daily_kline_to_sqlite(stock_code, stock_name, df):
    """
    保存日K数据到SQLite（批量插入）

    参数:
        stock_code: 股票代码
        stock_name: 股票名称
        df: DataFrame，包含date, open, high, low, close, volume, amount等列
    """
    try:
        conn = get_db_connection()
        rows = []
        for _, row in df.iterrows():
            date_val = row.get('date', '')
            if hasattr(date_val, 'strftime'):
                date_str = date_val.strftime('%Y-%m-%d')
            else:
                date_str = str(date_val)[:10]

            rows.append((
                stock_code,
                stock_name,
                date_str,
                float(row.get('open', 0)),
                float(row.get('high', 0)),
                float(row.get('low', 0)),
                float(row.get('close', 0)),
                int(row.get('volume', 0)),
                float(row.get('amount', 0)),
                float(row.get('turnover', 0)),
                float(row.get('pe_ratio', row.get('pe', 0)) or 0),
                float(row.get('pb_ratio', row.get('pb', 0)) or 0),
                float(row.get('ps_ratio', row.get('ps', 0)) or 0),
                float(row.get('pcf_ratio', row.get('pcf', 0)) or 0),
                float(row.get('volume_ratio', row.get('vol_ratio', 0)) or 0) if row.get('volume_ratio', row.get('vol_ratio')) else None,
            ))

        conn.executemany("""
            INSERT OR REPLACE INTO daily_kline
            (code, name, date, open, high, low, close, volume, amount,
             turnover, pe_ratio, pb_ratio, ps_ratio, pcf_ratio, volume_ratio)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        conn.close()
        logger.debug(f"SQLite: 保存 {stock_code}({stock_name}) {len(rows)} 条日K数据")
    except Exception as e:
        logger.warning(f"SQLite save_daily_kline失败: {e}")


def save_minute_kline_to_sqlite(stock_code, stock_name, df):
    """
    保存分钟线数据到SQLite

    参数:
        stock_code: 股票代码
        stock_name: 股票名称
        df: DataFrame，包含datetime, open, high, low, close, volume, amount
    """
    try:
        conn = get_db_connection()
        rows = []
        for _, row in df.iterrows():
            dt_val = row.get('datetime', '')
            dt_str = str(dt_val)

            # 从datetime提取date
            date_str = dt_str[:10]

            rows.append((
                stock_code,
                stock_name,
                date_str,
                dt_str,
                float(row.get('open', 0)),
                float(row.get('high', 0)),
                float(row.get('low', 0)),
                float(row.get('close', 0)),
                int(row.get('volume', 0)),
                float(row.get('amount', 0)),
            ))

        conn.executemany("""
            INSERT OR REPLACE INTO minute_kline
            (code, name, date, datetime, open, high, low, close, volume, amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        conn.close()
        logger.debug(f"SQLite: 保存 {stock_code}({stock_name}) {len(rows)} 条分钟线数据")
    except Exception as e:
        logger.warning(f"SQLite save_minute_kline失败: {e}")


def save_index_kline_to_sqlite(index_code, index_name, df):
    """
    保存指数K线数据到SQLite

    参数:
        index_code: 指数代码 (如 '000300')
        index_name: 指数名称 (如 '沪深300')
        df: DataFrame，包含date, open, high, low, close, volume, amount
    """
    try:
        conn = get_db_connection()
        rows = []
        for _, row in df.iterrows():
            date_val = row.get('date', '')
            if hasattr(date_val, 'strftime'):
                date_str = date_val.strftime('%Y-%m-%d')
            else:
                date_str = str(date_val)[:10]

            rows.append((
                index_code,
                index_name,
                date_str,
                float(row.get('open', 0)),
                float(row.get('high', 0)),
                float(row.get('low', 0)),
                float(row.get('close', 0)),
                int(row.get('volume', 0)),
                float(row.get('amount', 0)),
            ))

        conn.executemany("""
            INSERT OR REPLACE INTO index_kline
            (index_code, index_name, date, open, high, low, close, volume, amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        conn.close()
        logger.debug(f"SQLite: 保存 {index_code}({index_name}) {len(rows)} 条指数K线")
    except Exception as e:
        logger.warning(f"SQLite save_index_kline失败: {e}")


# ==================== 目录/文件辅助方法 ====================

def ensure_dirs():
    """确保目录存在"""
    os.makedirs(REPORT_SIGNAL_DIR, exist_ok=True)


def get_daily_data_from_sqlite(stock_code):
    """从SQLite获取单只股票日K数据"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        df = pd.read_sql_query("SELECT * FROM daily_kline WHERE code = ? ORDER BY date", conn, params=[stock_code])
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def merge_and_save_kline(stock_code, new_df, month_str=None, stock_name='', sector_name=''):
    """
    合并新数据到SQLite（增量更新）
    只写SQLite，不写CSV

    参数:
        stock_code: 股票代码
        new_df: 新获取的日K数据
        month_str: 年-月（不再使用，保留参数兼容）
        stock_name: 股票名称
        sector_name: 所属板块/题材
    """
    if new_df.empty:
        return new_df

    # 给新数据添加所属板块列
    if sector_name and '所属板块' not in new_df.columns:
        new_df['所属板块'] = sector_name

    # 检查SQLite中是否已有数据
    existing_df = get_daily_data_from_sqlite(stock_code)

    if existing_df.empty:
        # 全新数据，直接写入
        save_daily_kline_to_sqlite(stock_code, stock_name, new_df)
        return new_df

    # 合并：按日期去重
    combined = pd.concat([existing_df, new_df], ignore_index=True)
    combined['date'] = pd.to_datetime(combined['date'])
    combined = combined.drop_duplicates(subset='date', keep='last')
    combined = combined.sort_values('date').reset_index(drop=True)

    # 写入SQLite
    save_daily_kline_to_sqlite(stock_code, stock_name, combined)
    return combined


def save_signal(date_str, candidates):
    """
    保存筛选信号到共享文件夹
    """
    ensure_dirs()
    filename = os.path.join(REPORT_SIGNAL_DIR, f'signals_{date_str}.csv')

    if candidates:
        df = pd.DataFrame(candidates)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
    else:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("日期,状态\n")
            f.write(f"{date_str},无信号\n")

    return filename


def save_report(date_str, report_text):
    """
    保存报告文本到共享文件夹
    """
    ensure_dirs()
    filename = os.path.join(REPORTS_DIR, f'report_{date_str}.txt')
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(report_text)
    return filename


if __name__ == '__main__':
    logger.info("测试数据存储模块...")

    import pandas as pd
    test_df = pd.DataFrame({
        'date': pd.to_datetime(['2026-03-27', '2026-03-28']),
        'open': [10.0, 10.5],
        'high': [10.8, 11.0],
        'low': [9.8, 10.2],
        'close': [10.5, 10.8],
        'volume': [100000, 120000],
        'amount': [1050000, 1296000],
        'amplitude': [8.0, 7.6],
        'change_pct': [5.0, 2.86],
        'change_amt': [0.5, 0.3]
    })

    result = merge_and_save_kline('000001', test_df, stock_name='测试股票')
    logger.info(f"保存了 {len(result)} 条数据")
