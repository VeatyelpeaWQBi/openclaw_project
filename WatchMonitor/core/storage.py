"""
数据存储模块 - 盯盘助手专用
仅保留尾盘T+1程序必需的存储方法
"""

import os
import sqlite3
import logging

import pandas as pd

from typing import Optional
from datetime import datetime, timedelta

from core.paths import DATA_DIR, REPORTS_DIR, DB_PATH

logger = logging.getLogger(__name__)

# 报告输出到共享文件夹
REPORT_SIGNAL_DIR = os.path.join(REPORTS_DIR, 'signals')

# 初始获取天数（最近两个月约44个自然日，约30+交易日）
INITIAL_FETCH_DAYS = 50


# ==================== SQLite辅助方法 ====================

def get_db_connection() -> sqlite3.Connection:
    """获取SQLite连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def save_daily_kline_to_sqlite(stock_code: str, stock_name: str, df: pd.DataFrame) -> None:
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
        logger.debug(f"SQLite: 保存 {stock_code}({stock_name}) {len(rows)} 条日K数据")
    except Exception as e:
        logger.warning(f"SQLite save_daily_kline失败: {e}")
    finally:
        conn.close()


# ==================== 目录/文件辅助方法 ====================

def ensure_dirs():
    """确保目录存在"""
    os.makedirs(REPORT_SIGNAL_DIR, exist_ok=True)


def get_daily_data_from_sqlite(stock_code: str, days: Optional[int] = None) -> pd.DataFrame:
    """
    从SQLite获取单只股票日K数据

    参数:
        stock_code: 股票代码
        days: 最近N天的数据（None=全部）
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.row_factory = sqlite3.Row
            if days:
                today = datetime.now().strftime('%Y-%m-%d')
                start_date = get_trading_day_offset_from(today, -days)
                if not start_date:
                    start_date = (datetime.now() - timedelta(days=int(days * 1.5))).strftime('%Y-%m-%d')
                df = pd.read_sql_query(
                    "SELECT * FROM daily_kline WHERE code = ? AND date >= ? ORDER BY date",
                    conn, params=[stock_code, start_date]
                )
            else:
                df = pd.read_sql_query("SELECT * FROM daily_kline WHERE code = ? ORDER BY date", conn, params=[stock_code])
            return df
        finally:
            conn.close()
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
    common_cols = existing_df.columns.intersection(new_df.columns)
    combined = pd.concat([existing_df[common_cols], new_df[common_cols]], ignore_index=True)
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
    filename = os.path.join(REPORTS_DIR, f'report_{date_str}.md')
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(report_text)
    return filename


# ==================== 交易日历 ====================

def get_trading_day_offset_from(base_date, offset):
    """
    从指定日期往前/后推N个交易日

    参数:
        base_date: 基准日期 'YYYY-MM-DD'
        offset: 偏移量（正=往后, 负=往前）

    返回:
        str or None: 目标交易日
    """
    try:
        conn = get_db_connection()
        try:
            if offset >= 0:
                row = conn.execute("""
                    SELECT trade_date FROM trade_calendar
                    WHERE trade_status = 1 AND trade_date >= ?
                    ORDER BY trade_date ASC LIMIT 1 OFFSET ?
                """, (base_date, offset)).fetchone()
            else:
                row = conn.execute("""
                    SELECT trade_date FROM trade_calendar
                    WHERE trade_status = 1 AND trade_date <= ?
                    ORDER BY trade_date DESC LIMIT 1 OFFSET ?
                """, (base_date, abs(offset))).fetchone()
            return row['trade_date'] if row else None
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"获取交易日偏移失败: {e}")
        return None


# ==================== 指数成交额（用于成交量对比） ====================

def get_index_amount_before_date(index_code: str, before_date: str) -> float:
    """
    获取指定指数在某日期之前的最近一个交易日成交额

    参数:
        index_code: 指数代码（如 '000985'）
        before_date: 基准日期 'YYYY-MM-DD'

    返回:
        float: 成交额（元），无数据返回0
    """
    try:
        conn = get_db_connection()
        try:
            row = conn.execute("""
                SELECT amount FROM index_daily_kline
                WHERE index_code = ? AND date < ?
                ORDER BY date DESC LIMIT 1
            """, (index_code, before_date)).fetchone()
            return float(row['amount']) if row and row['amount'] else 0.0
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"获取指数成交额失败 [{index_code}]: {e}")
        return 0.0