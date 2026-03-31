"""
数据存储模块
负责保存原始日K数据和筛选信号
日K按月份归档: data/kline/年-月/股票代码_股票名称.csv
信号/报告输出到共享文件夹: ***REMOVED***/reports/
双写模式：同时写入CSV和SQLite
"""

import pandas as pd
import os
import glob
import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

from paths import DATA_DIR, REPORTS_DIR, DB_PATH

KLINE_DIR = os.path.join(DATA_DIR, 'kline')

# 报告输出到共享文件夹
REPORT_SIGNAL_DIR = os.path.join(REPORTS_DIR, 'signals')

# K线CSV的列顺序
KLINE_COLUMNS = ['date', 'open', 'close', 'high', 'low', 'volume', 'amount',
                 'amplitude', 'change_pct', 'change_amt', '所属板块']

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

def ensure_dirs(month_str=None):
    """确保目录存在"""
    os.makedirs(KLINE_DIR, exist_ok=True)
    os.makedirs(REPORT_SIGNAL_DIR, exist_ok=True)
    if month_str:
        os.makedirs(os.path.join(KLINE_DIR, month_str), exist_ok=True)


def get_month_str(dt=None):
    """获取年-月格式字符串"""
    if dt is None:
        dt = datetime.now()
    return dt.strftime('%Y-%m')


def make_filename(stock_code, stock_name=''):
    """
    生成文件名: 股票代码_股票名称.csv
    """
    stock_name = stock_name.replace('/', '_').replace('\\', '_')
    return f'{stock_code}_{stock_name}.csv'


def find_existing_file(stock_code, month_str):
    """
    查找已存在的文件（支持增量更新时不需要知道名称和板块）

    参数:
        stock_code: 股票代码
        month_str: 年-月

    返回:
        str: 文件路径，未找到返回None
    """
    month_dir = os.path.join(KLINE_DIR, month_str)
    if not os.path.exists(month_dir):
        return None

    # 搜索以股票代码开头的文件
    pattern = os.path.join(month_dir, f'{stock_code}_*.csv')
    matches = glob.glob(pattern)

    if matches:
        return matches[0]
    return None


def get_kline_filepath(stock_code, month_str=None, stock_name=''):
    """
    获取日K文件路径
    """
    if month_str is None:
        month_str = get_month_str()

    # 如果提供了名称，生成新文件名
    if stock_name:
        filename = make_filename(stock_code, stock_name)
        return os.path.join(KLINE_DIR, month_str, filename)

    # 否则查找已有文件
    existing = find_existing_file(stock_code, month_str)
    if existing:
        return existing

    # 都没有，用纯代码作为fallback
    return os.path.join(KLINE_DIR, month_str, f'{stock_code}.csv')


def load_kline(stock_code, month_str=None):
    """
    加载已有日K数据
    """
    filepath = get_kline_filepath(stock_code, month_str)
    if filepath and os.path.exists(filepath):
        df = pd.read_csv(filepath)
        df['date'] = pd.to_datetime(df['date'])
        return df
    return pd.DataFrame(columns=KLINE_COLUMNS)


def save_kline(stock_code, df, month_str=None, stock_name=''):
    """
    保存日K数据到CSV
    """
    if month_str is None:
        month_str = get_month_str()
    ensure_dirs(month_str)
    filepath = get_kline_filepath(stock_code, month_str, stock_name)
    cols = [c for c in KLINE_COLUMNS if c in df.columns]
    df[cols].to_csv(filepath, index=False, encoding='utf-8-sig')


def merge_and_save_kline(stock_code, new_df, month_str=None, stock_name='', sector_name=''):
    """
    合并新数据到已有日K文件（增量更新）
    双写模式：同时写入CSV和SQLite

    参数:
        stock_code: 股票代码
        new_df: 新获取的日K数据
        month_str: 年-月
        stock_name: 股票名称
        sector_name: 所属板块/题材
    """
    if month_str is None:
        month_str = get_month_str()

    existing_df = load_kline(stock_code, month_str)

    if new_df.empty:
        return existing_df

    # 给新数据添加所属板块列
    if sector_name and '所属板块' not in new_df.columns:
        new_df['所属板块'] = sector_name
    elif sector_name:
        new_df['所属板块'] = sector_name

    if existing_df.empty:
        # === 双写：CSV + SQLite ===
        save_kline(stock_code, new_df, month_str, stock_name)
        save_daily_kline_to_sqlite(stock_code, stock_name, new_df)
        return new_df

    # 合并：按日期去重，新数据覆盖旧数据
    combined = pd.concat([existing_df, new_df], ignore_index=True)
    combined['date'] = pd.to_datetime(combined['date'])
    combined = combined.drop_duplicates(subset='date', keep='last')
    combined = combined.sort_values('date').reset_index(drop=True)

    # 如果提供了新名称，重命名文件
    if stock_name:
        old_path = find_existing_file(stock_code, month_str)
        new_filename = make_filename(stock_code, stock_name)
        new_path = os.path.join(KLINE_DIR, month_str, new_filename)
        if old_path and old_path != new_path and os.path.exists(old_path):
            os.remove(old_path)
        combined.to_csv(new_path, index=False, encoding='utf-8-sig', columns=KLINE_COLUMNS)
    else:
        save_kline(stock_code, combined, month_str)

    # === SQLite双写 ===
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
    filename = os.path.join(REPORT_DIR, f'report_{date_str}.txt')
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(report_text)
    return filename


if __name__ == '__main__':
    print("测试数据存储模块...")

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
    print(f"✅ 保存了 {len(result)} 条数据")
    print(f"   文件: {get_kline_filepath('000001', stock_name='测试股票')}")
