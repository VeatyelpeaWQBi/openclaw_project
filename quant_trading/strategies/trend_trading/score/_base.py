"""
评分模块共享工具
避免 rs/vcp/adx 三个 core 模块重复实现通用逻辑
"""

import logging
from core.storage import get_db_connection, get_daily_data_from_sqlite

logger = logging.getLogger(__name__)


def get_all_stock_codes():
    """
    获取全市场有日K数据的股票代码列表

    返回:
        list[str]: 股票代码列表
    """
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT DISTINCT code FROM daily_kline
            WHERE volume > 0
            ORDER BY code
        """).fetchall()
        return [r['code'] for r in rows]
    finally:
        conn.close()


def get_trade_dates(start_date=None, end_date=None):
    """
    获取指定日期范围内的交易日列表

    参数:
        start_date: 起始日期 'YYYY-MM-DD'，None=最早
        end_date: 结束日期 'YYYY-MM-DD'，None=最晚

    返回:
        list[str]: 交易日列表（升序）
    """
    conn = get_db_connection()
    try:
        sql = "SELECT trade_date FROM trade_calendar WHERE trade_status = 1"
        params = []
        if start_date:
            sql += " AND trade_date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND trade_date <= ?"
            params.append(end_date)
        sql += " ORDER BY trade_date ASC"
        rows = conn.execute(sql, params).fetchall()
        return [r['trade_date'] for r in rows]
    finally:
        conn.close()


def get_recent_trade_dates(end_date, days):
    """
    获取指定日期之前最近N个交易日（含end_date）

    参数:
        end_date: 结束日期 'YYYY-MM-DD'
        days: 往前取的交易日数

    返回:
        list[str]: 交易日列表（升序，共days个）
    """
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT trade_date FROM trade_calendar
            WHERE trade_status = 1 AND trade_date <= ?
            ORDER BY trade_date DESC LIMIT ?
        """, (end_date, days)).fetchall()
        dates = sorted([r['trade_date'] for r in rows])
        return dates
    finally:
        conn.close()


def get_stock_data_earliest_date():
    """
    获取个股日K数据的最早日期
    作为评分计算的截断点，避免在无个股数据的日期上白跑循环

    返回:
        str or None: 最早日期 'YYYY-MM-DD'
    """
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT MIN(date) FROM daily_kline").fetchone()
        return row[0] if row and row[0] else None
    finally:
        conn.close()


def get_all_trade_dates():
    """
    获取全量交易日列表（从个股数据最早日期至今）

    自动截断：取个股日K最早日期作为起点，
    避免在指数有数据但个股无数据的日期上白跑循环。

    返回:
        list[str]: 交易日列表（升序）
    """
    earliest = get_stock_data_earliest_date()
    return get_trade_dates(start_date=earliest)


def get_index_members(index_code):
    """
    获取指定指数的成分股列表

    参数:
        index_code: 指数代码

    返回:
        list[str]: 股票代码列表
    """
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT DISTINCT stock_code FROM index_members
            WHERE index_code = ?
        """, (index_code,)).fetchall()
        return [r['stock_code'] for r in rows]
    finally:
        conn.close()


def load_index_closes(index_code, start_date, end_date):
    """
    获取指数在日期范围内的每日收盘价

    参数:
        index_code: 指数代码
        start_date: 起始日期
        end_date: 结束日期

    返回:
        dict: {date: close}
    """
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT date, close FROM index_daily_kline
            WHERE index_code = ? AND date >= ? AND date <= ?
            ORDER BY date
        """, (index_code, start_date, end_date)).fetchall()
        return {r['date']: float(r['close']) for r in rows if r['close'] is not None}
    finally:
        conn.close()


def load_stock_closes(stock_codes, start_date, end_date):
    """
    批量获取多只股票在日期范围内的每日收盘价

    参数:
        stock_codes: list[str] 股票代码列表
        start_date: 起始日期
        end_date: 结束日期

    返回:
        dict: {code: {date: close}}
    """
    if not stock_codes:
        return {}
    conn = get_db_connection()
    try:
        placeholders = ','.join(['?'] * len(stock_codes))
        rows = conn.execute(f"""
            SELECT code, date, close FROM daily_kline
            WHERE code IN ({placeholders})
              AND date >= ? AND date <= ? AND volume > 0
            ORDER BY code, date
        """, stock_codes + [start_date, end_date]).fetchall()

        result = {}
        for r in rows:
            code, date, close = r['code'], r['date'], r['close']
            if close is None:
                continue
            if code not in result:
                result[code] = {}
            result[code][date] = float(close)
        return result
    finally:
        conn.close()


def delete_score_rows(table_name, date_col='calc_date', where_date=None):
    """
    删除评分表中指定日期的旧数据

    参数:
        table_name: 表名（如 'rs_score', 'vcp_score', 'adx_score'）
        date_col: 日期列名
        where_date: 指定日期 'YYYY-MM-DD'，None=删除全部

    返回:
        int: 删除行数
    """
    conn = get_db_connection()
    try:
        if where_date:
            cursor = conn.execute(
                f"DELETE FROM {table_name} WHERE {date_col} = ?",
                (where_date,)
            )
        else:
            cursor = conn.execute(f"DELETE FROM {table_name}")
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        logger.error(f"删除 {table_name} 数据失败: {e}")
        return 0
    finally:
        conn.close()
