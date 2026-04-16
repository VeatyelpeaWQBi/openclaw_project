"""
数据存储模块
负责保存原始日K数据和筛选信号
日K数据存储在SQLite中
信号/报告输出到共享文件夹
"""

import pandas as pd
import os
import sqlite3
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

from core.paths import DATA_DIR, REPORTS_DIR, DB_PATH

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
        logger.debug(f"SQLite: 保存 {stock_code}({stock_name}) {len(rows)} 条日K数据")
    except Exception as e:
        logger.warning(f"SQLite save_daily_kline失败: {e}")
    finally:
        conn.close()


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
        logger.debug(f"SQLite: 保存 {stock_code}({stock_name}) {len(rows)} 条分钟线数据")
    except Exception as e:
        logger.warning(f"SQLite save_minute_kline失败: {e}")
    finally:
        conn.close()


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
            INSERT OR REPLACE INTO index_daily_kline
            (index_code, index_name, date, open, high, low, close, volume, amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        logger.debug(f"SQLite: 保存 {index_code}({index_name}) {len(rows)} 条指数K线")
    except Exception as e:
        logger.warning(f"SQLite save_index_kline失败: {e}")
    finally:
        conn.close()


# ==================== 目录/文件辅助方法 ====================

def ensure_dirs():
    """确保目录存在"""
    os.makedirs(REPORT_SIGNAL_DIR, exist_ok=True)



def get_trading_day_offset(days_ago):
    """
    从交易日历获取N个交易日前的日期

    参数:
        days_ago: 往前推多少个交易日（0=今天或最近交易日, 1=上一交易日）

    返回:
        str: 'YYYY-MM-DD' 或 None
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            row = conn.execute(
                "SELECT trade_date FROM trade_calendar WHERE trade_status=1 AND trade_date <= ? ORDER BY trade_date DESC LIMIT 1 OFFSET ?",
                (today, days_ago)
            ).fetchone()
            return row[0] if row else None
        finally:
            conn.close()
    except Exception:
        return None


def get_daily_data_from_sqlite(stock_code, days=None):
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
                start_date = get_trading_day_offset(days)
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


def get_daily_data_range(stock_code, start_date, end_date):
    """
    从SQLite获取指定日期范围内的日K数据

    参数:
        stock_code: 股票代码
        start_date: 起始日期 'YYYY-MM-DD'
        end_date: 结束日期 'YYYY-MM-DD'

    返回:
        DataFrame: 日K数据（date升序）
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.row_factory = sqlite3.Row
            df = pd.read_sql_query(
                "SELECT * FROM daily_kline WHERE code = ? AND date >= ? AND date <= ? ORDER BY date",
                conn, params=[stock_code, start_date, end_date]
            )
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

# ==================== RS Score 相关方法 ====================

def get_trade_days_range(start_date, end_date):
    """
    获取指定日期范围内的交易日列表

    参数:
        start_date: 起始日期 'YYYY-MM-DD'
        end_date: 结束日期 'YYYY-MM-DD'

    返回:
        list[str]: 交易日列表（升序）
    """
    try:
        conn = get_db_connection()
        try:
            rows = conn.execute("""
                SELECT trade_date FROM trade_calendar
                WHERE trade_status = 1 AND trade_date >= ? AND trade_date <= ?
                ORDER BY trade_date ASC
            """, (start_date, end_date)).fetchall()
            return [r['trade_date'] for r in rows]
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"获取交易日历失败: {e}")
        return []


def get_rs_score_last_date(benchmark_code):
    """
    获取指定基准指数的RS Score最新计算日期

    参数:
        benchmark_code: 基准指数代码

    返回:
        str or None: 最新日期 'YYYY-MM-DD'
    """
    try:
        conn = get_db_connection()
        try:
            row = conn.execute("""
                SELECT MAX(calc_date) FROM rs_score WHERE benchmark_code = ?
            """, (benchmark_code,)).fetchone()
            return row[0] if row and row[0] else None
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"获取RS Score最新日期失败: {e}")
        return None


def get_rs_score_by_code(code, calc_date, benchmark_code='000510'):
    """
    查询指定股票、指定日期的RS评分

    参数:
        code: 股票代码
        calc_date: 计算日期 'YYYY-MM-DD'
        benchmark_code: 基准指数代码，默认中证500

    返回:
        dict or None: RS评分记录
    """
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT * FROM rs_score
            WHERE code = ? AND calc_date = ? AND benchmark_code = ?
        """, (code, calc_date, benchmark_code)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_vcp_score_by_code(code, calc_date):
    """
    查询指定股票、指定日期的VCP评分

    参数:
        code: 股票代码
        calc_date: 计算日期 'YYYY-MM-DD'

    返回:
        dict or None: VCP评分记录
    """
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT * FROM vcp_score
            WHERE code = ? AND calc_date = ?
        """, (code, calc_date)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_adx_score_by_code(code, calc_date):
    """
    查询指定股票、指定日期的ADX评分

    参数:
        code: 股票代码
        calc_date: 计算日期 'YYYY-MM-DD'

    返回:
        dict or None: ADX评分记录
    """
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT * FROM adx_score
            WHERE code = ? AND calc_date = ?
        """, (code, calc_date)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_latest_rs_score(code, benchmark_code='000510'):
    """获取某只股票最新的RS评分"""
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT * FROM rs_score
            WHERE code = ? AND benchmark_code = ?
            ORDER BY calc_date DESC LIMIT 1
        """, (code, benchmark_code)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_latest_vcp_score(code, skip_latest=False):
    """获取某只股票最新的VCP评分（skip_latest=True时取倒数第二条）"""
    conn = get_db_connection()
    try:
        offset = 1 if skip_latest else 0
        row = conn.execute("""
            SELECT * FROM vcp_score
            WHERE code = ?
            ORDER BY calc_date DESC LIMIT 1 OFFSET ?
        """, (code, offset)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_latest_adx_score(code):
    """获取某只股票最新的ADX评分"""
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT * FROM adx_score
            WHERE code = ?
            ORDER BY calc_date DESC LIMIT 1
        """, (code,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_index_daily_closes(index_code, start_date, end_date):
    """
    获取指数在日期范围内的每日收盘价

    参数:
        index_code: 指数代码
        start_date: 起始日期
        end_date: 结束日期

    返回:
        dict: {date: close}
    """
    try:
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
    except Exception as e:
        logger.error(f"获取指数日K失败 [{index_code}]: {e}")
        return {}


def get_stocks_daily_closes(stock_codes, start_date, end_date):
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
    try:
        conn = get_db_connection()
        placeholders = ','.join(['?'] * len(stock_codes))
        rows = conn.execute(f"""
            SELECT code, date, close FROM daily_kline
            WHERE code IN ({placeholders})
              AND date >= ? AND date <= ? AND volume > 0
            ORDER BY code, date
        """, stock_codes + [start_date, end_date]).fetchall()
        conn.close()

        result = {}
        for r in rows:
            code, date, close = r['code'], r['date'], r['close']
            if close is None:
                continue
            if code not in result:
                result[code] = {}
            result[code][date] = float(close)
        return result
    except Exception as e:
        logger.error(f"批量获取日K失败: {e}")
        return {}


def get_all_stocks_daily_data(codes, start_date, end_date):
    """
    批量获取多只股票的完整日K数据（OHLCV），按 code 分组返回

    参数:
        codes: list[str] 股票代码列表
        start_date: 起始日期 'YYYY-MM-DD'
        end_date: 结束日期 'YYYY-MM-DD'

    返回:
        dict: {code: DataFrame} 每只股票的日K（date升序）
    """
    if not codes:
        return {}
    try:
        conn = get_db_connection()
        try:
            BATCH_SIZE = 500
            dfs = []
            for i in range(0, len(codes), BATCH_SIZE):
                batch = codes[i:i + BATCH_SIZE]
                placeholders = ','.join(['?'] * len(batch))
                df_batch = pd.read_sql_query(
                    f"SELECT * FROM daily_kline "
                    f"WHERE code IN ({placeholders}) AND date >= ? AND date <= ? AND volume > 0 "
                    f"ORDER BY code, date",
                    conn, params=batch + [start_date, end_date]
                )
                dfs.append(df_batch)
        finally:
            conn.close()

        df_all = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        result = {}
        for code, group in df_all.groupby('code', sort=False):
            result[code] = group.reset_index(drop=True)
        return result
    except Exception as e:
        logger.error(f"批量获取完整日K失败: {e}")
        return {}


def batch_upsert_rs_score(rows):
    """
    批量写入/更新RS Score

    参数:
        rows: list[tuple] 每行 (code, benchmark_code, calc_date, rs_ratio, rs_score,
               rs_rank, stock_return, benchmark_return, lookback_days, write_at)

    返回:
        int: 写入条数
    """
    if not rows:
        return 0
    conn = get_db_connection()
    try:
        conn.executemany("""
            INSERT OR REPLACE INTO rs_score
            (code, benchmark_code, calc_date, rs_ratio, rs_score, rs_rank,
             stock_return, benchmark_return, lookback_days, write_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        return len(rows)
    except Exception as e:
        logger.error(f"批量写入RS Score失败: {e}")
        return 0
    finally:
        conn.close()


def get_watchlist_index_codes():
    """
    获取候选池中关联的唯一指数代码列表

    返回:
        list[str]: 指数代码列表
    """
    try:
        conn = get_db_connection()
        try:
            rows = conn.execute("""
                SELECT DISTINCT index_code FROM watchlist
                WHERE active = 1 AND index_code IS NOT NULL
            """).fetchall()
            return [r['index_code'] for r in rows]
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"获取候选池指数列表失败: {e}")
        return []


def get_watchlist_stocks_by_index(index_code):
    """
    获取候选池中指定指数关联的股票代码

    参数:
        index_code: 指数代码

    返回:
        list[str]: 股票代码列表
    """
    try:
        conn = get_db_connection()
        try:
            rows = conn.execute("""
                SELECT DISTINCT code FROM watchlist
                WHERE active = 1 AND index_code = ?
            """, (index_code,)).fetchall()
            return [r['code'] for r in rows]
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"获取候选池成分股失败 [{index_code}]: {e}")
        return []


def get_tracked_indices():
    """
    获取 index_info 中跟踪的指数列表

    返回:
        dict: {code: short_name}
    """
    try:
        conn = get_db_connection()
        try:
            rows = conn.execute("SELECT code, short_name FROM index_info").fetchall()
            return {str(r['code']): r['short_name'] or '' for r in rows}
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"获取跟踪指数列表失败: {e}")
        return {}


def batch_upsert_daily_kline(rows):
    """
    批量写入/更新个股日K

    参数:
        rows: list[tuple] 每行 (code, name, date, open, high, low, close, volume, amount,
               change_pct, turnover, pe_ratio, pb_ratio, mktcap, nmc, outstanding_share, volume_ratio)

    返回:
        tuple: (success_count, error_count)
    """
    if not rows:
        return (0, 0)
    success = 0
    error = 0
    conn = get_db_connection()
    try:
        conn.execute("BEGIN TRANSACTION")
        for row in rows:
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO daily_kline
                    (code, name, date, open, high, low, close, volume, amount,
                     change_pct, turnover, pe_ratio, pb_ratio, mktcap, nmc,
                     outstanding_share, volume_ratio)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row)
                success += 1
            except Exception as e:
                logger.debug(f"写入失败 [{row[0]}]: {e}")
                error += 1
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"批量写入日K失败: {e}")
    finally:
        conn.close()
    return (success, error)


def batch_upsert_index_daily_kline(rows):
    """
    批量写入/更新指数日K

    参数:
        rows: list[tuple] 每行 (index_code, index_name, date, open, high, low, close,
               volume, amount, change, change_pct)

    返回:
        int: 写入条数
    """
    if not rows:
        return 0
    try:
        conn = get_db_connection()
        conn.executemany("""
            INSERT OR REPLACE INTO index_daily_kline
            (index_code, index_name, date, open, high, low, close,
             volume, amount, change, change_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        conn.close()
        return len(rows)
    except Exception as e:
        logger.error(f"批量写入指数日K失败: {e}")
        return 0


def get_daily_kline_max_date(code):
    """
    获取个股日K最新日期

    参数:
        code: 股票代码

    返回:
        str or None: 最新日期 'YYYY-MM-DD'
    """
    try:
        conn = get_db_connection()
        try:
            row = conn.execute(
                "SELECT MAX(date) FROM daily_kline WHERE code = ?", (code,)
            ).fetchone()
            return row[0] if row and row[0] else None
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"获取日K最新日期失败 [{code}]: {e}")
        return None


def get_index_daily_kline_max_date(index_code):
    """
    获取指数日K最新日期

    参数:
        index_code: 指数代码

    返回:
        str or None: 最新日期 'YYYY-MM-DD'
    """
    try:
        conn = get_db_connection()
        try:
            row = conn.execute(
                "SELECT MAX(date) FROM index_daily_kline WHERE index_code = ?", (index_code,)
            ).fetchone()
            return row[0] if row and row[0] else None
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"获取指数日K最新日期失败 [{index_code}]: {e}")
        return None


def get_recent_trade_dates(trade_date, limit=5):
    """
    获取指定日期之前的最近N个交易日

    参数:
        trade_date: 基准日期 'YYYY-MM-DD'
        limit: 回溯天数

    返回:
        list[str]: 交易日列表（从近到远）
    """
    try:
        conn = get_db_connection()
        try:
            rows = conn.execute("""
                SELECT trade_date FROM trade_calendar
                WHERE trade_status = 1 AND trade_date < ?
                ORDER BY trade_date DESC LIMIT ?
            """, (trade_date, limit)).fetchall()
            return [r['trade_date'] for r in rows]
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"获取最近交易日失败: {e}")
        return []


def get_avg_volume_by_code(start_date, end_date):
    """
    获取日期范围内所有股票的平均成交量

    参数:
        start_date: 起始日期（含）
        end_date: 结束日期（不含）

    返回:
        dict: {code: avg_volume}
    """
    try:
        conn = get_db_connection()
        try:
            rows = conn.execute("""
                SELECT code, AVG(volume) as avg_vol
                FROM daily_kline
                WHERE date >= ? AND date < ?
                GROUP BY code
                HAVING COUNT(*) >= 2
            """, (start_date, end_date)).fetchall()
            return {r['code']: float(r['avg_vol']) for r in rows if r['avg_vol'] and r['avg_vol'] > 0}
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"获取均量失败: {e}")
        return {}


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


def is_trade_day(date_str):
    """
    判断指定日期是否为交易日

    参数:
        date_str: 日期 'YYYY-MM-DD'

    返回:
        bool: True=交易日
    """
    try:
        conn = get_db_connection()
        try:
            row = conn.execute(
                "SELECT trade_status FROM trade_calendar WHERE trade_date = ?", (date_str,)
            ).fetchone()
            return bool(row and row['trade_status'] == 1)
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"查询交易日失败 [{date_str}]: {e}")
        return False


def get_trading_day_offset_from_end(end_date, offset):
    """
    从结束日期往前推N个交易日

    参数:
        end_date: 结束日期 'YYYY-MM-DD'
        offset: 偏移量（负=往前）

    返回:
        str or None: 目标交易日
    """
    return get_trading_day_offset_from(end_date, offset)


# ==================== VCP 评分存储 ====================

def save_vcp_score(records):
    """
    批量写入 VCP 评分结果（INSERT OR REPLACE）

    参数:
        records: list of dict，每个 dict 包含:
            code, calc_date, score,
            score_compression, score_contraction, score_credibility,
            score_swing_count, score_volume, score_triangle_type,
            data_start, data_end
    """
    if not records:
        return 0

    conn = get_db_connection()
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        rows = []
        for r in records:
            rows.append((
                r['code'],
                r['calc_date'],
                r['score'],
                r.get('score_compression'),
                r.get('score_contraction'),
                r.get('score_credibility'),
                r.get('score_swing_count'),
                r.get('score_volume'),
                r.get('score_triangle_type'),
                r.get('data_start'),
                r.get('data_end'),
                now,
            ))

        conn.executemany(
            """INSERT OR REPLACE INTO vcp_score
               (code, calc_date, score,
                score_compression, score_contraction, score_credibility,
                score_swing_count, score_volume, score_triangle_type,
                data_start, data_end, write_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def get_vcp_scores(calc_date=None, min_score=None, limit=None):
    """
    查询 VCP 评分

    参数:
        calc_date: 指定日期（YYYY-MM-DD），None=不限
        min_score: 最低分筛选，None=不限
        limit: 返回条数上限

    返回:
        list of dict
    """
    conn = get_db_connection()
    try:
        sql = "SELECT * FROM vcp_score WHERE 1=1"
        params = []
        if calc_date:
            sql += " AND calc_date = ?"
            params.append(calc_date)
        if min_score is not None:
            sql += " AND score >= ?"
            params.append(min_score)
        sql += " ORDER BY score DESC"
        if limit:
            sql += " LIMIT ?"
            params.append(limit)

        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_vcp_history(code, days=30):
    """
    查询某只股票的 VCP 评分历史

    参数:
        code: 股票代码
        days: 最近N天

    返回:
        list of dict，按日期降序
    """
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """SELECT * FROM vcp_score
               WHERE code = ? ORDER BY calc_date DESC LIMIT ?""",
            (code, days)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

# ==================== ADX Score 相关方法 ====================

def save_adx_score(records):
    """
    批量写入/更新 ADX 评分结果（INSERT OR REPLACE）

    参数:
        records: list of dict，每个 dict 包含:
            code, calc_date, period, adx, plus_di, minus_di, dx, adx_score_val

    返回:
        int: 写入条数
    """
    if not records:
        return 0

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    rows = []
    for r in records:
        rows.append((
            r['code'],
            r['calc_date'],
            r.get('period', 14),
            r['adx'],
            r['plus_di'],
            r['minus_di'],
            r['dx'],
            r['adx_score_val'],
            now,
        ))

    conn = get_db_connection()
    try:
        conn.executemany(
            """INSERT OR REPLACE INTO adx_score
               (code, calc_date, period, adx, plus_di, minus_di, dx,
                adx_score_val, write_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows
        )
        conn.commit()
        return len(rows)
    except Exception as e:
        logger.error(f"批量写入ADX Score失败: {e}")
        return 0
    finally:
        conn.close()


def get_adx_scores_by_date(calc_date, min_score=None, limit=None):
    """
    指定日期查询ADX评分

    参数:
        calc_date: 指定日期（YYYY-MM-DD），None=不限
        min_score: 最低分筛选，None=不限
        limit: 返回条数上限

    返回:
        list of dict
    """
    conn = get_db_connection()
    try:
        sql = "SELECT * FROM adx_score WHERE 1=1"
        params = []
        if calc_date:
            sql += " AND calc_date = ?"
            params.append(calc_date)
        if min_score is not None:
            sql += " AND adx_score_val >= ?"
            params.append(min_score)
        sql += " ORDER BY adx_score_val DESC"
        if limit:
            sql += " LIMIT ?"
            params.append(limit)

        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_index_amount_before_date(index_code, before_date):
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


def get_adx_history(code, days=30):
    """
    查询某只股票的 ADX 评分历史

    参数:
        code: 股票代码
        days: 最近N天

    返回:
        list of dict，按日期降序
    """
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """SELECT * FROM adx_score
               WHERE code = ? ORDER BY calc_date DESC LIMIT ?""",
            (code, days)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
