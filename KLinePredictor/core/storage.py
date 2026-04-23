"""
数据存储模块 - K线预测专用
支持持仓池/候选池和技术指标存储
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


# ==================== 持仓池/候选池表初始化 ====================

def init_pool_tables():
    """
    初始化持仓池和候选池表
    """
    conn = get_db_connection()
    try:
        # 持仓池表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS position_pool (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                entry_price REAL NOT NULL,
                entry_date TEXT NOT NULL,
                shares INTEGER NOT NULL DEFAULT 0,
                position_type TEXT NOT NULL DEFAULT '趋势',
                stop_loss REAL,
                take_profit REAL,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        # 候选池表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS candidate_pool (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                watch_price REAL NOT NULL,
                watch_date TEXT NOT NULL,
                target_price REAL,
                watch_type TEXT NOT NULL DEFAULT '趋势回调',
                watch_reason TEXT,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        conn.commit()
        logger.info("持仓池/候选池表初始化完成")
    except Exception as e:
        logger.error(f"初始化池表失败: {e}")
    finally:
        conn.close()


# ==================== 持仓池操作 ====================

def add_position(code: str, name: str, entry_price: float, entry_date: str,
                 shares: int = 0, position_type: str = '趋势',
                 stop_loss: float = None, take_profit: float = None,
                 notes: str = None) -> bool:
    """
    添加持仓到持仓池
    """
    conn = get_db_connection()
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute("""
            INSERT INTO position_pool
            (code, name, entry_price, entry_date, shares, position_type,
             stop_loss, take_profit, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (code, name, entry_price, entry_date, shares, position_type,
              stop_loss, take_profit, notes, now, now))
        conn.commit()
        logger.info(f"添加持仓: {code}({name})")
        return True
    except sqlite3.IntegrityError:
        logger.warning(f"持仓已存在: {code}")
        return False
    except Exception as e:
        logger.error(f"添加持仓失败: {e}")
        return False
    finally:
        conn.close()


def remove_position(code: str) -> bool:
    """
    从持仓池删除持仓
    """
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM position_pool WHERE code = ?", (code,))
        conn.commit()
        logger.info(f"删除持仓: {code}")
        return True
    except Exception as e:
        logger.error(f"删除持仓失败: {e}")
        return False
    finally:
        conn.close()


def update_position(code: str, **kwargs) -> bool:
    """
    更新持仓信息
    可更新字段: stop_loss, take_profit, shares, notes, position_type
    """
    if not kwargs:
        return False

    allowed_fields = ['stop_loss', 'take_profit', 'shares', 'notes', 'position_type']
    update_fields = {k: v for k, v in kwargs.items() if k in allowed_fields}

    if not update_fields:
        logger.warning(f"无有效更新字段: {kwargs}")
        return False

    conn = get_db_connection()
    try:
        update_fields['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = "UPDATE position_pool SET " + ", ".join([f"{k}=?" for k in update_fields.keys()]) + " WHERE code=?"
        params = list(update_fields.values()) + [code]
        conn.execute(sql, params)
        conn.commit()
        logger.info(f"更新持仓: {code}")
        return True
    except Exception as e:
        logger.error(f"更新持仓失败: {e}")
        return False
    finally:
        conn.close()


def get_all_positions() -> list:
    """
    获取所有持仓
    """
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT * FROM position_pool ORDER BY created_at DESC").fetchall()
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"获取持仓列表失败: {e}")
        return []
    finally:
        conn.close()


def get_position_by_code(code: str) -> dict:
    """
    根据代码获取持仓详情
    """
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT * FROM position_pool WHERE code = ?", (code,)).fetchone()
        return dict(row) if row else None
    except Exception as e:
        logger.error(f"获取持仓详情失败: {e}")
        return None
    finally:
        conn.close()


# ==================== 候选池操作 ====================

def add_candidate(code: str, name: str, watch_price: float, watch_date: str,
                  target_price: float = None, watch_type: str = '趋势回调',
                  watch_reason: str = None, notes: str = None) -> bool:
    """
    添加候选到候选池
    """
    conn = get_db_connection()
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute("""
            INSERT INTO candidate_pool
            (code, name, watch_price, watch_date, target_price, watch_type,
             watch_reason, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (code, name, watch_price, watch_date, target_price, watch_type,
              watch_reason, notes, now, now))
        conn.commit()
        logger.info(f"添加候选: {code}({name})")
        return True
    except sqlite3.IntegrityError:
        logger.warning(f"候选已存在: {code}")
        return False
    except Exception as e:
        logger.error(f"添加候选失败: {e}")
        return False
    finally:
        conn.close()


def remove_candidate(code: str) -> bool:
    """
    从候选池删除候选
    """
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM candidate_pool WHERE code = ?", (code,))
        conn.commit()
        logger.info(f"删除候选: {code}")
        return True
    except Exception as e:
        logger.error(f"删除候选失败: {e}")
        return False
    finally:
        conn.close()


def update_candidate(code: str, **kwargs) -> bool:
    """
    更新候选信息
    可更新字段: target_price, watch_type, watch_reason, notes
    """
    if not kwargs:
        return False

    allowed_fields = ['target_price', 'watch_type', 'watch_reason', 'notes']
    update_fields = {k: v for k, v in kwargs.items() if k in allowed_fields}

    if not update_fields:
        logger.warning(f"无有效更新字段: {kwargs}")
        return False

    conn = get_db_connection()
    try:
        update_fields['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = "UPDATE candidate_pool SET " + ", ".join([f"{k}=?" for k in update_fields.keys()]) + " WHERE code=?"
        params = list(update_fields.values()) + [code]
        conn.execute(sql, params)
        conn.commit()
        logger.info(f"更新候选: {code}")
        return True
    except Exception as e:
        logger.error(f"更新候选失败: {e}")
        return False
    finally:
        conn.close()


def get_all_candidates() -> list:
    """
    获取所有候选
    """
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT * FROM candidate_pool ORDER BY created_at DESC").fetchall()
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"获取候选列表失败: {e}")
        return []
    finally:
        conn.close()


def get_candidate_by_code(code: str) -> dict:
    """
    根据代码获取候选详情
    """
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT * FROM candidate_pool WHERE code = ?", (code,)).fetchone()
        return dict(row) if row else None
    except Exception as e:
        logger.error(f"获取候选详情失败: {e}")
        return None
    finally:
        conn.close()


# ==================== 技术指标表 ====================

def init_technical_indicators_table():
    """
    初始化技术指标表
    """
    conn = get_db_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS technical_indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                calc_date TEXT NOT NULL,

                -- 均线指标
                ma5 REAL,
                ma10 REAL,
                ma20 REAL,
                ma60 REAL,
                ma120 REAL,
                ma250 REAL,
                ma5_slope REAL,
                ma10_slope REAL,
                ma20_slope REAL,

                -- SuperTrend指标
                st_upper_band REAL,
                st_lower_band REAL,
                st_direction INTEGER,
                st_atr REAL,

                -- MACD指标
                macd_dif REAL,
                macd_dea REAL,
                macd_histogram REAL,
                macd_histogram_slope INTEGER DEFAULT 0,  -- MACD柱斜率: 1=向上, 0=趋平, -1=向下
                macd_dif_slope INTEGER DEFAULT 0,  -- DIF线斜率
                macd_dea_slope INTEGER DEFAULT 0,  -- DEA线斜率
                macd_slope_summary TEXT,  -- MACD综合判断文案

                -- RSI指标
                rsi_14 REAL,

                -- 量比
                volume_ratio_5 REAL,
                volume_ratio_20 REAL,

                -- K线形态
                is_long_upper_shadow INTEGER DEFAULT 0,
                is_long_lower_shadow INTEGER DEFAULT 0,
                is_bullish_candle INTEGER DEFAULT 0,
                is_bearish_candle INTEGER DEFAULT 0,

                -- 元数据
                created_at TEXT NOT NULL,

                UNIQUE(code, calc_date)
            )
        """)

        # 索引
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ti_code_date ON technical_indicators(code, calc_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ti_date ON technical_indicators(calc_date)")

        conn.commit()
        logger.info("技术指标表初始化完成")
    except Exception as e:
        logger.error(f"初始化技术指标表失败: {e}")
    finally:
        conn.close()


def save_technical_indicators(code: str, indicators: dict) -> bool:
    """
    保存技术指标到数据库
    """
    if not indicators or 'calc_date' not in indicators:
        logger.warning(f"技术指标数据无效: {code}")
        return False

    conn = get_db_connection()
    try:
        conn.execute("""
            INSERT OR REPLACE INTO technical_indicators
            (code, calc_date, ma5, ma10, ma20, ma60, ma120, ma250,
             ma5_slope, ma10_slope, ma20_slope,
             st_upper_band, st_lower_band, st_direction, st_atr,
             macd_dif, macd_dea, macd_histogram,
             macd_histogram_slope, macd_dif_slope, macd_dea_slope, macd_slope_summary,
             rsi_14, volume_ratio_5, volume_ratio_20,
             is_long_upper_shadow, is_long_lower_shadow,
             is_bullish_candle, is_bearish_candle,
             created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            code, indicators.get('calc_date'),
            indicators.get('ma5'), indicators.get('ma10'), indicators.get('ma20'),
            indicators.get('ma60'), indicators.get('ma120'), indicators.get('ma250'),
            indicators.get('ma5_slope'), indicators.get('ma10_slope'), indicators.get('ma20_slope'),
            indicators.get('st_upper_band'), indicators.get('st_lower_band'),
            indicators.get('st_direction'), indicators.get('st_atr'),
            indicators.get('macd_dif'), indicators.get('macd_dea'), indicators.get('macd_histogram'),
            indicators.get('macd_histogram_slope', 0), indicators.get('macd_dif_slope', 0),
            indicators.get('macd_dea_slope', 0), indicators.get('macd_slope_summary', '→震荡'),
            indicators.get('rsi_14'),
            indicators.get('volume_ratio_5'), indicators.get('volume_ratio_20'),
            indicators.get('is_long_upper_shadow', 0), indicators.get('is_long_lower_shadow', 0),
            indicators.get('is_bullish_candle', 0), indicators.get('is_bearish_candle', 0),
            indicators.get('created_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        ))
        conn.commit()
        logger.debug(f"保存技术指标: {code} {indicators.get('calc_date')}")
        return True
    except Exception as e:
        logger.error(f"保存技术指标失败: {e}")
        return False
    finally:
        conn.close()


def get_technical_indicators(code: str, calc_date: str) -> dict:
    """
    获取指定日期的技术指标
    """
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT * FROM technical_indicators WHERE code = ? AND calc_date = ?",
                          (code, calc_date)).fetchone()
        return dict(row) if row else None
    except Exception as e:
        logger.error(f"获取技术指标失败: {e}")
        return None
    finally:
        conn.close()


def get_technical_indicators_range(code: str, start_date: str, end_date: str) -> list:
    """
    获取指定时间范围的技术指标
    """
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT * FROM technical_indicators
            WHERE code = ? AND calc_date >= ? AND calc_date <= ?
            ORDER BY calc_date
        """, (code, start_date, end_date)).fetchall()
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"获取技术指标范围失败: {e}")
        return []
    finally:
        conn.close()


def get_daily_kline_range(stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    从SQLite获取指定日期范围的日K数据

    参数:
        stock_code: 股票代码
        start_date: 开始日期 'YYYY-MM-DD'
        end_date: 结束日期 'YYYY-MM-DD'

    返回:
        DataFrame: 包含date, open, high, low, close, volume, amount等列
    """
    try:
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        df = pd.read_sql_query("""
            SELECT date, open, high, low, close, volume, amount
            FROM daily_kline
            WHERE code = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
        """, conn, params=[stock_code, start_date, end_date])
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        logger.error(f"获取日K数据失败 [{stock_code}]: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


def get_trading_days_range(start_date: str, end_date: str) -> list:
    """
    获取指定日期范围内的交易日列表

    参数:
        start_date: 开始日期 'YYYY-MM-DD'
        end_date: 结束日期 'YYYY-MM-DD'

    返回:
        list: 交易日列表 ['YYYY-MM-DD', ...]
    """
    try:
        conn = get_db_connection()
        rows = conn.execute("""
            SELECT trade_date FROM trade_calendar
            WHERE trade_status = 1 AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date ASC
        """, (start_date, end_date)).fetchall()
        return [row['trade_date'] for row in rows]
    except Exception as e:
        logger.error(f"获取交易日列表失败: {e}")
        return []
    finally:
        conn.close()


def init_all_tables():
    """
    初始化所有新增表
    """
    init_pool_tables()
    init_technical_indicators_table()
    logger.info("所有新增表初始化完成")