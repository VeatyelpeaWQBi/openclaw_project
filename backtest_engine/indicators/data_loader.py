"""
数据加载模块

从数据库加载交易日历、股票列表、日K数据和技术指标
"""

import logging
from typing import Optional

import pandas as pd
import sqlite3

from core.paths import DB_PATH

logger = logging.getLogger(__name__)


class DataLoader:
    """数据加载器"""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or str(DB_PATH)

    def get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def load_trading_dates(self, start_date: str, end_date: str) -> list[str]:
        """
        加载指定时间范围内的交易日列表

        参数:
            start_date: 起始日期 'YYYY-MM-DD'
            end_date: 结束日期 'YYYY-MM-DD'

        返回:
            list[str]: 交易日列表（升序）
        """
        conn = self.get_connection()
        try:
            rows = conn.execute("""
                SELECT trade_date FROM trade_calendar
                WHERE trade_status = 1 AND trade_date >= ? AND trade_date <= ?
                ORDER BY trade_date ASC
            """, (start_date, end_date)).fetchall()
            return [r['trade_date'] for r in rows]
        finally:
            conn.close()

    def load_all_stock_codes(self) -> list[str]:
        """
        加载全市场有日K数据的股票代码列表

        返回:
            list[str]: 股票代码列表
        """
        conn = self.get_connection()
        try:
            rows = conn.execute("""
                SELECT DISTINCT code FROM daily_kline
                WHERE volume > 0
                ORDER BY code
            """).fetchall()
            return [r['code'] for r in rows]
        finally:
            conn.close()

    def load_daily_kline(self, code: str, start_date: str, end_date: str,
                         add_days: int = 100) -> pd.DataFrame:
        """
        加载单只股票的日K数据

        参数:
            code: 股票代码
            start_date: 起始日期
            end_date: 结束日期
            add_days: 额外加载的天数（用于技术指标计算预热）

        返回:
            DataFrame: 日K数据（date升序），包含 date, open, high, low, close, volume 列
        """
        conn = self.get_connection()
        try:
            df = pd.read_sql_query(
                """
                SELECT date, open, high, low, close, volume
                FROM daily_kline
                WHERE code = ? AND date >= ? AND date <= ? AND volume > 0
                ORDER BY date
                """,
                conn,
                params=[code, start_date, end_date]
            )
            return df
        finally:
            conn.close()

    def load_daily_kline_with_extra(self, code: str, start_date: str, end_date: str,
                                    add_days: int = 100) -> pd.DataFrame:
        """
        加载单只股票的日K数据（包含额外预热天数）

        参数:
            code: 股票代码
            start_date: 起始日期
            end_date: 结束日期
            add_days: 向前额外加载的天数

        返回:
            DataFrame: 日K数据（date升序）
        """
        conn = self.get_connection()
        try:
            from core.storage import get_trading_day_offset_from
            actual_start = get_trading_day_offset_from(start_date, -add_days)
            if not actual_start:
                actual_start = start_date

            df = pd.read_sql_query(
                """
                SELECT date, open, high, low, close, volume
                FROM daily_kline
                WHERE code = ? AND date >= ? AND date <= ? AND volume > 0
                ORDER BY date
                """,
                conn,
                params=[code, actual_start, end_date]
            )
            return df
        finally:
            conn.close()

    def load_technical_indicators(self, code: str, start_date: str,
                                   end_date: str, add_days: int = 100) -> pd.DataFrame:
        """
        加载单只股票的技术指标数据

        参数:
            code: 股票代码
            start_date: 起始日期
            end_date: 结束日期
            add_days: 向前额外加载的天数

        返回:
            DataFrame: 技术指标数据（calc_date升序）
        """
        conn = self.get_connection()
        try:
            from core.storage import get_trading_day_offset_from
            actual_start = get_trading_day_offset_from(start_date, -add_days)
            if not actual_start:
                actual_start = start_date

            df = pd.read_sql_query(
                """
                SELECT *
                FROM technical_indicators
                WHERE code = ? AND calc_date >= ? AND calc_date <= ?
                ORDER BY calc_date
                """,
                conn,
                params=[code, actual_start, end_date]
            )
            return df
        finally:
            conn.close()

    def load_stock_info(self, code: str) -> Optional[dict]:
        """
        加载股票基本信息

        参数:
            code: 股票代码

        返回:
            dict or None: 股票信息字典
        """
        conn = self.get_connection()
        try:
            row = conn.execute("""
                SELECT * FROM stock_info WHERE code = ?
            """, (code,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()