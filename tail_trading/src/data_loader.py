"""
回测数据加载器
支持日K数据和分钟线数据
优先从SQLite加载，CSV作为fallback
"""

import pandas as pd
import os
import glob
import sqlite3
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# SQLite数据库路径
DB_PATH = '***REMOVED***/tail_trading/data/stock_data.db'


def get_connection():
    """获取SQLite连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


class MinuteDataLoader:
    """分钟线数据加载器"""

    def __init__(self, data_dir, minute_data_dir=None):
        """
        参数:
            data_dir: 日K数据目录 (tail_trading/data)
            minute_data_dir: 分钟线数据目录 (tail_trading/minute_data)
        """
        self.data_dir = data_dir
        self.kline_dir = os.path.join(data_dir, 'kline')
        self.minute_data_dir = minute_data_dir or os.path.join(
            os.path.dirname(data_dir), 'minute_data'
        )

    # ==================== SQLite核心方法 ====================

    def _sqlite_get_stock_list(self):
        """从SQLite获取股票列表"""
        try:
            conn = get_connection()
            df = pd.read_sql_query(
                "SELECT DISTINCT code, name FROM daily_kline ORDER BY code",
                conn
            )
            conn.close()
            if not df.empty:
                stocks = []
                for _, row in df.iterrows():
                    stocks.append({'code': row['code'], 'name': row['name']})
                logger.debug(f"SQLite get_stock_list: {len(stocks)} 只股票")
                return stocks
        except Exception as e:
            logger.warning(f"SQLite get_stock_list失败: {e}")
        return None

    def _sqlite_get_daily_data(self, code, start_date=None, end_date=None):
        """从SQLite获取个股日K数据"""
        try:
            conn = get_connection()
            query = "SELECT date, open, high, low, close, volume, amount, turnover FROM daily_kline WHERE code = ?"
            params = [code]

            if start_date:
                start = start_date.replace('-', '')[:8]
                start_fmt = f'{start[:4]}-{start[4:6]}-{start[6:8]}' if len(start) == 8 else start
                query += " AND date >= ?"
                params.append(start_fmt)
            if end_date:
                end = end_date.replace('-', '')[:8]
                end_fmt = f'{end[:4]}-{end[4:6]}-{end[6:8]}' if len(end) == 8 else end
                query += " AND date <= ?"
                params.append(end_fmt)

            query += " ORDER BY date"
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()

            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                logger.debug(f"SQLite get_daily_data({code}): {len(df)} 条 (start={start_date}, end={end_date})")
                return df
        except Exception as e:
            logger.warning(f"SQLite get_daily_data失败: {e}")
        return None

    def _sqlite_get_minute_data(self, code, date_str):
        """从SQLite获取指定日期的分钟线数据"""
        try:
            conn = get_connection()
            # date_str格式: YYYYMMDD
            date_fmt = f'{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}'
            query = """SELECT datetime, open, high, low, close, volume, amount
                       FROM minute_kline
                       WHERE code = ? AND date = ?
                       ORDER BY datetime"""
            df = pd.read_sql_query(query, conn, params=[code, date_fmt])
            conn.close()

            if not df.empty:
                return df
        except Exception as e:
            logger.warning(f"SQLite get_minute_data失败: {e}")
        return None

    def _sqlite_get_trade_dates(self, start_date=None, end_date=None):
        """从SQLite获取交易日列表"""
        try:
            conn = get_connection()
            query = "SELECT DISTINCT date FROM daily_kline"
            params = []

            conditions = []
            if start_date:
                start = start_date.replace('-', '')[:8]
                start_fmt = f'{start[:4]}-{start[4:6]}-{start[6:8]}' if len(start) == 8 else start
                conditions.append("date >= ?")
                params.append(start_fmt)
            if end_date:
                end = end_date.replace('-', '')[:8]
                end_fmt = f'{end[:4]}-{end[4:6]}-{end[6:8]}' if len(end) == 8 else end
                conditions.append("date <= ?")
                params.append(end_fmt)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += " ORDER BY date"

            df = pd.read_sql_query(query, conn, params=params)
            conn.close()

            if not df.empty:
                return [str(d)[:10] for d in df['date']]
        except Exception as e:
            logger.warning(f"SQLite get_trade_dates失败: {e}")
        return None

    # ==================== 公共API（保持接口不变）====================

    def get_stock_list(self):
        """获取所有已下载日K数据的股票列表"""
        # 尝试SQLite
        result = self._sqlite_get_stock_list()
        if result is not None:
            return result

        # CSV fallback
        return self._csv_get_stock_list()

    def _csv_get_stock_list(self):
        """从CSV获取股票列表（fallback）"""
        stocks = []
        if not os.path.exists(self.kline_dir):
            return stocks

        for month_dir in sorted(os.listdir(self.kline_dir)):
            month_path = os.path.join(self.kline_dir, month_dir)
            if not os.path.isdir(month_path):
                continue
            for fname in os.listdir(month_path):
                if fname.endswith('.csv'):
                    parts = fname.replace('.csv', '').split('_', 1)
                    code = parts[0]
                    name = parts[1] if len(parts) > 1 else ''
                    stocks.append({'code': code, 'name': name, 'month': month_dir})

        # 去重（不同月份可能有相同股票）
        seen = set()
        unique = []
        for s in stocks:
            key = s['code']
            if key not in seen:
                seen.add(key)
                unique.append(s)
        return unique

    def get_daily_data(self, code, name='', month_str=None):
        """
        获取日K数据

        参数:
            code: 股票代码
            name: 股票名称（可选，用于定位CSV文件）
            month_str: 月份 'YYYY-MM'（可选，仅CSV模式使用）

        返回:
            DataFrame 或空DataFrame
        """
        # 尝试SQLite（除非指定了month_str，CSV模式更精确）
        if month_str is None:
            result = self._sqlite_get_daily_data(code)
            if result is not None and not result.empty:
                return result

        # CSV fallback
        return self._csv_get_daily_data(code, name, month_str)

    def _csv_get_daily_data(self, code, name='', month_str=None):
        """从CSV获取日K数据（fallback）"""
        if month_str:
            return self._load_daily_kline(code, name, month_str)

        # 如果没指定月份，加载所有可用月份
        all_data = []
        if not os.path.exists(self.kline_dir):
            return pd.DataFrame()

        for m in sorted(os.listdir(self.kline_dir)):
            df = self._load_daily_kline(code, name, m)
            if not df.empty:
                all_data.append(df)

        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            combined['date'] = pd.to_datetime(combined['date'])
            combined = combined.drop_duplicates(subset='date', keep='last')
            combined = combined.sort_values('date').reset_index(drop=True)
            return combined
        return pd.DataFrame()

    def _load_daily_kline(self, code, name, month_str):
        """加载指定月份的日K数据"""
        month_dir = os.path.join(self.kline_dir, month_str)
        if not os.path.isdir(month_dir):
            return pd.DataFrame()

        # 尝试精确匹配
        if name:
            exact = os.path.join(month_dir, f'{code}_{name}.csv')
            if os.path.exists(exact):
                df = pd.read_csv(exact)
                df['date'] = pd.to_datetime(df['date'])
                return df

        # 模糊匹配
        pattern = os.path.join(month_dir, f'{code}_*.csv')
        matches = glob.glob(pattern)
        if matches:
            df = pd.read_csv(matches[0])
            df['date'] = pd.to_datetime(df['date'])
            return df

        return pd.DataFrame()

    def get_minute_data(self, code, name, date_str):
        """
        获取指定日期的分钟线数据

        参数:
            code: 股票代码
            name: 股票名称
            date_str: 日期 'YYYYMMDD'

        返回:
            DataFrame 或 None
        """
        # 尝试SQLite
        result = self._sqlite_get_minute_data(code, date_str)
        if result is not None and not result.empty:
            return result

        # CSV fallback
        return self._csv_get_minute_data(code, name, date_str)

    def _csv_get_minute_data(self, code, name, date_str):
        """从CSV获取分钟线数据（fallback）"""
        year = date_str[:4]
        month = date_str[4:6]
        month_key = f'{year}-{month}'

        file_path = os.path.join(
            self.minute_data_dir,
            month_key,
            f'{code}_{name}',
            f'{code}_{name}_{date_str}.csv'
        )

        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            return df

        # 也尝试不分目录的格式
        alt_path = os.path.join(
            self.minute_data_dir,
            month_key,
            f'{code}_{name}_{date_str}.csv'
        )
        if os.path.exists(alt_path):
            return pd.read_csv(alt_path)

        return None

    def get_trade_dates(self, start_date=None, end_date=None):
        """
        获取交易日列表

        参数:
            start_date: 起始日期 'YYYYMMDD' 或 'YYYY-MM-DD'
            end_date: 结束日期

        返回:
            list[str]: 交易日列表 'YYYY-MM-DD'
        """
        # 尝试SQLite
        result = self._sqlite_get_trade_dates(start_date, end_date)
        if result is not None:
            return result

        # CSV fallback
        return self._csv_get_trade_dates(start_date, end_date)

    def _csv_get_trade_dates(self, start_date=None, end_date=None):
        """从CSV获取交易日列表（fallback）"""
        all_dates = set()
        if not os.path.exists(self.kline_dir):
            return []

        for month_dir in os.listdir(self.kline_dir):
            month_path = os.path.join(self.kline_dir, month_dir)
            if not os.path.isdir(month_path):
                continue
            for fname in os.listdir(month_path):
                if fname.endswith('.csv'):
                    try:
                        df = pd.read_csv(os.path.join(month_path, fname), usecols=['date'])
                        for d in df['date']:
                            all_dates.add(str(d)[:10])
                    except Exception:
                        continue

        dates = sorted(all_dates)

        if start_date:
            start = start_date.replace('-', '')[:8]
            start_fmt = f'{start[:4]}-{start[4:6]}-{start[6:8]}'
            dates = [d for d in dates if d >= start_fmt]

        if end_date:
            end = end_date.replace('-', '')[:8]
            end_fmt = f'{end[:4]}-{end[4:6]}-{end[6:8]}'
            dates = [d for d in dates if d <= end_fmt]

        return dates

    @staticmethod
    def get_next_trade_date(current_date, trade_dates):
        """
        获取下一个交易日

        参数:
            current_date: 当前日期 'YYYYMMDD' 或 'YYYY-MM-DD'
            trade_dates: 交易日列表 ['YYYY-MM-DD', ...]

        返回:
            str: 下一个交易日 'YYYY-MM-DD' 或 None
        """
        current = current_date.replace('-', '')
        current_fmt = f'{current[:4]}-{current[4:6]}-{current[6:8]}'

        for d in trade_dates:
            if d > current_fmt:
                return d
        return None

    @staticmethod
    def date_to_str(date_obj):
        """日期对象转 'YYYYMMDD' 字符串"""
        if isinstance(date_obj, str):
            return date_obj.replace('-', '')
        return date_obj.strftime('%Y%m%d')

    @staticmethod
    def str_to_datefmt(date_str):
        """'YYYYMMDD' -> 'YYYY-MM-DD'"""
        d = date_str.replace('-', '')
        return f'{d[:4]}-{d[4:6]}-{d[6:8]}'
