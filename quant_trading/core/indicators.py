"""
通用技术指标模块
包含：SuperTrend指标、量比计算、周K转换等通用指标函数
"""

import pandas as pd
import numpy as np


# ==================== SuperTrend 指标 ====================

def calculate_atr(df, period=14):
    """
    计算ATR (Average True Range) - 使用RMA（Wilder平滑），与TradingView一致

    参数:
        df: DataFrame，需要包含 high, low, close 列
        period: ATR周期，默认14

    返回:
        pandas.Series: ATR值
    """
    high = df['high']
    low = df['low']
    close = df['close']

    # True Range
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(close.shift() - low)

    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # 使用RMA计算ATR（与TradingView ta.atr一致）
    # RMA: RMA[i] = (RMA[i-1] * (n-1) + TR[i]) / n，初始值 = 前n个TR的均值
    atr = pd.Series(np.nan, index=true_range.index, dtype=float)
    if len(true_range) >= period:
        rma = true_range.iloc[:period].mean()
        atr.iloc[period - 1] = rma
        for i in range(period, len(true_range)):
            rma = (rma * (period - 1) + true_range.iloc[i]) / period
            atr.iloc[i] = rma

    return atr


def calculate_supertrend(df, atr_period=10, multiplier=3.0):
    """
    计算SuperTrend指标（对齐TradingView ta.supertrend）

    参数:
        df: DataFrame，需要包含 high, low, close 列
        atr_period: ATR周期，默认10
        multiplier: ATR乘数，默认3.0

    返回:
        DataFrame: 包含 supertrend(布尔值，True=多头), upper_band, lower_band
    """
    n = len(df)
    if n == 0:
        return pd.DataFrame({'supertrend': [], 'upper_band': [], 'lower_band': [], 'atr': []})

    high = df['high']
    low = df['low']
    close = df['close']

    # 计算ATR
    atr = calculate_atr(df, atr_period)

    # 计算基础上下轨
    hl2 = (high + low) / 2
    basic_upper = hl2 + (multiplier * atr)
    basic_lower = hl2 - (multiplier * atr)

    # 初始化
    supertrend = [True] * n  # True=多头, False=空头
    final_upper = basic_upper.copy()
    final_lower = basic_lower.copy()

    # 从第一个有效ATR索引开始循环
    first_valid = atr_period - 1
    if first_valid >= n:
        return pd.DataFrame({'supertrend': supertrend, 'upper_band': final_upper, 'lower_band': final_lower, 'atr': atr})

    # 初始化第一根有效K线的方向（默认多头）
    direction = -1  # -1=多头(bullish), 1=空头(bearish)

    for i in range(first_valid + 1, n):
        # 调整上轨（对齐TradingView: 用prev_close vs prev_final_upper）
        prev_fu = final_upper.iloc[i - 1]
        prev_fl = final_lower.iloc[i - 1]
        prev_close = close.iloc[i - 1]

        if basic_upper.iloc[i] < prev_fu or prev_close > prev_fu:
            pass  # 保持 basic_upper 原值
        else:
            final_upper.iloc[i] = prev_fu

        # 调整下轨
        if basic_lower.iloc[i] > prev_fl or prev_close < prev_fl:
            pass  # 保持 basic_lower 原值
        else:
            final_lower.iloc[i] = prev_fl

        # 方向判断（对齐TradingView: 用prev方向决定检查哪条轨）
        if direction == -1:  # 之前多头
            if close.iloc[i] < final_lower.iloc[i - 1]:
                direction = 1  # 翻空
        else:  # 之前空头
            if close.iloc[i] > final_upper.iloc[i - 1]:
                direction = -1  # 翻多

        supertrend[i] = (direction == -1)

    return pd.DataFrame({
        'supertrend': supertrend,
        'upper_band': final_upper,
        'lower_band': final_lower,
        'atr': atr
    })


def is_supertrend_bullish(df, atr_period=10, multiplier=3.0):
    """
    判断SuperTrend是否为多头趋势

    参数:
        df: DataFrame，需要包含 high, low, close 列
        atr_period: ATR周期，默认10
        multiplier: ATR乘数，默认3.0

    返回:
        bool: True=多头趋势, False=空头趋势
    """
    if df.empty or len(df) < atr_period:
        return False

    st = calculate_supertrend(df, atr_period, multiplier)
    return st['supertrend'].iloc[-1]


def check_multi_timeframe_supertrend(df_daily, df_weekly, atr_period=10, multiplier=3.0):
    """
    检查日线和周线是否都处于SuperTrend多头趋势

    参数:
        df_daily: 日K DataFrame
        df_weekly: 周K DataFrame
        atr_period: ATR周期，默认10
        multiplier: ATR乘数，默认3.0

    返回:
        tuple: (daily_bullish, weekly_bullish, both_bullish)
    """
    daily_bullish = is_supertrend_bullish(df_daily, atr_period, multiplier) if not df_daily.empty else False
    weekly_bullish = is_supertrend_bullish(df_weekly, atr_period, multiplier) if not df_weekly.empty else False
    return daily_bullish, weekly_bullish, daily_bullish and weekly_bullish


def get_supertrend_at_date(stock_code, target_date, lookback_days=60, atr_period=10, multiplier=3.0, db_path=None):
    """
    查询指定日期的SuperTrend多空趋势

    基于交易日历回溯指定天数的交易日，查询日K数据后计算SuperTrend，
    返回目标日期的趋势状态。

    参数:
        stock_code: 股票代码，如 '002261'
        target_date: 目标日期，如 '2026-01-12'
        lookback_days: 回溯交易日天数，默认60
        atr_period: ATR周期，默认10
        multiplier: ATR乘数，默认3.0
        db_path: 数据库路径，默认使用项目配置

    返回:
        dict: {
            'stock_code': str,
            'target_date': str,
            'is_bullish': bool,  # True=多头, False=空头
            'close': float,     # 目标日期收盘价
            'upper_band': float, # 上轨值
            'lower_band': float, # 下轨值
            'valid': bool       # 数据是否有效
        }
    """
    from core.paths import DB_PATH
    import sqlite3

    if db_path is None:
        db_path = DB_PATH

    conn = sqlite3.connect(db_path)

    # 1. 查询交易日历，找到target_date及往前lookback_days个交易日
    cursor = conn.execute(
        "SELECT trade_date FROM trade_calendar WHERE trade_status=1 AND trade_date <= ? ORDER BY trade_date DESC LIMIT ?",
        (target_date, lookback_days + 1)
    )
    trade_dates = [row[0] for row in cursor.fetchall()]
    trade_dates.reverse()  # 升序

    if len(trade_dates) < atr_period + 1:
        conn.close()
        return {
            'stock_code': stock_code,
            'target_date': target_date,
            'is_bullish': False,
            'close': 0.0,
            'upper_band': 0.0,
            'lower_band': 0.0,
            'valid': False
        }

    start_date = trade_dates[0]
    end_date = trade_dates[-1]

    # 2. 查询该区间内的日K数据
    df = pd.read_sql(
        f'SELECT date, open, high, low, close, volume FROM daily_kline WHERE code=? AND date >= ? AND date <= ? ORDER BY date ASC',
        conn, params=(stock_code, start_date, end_date)
    )
    conn.close()

    if df.empty or len(df) < atr_period + 1:
        return {
            'stock_code': stock_code,
            'target_date': target_date,
            'is_bullish': False,
            'close': 0.0,
            'upper_band': 0.0,
            'lower_band': 0.0,
            'valid': False
        }

    df = df.set_index('date')

    # 3. 计算SuperTrend
    st = calculate_supertrend(df, atr_period, multiplier)

    # 4. 找到目标日期的结果
    if target_date not in st.index:
        return {
            'stock_code': stock_code,
            'target_date': target_date,
            'is_bullish': False,
            'close': 0.0,
            'upper_band': 0.0,
            'lower_band': 0.0,
            'valid': False
        }

    idx = st.index.get_loc(target_date)
    return {
        'stock_code': stock_code,
        'target_date': target_date,
        'is_bullish': bool(st['supertrend'].iloc[idx]),
        'close': float(df['close'].iloc[idx]),
        'upper_band': float(st['upper_band'].iloc[idx]) if pd.notna(st['upper_band'].iloc[idx]) else 0.0,
        'lower_band': float(st['lower_band'].iloc[idx]) if pd.notna(st['lower_band'].iloc[idx]) else 0.0,
        'valid': True
    }


# ==================== 量比计算 ====================

def calculate_volume_ratio(df, days=5):
    """
    计算量比（当日成交量 / 近N日平均成交量）

    参数:
        df: DataFrame，需要包含 volume 列
        days: 近N日均量周期，默认5

    返回:
        float: 量比值
    """
    if len(df) < days + 1:
        return 1.0

    today_volume = df['volume'].iloc[-1]
    avg_volume = df['volume'].iloc[-(days + 1):-1].mean()

    if avg_volume == 0:
        return 1.0

    return round(today_volume / avg_volume, 2)


# ==================== 周K转换 ====================

def daily_to_weekly(df):
    """
    将日K数据转换为周K数据

    参数:
        df: DataFrame，需要包含 date, open, high, low, close, volume 列

    返回:
        DataFrame: 周K数据
    """
    if df.empty:
        return df

    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')

    weekly = df.resample('W').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()

    weekly = weekly.reset_index()
    return weekly
