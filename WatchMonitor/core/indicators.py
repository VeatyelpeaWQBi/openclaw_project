"""
通用技术指标模块 - 盯盘助手专用
仅保留尾盘T+1程序必需的指标函数
"""

import pandas as pd
import numpy as np


# ==================== SuperTrend 指标 ====================

def calculate_atr(df, period=14):
    """
    计算ATR (Average True Range) - 使用RMA（Wilder平滑）

    参数:
        df: DataFrame，需要包含 high, low, close 列
        period: ATR周期，默认14

    返回:
        pandas.Series: ATR值
    """
    high = df['high']
    low = df['low']
    close = df['close']

    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(close.shift() - low)

    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

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
    计算SuperTrend指标

    参数:
        df: DataFrame，需要包含 high, low, close 列
        atr_period: ATR周期，默认10
        multiplier: ATR乘数，默认3.0

    返回:
        DataFrame: 包含 supertrend(布尔值), upper_band, lower_band, atr
    """
    n = len(df)
    if n == 0:
        return pd.DataFrame({'supertrend': [], 'upper_band': [], 'lower_band': [], 'atr': []})

    high = df['high']
    low = df['low']
    close = df['close']

    atr = calculate_atr(df, atr_period)

    hl2 = (high + low) / 2
    basic_upper = hl2 + (multiplier * atr)
    basic_lower = hl2 - (multiplier * atr)

    supertrend = [True] * n
    final_upper = basic_upper.copy()
    final_lower = basic_lower.copy()

    first_valid = atr_period - 1
    if first_valid >= n:
        return pd.DataFrame({'supertrend': supertrend, 'upper_band': final_upper, 'lower_band': final_lower, 'atr': atr})

    if close.iloc[first_valid] > basic_upper.iloc[first_valid]:
        direction = -1
    elif close.iloc[first_valid] < basic_lower.iloc[first_valid]:
        direction = 1
    else:
        direction = -1

    for i in range(first_valid + 1, n):
        prev_fu = final_upper.iloc[i - 1]
        prev_fl = final_lower.iloc[i - 1]
        prev_close = close.iloc[i - 1]

        if basic_upper.iloc[i] < prev_fu or prev_close > prev_fu:
            pass
        else:
            final_upper.iloc[i] = prev_fu

        if basic_lower.iloc[i] > prev_fl or prev_close < prev_fl:
            pass
        else:
            final_lower.iloc[i] = prev_fl

        if direction == -1:
            if close.iloc[i] < final_lower.iloc[i]:
                direction = 1
        else:
            if close.iloc[i] > final_upper.iloc[i]:
                direction = -1

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

    if pd.isna(today_volume) or pd.isna(avg_volume) or avg_volume == 0:
        return 1.0

    return round(today_volume / avg_volume, 2)


# ==================== 周K转换 ====================

def get_weekly_kline(daily_df):
    """
    将日K数据转换为周K数据

    参数:
        daily_df: 日K DataFrame，需要包含 date, open, high, low, close, volume 列

    返回:
        DataFrame: 周K数据
    """
    if daily_df.empty:
        return pd.DataFrame()

    df = daily_df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    iso = df['date'].dt.isocalendar()
    df['iso_year'] = iso.year.astype(int)
    df['iso_week'] = iso.week.astype(int)

    agg_dict = {
        'date': 'last',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
    }
    if 'amount' in df.columns:
        agg_dict['amount'] = 'sum'

    weekly = df.groupby(['iso_year', 'iso_week']).agg(agg_dict).reset_index(drop=True)
    return weekly