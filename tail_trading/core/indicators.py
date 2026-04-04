"""
通用技术指标模块
包含：SuperTrend指标、量比计算、周K转换等通用指标函数
"""

import pandas as pd
import numpy as np


# ==================== SuperTrend 指标 ====================

def calculate_atr(df, period=14):
    """
    计算ATR (Average True Range)

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

    # 使用EMA计算ATR
    atr = true_range.ewm(alpha=1/period, min_periods=period).mean()

    return atr


def calculate_supertrend(df, atr_period=10, multiplier=3.0):
    """
    计算SuperTrend指标

    参数:
        df: DataFrame，需要包含 high, low, close 列
        atr_period: ATR周期，默认10
        multiplier: ATR乘数，默认3.0

    返回:
        DataFrame: 包含 supertrend(布尔值，True=多头), upper_band, lower_band
    """
    high = df['high']
    low = df['low']
    close = df['close']

    # 计算ATR
    atr = calculate_atr(df, atr_period)

    # 计算基础上下轨
    hl2 = (high + low) / 2
    upper_band = hl2 + (multiplier * atr)
    lower_band = hl2 - (multiplier * atr)

    # 初始化SuperTrend
    supertrend = [True] * len(df)  # True=多头, False=空头
    final_upper = upper_band.copy()
    final_lower = lower_band.copy()

    for i in range(1, len(df)):
        # 调整上轨
        if final_upper.iloc[i] < final_upper.iloc[i-1] or close.iloc[i-1] > final_upper.iloc[i-1]:
            pass  # 保持原值
        else:
            final_upper.iloc[i] = final_upper.iloc[i-1]

        # 调整下轨
        if final_lower.iloc[i] > final_lower.iloc[i-1] or close.iloc[i-1] < final_lower.iloc[i-1]:
            pass  # 保持原值
        else:
            final_lower.iloc[i] = final_lower.iloc[i-1]

        # 判断趋势
        if close.iloc[i] > final_upper.iloc[i-1]:
            supertrend[i] = True  # 多头
        elif close.iloc[i] < final_lower.iloc[i-1]:
            supertrend[i] = False  # 空头
        else:
            supertrend[i] = supertrend[i-1]  # 延续之前趋势

        # 根据趋势方向设置显示的轨道
        if supertrend[i]:
            final_upper.iloc[i] = np.nan  # 多头时不显示上轨
        else:
            final_lower.iloc[i] = np.nan  # 空头时不显示下轨

    return pd.DataFrame({
        'supertrend': supertrend,
        'upper_band': final_upper,
        'lower_band': final_lower,
        'atr': atr
    }, index=df.index)


def is_supertrend_bullish(df, atr_period=10, multiplier=3.0):
    """
    判断SuperTrend是否为多头趋势

    参数:
        df: DataFrame，日K数据
        atr_period: ATR周期
        multiplier: 乘数

    返回:
        bool: True=多头趋势, False=空头趋势
    """
    if len(df) < atr_period + 5:
        return False

    st = calculate_supertrend(df, atr_period, multiplier)
    return st['supertrend'].iloc[-1]


def check_multi_timeframe_supertrend(df_daily, df_weekly, atr_period=10, multiplier=3.0):
    """
    检查日线和周线是否都处于SuperTrend多头趋势

    参数:
        df_daily: 日线数据
        df_weekly: 周线数据
        atr_period: ATR周期
        multiplier: 乘数

    返回:
        tuple: (日线多头, 周线多头)
    """
    daily_bullish = is_supertrend_bullish(df_daily, atr_period, multiplier) if not df_daily.empty else False
    weekly_bullish = is_supertrend_bullish(df_weekly, atr_period, multiplier) if not df_weekly.empty else False

    return daily_bullish, weekly_bullish


# ==================== 通用辅助指标 ====================

def get_weekly_kline(daily_df):
    """
    将日K数据转换为周K数据

    参数:
        daily_df: 日K DataFrame

    返回:
        DataFrame: 周K数据
    """
    if daily_df.empty:
        return pd.DataFrame()

    df = daily_df.copy()
    df['week'] = df['date'].dt.isocalendar().week
    df['year'] = df['date'].dt.year

    weekly = df.groupby(['year', 'week']).agg({
        'date': 'last',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'amount': 'sum'
    }).reset_index(drop=True)

    return weekly


def calculate_volume_ratio(df, period=5):
    """
    计算量比（当日成交量/5日均量）

    参数:
        df: 日K数据
        period: 均量周期

    返回:
        float: 量比
    """
    if len(df) < period + 1:
        return 0

    avg_vol = df['volume'].iloc[-period-1:-1].mean()
    today_vol = df['volume'].iloc[-1]

    if avg_vol > 0:
        return today_vol / avg_vol
    return 0
