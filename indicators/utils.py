"""
通用技术指标计算函数

提供基础的指标计算函数，用于量化交易项目
包含：SuperTrend指标、ATR、量比计算、周K转换等

使用方式：
    from indicators.utils import calculate_supertrend, calculate_atr, is_supertrend_bullish
"""

import pandas as pd
import numpy as np


# ==================== ATR ====================

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


# ==================== SuperTrend 指标 ====================

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

    # 初始化方向：1=空头(bearish), -1=多头(bullish)，与TradingView对齐
    # 根据首根有效K线的收盘价与基本轨道的关系确定初始方向
    if close.iloc[first_valid] > basic_upper.iloc[first_valid]:
        direction = -1  # 收盘高于上轨 → 多头
    elif close.iloc[first_valid] < basic_lower.iloc[first_valid]:
        direction = 1   # 收盘低于下轨 → 空头
    else:
        direction = -1  # 默认多头（与TradingView一致）

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

        # 方向判断（对齐TradingView: 用当前K线的轨）
        if direction == -1:  # 之前多头
            if close.iloc[i] < final_lower.iloc[i]:
                direction = 1  # 翻空
        else:  # 之前空头
            if close.iloc[i] > final_upper.iloc[i]:
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
    # 确保 date 列为 datetime 类型
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    # 使用 ISO year + week 组合分组，避免跨年归属错误
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