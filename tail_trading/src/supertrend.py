"""
SuperTrend指标计算模块
用于判断股票趋势方向
"""

import pandas as pd
import numpy as np

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

if __name__ == '__main__':
    # 测试
    import sys
    sys.path.append('***REMOVED***/tail_trading/src')
    from stock_data import get_stock_daily_kline
    
    print("测试SuperTrend计算...")
    df = get_stock_daily_kline('002409', market='sz', days=60)
    
    if not df.empty:
        st = calculate_supertrend(df)
        print(f"最近5天SuperTrend状态:")
        for i in range(-5, 0):
            trend = "多头" if st['supertrend'].iloc[i] else "空头"
            print(f"  {df['date'].iloc[i].strftime('%Y-%m-%d')}: {trend}")
        
        print(f"\n当前趋势: {'多头' if st['supertrend'].iloc[-1] else '空头'}")
    else:
        print("未获取到测试数据")
