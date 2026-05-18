"""
通用技术指标计算函数

提供基础的指标计算函数，用于量化交易项目
包含：SuperTrend指标、ATR、量比计算、周K转换等

使用方式：
    from indicators.utils import calculate_supertrend, calculate_atr, is_supertrend_bullish
"""

import pandas as pd
import numpy as np
from datetime import datetime


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


# ==================== 均线指标 ====================

def calculate_ma(df, periods=[5, 10, 20, 60, 120, 250]):
    """计算多周期均线（最后一天值）"""
    if df.empty or 'close' not in df.columns:
        return {}

    result = {}
    for period in periods:
        if len(df) >= period:
            ma_value = df['close'].rolling(window=period).mean().iloc[-1]
            result[f'ma{period}'] = round(ma_value, 3) if not pd.isna(ma_value) else None
        else:
            result[f'ma{period}'] = None

    return result


def calculate_ma_series(df, periods=[5, 10, 20, 60, 120, 250]):
    """计算多周期均线（返回完整序列）"""
    if df.empty or 'close' not in df.columns:
        return pd.DataFrame()

    result = pd.DataFrame(index=df.index)
    for period in periods:
        result[f'ma{period}'] = df['close'].rolling(window=period).mean()

    return result


def calculate_ma_slope(df, periods=[5, 10, 20]):
    """计算均线斜率方向 (-1=向下, 0=走平, 1=向上)"""
    if df.empty or len(df) < 2:
        return {}

    ma_series = calculate_ma_series(df, periods)
    result = {}

    for period in periods:
        col = f'ma{period}'
        if col in ma_series.columns and len(ma_series) >= 2:
            today = ma_series[col].iloc[-1]
            yesterday = ma_series[col].iloc[-2]

            if pd.isna(today) or pd.isna(yesterday):
                result[f'ma{period}_slope'] = None
            else:
                diff = today - yesterday
                threshold = today * 0.001
                if diff > threshold:
                    result[f'ma{period}_slope'] = 1
                elif diff < -threshold:
                    result[f'ma{period}_slope'] = -1
                else:
                    result[f'ma{period}_slope'] = 0

    return result


def check_ma_breakdown(df, ma_periods=[5, 10, 20, 60, 120, 250]):
    """检测均线破位信号"""
    if df.empty:
        return []

    close = df['close'].iloc[-1]
    ma_values = calculate_ma(df, ma_periods)
    signals = []

    severity_map = {5: 'low', 10: 'medium', 20: 'high', 60: 'critical', 120: 'critical', 250: 'critical'}

    for period in ma_periods:
        ma_key = f'ma{period}'
        if ma_key in ma_values and ma_values[ma_key] is not None:
            if close < ma_values[ma_key]:
                signals.append({
                    'type': 'ma_breakdown',
                    'period': period,
                    'ma_value': ma_values[ma_key],
                    'close': close,
                    'severity': severity_map.get(period, 'medium')
                })

    return signals


def check_ma_cross(df, fast=5, slow=10):
    """检测均线交叉（金叉/死叉）"""
    if df.empty or len(df) < 2:
        return None

    ma_series = calculate_ma_series(df, [fast, slow])
    fast_col = f'ma{fast}'
    slow_col = f'ma{slow}'

    today_fast = ma_series[fast_col].iloc[-1]
    today_slow = ma_series[slow_col].iloc[-1]
    yesterday_fast = ma_series[fast_col].iloc[-2]
    yesterday_slow = ma_series[slow_col].iloc[-2]

    if pd.isna(today_fast) or pd.isna(today_slow) or pd.isna(yesterday_fast) or pd.isna(yesterday_slow):
        return None

    if yesterday_fast > yesterday_slow and today_fast < today_slow:
        return {'type': 'death_cross', 'fast': fast, 'slow': slow, 'severity': 'medium'}

    if yesterday_fast < yesterday_slow and today_fast > today_slow:
        return {'type': 'golden_cross', 'fast': fast, 'slow': slow, 'severity': 'positive'}

    return None


# ==================== MACD指标 ====================

def calculate_ema(series, period):
    """计算EMA（指数移动平均）"""
    alpha = 2 / (period + 1)
    return series.ewm(alpha=alpha, adjust=False).mean()


def calculate_macd(df, fast=12, slow=26, signal=9):
    """计算MACD指标"""
    if df.empty or 'close' not in df.columns:
        return {'dif': None, 'dea': None, 'histogram': None}

    close = df['close']
    ema_fast = calculate_ema(close, fast)
    ema_slow = calculate_ema(close, slow)

    dif = ema_fast - ema_slow
    dea = calculate_ema(dif, signal)
    histogram = (dif - dea) * 2

    return {
        'dif': round(dif.iloc[-1], 4) if not pd.isna(dif.iloc[-1]) else None,
        'dea': round(dea.iloc[-1], 4) if not pd.isna(dea.iloc[-1]) else None,
        'histogram': round(histogram.iloc[-1], 4) if not pd.isna(histogram.iloc[-1]) else None,
        'dif_series': dif,
        'dea_series': dea,
        'histogram_series': histogram
    }


def calculate_macd_slope(df, threshold=0.01):
    """计算MACD三线斜率"""
    if df.empty or len(df) < 2:
        return {
            'macd_histogram_slope': 0,
            'macd_dif_slope': 0,
            'macd_dea_slope': 0,
            'macd_slope_summary': '无数据'
        }

    macd_data = calculate_macd(df)
    dif_series = macd_data.get('dif_series')
    dea_series = macd_data.get('dea_series')
    histogram_series = macd_data.get('histogram_series')

    if dif_series is None or len(dif_series) < 2:
        return {
            'macd_histogram_slope': 0,
            'macd_dif_slope': 0,
            'macd_dea_slope': 0,
            'macd_slope_summary': '无数据'
        }

    today_hist = histogram_series.iloc[-1] if histogram_series is not None else 0
    yesterday_hist = histogram_series.iloc[-2] if histogram_series is not None and len(histogram_series) >= 2 else today_hist
    hist_change = today_hist - yesterday_hist
    hist_slope = 1 if hist_change > threshold else (-1 if hist_change < -threshold else 0)

    today_dif = dif_series.iloc[-1]
    yesterday_dif = dif_series.iloc[-2]
    dif_change = today_dif - yesterday_dif
    dif_slope = 1 if dif_change > threshold else (-1 if dif_change < -threshold else 0)

    if dea_series is not None and len(dea_series) >= 2:
        today_dea = dea_series.iloc[-1]
        yesterday_dea = dea_series.iloc[-2]
        dea_change = today_dea - yesterday_dea
        dea_slope = 1 if dea_change > threshold else (-1 if dea_change < -threshold else 0)
    else:
        dea_slope = 0

    slope_summary = _get_macd_slope_summary(hist_slope, dif_slope, dea_slope)

    return {
        'macd_histogram_slope': hist_slope,
        'macd_dif_slope': dif_slope,
        'macd_dea_slope': dea_slope,
        'macd_slope_summary': slope_summary
    }


def _get_macd_slope_summary(hist_slope, dif_slope, dea_slope):
    """根据三线斜率综合判断MACD趋势状态"""
    if hist_slope == 1 and dif_slope == 1 and dea_slope == 1:
        return '🚀向上加速'
    elif hist_slope == 1 and dif_slope == 1 and dea_slope == 0:
        return '🚀向上延续'
    elif hist_slope == 1 and dif_slope == 0 and dea_slope == 0:
        return '🚀整理蓄势'
    elif hist_slope == 0 and dif_slope == 1 and dea_slope == 1:
        return '→走平中'
    elif hist_slope == 0 and dif_slope == 0 and dea_slope == 0:
        return '→无方向'
    elif hist_slope == -1 and dif_slope == -1 and dea_slope == -1:
        return '🪂向下加速'
    elif hist_slope == -1 and dif_slope == -1 and dea_slope == 0:
        return '🪂向下延续'
    elif hist_slope == -1 and dif_slope == 0 and dea_slope == 0:
        return '🪂下跌趋缓'
    elif hist_slope == 1 and dif_slope == 0 and dea_slope == -1:
        return '→震荡'
    elif hist_slope == -1 and dif_slope == 1 and dea_slope == 1:
        return '→反转中'
    else:
        return '→震荡'


def check_divergence(df, lookback=20, indicator='macd'):
    """检测顶背离/底背离"""
    if df.empty or len(df) < lookback + 1:
        return None

    close = df['close']
    recent_close = close.iloc[-lookback:]

    price_min = recent_close.min()
    price_max = recent_close.max()
    today_close = close.iloc[-1]

    if indicator == 'macd':
        macd_result = calculate_macd(df)
        if macd_result['histogram_series'] is None:
            return None
        histogram = macd_result['histogram_series'].iloc[-lookback:]
        indicator_min = histogram.min()
        indicator_max = histogram.max()
        today_indicator = histogram.iloc[-1]
    elif indicator == 'rsi':
        rsi_series = calculate_rsi_series(df, 14)
        if rsi_series is None or len(rsi_series) < lookback:
            return None
        rsi_recent = rsi_series.iloc[-lookback:]
        indicator_min = rsi_recent.min()
        indicator_max = rsi_recent.max()
        today_indicator = rsi_series.iloc[-1]
    else:
        return None

    if today_close <= price_min * 1.01:
        if today_indicator > indicator_min * 1.1:
            return {'type': 'bottom_divergence', 'indicator': indicator, 'severity': 'strong'}

    if today_close >= price_max * 0.99:
        if today_indicator < indicator_max * 0.9:
            return {'type': 'top_divergence', 'indicator': indicator, 'severity': 'high'}

    return None


# ==================== RSI指标 ====================

def calculate_rsi_series(df, period=14):
    """计算RSI序列"""
    if df.empty or 'close' not in df.columns:
        return None

    close = df['close']
    delta = close.diff()

    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)

    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


def calculate_rsi(df, period=14):
    """计算RSI（最后一天值）"""
    rsi_series = calculate_rsi_series(df, period)
    if rsi_series is None or rsi_series.empty:
        return None
    return round(rsi_series.iloc[-1], 2) if not pd.isna(rsi_series.iloc[-1]) else None


# ==================== K线形态识别 ====================

def identify_candle_patterns(df):
    """识别K线形态"""
    if df.empty or len(df) < 1:
        return {}

    open_price = df['open'].iloc[-1]
    high_price = df['high'].iloc[-1]
    low_price = df['low'].iloc[-1]
    close_price = df['close'].iloc[-1]

    body = abs(close_price - open_price)
    upper_shadow = high_price - max(open_price, close_price)
    lower_shadow = min(open_price, close_price) - low_price

    is_bullish = close_price > open_price
    is_bearish = close_price < open_price

    is_long_upper_shadow = upper_shadow > body * 2 if body > 0 else False
    is_long_lower_shadow = lower_shadow > body * 2 if body > 0 else False

    return {
        'is_bullish_candle': int(is_bullish),
        'is_bearish_candle': int(is_bearish),
        'is_long_upper_shadow': int(is_long_upper_shadow),
        'is_long_lower_shadow': int(is_long_lower_shadow),
        'body': body,
        'upper_shadow': upper_shadow,
        'lower_shadow': lower_shadow
    }


def check_volume_stagnation(df, threshold_ratio=1.5, threshold_change=1.0):
    """检测放量滞涨"""
    if df.empty or 'change_pct' not in df.columns:
        return None

    volume_ratio = calculate_volume_ratio(df, 5)
    change_pct = df['change_pct'].iloc[-1] if 'change_pct' in df.columns else 0

    if volume_ratio > threshold_ratio and change_pct < threshold_change:
        return {
            'type': 'volume_stagnation',
            'volume_ratio': volume_ratio,
            'change_pct': change_pct,
            'severity': 'medium'
        }

    return None


def check_high_long_upper_shadow(df, lookback=20, min_shadow_pct=3.0):
    """检测高位长上影线"""
    if df.empty or len(df) < lookback:
        return None

    open_price = df['open'].iloc[-1]
    high_price = df['high'].iloc[-1]
    close_price = df['close'].iloc[-1]

    is_bullish = close_price > open_price
    is_bearish = close_price < open_price

    body = abs(close_price - open_price)

    if is_bullish:
        upper_shadow = high_price - close_price
    else:
        upper_shadow = high_price - open_price

    shadow_pct = (upper_shadow / max(close_price, open_price)) * 100 if close_price > 0 else 0
    if shadow_pct < min_shadow_pct:
        return None

    if is_bearish:
        is_long_upper_shadow = upper_shadow >= body
    else:
        is_long_upper_shadow = upper_shadow >= body * 2

    if not is_long_upper_shadow:
        return None

    recent_high = df['high'].iloc[-lookback:].max()

    if close_price >= recent_high * 0.90:
        severity = 'high' if is_bearish else 'medium'
        return {
            'type': 'high_long_upper_shadow',
            'is_bearish': is_bearish,
            'close': close_price,
            'open': open_price,
            'recent_high': recent_high,
            'upper_shadow': upper_shadow,
            'shadow_pct': round(shadow_pct, 2),
            'severity': severity
        }

    return None


def check_breakdown_big_bull_candle(df, lookback=20, min_change_pct=5.0):
    """检测跌破新高大阳线底部（5%以上为大阳线）"""
    if df.empty or len(df) < lookback + 1:
        return None

    recent_data = df.iloc[-lookback:]
    new_high_idx = recent_data['high'].idxmax()
    new_high_price = recent_data['high'].max()

    if pd.isna(new_high_idx):
        return None

    for i in range(max(0, new_high_idx - 5), min(len(df), new_high_idx + 3)):
        row = df.iloc[i]
        open_price = row['open']
        close_price = row['close']
        low_price = row['low']

        is_bullish = close_price > open_price
        change_pct = (close_price - open_price) / open_price * 100 if open_price > 0 else 0
        near_new_high = row['high'] >= new_high_price * 0.95

        if is_bullish and change_pct >= min_change_pct and near_new_high:
            big_bull_open = open_price
            big_bull_date = str(row['date'])[:10] if 'date' in df.columns else None
            big_bull_change = change_pct

            current_close = df['close'].iloc[-1]

            if current_close < big_bull_open:
                return {
                    'type': 'breakdown_big_bull_candle',
                    'severity': 'high',
                    'message': f'跌破新高大阳线开盘价（大阳线涨幅{big_bull_change:.1f}%，开盘{big_bull_open:.2f}）',
                    'big_bull_open': big_bull_open,
                    'big_bull_date': big_bull_date,
                    'big_bull_change': big_bull_change,
                    'current_close': current_close,
                    'new_high': new_high_price
                }

    return None


def check_breakdown_medium_bull_candle(df, lookback=20, min_change_pct=2.5, max_change_pct=5.0):
    """检测跌破中阳线开盘价（2.5%~5%为中阳线）"""
    if df.empty or len(df) < lookback + 1:
        return None

    recent_data = df.iloc[-lookback:]

    for i in range(len(recent_data)):
        idx = len(df) - lookback + i
        row = df.iloc[idx]
        open_price = row['open']
        close_price = row['close']

        is_bullish = close_price > open_price
        change_pct = (close_price - open_price) / open_price * 100 if open_price > 0 else 0

        if is_bullish and min_change_pct <= change_pct < max_change_pct:
            medium_bull_open = open_price
            medium_bull_date = str(row['date'])[:10] if 'date' in df.columns else None
            medium_bull_change = change_pct

            current_close = df['close'].iloc[-1]

            if current_close < medium_bull_open:
                return {
                    'type': 'breakdown_medium_bull_candle',
                    'severity': 'medium',
                    'message': f'跌破中阳线开盘价（中阳线涨幅{medium_bull_change:.1f}%，开盘{medium_bull_open:.2f}，日期{medium_bull_date}）',
                    'medium_bull_open': medium_bull_open,
                    'medium_bull_date': medium_bull_date,
                    'medium_bull_change': medium_bull_change,
                    'current_close': current_close
                }

    return None


# ==================== SuperTrend翻空检测 ====================

def check_supertrend_flip(df, atr_period=90, multiplier=3.0):
    """检测SuperTrend翻空/翻多"""
    if df.empty or len(df) < atr_period + 1:
        return None

    st = calculate_supertrend(df, atr_period, multiplier)

    if len(st) < 2:
        return None

    today_direction = st['supertrend'].iloc[-1]
    yesterday_direction = st['supertrend'].iloc[-2]

    if yesterday_direction and not today_direction:
        return {
            'type': 'flip_to_bear',
            'upper_band': st['upper_band'].iloc[-1],
            'severity': 'high'
        }

    if not yesterday_direction and today_direction:
        return {
            'type': 'flip_to_bull',
            'lower_band': st['lower_band'].iloc[-1],
            'severity': 'positive'
        }

    return None


def check_weekly_supertrend_flip(daily_df, atr_period=20, multiplier=3.0):
    """检测周线SuperTrend翻空/翻多"""
    weekly_df = get_weekly_kline(daily_df)
    if weekly_df.empty or len(weekly_df) < atr_period + 1:
        return None

    return check_supertrend_flip(weekly_df, atr_period, multiplier)


# ==================== 综合指标计算 ====================

def calculate_all_indicators(df):
    """计算所有技术指标（用于存储到technical_indicators表）"""
    if df.empty:
        return {}

    result = {
        'calc_date': str(df['date'].iloc[-1])[:10] if 'date' in df.columns else None,
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    ma_values = calculate_ma(df, [5, 10, 20, 60, 120, 250])
    result.update(ma_values)

    ma_slopes = calculate_ma_slope(df, [5, 10, 20])
    result.update(ma_slopes)

    st = calculate_supertrend(df, atr_period=90, multiplier=3.0)
    if not st.empty and len(st) > 0:
        result['st_upper_band'] = round(st['upper_band'].iloc[-1], 3) if not pd.isna(st['upper_band'].iloc[-1]) else None
        result['st_lower_band'] = round(st['lower_band'].iloc[-1], 3) if not pd.isna(st['lower_band'].iloc[-1]) else None
        result['st_direction'] = 1 if st['supertrend'].iloc[-1] else -1
        result['st_atr'] = round(st['atr'].iloc[-1], 3) if not pd.isna(st['atr'].iloc[-1]) else None

    macd_result = calculate_macd(df)
    result['macd_dif'] = macd_result.get('dif')
    result['macd_dea'] = macd_result.get('dea')
    result['macd_histogram'] = macd_result.get('histogram')

    macd_slope = calculate_macd_slope(df)
    result['macd_histogram_slope'] = macd_slope.get('macd_histogram_slope', 0)
    result['macd_dif_slope'] = macd_slope.get('macd_dif_slope', 0)
    result['macd_dea_slope'] = macd_slope.get('macd_dea_slope', 0)
    result['macd_slope_summary'] = macd_slope.get('macd_slope_summary', '→震荡')

    result['rsi_14'] = calculate_rsi(df, 14)

    result['volume_ratio_5'] = calculate_volume_ratio(df, 5)
    result['volume_ratio_20'] = calculate_volume_ratio(df, 20)

    patterns = identify_candle_patterns(df)
    result['is_long_upper_shadow'] = patterns.get('is_long_upper_shadow', 0)
    result['is_long_lower_shadow'] = patterns.get('is_long_lower_shadow', 0)
    result['is_bullish_candle'] = patterns.get('is_bullish_candle', 0)
    result['is_bearish_candle'] = patterns.get('is_bearish_candle', 0)

    obv_values = [0]
    close = df['close'].values
    volume = df['volume'].values
    for i in range(1, len(close)):
        prev_obv = obv_values[-1]
        if close[i] > close[i-1]:
            obv_values.append(prev_obv + volume[i])
        elif close[i] < close[i-1]:
            obv_values.append(prev_obv - volume[i])
        else:
            obv_values.append(prev_obv)

    result['obv'] = int(obv_values[-1])

    if len(obv_values) >= 30:
        result['ma_obv'] = round(pd.Series(obv_values).rolling(30).mean().iloc[-1], 2)
    else:
        result['ma_obv'] = None

    return result
