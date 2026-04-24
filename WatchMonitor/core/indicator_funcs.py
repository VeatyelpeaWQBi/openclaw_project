"""
通用技术指标模块 - 盯盘助手专用
支持持仓池/候选池信号检测所需的技术指标计算
"""

import pandas as pd
import numpy as np
from datetime import datetime


# ==================== 均线指标 ====================

def calculate_ma(df, periods=[5, 10, 20, 60, 120, 250]):
    """
    计算多周期均线

    参数:
        df: DataFrame，需要包含 close 列
        periods: 均线周期列表，默认 [5, 10, 20, 60, 120, 250]

    返回:
        dict: 各周期均线值（最后一天）
    """
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
    """
    计算多周期均线（返回完整序列）

    参数:
        df: DataFrame，需要包含 close 列
        periods: 均线周期列表

    返回:
        DataFrame: 各周期均线序列
    """
    if df.empty or 'close' not in df.columns:
        return pd.DataFrame()

    result = pd.DataFrame(index=df.index)
    for period in periods:
        result[f'ma{period}'] = df['close'].rolling(window=period).mean()

    return result


def calculate_ma_slope(df, periods=[5, 10, 20]):
    """
    计算均线斜率方向

    参数:
        df: DataFrame，需要包含 close 列
        periods: 需计算斜率的周期列表

    返回:
        dict: 各周期均线斜率方向 (-1=向下, 0=走平, 1=向上)
    """
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
                threshold = today * 0.001  # 0.1%变化视为走平
                if diff > threshold:
                    result[f'ma{period}_slope'] = 1  # 向上
                elif diff < -threshold:
                    result[f'ma{period}_slope'] = -1  # 向下
                else:
                    result[f'ma{period}_slope'] = 0  # 走平

    return result


def check_ma_breakdown(df, ma_periods=[5, 10, 20, 60, 120, 250]):
    """
    检测均线破位信号

    参数:
        df: DataFrame，需要包含 close 列
        ma_periods: 检测的均线周期列表

    返回:
        list: 破位信号列表 [{period, severity}]
    """
    if df.empty:
        return []

    close = df['close'].iloc[-1]
    ma_values = calculate_ma(df, ma_periods)
    signals = []

    # 严重度映射
    severity_map = {
        5: 'low',      # 短线关注
        10: 'medium',  # 波段关注
        20: 'high',    # 趋势关注
        60: 'critical', # 重要支撑
        120: 'critical', # 重要支撑
        250: 'critical'  # 年线支撑
    }

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
    """
    检测均线交叉（金叉/死叉）

    参数:
        df: DataFrame
        fast: 快线周期
        slow: 慢线周期

    返回:
        dict: {'type': 'golden_cross'|'death_cross', 'fast': fast, 'slow': slow} 或 None
    """
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

    # 死叉：快线从上方穿越慢线下方
    if yesterday_fast > yesterday_slow and today_fast < today_slow:
        return {'type': 'death_cross', 'fast': fast, 'slow': slow, 'severity': 'medium'}

    # 金叉：快线从下方穿越慢线上方
    if yesterday_fast < yesterday_slow and today_fast > today_slow:
        return {'type': 'golden_cross', 'fast': fast, 'slow': slow, 'severity': 'positive'}

    return None


# ==================== MACD指标 ====================

def calculate_ema(series, period):
    """
    计算EMA（指数移动平均）

    参数:
        series: 价格序列
        period: 周期

    返回:
        pandas.Series: EMA序列
    """
    alpha = 2 / (period + 1)
    return series.ewm(alpha=alpha, adjust=False).mean()


def calculate_macd(df, fast=12, slow=26, signal=9):
    """
    计算MACD指标

    参数:
        df: DataFrame，需要包含 close 列
        fast: 快线周期，默认12
        slow: 慢线周期，默认26
        signal: 信号线周期，默认9

    返回:
        dict: {'dif', 'dea', 'histogram', 'dif_series', 'dea_series', 'histogram_series'}
    """
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
    """
    计算MACD三线斜率（柱状图、DIF、DEA）

    参数:
        df: DataFrame，需包含MACD数据
        threshold: 判断趋平的阈值，默认0.01

    返回:
        dict: {
            'histogram_slope': int (1=向上, 0=趋平, -1=向下),
            'dif_slope': int,
            'dea_slope': int,
            'slope_summary': str (综合判断文案)
        }
    """
    if df.empty or len(df) < 2:
        return {
            'histogram_slope': 0,
            'dif_slope': 0,
            'dea_slope': 0,
            'slope_summary': '无数据'
        }

    # 先计算MACD数据
    macd_data = calculate_macd(df)
    dif_series = macd_data.get('dif_series')
    dea_series = macd_data.get('dea_series')
    histogram_series = macd_data.get('histogram_series')

    if dif_series is None or len(dif_series) < 2:
        return {
            'histogram_slope': 0,
            'dif_slope': 0,
            'dea_slope': 0,
            'slope_summary': '无数据'
        }

    # 计算三线斜率
    # 1. MACD柱状图斜率
    today_hist = histogram_series.iloc[-1] if histogram_series is not None else 0
    yesterday_hist = histogram_series.iloc[-2] if histogram_series is not None and len(histogram_series) >= 2 else today_hist
    hist_change = today_hist - yesterday_hist
    hist_slope = 1 if hist_change > threshold else (-1 if hist_change < -threshold else 0)

    # 2. DIF线斜率
    today_dif = dif_series.iloc[-1]
    yesterday_dif = dif_series.iloc[-2]
    dif_change = today_dif - yesterday_dif
    dif_slope = 1 if dif_change > threshold else (-1 if dif_change < -threshold else 0)

    # 3. DEA线斜率
    if dea_series is not None and len(dea_series) >= 2:
        today_dea = dea_series.iloc[-1]
        yesterday_dea = dea_series.iloc[-2]
        dea_change = today_dea - yesterday_dea
        dea_slope = 1 if dea_change > threshold else (-1 if dea_change < -threshold else 0)
    else:
        dea_slope = 0

    # 综合判断
    slope_summary = _get_macd_slope_summary(hist_slope, dif_slope, dea_slope)

    return {
        'histogram_slope': hist_slope,
        'dif_slope': dif_slope,
        'dea_slope': dea_slope,
        'slope_summary': slope_summary
    }


def _get_macd_slope_summary(hist_slope, dif_slope, dea_slope):
    """
    根据三线斜率综合判断MACD趋势状态

    参数:
        hist_slope: 柱状图斜率 (1=向上, 0=趋平, -1=向下)
        dif_slope: DIF线斜率
        dea_slope: DEA线斜率

    返回:
        str: 综合判断文案
    """
    # 综合判断规则表
    if hist_slope == 1 and dif_slope == 1 and dea_slope == 1:
        return '⬆向上加速'
    elif hist_slope == 1 and dif_slope == 1 and dea_slope == 0:
        return '⬆向上延续'
    elif hist_slope == 1 and dif_slope == 0 and dea_slope == 0:
        return '⬆整理蓄势'
    elif hist_slope == 0 and dif_slope == 1 and dea_slope == 1:
        return '→走平中'
    elif hist_slope == 0 and dif_slope == 0 and dea_slope == 0:
        return '→无方向'
    elif hist_slope == -1 and dif_slope == -1 and dea_slope == -1:
        return '⬇向下加速'
    elif hist_slope == -1 and dif_slope == -1 and dea_slope == 0:
        return '⬇向下延续'
    elif hist_slope == -1 and dif_slope == 0 and dea_slope == 0:
        return '⬇下跌趋缓'
    elif hist_slope == -1 and dif_slope == -1 and dea_slope == -1:
        return '⬇向下加速'
    elif hist_slope == 1 and dif_slope == 0 and dea_slope == -1:
        return '→震荡'
    elif hist_slope == -1 and dif_slope == 1 and dea_slope == 1:
        return '→反转中'
    else:
        return '→震荡'


def check_divergence(df, lookback=20, indicator='macd'):
    """
    检测顶背离/底背离

    参数:
        df: DataFrame
        lookback: 回看天数，默认20
        indicator: 指标类型，'macd' 或 'rsi'

    返回:
        dict: {'type': 'top_divergence'|'bottom_divergence'} 或 None
    """
    if df.empty or len(df) < lookback + 1:
        return None

    close = df['close']
    recent_close = close.iloc[-lookback:]

    # 价格极值
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

    # 底背离：价格创新低，但指标未创新低
    if today_close <= price_min * 1.01:  # 接近新低（容忍1%误差）
        if today_indicator > indicator_min * 1.1:  # 指标明显高于之前低点
            return {'type': 'bottom_divergence', 'indicator': indicator, 'severity': 'strong'}

    # 顶背离：价格创新高，但指标未创新高
    if today_close >= price_max * 0.99:  # 接近新高（容忍1%误差）
        if today_indicator < indicator_max * 0.9:  # 指标明显低于之前高点
            return {'type': 'top_divergence', 'indicator': indicator, 'severity': 'high'}

    return None


# ==================== RSI指标 ====================

def calculate_rsi_series(df, period=14):
    """
    计算RSI序列

    参数:
        df: DataFrame，需要包含 close 列
        period: 周期，默认14

    返回:
        pandas.Series: RSI序列
    """
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
    """
    计算RSI（最后一天值）

    参数:
        df: DataFrame
        period: 周期

    返回:
        float: RSI值
    """
    rsi_series = calculate_rsi_series(df, period)
    if rsi_series is None or rsi_series.empty:
        return None
    return round(rsi_series.iloc[-1], 2) if not pd.isna(rsi_series.iloc[-1]) else None


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


# ==================== K线形态识别 ====================

def identify_candle_patterns(df):
    """
    识别K线形态

    参数:
        df: DataFrame，需要包含 open, high, low, close 列

    返回:
        dict: K线形态识别结果
    """
    if df.empty or len(df) < 1:
        return {}

    open_price = df['open'].iloc[-1]
    high_price = df['high'].iloc[-1]
    low_price = df['low'].iloc[-1]
    close_price = df['close'].iloc[-1]

    body = abs(close_price - open_price)  # 实体
    upper_shadow = high_price - max(open_price, close_price)  # 上影线
    lower_shadow = min(open_price, close_price) - low_price  # 下影线

    is_bullish = close_price > open_price  # 阳线
    is_bearish = close_price < open_price  # 阴线

    # 长上影线：上影线 > 实体×2
    is_long_upper_shadow = upper_shadow > body * 2 if body > 0 else False

    # 长下影线：下影线 > 实体×2
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
    """
    检测放量滞涨

    参数:
        df: DataFrame
        threshold_ratio: 量比阈值，默认1.5
        threshold_change: 涨跌幅阈值，默认1%

    返回:
        dict 或 None
    """
    if df.empty or 'change_pct' not in df.columns:
        return None

    volume_ratio = calculate_volume_ratio(df, 5)
    change_pct = df['change_pct'].iloc[-1] if 'change_pct' in df.columns else 0

    # 放量滞涨：量比>1.5 且 涨幅<1%
    if volume_ratio > threshold_ratio and change_pct < threshold_change:
        return {
            'type': 'volume_stagnation',
            'volume_ratio': volume_ratio,
            'change_pct': change_pct,
            'severity': 'medium'
        }

    return None


def check_high_long_upper_shadow(df, lookback=20, min_shadow_pct=3.0):
    """
    检测高位长上影线

    区分阴线和阳线：
    - 阴线长上影线：开盘后冲高再下跌，更强的见顶信号
    - 阳线长上影线：盘中冲高回落，仍收涨，次强见顶信号

    参数:
        df: DataFrame
        lookback: 回看天数
        min_shadow_pct: 上影线最小长度占收盘价的百分比，默认3.0%

    返回:
        dict 或 None
    """
    if df.empty or len(df) < lookback:
        return None

    open_price = df['open'].iloc[-1]
    high_price = df['high'].iloc[-1]
    close_price = df['close'].iloc[-1]

    # 判断阴阳线
    is_bullish = close_price > open_price  # 阳线
    is_bearish = close_price < open_price  # 阴线

    # 计算实体和上影线
    body = abs(close_price - open_price)

    # 上影线：最高价到实体顶部的距离
    # 阳线时：实体顶部是收盘价，上影线 = high - close
    # 阴线时：实体顶部是开盘价，上影线 = high - open
    if is_bullish:
        upper_shadow = high_price - close_price
    else:
        upper_shadow = high_price - open_price

    # 上影线长度判断（绝对长度）
    shadow_pct = (upper_shadow / max(close_price, open_price)) * 100 if close_price > 0 else 0
    if shadow_pct < min_shadow_pct:
        return None

    # 长上影线判断（相对实体）
    # 阴线：上影线 >= 实体长度（冲高幅度 >= 下跌幅度）
    # 阳线：上影线 >= 实体×2（回落幅度 >= 实体涨幅×2）
    if is_bearish:
        is_long_upper_shadow = upper_shadow >= body
    else:
        is_long_upper_shadow = upper_shadow >= body * 2

    if not is_long_upper_shadow:
        return None

    # 检查是否在高位（接近20日最高点）
    recent_high = df['high'].iloc[-lookback:].max()

    # 高位判断：收盘价接近20日最高价的90%以上
    if close_price >= recent_high * 0.90:
        # 阴线长上影线信号更强
        severity = 'high' if is_bearish else 'medium'

        return {
            'type': 'high_long_upper_shadow',
            'is_bearish': is_bearish,  # 是否阴线
            'close': close_price,
            'open': open_price,
            'recent_high': recent_high,
            'upper_shadow': upper_shadow,
            'shadow_pct': round(shadow_pct, 2),
            'severity': severity
        }

    return None


def check_breakdown_big_bull_candle(df, lookback=20, min_change_pct=5.0):
    """
    检测跌破新高大阳线底部（5%以上为大阳线）

    参数:
        df: DataFrame
        lookback: 回看天数，默认20
        min_change_pct: 大阳线涨幅阈值，默认5.0%

    返回:
        dict 或 None
    """
    if df.empty or len(df) < lookback + 1:
        return None

    # 找近期新高点
    recent_data = df.iloc[-lookback:]
    new_high_idx = recent_data['high'].idxmax()
    new_high_price = recent_data['high'].max()

    if pd.isna(new_high_idx):
        return None

    # 找新高位置的大阳线
    # 从新高点往前找，找到涨幅超过阈值(5%)的阳线
    for i in range(max(0, new_high_idx - 5), min(len(df), new_high_idx + 3)):
        row = df.iloc[i]
        open_price = row['open']
        close_price = row['close']
        low_price = row['low']
        
        # 判断是否为阳线且涨幅超过阈值
        is_bullish = close_price > open_price
        change_pct = (close_price - open_price) / open_price * 100 if open_price > 0 else 0
        
        # 判断是否接近新高（最高价接近20日最高点）
        near_new_high = row['high'] >= new_high_price * 0.95
        
        if is_bullish and change_pct >= min_change_pct and near_new_high:
            # 找到了新高位置的大阳线
            big_bull_open = open_price
            big_bull_date = str(row['date'])[:10] if 'date' in df.columns else None
            big_bull_change = change_pct
            
            # 判断当前价格是否跌破大阳线开盘价
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
    """
    检测跌破中阳线开盘价（2.5%~5%为中阳线）
    不限制在新高点附近，检查回看期内所有中阳线

    参数:
        df: DataFrame
        lookback: 回看天数，默认20
        min_change_pct: 中阳线最小涨幅，默认2.5%
        max_change_pct: 中阳线最大涨幅，默认5.0%

    返回:
        dict 或 None
    """
    if df.empty or len(df) < lookback + 1:
        return None

    # 遍历回看期内的所有K线，找中阳线
    recent_data = df.iloc[-lookback:]
    
    for i in range(len(recent_data)):
        idx = len(df) - lookback + i
        row = df.iloc[idx]
        open_price = row['open']
        close_price = row['close']
        
        # 判断是否为阳线且涨幅在中阳线范围内
        is_bullish = close_price > open_price
        change_pct = (close_price - open_price) / open_price * 100 if open_price > 0 else 0
        
        if is_bullish and min_change_pct <= change_pct < max_change_pct:
            # 找到了中阳线
            medium_bull_open = open_price
            medium_bull_date = str(row['date'])[:10] if 'date' in df.columns else None
            medium_bull_change = change_pct
            
            # 判断当前价格是否跌破中阳线开盘价
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

def check_supertrend_flip(df, atr_period=10, multiplier=3.0):
    """
    检测SuperTrend翻空/翻多

    参数:
        df: DataFrame
        atr_period: ATR周期
        multiplier: ATR乘数

    返回:
        dict: {'type': 'flip_to_bear'|'flip_to_bull', 'severity': str} 或 None
    """
    if df.empty or len(df) < atr_period + 1:
        return None

    st = calculate_supertrend(df, atr_period, multiplier)

    if len(st) < 2:
        return None

    today_direction = st['supertrend'].iloc[-1]  # True=多头, False=空头
    yesterday_direction = st['supertrend'].iloc[-2]

    # 翻空：从多头翻为空头
    if yesterday_direction and not today_direction:
        return {
            'type': 'flip_to_bear',
            'upper_band': st['upper_band'].iloc[-1],
            'severity': 'high'
        }

    # 翻多：从空头翻为多头
    if not yesterday_direction and today_direction:
        return {
            'type': 'flip_to_bull',
            'lower_band': st['lower_band'].iloc[-1],
            'severity': 'positive'
        }

    return None


def check_weekly_supertrend_flip(daily_df, atr_period=10, multiplier=3.0):
    """
    检测周线SuperTrend翻空/翻多

    参数:
        daily_df: 日K DataFrame
        atr_period: ATR周期
        multiplier: ATR乘数

    返回:
        dict 或 None
    """
    weekly_df = get_weekly_kline(daily_df)
    if weekly_df.empty or len(weekly_df) < atr_period + 1:
        return None

    return check_supertrend_flip(weekly_df, atr_period, multiplier)


# ==================== 综合指标计算 ====================

def calculate_all_indicators(df):
    """
    计算所有技术指标（用于存储到technical_indicators表）

    参数:
        df: DataFrame，需要包含 date, open, high, low, close, volume 列

    返回:
        dict: 所有技术指标值
    """
    if df.empty:
        return {}

    result = {
        'calc_date': str(df['date'].iloc[-1])[:10] if 'date' in df.columns else None,
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    # 均线
    ma_values = calculate_ma(df, [5, 10, 20, 60, 120, 250])
    result.update(ma_values)

    # 均线斜率
    ma_slopes = calculate_ma_slope(df, [5, 10, 20])
    result.update(ma_slopes)

    # SuperTrend
    st = calculate_supertrend(df, atr_period=10, multiplier=3.0)
    if not st.empty and len(st) > 0:
        result['st_upper_band'] = round(st['upper_band'].iloc[-1], 3) if not pd.isna(st['upper_band'].iloc[-1]) else None
        result['st_lower_band'] = round(st['lower_band'].iloc[-1], 3) if not pd.isna(st['lower_band'].iloc[-1]) else None
        result['st_direction'] = 1 if st['supertrend'].iloc[-1] else -1
        result['st_atr'] = round(st['atr'].iloc[-1], 3) if not pd.isna(st['atr'].iloc[-1]) else None

    # MACD
    macd_result = calculate_macd(df)
    result['macd_dif'] = macd_result.get('dif')
    result['macd_dea'] = macd_result.get('dea')
    result['macd_histogram'] = macd_result.get('histogram')

    # MACD斜率
    macd_slope = calculate_macd_slope(df)
    result['macd_histogram_slope'] = macd_slope.get('histogram_slope', 0)
    result['macd_dif_slope'] = macd_slope.get('dif_slope', 0)
    result['macd_dea_slope'] = macd_slope.get('dea_slope', 0)
    result['macd_slope_summary'] = macd_slope.get('slope_summary', '→震荡')

    # RSI
    result['rsi_14'] = calculate_rsi(df, 14)

    # 量比
    result['volume_ratio_5'] = calculate_volume_ratio(df, 5)
    result['volume_ratio_20'] = calculate_volume_ratio(df, 20)

    # K线形态
    patterns = identify_candle_patterns(df)
    result['is_long_upper_shadow'] = patterns.get('is_long_upper_shadow', 0)
    result['is_long_lower_shadow'] = patterns.get('is_long_lower_shadow', 0)
    result['is_bullish_candle'] = patterns.get('is_bullish_candle', 0)
    result['is_bearish_candle'] = patterns.get('is_bearish_candle', 0)

    return result