"""
信号检测模块

检测买入/卖出信号
"""

import logging
from typing import Optional, Tuple

import numpy as np
import pandas as pd

from .config import BacktestConfig

logger = logging.getLogger(__name__)


def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    """计算EMA"""
    alpha = 2 / (period + 1)
    return series.ewm(alpha=alpha, adjust=False).mean()


def calculate_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """
    计算MACD指标

    返回:
        DataFrame: 新增 dif, dea, histogram, dif_series, dea_series, histogram_series 列
    """
    close = df['close']
    ema_fast = calculate_ema(close, fast)
    ema_slow = calculate_ema(close, slow)

    dif = ema_fast - ema_slow
    dea = calculate_ema(dif, signal)
    histogram = (dif - dea) * 2

    result = df.copy()
    result['dif_series'] = dif
    result['dea_series'] = dea
    result['histogram_series'] = histogram
    result['dif'] = dif
    result['dea'] = dea
    result['histogram'] = histogram

    return result


def calculate_obv(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """
    计算OBV能量潮

    返回:
        tuple: (obv_series, ma_obv_series)
    """
    if df.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float)

    close = df['close'].values
    volume = df['volume'].values

    obv_values = [0]
    for i in range(1, len(close)):
        prev_obv = obv_values[-1]
        if close[i] > close[i-1]:
            obv_values.append(prev_obv + volume[i])
        elif close[i] < close[i-1]:
            obv_values.append(prev_obv - volume[i])
        else:
            obv_values.append(prev_obv)

    obv_series = pd.Series(obv_values, index=df.index)
    ma_obv_series = obv_series.rolling(30).mean()

    return obv_series, ma_obv_series


def is_macd_golden_cross(dif_series: pd.Series, dea_series: pd.Series) -> bool:
    """
    检测MACD是否金叉

    返回:
        bool: True=今日金叉
    """
    if len(dif_series) < 2 or len(dea_series) < 2:
        return False

    today_dif = dif_series.iloc[-1]
    today_dea = dea_series.iloc[-1]
    yesterday_dif = dif_series.iloc[-2]
    yesterday_dea = dea_series.iloc[-2]

    return yesterday_dif < yesterday_dea and today_dif >= today_dea


def is_macd_soon_cross(dif_series: pd.Series, dea_series: pd.Series,
                       dif_slope: float, dea_slope: float,
                       config: BacktestConfig) -> bool:
    """
    检测MACD是否即将金叉

    按斜率+差值互相验算

    返回:
        bool: True=即将金叉
    """
    if len(dif_series) < 1 or len(dea_series) < 1:
        return False

    today_dif = dif_series.iloc[-1]
    today_dea = dea_series.iloc[-1]

    # 条件1: 快线斜率向上
    if dif_slope <= config.SOON_CROSS_SLOPE_MIN:
        return False

    # 条件2: 差值相对较小
    gap = today_dea - today_dif
    if today_dea != 0:
        gap_ratio = abs(gap / today_dea)
        if gap_ratio > config.SOON_CROSS_GAP_RATIO:
            return False
    else:
        if abs(gap) > 0.01:  # DEA接近0时的绝对值判断
            return False

    # 条件3: DEA斜率不大于快线太多（说明快线追得上）
    if dif_slope > 0 and dea_slope > dif_slope * 0.8:
        return False

    return True


def is_obv_uptrend(obv_series: pd.Series, window: int = 5,
                     min_slope: float = 0.01) -> bool:
    """
    检测OBV是否呈上升趋势

    参数:
        obv_series: OBV序列
        window: 检测窗口
        min_slope: 最小平均斜率

    返回:
        bool: True=上升趋势
    """
    if len(obv_series) < window:
        return False

    recent_obv = obv_series.iloc[-window:]

    # 检查是否单调上升
    is_monotonic = recent_obv.is_monotonic_increasing

    # 计算平均斜率
    slope = (recent_obv.iloc[-1] - recent_obv.iloc[0]) / window

    return is_monotonic and slope > min_slope


def is_obv_soon_cross(obv_series: pd.Series, ma_obv_series: pd.Series,
                      window: int = 5, ratio: float = 0.05) -> bool:
    """
    检测OBV是否即将上叉MAOBV

    参数:
        obv_series: OBV序列
        ma_obv_series: MAOBV序列
        window: 检测窗口
        ratio: 相对差值阈值

    返回:
        bool: True=即将上叉
    """
    if len(obv_series) < window or len(ma_obv_series) < window:
        return False

    today_obv = obv_series.iloc[-1]
    today_ma = ma_obv_series.iloc[-1]

    if pd.isna(today_obv) or pd.isna(today_ma):
        return False

    # 条件1: OBV呈上升趋势
    if not is_obv_uptrend(obv_series, window):
        return False

    # 条件2: OBV接近MAOBV
    gap = today_ma - today_obv
    if today_ma != 0:
        gap_ratio = abs(gap / today_ma)
        if gap_ratio > ratio:
            return False
    else:
        if abs(gap) > 100:  # MAOBV接近0时的绝对值判断
            return False

    # 条件3: OBV在上涨
    recent_slope = obv_series.iloc[-1] - obv_series.iloc[-2]
    return recent_slope > 0


def is_ema_cross(ema_fast: float, ema_slow: float) -> bool:
    """
    检测EMA是否金叉

    参数:
        ema_fast: 快线值
        ema_slow: 慢线值

    返回:
        bool: True=快线上穿慢线
    """
    return ema_fast >= ema_slow


def is_ema_soon_cross(price_high: float, ema_slow: float,
                       ema_fast_slope: float, min_slope: float = 0.001,
                       ratio: float = 0.05) -> bool:
    """
    检测日K实体顶部是否即将向上穿越EMA40

    参数:
        price_high: 日K最高价
        ema_slow: EMA40值
        ema_fast_slope: EMA18斜率
        min_slope: 最小斜率
        ratio: 相对差值阈值

    返回:
        bool: True=即将上穿
    """
    if pd.isna(price_high) or pd.isna(ema_slow):
        return False

    # 条件1: 快线斜率向上
    if ema_fast_slope <= min_slope:
        return False

    # 条件2: 价格接近EMA慢线
    gap = ema_slow - price_high
    if ema_slow != 0:
        gap_ratio = abs(gap / ema_slow)
        if gap_ratio > ratio:
            return False
    else:
        if abs(gap) > 0.01:
            return False

    return True


def check_buy_signal(df: pd.DataFrame, config: BacktestConfig) -> Optional[dict]:
    """
    检测买入信号

    参数:
        df: 日K数据（需包含 date, open, high, low, close, volume 列）
        config: 回测配置

    返回:
        dict or None: 信号信息 {'date', 'code', 'strength', 'macd_cross', 'obv_uptrend', 'ema_cross'}
    """
    if df.empty or len(df) < config.SIGNAL_WINDOW + 1:
        return None

    # 计算技术指标
    df_macd = calculate_macd(df, config.MACD_FAST, config.MACD_SLOW, config.MACD_SIGNAL)
    obv_series, ma_obv_series = calculate_obv(df)

    # EMA18, EMA40
    ema_fast_series = calculate_ema(df['close'], config.EMA_FAST)
    ema_slow_series = calculate_ema(df['close'], config.EMA_SLOW)

    # 检查窗口内的信号
    window = config.SIGNAL_WINDOW
    signal_date = df['date'].iloc[-1]

    # MACD信号（窗口内）
    macd_ok = False
    for i in range(max(0, len(df_macd) - window), len(df_macd)):
        dif = df_macd['dif_series'].iloc[i]
        dea = df_macd['dea_series'].iloc[i]

        # 已金叉
        if i > 0:
            prev_dif = df_macd['dif_series'].iloc[i-1]
            prev_dea = df_macd['dea_series'].iloc[i-1]
            if prev_dif < prev_dea and dif >= dea:
                macd_ok = True
                break

        # 即将金叉
        if i > 0 and i < len(df_macd):
            dif_slope = dif - df_macd['dif_series'].iloc[i-1]
            dea_slope = dea - df_macd['dea_series'].iloc[i-1]
            if is_macd_soon_cross(
                df_macd['dif_series'].iloc[:i+1],
                df_macd['dea_series'].iloc[:i+1],
                dif_slope, dea_slope, config
            ):
                macd_ok = True
                break

    if not macd_ok:
        return None

    # OBV信号（窗口内）
    obv_ok = False
    for i in range(max(0, len(obv_series) - window), len(obv_series)):
        # 斜率向上
        if is_obv_uptrend(obv_series.iloc[:i+1], config.OBV_UPTREND_WINDOW,
                         config.OBV_UPTREND_MIN_SLOPE):
            obv_ok = True
            break

        # 即将上叉MAOBV
        if is_obv_soon_cross(obv_series.iloc[:i+1], ma_obv_series.iloc[:i+1],
                           config.OBV_UPTREND_WINDOW, config.SOON_CROSS_OBV_RATIO):
            obv_ok = True
            break

    if not obv_ok:
        return None

    # EMA信号（窗口内）
    ema_ok = False
    for i in range(max(0, len(df) - window), len(df)):
        ema_fast = ema_fast_series.iloc[i]
        ema_slow = ema_slow_series.iloc[i]
        price_high = df['high'].iloc[i]

        # EMA18上穿EMA40
        if i > 0:
            prev_fast = ema_fast_series.iloc[i-1]
            prev_slow = ema_slow_series.iloc[i-1]
            if prev_fast < prev_slow and ema_fast >= ema_slow:
                ema_ok = True
                break

        # 日K实体顶部向上穿越EMA40
        if i > 0:
            ema_fast_slope = ema_fast - ema_fast_series.iloc[i-1]
            if is_ema_soon_cross(price_high, ema_slow, ema_fast_slope):
                ema_ok = True
                break

    if not ema_ok:
        return None

    # 计算信号强度
    strength = 0.5  # 基础强度

    # OBV斜率加分
    obv_slope = (obv_series.iloc[-1] - obv_series.iloc[-config.OBV_UPTREND_WINDOW]) / config.OBV_UPTREND_WINDOW
    strength += min(obv_slope / 1000, 0.5)  # 最多加0.5

    return {
        'date': signal_date,
        'strength': strength,
        'macd_cross': macd_ok,
        'obv_uptrend': obv_ok,
        'ema_cross': ema_ok,
    }


def check_sell_signal(df: pd.DataFrame, position_entry_date: str,
                       config: BacktestConfig) -> bool:
    """
    检测卖出信号

    参数:
        df: 日K数据
        position_entry_date: 建仓日期
        config: 回测配置

    返回:
        bool: True=卖出
    """
    if df.empty or len(df) < config.DIF_FLAT_WINDOW + 1:
        return False

    # 计算MACD
    df_macd = calculate_macd(df, config.MACD_FAST, config.MACD_SLOW, config.MACD_SIGNAL)

    # 检查DIF走平（3天滑动窗口内升幅连续<0.02或不升反跌）
    window = config.DIF_FLAT_WINDOW
    threshold = config.DIF_FLAT_THRESHOLD

    dif_flat = True
    dif_declining = False

    for i in range(len(df_macd) - window, len(df_macd)):
        if i == 0:
            continue

        change = df_macd['dif'].iloc[i] - df_macd['dif'].iloc[i-1]

        # 不升反跌
        if change < 0:
            dif_declining = True
            break

        # 升幅>=0.02，没走平
        if change >= threshold:
            dif_flat = False
            break

    # DIF回落（比最近高点下降）
    if not dif_declining and not dif_flat:
        max_dif = df_macd['dif'].iloc[-20:].max() if len(df_macd) >= 20 else df_macd['dif'].max()
        current_dif = df_macd['dif'].iloc[-1]
        if current_dif < max_dif * 0.95:  # 回落超过5%
            dif_declining = True

    return dif_flat or dif_declining


class SignalDetector:
    """信号检测器"""

    def __init__(self, config: BacktestConfig):
        self.config = config

    def detect_buy_signals(self, df: pd.DataFrame, code: str) -> Optional[dict]:
        """检测买入信号"""
        signal = check_buy_signal(df, self.config)
        if signal:
            signal['code'] = code
        return signal

    def detect_sell_signal(self, df: pd.DataFrame, code: str,
                            entry_date: str) -> bool:
        """检测卖出信号"""
        return check_sell_signal(df, entry_date, self.config)

    def get_obv_slope(self, df: pd.DataFrame) -> float:
        """获取OBV斜率（用于排序）"""
        obv_series, _ = calculate_obv(df)
        window = self.config.OBV_UPTREND_WINDOW

        if len(obv_series) < window:
            return 0.0

        slope = (obv_series.iloc[-1] - obv_series.iloc[-window]) / window
        return float(slope)