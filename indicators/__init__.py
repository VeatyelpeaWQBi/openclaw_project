"""
技术指标模块

公共模块：可被 WatchMonitor 和 quant_trading 两个项目引用

使用方式：
    # 面向对象风格（WatchMonitor使用）
    from indicators import IndicatorManager
    manager = IndicatorManager()
    result = manager.analyze_stock(code, df, context)

    # 函数式风格（量化交易使用）
    from indicators.utils import calculate_supertrend, calculate_atr, is_supertrend_bullish

# result包含:
# - signals: 所有信号列表
# - report_lines: 报告内容列表（可直接输出）
# - total_score: 综合评分
# - score_reasons: 评分原因
"""

from .base import BaseIndicator
from .manager import IndicatorManager

# 各指标类（供扩展使用）
from .supertrend_indicator import SuperTrendIndicator
from .rsi_indicator import RSIIndicator
from .macd_indicator import MACDIndicator
from .ma_indicator import MAIndicator
from .adx_indicator import ADXIndicator
from .volume_indicator import VolumeIndicator
from .candle_indicator import CandleIndicator

# 通用计算函数（量化交易使用）
from .utils import (
    calculate_atr,
    calculate_supertrend,
    is_supertrend_bullish,
    check_multi_timeframe_supertrend,
    calculate_volume_ratio,
    get_weekly_kline,
    calculate_ma,
    calculate_ma_series,
    calculate_ma_slope,
    check_ma_breakdown,
    check_ma_cross,
    calculate_ema,
    calculate_macd,
    calculate_macd_slope,
    check_divergence,
    calculate_rsi_series,
    calculate_rsi,
    identify_candle_patterns,
    check_volume_stagnation,
    check_high_long_upper_shadow,
    check_breakdown_big_bull_candle,
    check_breakdown_medium_bull_candle,
    check_supertrend_flip,
    check_weekly_supertrend_flip,
    calculate_all_indicators,
)

__all__ = [
    'BaseIndicator',
    'IndicatorManager',
    'SuperTrendIndicator',
    'RSIIndicator',
    'MACDIndicator',
    'MAIndicator',
    'ADXIndicator',
    'VolumeIndicator',
    'CandleIndicator',
    'calculate_atr',
    'calculate_supertrend',
    'is_supertrend_bullish',
    'check_multi_timeframe_supertrend',
    'calculate_volume_ratio',
    'get_weekly_kline',
    'calculate_ma',
    'calculate_ma_series',
    'calculate_ma_slope',
    'check_ma_breakdown',
    'check_ma_cross',
    'calculate_ema',
    'calculate_macd',
    'calculate_macd_slope',
    'check_divergence',
    'calculate_rsi_series',
    'calculate_rsi',
    'identify_candle_patterns',
    'check_volume_stagnation',
    'check_high_long_upper_shadow',
    'check_breakdown_big_bull_candle',
    'check_breakdown_medium_bull_candle',
    'check_supertrend_flip',
    'check_weekly_supertrend_flip',
    'calculate_all_indicators',
]