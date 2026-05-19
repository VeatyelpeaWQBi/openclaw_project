"""
通用技术指标模块 - 盯盘助手兼容层
所有函数已从公共 indicators.utils 迁移
"""

import sys, os
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from indicators.utils import (
    calculate_ma,
    calculate_ma_series,
    calculate_ma_slope,
    check_ma_breakdown,
    check_ma_cross,
    calculate_ema,
    calculate_macd,
    calculate_macd_slope,
    _get_macd_slope_summary,
    check_divergence,
    calculate_rsi_series,
    calculate_rsi,
    calculate_atr,
    calculate_supertrend,
    is_supertrend_bullish,
    calculate_volume_ratio,
    get_weekly_kline,
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
    'calculate_ma',
    'calculate_ma_series',
    'calculate_ma_slope',
    'check_ma_breakdown',
    'check_ma_cross',
    'calculate_ema',
    'calculate_macd',
    'calculate_macd_slope',
    '_get_macd_slope_summary',
    'check_divergence',
    'calculate_rsi_series',
    'calculate_rsi',
    'calculate_atr',
    'calculate_supertrend',
    'is_supertrend_bullish',
    'calculate_volume_ratio',
    'get_weekly_kline',
    'identify_candle_patterns',
    'check_volume_stagnation',
    'check_high_long_upper_shadow',
    'check_breakdown_big_bull_candle',
    'check_breakdown_medium_bull_candle',
    'check_supertrend_flip',
    'check_weekly_supertrend_flip',
    'calculate_all_indicators',
]
