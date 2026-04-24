"""
技术指标模块

使用方式：
    from core.indicators import IndicatorManager

    manager = IndicatorManager()
    result = manager.analyze_stock(code, df, context)

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
]