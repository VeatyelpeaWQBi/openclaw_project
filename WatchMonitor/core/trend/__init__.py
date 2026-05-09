"""
趋势分析模块

提供趋势检测和场景分析功能：
- TrendDetector: 检测趋势类型（上升/下降/震荡）
- BaseScenario: 场景分析基类
- UptrendScenario: 上升趋势场景分析
- DowntrendScenario: 下跌趋势场景分析
- SidewaysScenario: 震荡场景分析
- TrendAnalyzer: 趋势分析管理器
"""

from .detector import TrendDetector
from .analyzer import TrendAnalyzer
from .scenarios.base import BaseScenario

__all__ = [
    'TrendDetector',
    'TrendAnalyzer',
    'BaseScenario',
]
