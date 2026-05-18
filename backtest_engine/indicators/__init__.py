"""
回测引擎模块

基于 MACD、OBV、EMA 技术指标的回测引擎
"""

from .config import BacktestConfig
from .data_loader import DataLoader
from .signal_detector import SignalDetector
from .cross_ranker import CrossRanker
from .position_manager import PositionManager
from .backtest_engine import BacktestEngine
from .report_generator import BacktestResult

__all__ = [
    'BacktestConfig',
    'DataLoader',
    'SignalDetector',
    'CrossRanker',
    'PositionManager',
    'BacktestEngine',
    'BacktestResult',
]