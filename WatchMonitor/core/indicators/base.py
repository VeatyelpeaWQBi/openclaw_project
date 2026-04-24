"""
技术指标基类 - 完全独立封装设计

设计原则：
- 每个指标实例对应一只股票
- 初始化时传入df，内部完成所有计算
- 提供统一的输出接口：信号、报告、评分
- 对象内部完全黑盒，外部只获取结果
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Any, Optional
from pandas import DataFrame


class BaseIndicator(ABC):
    """
    技术指标基类 - 单一股票实例

    生命周期：
    1. 实例化：__init__(df, config) - 接收df和配置，完成计算
    2. 输出：get_signals(), get_report_lines(), get_score() - 获取结果
    3. 释放：由Manager负责销毁
    """

    # 类属性：指标基本信息（子类必须定义）
    name: str = ""              # 指标名称（用于配置匹配）
    display_name: str = ""      # 报告展示名称

    def __init__(self, df: DataFrame, config: Dict, context: Dict):
        """
        初始化指标实例

        参数:
            df: 日K数据DataFrame（包含open, high, low, close, volume）
            config: 指标配置（来自YAML）
            context: 上下文信息（code, current_price, is_position等）
        """
        self.df = df
        self.config = config
        self.context = context

        # 从config读取参数
        self.params = config.get('params', {})
        self.weight = config.get('weight', 1.0)
        self.enabled = config.get('enabled', True)
        self.report_order = config.get('report_order', 99)

        # 内部存储计算结果（黑盒）
        self._data: Dict = {}        # 指标计算数据
        self._signals: List[Dict] = []  # 检测到的信号
        self._report_lines: List[str] = []  # 报告展示内容
        self._score: float = 0.0     # 评分
        self._score_reasons: List[str] = []  # 评分原因

        # 初始化时完成所有计算
        if df is not None and not df.empty:
            self._calculate()
            self._detect_signals()
            self._generate_report()
            self._calculate_score()

    # ========== 内部计算方法（子类实现） ==========

    @abstractmethod
    def _calculate(self) -> None:
        """
        计算指标值（内部方法）
        结果存入self._data
        """
        pass

    @abstractmethod
    def _detect_signals(self) -> None:
        """
        检测信号（内部方法）
        结果存入self._signals
        """
        pass

    @abstractmethod
    def _generate_report(self) -> None:
        """
        生成报告内容（内部方法）
        结果存入self._report_lines
        每行格式："    - 内容"
        """
        pass

    @abstractmethod
    def _calculate_score(self) -> None:
        """
        计算评分（内部方法）
        结果存入self._score和self._score_reasons
        """
        pass

    # ========== 外部输出接口（统一API） ==========

    def get_signals(self) -> List[Dict]:
        """获取检测到的信号列表"""
        return self._signals

    def get_report_lines(self) -> List[str]:
        """获取报告展示内容（已格式化，可直接输出）"""
        return self._report_lines

    def get_score(self) -> Tuple[float, List[str]]:
        """获取评分结果"""
        return self._score, self._score_reasons

    def get_weighted_score(self) -> float:
        """获取加权评分"""
        return self._score * self.weight

    def get_data(self) -> Dict:
        """获取指标计算数据（供调试或扩展使用）"""
        return self._data

    def is_enabled(self) -> bool:
        """是否启用"""
        return self.enabled

    def cleanup(self) -> None:
        """释放资源（可选实现）"""
        self.df = None
        self._data.clear()
        self._signals.clear()
        self._report_lines.clear()
        self._score_reasons.clear()