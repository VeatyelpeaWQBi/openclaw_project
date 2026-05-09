"""
场景分析基类 - 独立文件

设计原则：
- 不负责趋势检测，只负责特定趋势场景下的分析
- 提供统一的输出接口
- 职责分离，易于扩展
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from pandas import DataFrame


class BaseScenario(ABC):
    """
    场景分析基类

    职责分离：
    - 不负责趋势检测，只负责特定趋势场景下的分析
    - 提供统一的输出接口
    """

    # 场景类型标识（子类必须定义）
    scenario_type: str = ""
    scenario_name: str = ""

    def __init__(self, df: DataFrame, context: Dict):
        """
        初始化场景分析器

        参数:
            df: 日K数据
            context: 上下文信息 {
                'code': str,
                'name': str,
                'current_price': float,
                'trend_type': str,
                'trend_score': float,
                'trend_strength': str,
                # ... 其他上下文
            }
        """
        self.df = df
        self.context = context

        # 内部存储计算结果
        self._analysis_result: Dict = {}
        self._signals: List[Dict] = []
        self._report_lines: List[str] = []

        # ✅ 初始化时自动执行analyze()，确保分析数据被计算
        if df is not None and not df.empty:
            self.analyze()

    def execute(self) -> None:
        """
        统一执行入口（推荐使用）

        执行完整的分析流程：准备 -> 信号检测 -> 报告生成
        """
        if self.df is not None and not self.df.empty:
            self.prepare_analysis()
            self.detect_signals()
            self.generate_report()

    def prepare_analysis(self) -> None:
        """
        准备分析数据（可选）

        子类可以重写此方法进行预处理
        默认不执行任何操作
        """
        pass

    # ========== 核心分析方法（子类实现） ==========

    @abstractmethod
    def analyze(self) -> None:
        """
        执行场景分析，结果存入 self._analysis_result
        """
        pass

    @abstractmethod
    def detect_signals(self) -> None:
        """
        检测特定场景下的信号，结果存入 self._signals
        """
        pass

    @abstractmethod
    def generate_report(self) -> None:
        """
        生成报告内容，结果存入 self._report_lines
        每行格式："    - 内容"
        """
        pass

    # ========== 统一输出接口 ==========

    def get_analysis_result(self) -> Dict:
        """获取分析结果"""
        return self._analysis_result

    def get_signals(self) -> List[Dict]:
        """获取检测到的信号"""
        return self._signals

    def get_report_lines(self) -> List[str]:
        """获取报告内容（已格式化，可直接输出）"""
        return self._report_lines

    def get_summary(self) -> str:
        """获取场景摘要（一句话）"""
        if not self._analysis_result:
            return f"{self.scenario_name}分析完成"
        return f"{self.scenario_name}分析完成"
