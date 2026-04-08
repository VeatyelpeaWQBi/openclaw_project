"""
策略抽象基类
所有具体策略必须继承 BaseStrategy 并实现 run() 和 generate_report()
"""

from abc import ABC, abstractmethod


class BaseStrategy(ABC):
    """策略抽象基类"""

    @property
    @abstractmethod
    def name(self) -> str:
        """策略名称"""
        ...

    @abstractmethod
    def run(self) -> dict:
        """
        执行策略，返回结果字典

        返回:
            dict: {
                'date_str': str,           # 日期字符串
                'candidates': list,        # 候选股列表
                'signals': list,           # 信号列表（可选）
                'has_signal': bool,        # 是否有信号
                'skip_reason': str,        # 跳过原因（无信号时）
                'metadata': dict,          # 策略特定的元数据
            }
        """
        ...

    @abstractmethod
    def generate_report(self, result: dict) -> str:
        """
        根据策略结果生成报告

        参数:
            result: run() 的返回值

        返回:
            str: 报告文本
        """
        ...
