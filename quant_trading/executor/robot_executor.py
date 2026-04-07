"""
Robot Executor — 海龟机器人交易执行器（已简化）
代理 TrendTradingExecutor，保持对外接口兼容

职责：
  - 接收信号检测器输出的动作队列
  - 委托给 TrendTradingExecutor 执行
  - 汇总执行结果

设计：
  - 本文件为薄代理层，所有 turtle 业务逻辑在 TrendTradingExecutor 中
  - 保留本文件以兼容 strategy.py 的现有调用
"""

import logging
from strategies.trend_trading.trend_trading_executor import TrendTradingExecutor

logger = logging.getLogger(__name__)


class RobotExecutor:
    """海龟机器人交易执行器（代理 TrendTradingExecutor）"""

    def __init__(self):
        self.tt_executor = TrendTradingExecutor()

    def execute_signals(self, account_id, action_queue: list) -> dict:
        """执行信号检测器输出的动作队列"""
        return self.tt_executor.execute_signals(account_id, action_queue)
