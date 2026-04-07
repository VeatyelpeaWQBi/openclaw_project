"""
交易执行器模块

延迟导入，避免循环依赖：
  from infra.trade_executor import TradeExecutor
  from executor.robot_executor import RobotExecutor
  from infra.models import TradeCommand, TradeResult, TradeAction, TradeStatus
"""
