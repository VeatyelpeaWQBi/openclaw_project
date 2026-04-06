"""
交易执行器模块

延迟导入，避免循环依赖：
  from executor.trade_executor import TradeExecutor
  from executor.robot_executor import RobotExecutor
  from executor.models import TradeCommand, TradeResult, TradeAction, TradeStatus
"""
