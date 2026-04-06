"""
Robot Executor — 海龟机器人交易执行器
调用 Trade Executor 执行具体的加减仓/平仓操作

职责：
  1. 接收信号检测器输出的动作队列
  2. 按海龟交易法优先级排序执行
  3. 平仓 > 减仓 > 加仓 > 开仓（开仓留空）
  4. 汇总执行结果，输出报告

设计：
  - 不直接操作数据库，所有交易通过 TradeExecutor 执行
  - 包含海龟交易法的业务判断逻辑
  - 区分"该不该做"（业务逻辑）和"能不能做"（执行校验）
"""

import logging
from datetime import datetime
from typing import Optional

from executor.models import TradeCommand, TradeResult, TradeAction, TradeStatus
from executor.trade_executor import TradeExecutor
from strategies.turtle.position_manager import PositionManager
from strategies.turtle.account_manager import AccountManager

logger = logging.getLogger(__name__)


class RobotExecutor:
    """
    海龟机器人交易执行器

    用法:
        robot = RobotExecutor()
        results = robot.execute_signals(account_id, action_queue)
    """

    def __init__(self):
        self.trade_executor = TradeExecutor()
        self.position_manager = PositionManager()
        self.account_manager = AccountManager()

    def execute_signals(self, account_id, action_queue: list) -> dict:
        """
        执行信号检测器输出的动作队列

        执行流程：
          Step 1: 将动作队列转换为 TradeCommand 列表
          Step 2: 按优先级分组排序
          Step 3: 依次执行（平仓 → 减仓 → 加仓 → 开仓）
          Step 4: 汇总结果

        参数:
            account_id: 账户ID
            action_queue: 信号检测器输出的动作列表

        返回:
            dict: {
                'account_id': int,
                'total': int,           # 总指令数
                'success': int,         # 成功数
                'failed': int,          # 失败数
                'skipped': int,         # 跳过数
                'results': list,        # 详细结果列表
                'summary': str,         # 人可读的汇总
            }
        """
        if not action_queue:
            return {
                'account_id': account_id,
                'total': 0,
                'success': 0,
                'failed': 0,
                'skipped': 0,
                'results': [],
                'summary': '无交易动作',
            }

        # Step 1: 转换为 TradeCommand
        commands = self._to_commands(action_queue)

        # Step 2: 按优先级排序执行
        sorted_commands = self._prioritize(commands)

        # Step 3: 执行（含仓位控制校验）
        results = []
        for cmd in sorted_commands:
            # 机器人账户：严格执行仓位控制
            if cmd.action in (TradeAction.OPEN, TradeAction.ADD):
                is_new = cmd.action == TradeAction.OPEN
                target = cmd.code if cmd.action == TradeAction.ADD else None
                limit_check = self.trade_executor.check_position_limits(account_id, is_new_position=is_new, target_code=target)
                if limit_check['will_exceed']:
                    warning_msg = '; '.join(limit_check['warnings'])
                    logger.warning(f"[账户{account_id}] 仓位控制拦截: {cmd.code} - {warning_msg}")
                    results.append(TradeResult(
                        success=False,
                        status=TradeStatus.SKIPPED,
                        command=cmd,
                        message=f"仓位控制: {warning_msg}",
                    ))
                    continue

            result = self.trade_executor.execute(account_id, cmd)
            results.append(result)
            logger.info(f"  → {result}")

        # Step 4: 汇总
        success = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success and r.status == TradeStatus.FAILED)
        skipped = sum(1 for r in results if r.status == TradeStatus.SKIPPED)

        summary = self._build_summary(account_id, results)

        return {
            'account_id': account_id,
            'total': len(results),
            'success': success,
            'failed': failed,
            'skipped': skipped,
            'results': [r.to_dict() for r in results],
            'summary': summary,
        }

    def _to_commands(self, action_queue: list) -> list:
        """
        将动作队列转换为 TradeCommand 列表

        动作队列格式（来自 SignalChecker._to_action_queue）:
          {
            'action': '平仓'|'减仓'|'加仓'|'开仓',
            'code': '000001',
            'name': '平安银行',
            'price': 12.50,
            'atr': 0.85,
            'reason': 'stop_loss'|'exit'|None,
            'signal_type': 'stop_loss'|'exit'|'add'|'entry',
            'urgency': 'critical'|'high'|'medium',
            'system_type': 'S1'|'S2',
          }
        """
        action_map = {
            '平仓': TradeAction.CLOSE,
            '减仓': TradeAction.REDUCE,
            '加仓': TradeAction.ADD,
            '开仓': TradeAction.OPEN,
        }

        commands = []
        for item in action_queue:
            raw_action = item.get('action', '')
            action = action_map.get(raw_action)
            if action is None:
                logger.warning(f"未知动作类型: {raw_action}，跳过")
                continue

            # 平仓动作根据reason区分止损/退出
            if action == TradeAction.CLOSE:
                reason = item.get('reason', '')
                if reason == 'stop_loss':
                    action = TradeAction.CLOSE_STOP_LOSS
                elif reason == 'exit':
                    action = TradeAction.CLOSE_TAKE_PROFIT

            cmd = TradeCommand(
                action=action,
                code=item.get('code', ''),
                name=item.get('name', ''),
                price=item.get('price', 0),
                atr=item.get('atr', 0),
                reason=item.get('signal_type', ''),
                system_type=item.get('system_type'),
                source='robot_signal',
                metadata={
                    'urgency': item.get('urgency', ''),
                    'signal_type': item.get('signal_type', ''),
                },
            )
            commands.append(cmd)

        return commands

    def _prioritize(self, commands: list) -> list:
        """
        按海龟交易法优先级排序

        优先级（从高到低）：
          1. 止损平仓（CLOSE_STOP_LOSS）— 最紧急
          2. 退出平仓（CLOSE_TAKE_PROFIT/CLOSE）
          3. 减仓（REDUCE）
          4. 加仓（ADD）
          5. 开仓（OPEN）— 最后

        同优先级按原始顺序执行
        """
        priority = {
            TradeAction.CLOSE_STOP_LOSS: 0,
            TradeAction.CLOSE: 1,
            TradeAction.CLOSE_TAKE_PROFIT: 2,
            TradeAction.REDUCE: 3,
            TradeAction.ADD: 4,
            TradeAction.OPEN: 5,
        }

        return sorted(commands, key=lambda c: priority.get(c.action, 99))

    def _build_summary(self, account_id, results: list) -> str:
        """构建人可读的执行汇总"""
        if not results:
            return "无交易动作"

        lines = [f"📊 账户{account_id} 执行汇总:"]

        for r in results:
            lines.append(f"  {r}")

        success = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success and r.status == TradeStatus.FAILED)
        skipped = sum(1 for r in results if r.status == TradeStatus.SKIPPED)

        lines.append(f"  成功: {success} | 失败: {failed} | 跳过: {skipped}")

        # 资金变动汇总
        total_profit = sum(r.profit for r in results if r.success)
        if total_profit != 0:
            emoji = "📈" if total_profit > 0 else "📉"
            lines.append(f"  {emoji} 本次盈亏: {total_profit:.2f}")

        return '\n'.join(lines)

    # ==================== 单笔操作接口 ====================
    # 供人工调用或外部系统调用

    def close_position(self, account_id, code, sell_price, reason='exit') -> TradeResult:
        """
        平仓（供外部调用的便捷接口）

        参数:
            account_id: 账户ID
            code: 股票代码
            sell_price: 卖出价格
            reason: 平仓原因 ('stop_loss' 或 'exit')

        返回:
            TradeResult
        """
        action = TradeAction.CLOSE_STOP_LOSS if reason == 'stop_loss' else TradeAction.CLOSE_TAKE_PROFIT
        pos = self.position_manager.get_position(account_id, code)
        name = pos.get('name', '') if pos else ''

        cmd = TradeCommand(
            action=action,
            code=code,
            name=name,
            price=sell_price,
            reason=reason,
            source='manual',
        )
        return self.trade_executor.execute(account_id, cmd)

    def reduce_position(self, account_id, code, sell_price) -> TradeResult:
        """
        减仓（供外部调用的便捷接口）

        参数:
            account_id: 账户ID
            code: 股票代码
            sell_price: 卖出价格

        返回:
            TradeResult
        """
        pos = self.position_manager.get_position(account_id, code)
        name = pos.get('name', '') if pos else ''

        cmd = TradeCommand(
            action=TradeAction.REDUCE,
            code=code,
            name=name,
            price=sell_price,
            source='manual',
        )
        return self.trade_executor.execute(account_id, cmd)

    def add_position(self, account_id, code, buy_price, atr) -> TradeResult:
        """
        加仓（供外部调用的便捷接口）

        参数:
            account_id: 账户ID
            code: 股票代码
            buy_price: 买入价格
            atr: ATR值

        返回:
            TradeResult
        """
        pos = self.position_manager.get_position(account_id, code)
        name = pos.get('name', '') if pos else ''

        cmd = TradeCommand(
            action=TradeAction.ADD,
            code=code,
            name=name,
            price=buy_price,
            atr=atr,
            source='manual',
        )
        return self.trade_executor.execute(account_id, cmd)

    # ==================== 开仓 ====================

    def open_position(self, account_id, code, name, buy_price, atr, system_type='S1', units=1) -> TradeResult:
        """
        开仓（供外部调用的便捷接口）

        参数:
            account_id: 账户ID
            code: 股票代码
            name: 股票名称
            buy_price: 买入价格
            atr: ATR值
            system_type: 系统类型 'S1' 或 'S2'
            units: 买入单位数，默认1

        返回:
            TradeResult
        """
        cmd = TradeCommand(
            action=TradeAction.OPEN,
            code=code,
            name=name,
            price=buy_price,
            atr=atr,
            units=units,
            system_type=system_type,
            source='manual',
        )
        return self.trade_executor.execute(account_id, cmd)
