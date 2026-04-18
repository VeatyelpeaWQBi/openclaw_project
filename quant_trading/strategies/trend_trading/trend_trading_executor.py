"""
趋势交易策略 — 交易执行层
封装趋势交易特有的交易执行逻辑，底层委托给 infra TradeExecutor

职责：
  - ATR仓位大小计算
  - turtle规则前置校验（冷却、4单位上限、has_reduced）
  - 仓位控制（持仓数上限、单日开仓数）
  - 将 turtle 动作队列转换为通用 TradeCommand
  - 执行优先级排序

设计：
  - TradeExecutor（infra）= 纯通用执行：T+1校验、资金扣减、指令分发
  - TrendTradingExecutor（strategy）= turtle业务逻辑：该不该做、做多少
  - RobotExecutor 调用 TrendTradingExecutor，TrendTradingExecutor 调用 TradeExecutor
"""

import logging
from typing import Optional

from infra.models import TradeCommand, TradeResult, TradeAction, TradeStatus
from infra.trade_executor import TradeExecutor
from infra.position_manager import PositionManager
from infra.account_manager import AccountManager
from strategies.trend_trading.trend_trading_position_manager import TrendTradingPositionManager

logger = logging.getLogger(__name__)


class TrendTradingExecutor:
    """趋势交易策略执行器"""

    def __init__(self):
        self.trade_executor = TradeExecutor()
        self.pm = PositionManager()
        self.tt_pm = TrendTradingPositionManager()
        self.account_manager = AccountManager()

    # ==================== 批量执行（主要入口） ====================

    def execute_signals(self, account_id, action_queue: list, target_date=None) -> dict:
        """
        执行信号检测器输出的动作队列

        执行流程：
          Step 1: 将动作队列转换为 TradeCommand 列表
          Step 2: 按优先级排序（止损 > 退出 > 减仓 > 加仓 > 开仓）
          Step 3: turtle规则校验 + 执行
          Step 4: 汇总结果

        参数:
            account_id: 账户ID
            action_queue: SignalChecker输出的动作列表
            target_date: 业务日期（回测时传入）

        返回:
            dict: 执行结果汇总
        """
        if not action_queue:
            return {
                'account_id': account_id, 'total': 0,
                'success': 0, 'failed': 0, 'skipped': 0,
                'results': [], 'summary': '无交易动作',
            }

        commands = self._to_commands(action_queue)
        sorted_commands = self._prioritize(commands)

        results = []
        for cmd in sorted_commands:
            # turtle规则校验
            can_execute, reason = self._check_turtle_rules(account_id, cmd, target_date=target_date)
            if not can_execute:
                logger.warning(f"[账户{account_id}] turtle规则拦截: {cmd.code} - {reason}")
                results.append(TradeResult(
                    success=False, status=TradeStatus.SKIPPED,
                    command=cmd, message=f"turtle规则: {reason}",
                ))
                continue

            result = self.trade_executor.execute(account_id, cmd, target_date=target_date)
            results.append(result)
            logger.info(f"  → {result}")

        success = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success and r.status == TradeStatus.FAILED)
        skipped = sum(1 for r in results if r.status == TradeStatus.SKIPPED)

        return {
            'account_id': account_id,
            'total': len(results),
            'success': success, 'failed': failed, 'skipped': skipped,
            'results': [r.to_dict() for r in results],
            'summary': self._build_summary(account_id, results),
        }

    # ==================== 单笔操作接口 ====================

    def open_position(self, account_id, code, name, buy_price, atr,
                      entry_system='S1', units=1, target_date=None) -> TradeResult:
        """开仓（turtle增强）"""
        # 获取账户资金
        summary = self.account_manager.get_summary(account_id)
        capital = summary.get('total_capital', 0) if summary else 0
        if capital <= 0:
            return TradeResult(
                success=False, status=TradeStatus.FAILED,
                command=TradeCommand(action=TradeAction.OPEN, code=code, name=name, price=buy_price),
                error="账户资金为0",
            )

        # turtle特有：仓位计算
        shares_per_unit = self.tt_pm.calc_shares_per_unit(capital, atr, buy_price)
        if shares_per_unit <= 0:
            return TradeResult(
                success=False, status=TradeStatus.FAILED,
                command=TradeCommand(action=TradeAction.OPEN, code=code, name=name, price=buy_price),
                error=f"计算仓位失败(capital={capital}, atr={atr}, price={buy_price})",
            )

        # turtle特有：持仓数上限检查
        positions = self.pm.get_active_positions(account_id)
        config = self.account_manager.get_position_config(account_id)
        if len(positions) >= config['max_holdings']:
            return TradeResult(
                success=False, status=TradeStatus.SKIPPED,
                command=TradeCommand(action=TradeAction.OPEN, code=code, name=name, price=buy_price),
                message=f"持仓数已达上限({config['max_holdings']})",
            )

        # turtle特有：单日开仓数检查
        today_opens = self.pm.count_today_opens(account_id, target_date=target_date)
        max_daily = config.get('max_daily_open', 2)
        if today_opens >= max_daily:
            return TradeResult(
                success=False, status=TradeStatus.SKIPPED,
                command=TradeCommand(action=TradeAction.OPEN, code=code, name=name, price=buy_price),
                message=f"今日已开仓{today_opens}个，达到单日上限({max_daily})",
            )

        cmd = TradeCommand(
            action=TradeAction.OPEN, code=code, name=name, price=buy_price,
            atr=atr, units=units, turtle_entry_system=entry_system, source='manual',
        )
        return self.trade_executor.execute(account_id, cmd, target_date=target_date)

    def add_position(self, account_id, code, buy_price, atr, target_date=None) -> TradeResult:
        """加仓（turtle增强）"""
        pos = self.pm.get_position(account_id, code)
        if not pos:
            return TradeResult(
                success=False, status=TradeStatus.FAILED,
                command=TradeCommand(action=TradeAction.ADD, code=code, price=buy_price),
                error=f"持仓不存在: {code}",
            )
        name = pos.get('name', '')

        # turtle特有：4单位上限
        if pos['turtle_units'] >= 4:
            return TradeResult(
                success=False, status=TradeStatus.SKIPPED,
                command=TradeCommand(action=TradeAction.ADD, code=code, name=name, price=buy_price),
                message=f"已达4单位上限",
            )

        cmd = TradeCommand(
            action=TradeAction.ADD, code=code, name=name, price=buy_price,
            atr=atr, source='manual',
        )
        return self.trade_executor.execute(account_id, cmd, target_date=target_date)

    def reduce_position(self, account_id, code, sell_price, target_date=None) -> TradeResult:
        """减仓（turtle增强）"""
        pos = self.pm.get_position(account_id, code)
        if not pos:
            return TradeResult(
                success=False, status=TradeStatus.FAILED,
                command=TradeCommand(action=TradeAction.REDUCE, code=code, price=sell_price),
                error=f"持仓不存在: {code}",
            )
        name = pos.get('name', '')

        # turtle特有：has_reduced 检查
        if pos.get('has_reduced', 0):
            return TradeResult(
                success=False, status=TradeStatus.SKIPPED,
                command=TradeCommand(action=TradeAction.REDUCE, code=code, name=name, price=sell_price),
                message="已减过仓",
            )

        # turtle特有：至少2单位
        if pos['turtle_units'] < 2:
            return TradeResult(
                success=False, status=TradeStatus.SKIPPED,
                command=TradeCommand(action=TradeAction.REDUCE, code=code, name=name, price=sell_price),
                message=f"仅{pos['turtle_units']}单位，无法减仓",
            )

        cmd = TradeCommand(
            action=TradeAction.REDUCE, code=code, name=name, price=sell_price,
            source='manual',
        )
        return self.trade_executor.execute(account_id, cmd, target_date=target_date)

    def close_position(self, account_id, code, sell_price, reason='exit', target_date=None) -> TradeResult:
        """平仓（turtle增强：S1/S2冷却天数 + 过滤逻辑）"""
        pos = self.pm.get_position(account_id, code)
        if not pos:
            return TradeResult(
                success=False, status=TradeStatus.FAILED,
                command=TradeCommand(action=TradeAction.CLOSE, code=code, price=sell_price),
                error=f"持仓不存在: {code}",
            )
        name = pos.get('name', '')

        # turtle特有：冷却天数决策
        gross_profit = (sell_price - pos['avg_cost']) * pos['total_shares']
        if reason == 'stop_loss':
            cooldown_days = 20
        elif gross_profit > 0:
            cooldown_days = 10
        else:
            cooldown_days = 10

        action = TradeAction.CLOSE_STOP_LOSS if reason == 'stop_loss' else TradeAction.CLOSE_TAKE_PROFIT
        cmd = TradeCommand(
            action=action, code=code, name=name, price=sell_price,
            reason=reason, source='manual',
            metadata={'cooldown_days': cooldown_days},
        )
        result = self.trade_executor.execute(account_id, cmd, target_date=target_date)

        # turtle特有：S1/S2过滤逻辑
        if result.success:
            entry_system = pos.get('turtle_entry_system', '')
            if entry_system == 'S2' and result.profit > 0:
                self.account_manager.clear_s1_filter(account_id)
            elif entry_system == 'S1' and result.profit > 0:
                self.account_manager.set_s1_filter(account_id)

        return result

    # ==================== turtle规则校验 ====================

    def _check_turtle_rules(self, account_id, command: TradeCommand, target_date=None) -> tuple:
        """
        turtle规则前置校验

        参数:
            account_id: 账户ID
            command: 交易指令
            target_date: 业务日期（回测时传入）

        返回:
            (can_execute: bool, reason: str)
        """
        code = command.code

        if command.action == TradeAction.OPEN:
            # 是否已持仓
            pos = self.pm.get_position(account_id, code)
            if pos:
                return False, f"已持有({pos['turtle_units']}单位)"

            # 冷却期检查
            cooling = self.pm.get_cooling_positions(account_id)
            if code in {p['code'] for p in cooling}:
                return False, "在冷却期内"

            # 持仓数上限
            positions = self.pm.get_active_positions(account_id)
            config = self.account_manager.get_position_config(account_id)
            if len(positions) >= config['max_holdings']:
                return False, f"持仓数已达上限({config['max_holdings']})"

            # 单日开仓数
            today_opens = self.pm.count_today_opens(account_id, target_date=target_date)
            max_daily = config.get('max_daily_open', 2)
            if today_opens >= max_daily:
                return False, f"今日已开仓{today_opens}个(上限{max_daily})"

        elif command.action == TradeAction.ADD:
            pos = self.pm.get_position(account_id, code)
            if not pos:
                return False, "持仓不存在"
            if pos['turtle_units'] >= 4:
                return False, "已达4单位上限"

        elif command.action == TradeAction.REDUCE:
            pos = self.pm.get_position(account_id, code)
            if not pos:
                return False, "持仓不存在"
            if pos.get('has_reduced', 0):
                return False, "已减过仓"
            if pos['turtle_units'] < 2:
                return False, f"仅{pos['turtle_units']}单位，无法减仓"

        elif command.action in (TradeAction.CLOSE, TradeAction.CLOSE_STOP_LOSS, TradeAction.CLOSE_TAKE_PROFIT):
            pos = self.pm.get_position(account_id, code)
            if not pos:
                return False, "持仓不存在"

        return True, ""

    # ==================== 动作队列转换 ====================

    def _to_commands(self, action_queue: list) -> list:
        """将SignalChecker动作队列转换为TradeCommand列表"""
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

            if action == TradeAction.CLOSE:
                reason = item.get('reason', '')
                if reason == 'stop_loss':
                    action = TradeAction.CLOSE_STOP_LOSS
                elif reason == 'take_profit':
                    action = TradeAction.CLOSE_TAKE_PROFIT

            cmd = TradeCommand(
                action=action,
                code=item.get('code', ''),
                name=item.get('name', ''),
                price=item.get('price', 0),
                atr=item.get('atr', 0),
                shares=item.get('shares', 0),
                reason=item.get('signal_type', ''),
                turtle_entry_system=item.get('turtle_entry_system'),
                source='robot_signal',
                metadata={
                    'urgency': item.get('urgency', ''),
                    'signal_type': item.get('signal_type', ''),
                    'position_params': item.get('position_params', {}),
                },
            )
            commands.append(cmd)

        return commands

    def _prioritize(self, commands: list) -> list:
        """按turtle优先级排序：止损 > 退出 > 减仓 > 加仓 > 开仓"""
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

        total_profit = sum(r.profit for r in results if r.success)
        if total_profit != 0:
            emoji = "📈" if total_profit > 0 else "📉"
            lines.append(f"  {emoji} 本次盈亏: {total_profit:.2f}")

        return '\n'.join(lines)
