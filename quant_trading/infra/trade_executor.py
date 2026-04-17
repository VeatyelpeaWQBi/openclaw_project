"""
Trade Executor — 通用交易执行器（基础设施层）
纯通用执行逻辑，不绑定任何交易策略

职责：
  1. 接收 TradeCommand 指令
  2. 校验基本前置条件（价格有效、持仓存在、T+1锁定）
  3. 调用 PositionManager + AccountManager 执行
  4. 返回 TradeResult 结果

注意：
  - 不包含任何策略特有的业务判断
  - 所有 turtle 特有逻辑在 TrendTradingExecutor 中
  - 手工账户可直接调用本执行器
"""

import logging
from datetime import datetime
from typing import Optional

from infra.models import TradeCommand, TradeResult, TradeAction, TradeStatus
from infra.position_manager import PositionManager
from infra.account_manager import AccountManager

logger = logging.getLogger(__name__)


class TradeExecutor:
    """
    通用交易执行器

    用法:
        executor = TradeExecutor()
        cmd = TradeCommand(action=TradeAction.CLOSE, code='000001', name='平安银行', price=12.50, reason='止损')
        result = executor.execute(account_id=12345, command=cmd)
    """

    _target_date = None  # 由 strategy.py 注入

    def set_target_date(self, target_date):
        """设置回测目标日期"""
        self._target_date = target_date

    def __init__(self):
        self.position_manager = PositionManager()
        self.account_manager = AccountManager()

    def _now(self):
        """获取当前时间戳（回测时用 target_date）"""
        if self._target_date:
            return f"{self._target_date} 00:00:00"
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # ==================== 公开接口 ====================

    def execute(self, account_id, command: TradeCommand) -> TradeResult:
        """
        执行一条交易指令（统一入口）

        参数:
            account_id: 账户ID
            command: 交易指令

        返回:
            TradeResult: 执行结果
        """
        logger.info(f"[账户{account_id}] 收到指令: {command}")

        dispatch = {
            TradeAction.OPEN: self._execute_open,
            TradeAction.ADD: self._execute_add,
            TradeAction.REDUCE: self._execute_reduce,
            TradeAction.CLOSE: self._execute_close,
            TradeAction.CLOSE_STOP_LOSS: self._execute_close,
            TradeAction.CLOSE_TAKE_PROFIT: self._execute_close,
        }

        handler = dispatch.get(command.action)
        if handler is None:
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error=f"未知交易动作: {command.action}",
            )

        return handler(account_id, command)

    def execute_batch(self, account_id, commands: list) -> list:
        """
        批量执行交易指令

        执行顺序：先平仓/减仓（释放资金），后加仓/开仓（消耗资金）
        同类型按传入顺序执行

        参数:
            account_id: 账户ID
            commands: 交易指令列表

        返回:
            list[TradeResult]: 执行结果列表
        """
        if not commands:
            return []

        # 分组：释放资金的动作优先执行
        sell_actions = {TradeAction.REDUCE, TradeAction.CLOSE, TradeAction.CLOSE_STOP_LOSS, TradeAction.CLOSE_TAKE_PROFIT}
        buy_actions = {TradeAction.OPEN, TradeAction.ADD}

        sell_cmds = [c for c in commands if c.action in sell_actions]
        buy_cmds = [c for c in commands if c.action in buy_actions]

        results = []

        # 第一轮：卖出类操作（释放资金）
        for cmd in sell_cmds:
            result = self.execute(account_id, cmd)
            results.append(result)

        # 第二轮：买入类操作（消耗资金）
        for cmd in buy_cmds:
            result = self.execute(account_id, cmd)
            results.append(result)

        return results


    # ==================== 仓位控制 ====================

    def check_position_limits(self, account_id, is_new_position=False, target_code=None):
        """
        检查账户仓位控制状态

        参数:
            account_id: 账户ID
            is_new_position: 是否为新开仓（增加持仓标的数）
            target_code: 加仓时的目标标的代码（检查该标的unit上限）

        返回:
            dict: {
                'holding_count': 当前持仓标的数,
                'max_holdings': 最大持仓标的数,
                'unit_pct': 单位仓位百分比,
                'will_exceed': 本次操作后是否超限,
                'warnings': list[str] 警告信息列表,
            }
        """
        config = self.account_manager.get_position_config(account_id)
        unit_pct = config['unit_pct']
        max_holdings = config['max_holdings']

        # 当前持仓标的数
        positions = self.position_manager.get_active_positions(account_id)
        holding_count = len(positions)

        warnings = []
        will_exceed = False

        # 检查1：开仓时检查持仓标的数上限
        max_daily_open = config.get('max_daily_open', 2)
        if is_new_position:
            if holding_count >= max_holdings:
                will_exceed = True
                warnings.append(f"持仓标的数({holding_count})已达上限({max_holdings})，不可开新仓")
            # 单日开仓数检查
            today_opens = self.position_manager.count_today_opens(account_id)
            if today_opens >= max_daily_open:
                will_exceed = True
                warnings.append(f"今日已开仓{today_opens}个标的，达到单日上限({max_daily_open})")

        # 检查2：加仓时检查目标标的的 unit 上限（海龟最大4单位）
        if target_code:
            target_pos = None
            for p in positions:
                if p['code'] == target_code:
                    target_pos = p
                    break
            if target_pos:
                current_units = target_pos.get('turtle_units', 0)
                if current_units >= 4:
                    will_exceed = True
                    warnings.append(f"[{target_code}]已{current_units}单位，已达上限(4)，不可加仓")
                elif current_units >= 3:
                    warnings.append(f"[{target_code}]已{current_units}单位，接近上限(4)")

        # 检查3：各持仓整除检查 + 实际仓位占比预警
        account_summary = self.account_manager.get_summary(account_id)
        total_capital = account_summary.get('total_capital', 0) if account_summary else 0

        for pos in positions:
            units = pos.get('turtle_units', 0)
            code = pos.get('code', '')
            name = pos.get('name', '')
            total_shares = pos.get('total_shares', 0)
            avg_cost = pos.get('avg_cost', 0)

            # 实际持仓金额占比
            position_value = total_shares * avg_cost
            actual_pct = (position_value / total_capital * 100) if total_capital > 0 else 0
            planned_pct = units * unit_pct
            over_pct = actual_pct - planned_pct

            if abs(over_pct) > 1:
                sign = "+" if over_pct > 0 else ""
                warnings.append(f"[{code}]{name} {units}单位{total_shares}股，{actual_pct:.1f}%({sign}{over_pct:.1f}%vs计划{planned_pct:.1f}%)")

            # 整除检查
            spu = pos.get('shares_per_unit', 0)
            if not spu and units > 0:
                spu = total_shares // units
            if spu > 0 and total_shares > 0:
                remainder = total_shares % spu
                if remainder != 0:
                    warnings.append(f"[{code}]{name} 持仓{total_shares}股/{spu}股/单位，余{remainder}股不整除")

        return {
            'holding_count': holding_count,
            'max_holdings': max_holdings,
            'unit_pct': unit_pct,
            'will_exceed': will_exceed,
            'warnings': warnings,
        }

    # ==================== 开仓 ====================

    def _execute_open(self, account_id, command: TradeCommand) -> TradeResult:
        """
        开仓执行（通用）

        前置条件：
          1. 价格 > 0
          2. 账户可用资金充足（委托 PositionManager.open_position 校验）

        策略层校验（持仓重复、冷却期、仓位计算等）由 TrendTradingExecutor 负责
        """
        code = command.code
        name = command.name
        price = command.price
        atr = command.atr
        units = command.units if command.units > 0 else 1
        turtle_entry_system = command.turtle_entry_system

        # 校验：价格有效性
        if price <= 0:
            return TradeResult(
                success=False, status=TradeStatus.FAILED,
                command=command, error=f"无效价格: {price}",
            )

        # 从command获取预计算的仓位参数（由 TrendTradingExecutor 填入）
        pp = command.metadata.get('position_params', {})
        total_shares = pp.get('total_shares', 0)
        shares_per_unit = pp.get('shares_per_unit', 0)
        stop_price = pp.get('stop_price', 0)
        next_add_price = pp.get('next_add_price', 0)

        # 如果没有预计算值（手工直接调用），用简单估算
        if total_shares <= 0:
            total_shares = command.shares or units * 100  # 最小1手
            shares_per_unit = total_shares // units if units > 0 else total_shares
            stop_price = 0
            next_add_price = 0

        # 执行开仓
        result_pos = self.position_manager.open_position(
            account_id=account_id, code=code, name=name, price=price,
            total_shares=total_shares, stop_price=stop_price,
            next_add_price=next_add_price, shares_per_unit=shares_per_unit,
            account_manager=self.account_manager, units=units, atr=atr,
            entry_system=turtle_entry_system, exit_price=0.0,
        )

        if result_pos is None:
            return TradeResult(
                success=False, status=TradeStatus.FAILED,
                command=command, error="开仓执行失败（PositionManager返回None）",
            )

        logger.info(f"[账户{account_id}] 开仓成功: {code} {name} {result_pos['total_shares']}股@{price}")

        return TradeResult(
            success=True, status=TradeStatus.SUCCESS, command=command,
            executed_shares=result_pos['total_shares'], executed_price=price,
            executed_amount=price * result_pos['total_shares'],
            fees=result_pos.get('fees', {}), profit=0.0,
            units_before=0, units_after=result_pos['turtle_units'],
            position_after=result_pos,
            message=f"开仓成功: {result_pos['total_shares']}股@{price:.2f}",
        )

    # ==================== 加仓 ====================

    def _execute_add(self, account_id, command: TradeCommand) -> TradeResult:
        """
        加仓执行（通用）

        前置条件：
          1. 持仓必须存在且状态为HOLDING
          2. 账户可用资金充足
          3. 价格 > 0

        单位数上限等策略校验由 TrendTradingExecutor 负责
        """
        code = command.code
        price = command.price
        atr = command.atr

        # 校验1：持仓是否存在
        pos = self.position_manager.get_position(account_id, code)
        if not pos:
            return TradeResult(
                success=False, status=TradeStatus.FAILED,
                command=command, error=f"持仓不存在: {code}",
            )

        # 校验2：价格有效性
        if price <= 0:
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error=f"无效价格: {price}",
            )

        # 从command获取预计算参数
        pp = command.metadata.get('position_params', {})
        shares_per_unit = pp.get('shares_per_unit', pos.get('shares_per_unit', 0))
        new_stop_price = pp.get('stop_price', 0)
        new_next_add_price = pp.get('next_add_price', 0)

        # 执行加仓
        result_pos = self.position_manager.add_position(
            account_id=account_id, code=code, new_price=price,
            shares_per_unit=shares_per_unit,
            new_stop_price=new_stop_price, new_next_add_price=new_next_add_price,
            account_manager=self.account_manager, atr=atr,
        )

        if result_pos is None:
            # 可能是资金不足
            available = self.account_manager.get_available(account_id)
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error=f"加仓失败（可能资金不足，可用: {available:.2f}）",
            )

        # 计算本次加仓的股数（从前后差值推算）
        added_shares = result_pos['total_shares'] - pos['total_shares']

        logger.info(f"[账户{account_id}] 加仓成功: {code} +{added_shares}股@{price}")

        return TradeResult(
            success=True,
            status=TradeStatus.SUCCESS,
            command=command,
            executed_shares=added_shares,
            executed_price=price,
            executed_amount=price * added_shares,
            units_before=pos['turtle_units'],
            units_after=result_pos['turtle_units'],
            position_after=result_pos,
            message=f"加仓成功: {added_shares}股@{price:.2f}，当前{result_pos['turtle_units']}单位",
        )

    # ==================== 减仓 ====================

    def _execute_reduce(self, account_id, command: TradeCommand) -> TradeResult:
        """
        减仓执行（通用）

        前置条件：
          1. 持仓必须存在且状态为HOLDING
          2. T+1锁定检查：今日买入部分不可卖
          3. 价格 > 0

        策略层校验（单位数、has_reduced等）由 TrendTradingExecutor 负责
        """
        code = command.code
        price = command.price

        # 校验1：持仓是否存在
        pos = self.position_manager.get_position(account_id, code)
        if not pos:
            return TradeResult(
                success=False, status=TradeStatus.FAILED,
                command=command, error=f"持仓不存在: {code}",
            )

        # 确定减仓股数
        shares_to_sell = command.shares or pos.get('shares_per_unit', pos['total_shares'])

        # 校验2：T+1锁定
        t1_status = self.position_manager.get_position_status(account_id, code)
        if t1_status and t1_status['locked_shares'] > 0:
            if t1_status['available_shares'] < shares_to_sell:
                return TradeResult(
                    success=False, status=TradeStatus.SKIPPED,
                    command=command,
                    units_before=pos['turtle_units'],
                    message=f"T+1锁定: 可卖{t1_status['available_shares']}股 < 需要{shares_to_sell}股",
                )

        # 校验3：价格有效性
        if price <= 0:
            return TradeResult(
                success=False, status=TradeStatus.FAILED,
                command=command, error=f"无效价格: {price}",
            )

        # 执行减仓
        result_pos = self.position_manager.reduce_position(
            account_id=account_id, code=code, sell_price=price,
            shares_to_sell=shares_to_sell, account_manager=self.account_manager,
        )

        if result_pos is None:
            return TradeResult(
                success=False, status=TradeStatus.FAILED,
                command=command, error="减仓执行失败",
            )

        reduced_shares = pos['total_shares'] - result_pos['total_shares']
        profit = (price - pos['avg_cost']) * reduced_shares

        logger.info(f"[账户{account_id}] 减仓成功: {code} -{reduced_shares}股@{price} 盈亏={profit:.2f}")

        return TradeResult(
            success=True, status=TradeStatus.SUCCESS, command=command,
            executed_shares=reduced_shares, executed_price=price,
            executed_amount=price * reduced_shares, profit=round(profit, 2),
            units_before=pos['turtle_units'], units_after=result_pos['turtle_units'],
            position_after=result_pos,
            message=f"减仓成功: {reduced_shares}股@{price:.2f}，盈亏={profit:.2f}",
        )

    # ==================== 平仓 ====================

    def _execute_close(self, account_id, command: TradeCommand) -> TradeResult:
        """
        平仓执行（清仓全部卖出）

        前置条件：
          1. 持仓必须存在且状态为HOLDING
          2. T+1锁定检查：全部可卖股数 > 0
          3. 注意：如果今日有加仓，加仓部分不可卖

        参数:
            account_id: 账户ID
            command: TradeCommand（action决定平仓原因）

        返回:
            TradeResult
        """
        code = command.code
        price = command.price

        # 校验1：持仓是否存在
        pos = self.position_manager.get_position(account_id, code)
        if not pos:
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error=f"持仓不存在: {code}",
            )

        # 校验2：T+1锁定
        t1_status = self.position_manager.get_position_status(account_id, code)
        if t1_status and t1_status['available_shares'] <= 0:
            return TradeResult(
                success=False,
                status=TradeStatus.SKIPPED,
                command=command,
                units_before=pos['turtle_units'],
                message=f"T+1锁定: 全部{t1_status['total_shares']}股均不可卖",
            )

        # 校验3：价格有效性
        if price <= 0:
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error=f"无效价格: {price}",
            )

        # 确定平仓原因
        if command.action == TradeAction.CLOSE_STOP_LOSS:
            reason = 'stop_loss'
        elif command.action == TradeAction.CLOSE_TAKE_PROFIT:
            reason = 'take_profit'
        else:
            reason = command.reason if command.reason in ('stop_loss', 'exit', 'take_profit') else 'exit'

        # 计算实际可卖股数（T+1限制）
        can_sell_shares = t1_status['available_shares'] if t1_status else pos['total_shares']
        locked_shares = t1_status['locked_shares'] if t1_status else 0

        # 如果部分锁定，做部分平仓
        if locked_shares > 0 and can_sell_shares < pos['total_shares']:
            logger.info(f"[{code}] T+1锁定: 总持仓{pos['total_shares']}股, 可卖{can_sell_shares}股, 锁定{locked_shares}股")
            # 部分平仓：先卖出可卖部分
            return self._partial_close(account_id, command, pos, can_sell_shares, locked_shares, reason)

        # 全部可卖，正常平仓
        # cooldown_days 由策略层决定，通用默认10天
        cooldown_days = command.metadata.get('cooldown_days', 10)
        result = self.position_manager.close_position(
            account_id=account_id, code=code, reason=reason,
            sell_price=price, cooldown_days=cooldown_days,
            account_manager=self.account_manager,
        )

        if result is None:
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error="平仓执行失败",
            )

        logger.info(f"[账户{account_id}] 平仓成功: {code} {result.get('name', '')} 净利={result['net_profit']:.2f}")

        return TradeResult(
            success=True,
            status=TradeStatus.SUCCESS,
            command=command,
            executed_shares=result['shares'],
            executed_price=price,
            executed_amount=price * result['shares'],
            fees=result.get('fees', {}),
            profit=result['net_profit'],
            units_before=pos['turtle_units'],
            units_after=0,
            message=f"平仓成功: {result['shares']}股@{price:.2f} 净利={result['net_profit']:.2f} ({result.get('cooldown_until', '')}冷却)",
            close_reason=reason,  # 记录平仓真实原因
        )

    def _partial_close(self, account_id, command, pos, can_sell_shares, locked_shares, reason):
        """
        部分平仓（T+1场景：今日有加仓，只能卖出可卖部分）

        策略：
          - 可卖部分：手动计算盈亏，更新持仓数据
          - 锁定部分：保持持仓，等待下一个交易日
        """
        code = command.code
        price = command.price

        # 计算可卖部分的盈亏
        sell_amount = price * can_sell_shares
        avg_cost = pos['avg_cost']
        gross_profit = (price - avg_cost) * can_sell_shares

        # 费用计算（复用 PositionManager 的费率逻辑）
        fees = self.position_manager._calc_fees(sell_amount, is_sell=True)
        net_profit = gross_profit - fees['total']

        now = self._now()
        from core.storage import get_db_connection

        conn = get_db_connection()
        try:
            # 更新持仓：减少可卖股数
            new_total = pos['total_shares'] - can_sell_shares
            # 重新计算单位数（保持比例）
            if pos['total_shares'] > 0:
                remaining_ratio = new_total / pos['total_shares']
                new_units = max(1, int(pos['turtle_units'] * remaining_ratio))
            else:
                new_units = 0

            conn.execute("""
                UPDATE positions SET
                    total_shares = ?,
                    turtle_units = ?,
                    has_reduced = 1,
                    updated_at = ?
                WHERE account_id = ? AND code = ? AND status = 'HOLDING'
            """, (new_total, new_units, now, account_id, code))

            # 写流水
            self.position_manager._write_flow(
                conn, account_id, code, pos['name'], '部分平仓',
                shares=can_sell_shares, price=price, amount=sell_amount,
                profit=round(net_profit, 2), fees=fees['total'],
                units_before=pos['turtle_units'], units_after=new_units,
                reason=f"{reason}(T+1部分平仓, 锁定{locked_shares}股)"
            )

            conn.commit()
        finally:
            conn.close()

        # 更新账户资金
        self.account_manager.on_sell(account_id, sell_amount - fees['total'], net_profit)

        logger.info(f"[账户{account_id}] 部分平仓: {code} 卖出{can_sell_shares}股@{price} 净利={net_profit:.2f} (锁定{locked_shares}股)")

        return TradeResult(
            success=True,
            status=TradeStatus.PARTIAL,
            command=command,
            executed_shares=can_sell_shares,
            executed_price=price,
            executed_amount=sell_amount,
            fees=fees,
            profit=round(net_profit, 2),
            units_before=pos['turtle_units'],
            units_after=new_units,
            message=f"部分平仓: {can_sell_shares}股@{price:.2f} 净利={net_profit:.2f} (锁定{locked_shares}股待下个交易日)",
            close_reason=reason,
        )
