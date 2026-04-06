"""
Trade Executor — 通用交易执行器
封装所有交易操作的执行逻辑，不绑定账户类型
人工账户和机器模拟账户均可通过此接口执行交易

职责：
  1. 接收 TradeCommand 指令
  2. 校验前置条件（资金/持仓/T+1锁定）
  3. 调用 PositionManager + AccountManager 执行
  4. 返回 TradeResult 结果

注意：
  - 不包含业务判断逻辑（何时该加减仓由调用方决定）
  - 包含执行层面的校验（资金够不够、持仓是否存在、T+1锁定等）
  - 费率计算委托给 PositionManager（已有实现）
"""

import logging
from datetime import datetime
from typing import Optional

from executor.models import TradeCommand, TradeResult, TradeAction, TradeStatus
from strategies.turtle.position_manager import PositionManager
from strategies.turtle.account_manager import AccountManager

logger = logging.getLogger(__name__)


class TradeExecutor:
    """
    通用交易执行器

    用法:
        executor = TradeExecutor()
        cmd = TradeCommand(action=TradeAction.CLOSE, code='000001', name='平安银行', price=12.50, reason='止损')
        result = executor.execute(account_id=12345, command=cmd)
    """

    def __init__(self):
        self.position_manager = PositionManager()
        self.account_manager = AccountManager()

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
        if is_new_position and holding_count >= max_holdings:
            will_exceed = True
            warnings.append(f"持仓标的数({holding_count})已达上限({max_holdings})，不可开新仓")

        # 检查2：加仓时检查目标标的的 unit 上限（海龟最大4单位）
        if target_code:
            target_pos = None
            for p in positions:
                if p['code'] == target_code:
                    target_pos = p
                    break
            if target_pos:
                current_units = target_pos.get('units', 0)
                if current_units >= 4:
                    will_exceed = True
                    warnings.append(f"[{target_code}]已{current_units}单位，已达上限(4)，不可加仓")
                elif current_units >= 3:
                    warnings.append(f"[{target_code}]已{current_units}单位，接近上限(4)")

        # 检查3：各持仓整除检查 + 实际仓位占比预警
        account_summary = self.account_manager.get_summary(account_id)
        total_capital = account_summary.get('total', 0) if account_summary else 0

        for pos in positions:
            units = pos.get('units', 0)
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
        开仓执行

        前置条件：
          1. 持仓中不能已有该股票（防重复开仓）
          2. 冷却中不能有该股票（防冷却期内重入）
          3. 价格 > 0 且 ATR > 0
          4. 账户可用资金充足（委托 PositionManager.open_position 校验）

        参数:
            account_id: 账户ID
            command: TradeCommand

        返回:
            TradeResult
        """
        code = command.code
        name = command.name
        price = command.price
        atr = command.atr
        system_type = command.system_type
        units = command.units if command.units > 0 else 1

        # 校验1：是否已持仓
        pos = self.position_manager.get_position(account_id, code)
        if pos:
            return TradeResult(
                success=False,
                status=TradeStatus.SKIPPED,
                command=command,
                units_before=pos['units'],
                error=f"已持有该股票({pos['units']}单位)，不可重复开仓",
            )

        # 校验2：是否在冷却期
        cooling = self.position_manager.get_cooling_positions(account_id)
        cooling_codes = {p['code'] for p in cooling}
        if code in cooling_codes:
            return TradeResult(
                success=False,
                status=TradeStatus.SKIPPED,
                command=command,
                error=f"该股票在冷却期内，不可开仓",
            )

        # 校验3：价格和ATR有效性
        if price <= 0:
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error=f"无效价格: {price}",
            )
        if atr <= 0:
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error=f"无效ATR: {atr}",
            )

        # 校验4：账户资金充足性（预估）
        from strategies.turtle.atr import calc_unit_size
        account_summary = self.account_manager.get_summary(account_id)
        capital = account_summary.get('total', 0) if account_summary else 0
        if capital <= 0:
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error="账户资金为0",
            )
        shares_per_unit = calc_unit_size(capital, atr, price)
        if shares_per_unit <= 0:
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error=f"计算仓位失败(capital={capital}, atr={atr}, price={price})",
            )
        estimated_shares = shares_per_unit * units
        estimated_cost = price * estimated_shares
        available = self.account_manager.get_available(account_id)
        if available < estimated_cost * 1.002:  # 预留0.2%费用空间
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error=f"资金不足(可用:{available:.2f}, 预估:{estimated_cost:.2f})",
            )

        # 执行开仓
        result_pos = self.position_manager.open_position(
            account_id=account_id,
            code=code,
            name=name,
            price=price,
            atr=atr,
            units=units,
            account_manager=self.account_manager,
            system_type=system_type,
        )

        if result_pos is None:
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error="开仓执行失败（PositionManager返回None）",
            )

        logger.info(f"[账户{account_id}] 开仓成功: {code} {name} {result_pos['total_shares']}股@{price}")

        return TradeResult(
            success=True,
            status=TradeStatus.SUCCESS,
            command=command,
            executed_shares=result_pos['total_shares'],
            executed_price=price,
            executed_amount=price * result_pos['total_shares'],
            fees=result_pos.get('fees', {}),
            profit=0.0,
            units_before=0,
            units_after=result_pos['units'],
            position_after=result_pos,
            message=f"开仓成功: {result_pos['total_shares']}股@{price:.2f}，{result_pos['units']}单位，止损{result_pos['current_stop']:.2f}",
        )

    # ==================== 加仓 ====================

    def _execute_add(self, account_id, command: TradeCommand) -> TradeResult:
        """
        加仓执行

        前置条件：
          1. 持仓必须存在且状态为HOLDING
          2. 持仓单位数 < 4（海龟最大4单位）
          3. 账户可用资金充足
          4. 价格 >= next_add_price（由调用方校验，此处只执行）

        参数:
            account_id: 账户ID
            command: TradeCommand

        返回:
            TradeResult
        """
        code = command.code
        price = command.price
        atr = command.atr

        # 校验1：持仓是否存在
        pos = self.position_manager.get_position(account_id, code)
        if not pos:
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error=f"持仓不存在: {code}",
            )

        # 校验2：持仓单位数上限
        if pos['units'] >= 4:
            return TradeResult(
                success=False,
                status=TradeStatus.SKIPPED,
                command=command,
                units_before=pos['units'],
                message=f"已满4单位，无法继续加仓",
            )

        # 校验3：价格有效性
        if price <= 0:
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error=f"无效价格: {price}",
            )

        # 执行加仓
        result_pos = self.position_manager.add_position(
            account_id=account_id,
            code=code,
            new_price=price,
            atr=atr,
            account_manager=self.account_manager,
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
            units_before=pos['units'],
            units_after=result_pos['units'],
            position_after=result_pos,
            message=f"加仓成功: {added_shares}股@{price:.2f}，当前{result_pos['units']}单位",
        )

    # ==================== 减仓 ====================

    def _execute_reduce(self, account_id, command: TradeCommand) -> TradeResult:
        """
        减仓执行（卖出1单位）

        前置条件：
          1. 持仓必须存在且状态为HOLDING
          2. 持仓单位数 >= 2
          3. 未减过仓（has_reduced=0）
          4. T+1锁定检查：今日买入部分不可卖

        参数:
            account_id: 账户ID
            command: TradeCommand

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

        # 校验2：单位数
        if pos['units'] < 2:
            return TradeResult(
                success=False,
                status=TradeStatus.SKIPPED,
                command=command,
                units_before=pos['units'],
                message=f"仅{pos['units']}单位，无法减仓",
            )

        # 校验3：是否已减过仓
        if pos.get('has_reduced', 0):
            return TradeResult(
                success=False,
                status=TradeStatus.SKIPPED,
                command=command,
                units_before=pos['units'],
                message="已减过仓，不可重复减仓",
            )

        # 校验4：T+1锁定
        t1_status = self.position_manager.get_position_status(account_id, code)
        if t1_status and t1_status['locked_shares'] > 0:
            shares_per_unit = pos['total_shares'] // pos['units']
            if t1_status['available_shares'] < shares_per_unit:
                return TradeResult(
                    success=False,
                    status=TradeStatus.SKIPPED,
                    command=command,
                    units_before=pos['units'],
                    message=f"T+1锁定: 可卖{t1_status['available_shares']}股 < 减仓需{shares_per_unit}股",
                )

        # 校验5：价格有效性
        if price <= 0:
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error=f"无效价格: {price}",
            )

        # 执行减仓
        result_pos = self.position_manager.reduce_position(
            account_id=account_id,
            code=code,
            sell_price=price,
            account_manager=self.account_manager,
        )

        if result_pos is None:
            return TradeResult(
                success=False,
                status=TradeStatus.FAILED,
                command=command,
                error="减仓执行失败",
            )

        # 推算减仓股数和盈亏
        reduced_shares = pos['total_shares'] - result_pos['total_shares']
        profit = (price - pos['avg_cost']) * reduced_shares

        logger.info(f"[账户{account_id}] 减仓成功: {code} -{reduced_shares}股@{price} 盈亏={profit:.2f}")

        return TradeResult(
            success=True,
            status=TradeStatus.SUCCESS,
            command=command,
            executed_shares=reduced_shares,
            executed_price=price,
            executed_amount=price * reduced_shares,
            profit=round(profit, 2),
            units_before=pos['units'],
            units_after=result_pos['units'],
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
                units_before=pos['units'],
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
            reason = 'exit'
        else:
            reason = command.reason if command.reason in ('stop_loss', 'exit') else 'exit'

        # 计算实际可卖股数（T+1限制）
        can_sell_shares = t1_status['available_shares'] if t1_status else pos['total_shares']
        locked_shares = t1_status['locked_shares'] if t1_status else 0

        # 如果部分锁定，做部分平仓
        if locked_shares > 0 and can_sell_shares < pos['total_shares']:
            logger.info(f"[{code}] T+1锁定: 总持仓{pos['total_shares']}股, 可卖{can_sell_shares}股, 锁定{locked_shares}股")
            # 部分平仓：先卖出可卖部分
            return self._partial_close(account_id, command, pos, can_sell_shares, locked_shares, reason)

        # 全部可卖，正常平仓
        result = self.position_manager.close_position(
            account_id=account_id,
            code=code,
            reason=reason,
            sell_price=price,
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
            units_before=pos['units'],
            units_after=0,
            message=f"平仓成功: {result['shares']}股@{price:.2f} 净利={result['net_profit']:.2f} ({result.get('cooldown_until', '')}冷却)",
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

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        from core.storage import get_db_connection

        conn = get_db_connection()
        try:
            # 更新持仓：减少可卖股数
            new_total = pos['total_shares'] - can_sell_shares
            # 重新计算单位数（保持比例）
            if pos['total_shares'] > 0:
                remaining_ratio = new_total / pos['total_shares']
                new_units = max(1, int(pos['units'] * remaining_ratio))
            else:
                new_units = 0

            conn.execute("""
                UPDATE turtle_positions SET
                    total_shares = ?,
                    units = ?,
                    updated_at = ?
                WHERE account_id = ? AND code = ? AND status = 'HOLDING'
            """, (new_total, new_units, now, account_id, code))

            # 写流水
            self.position_manager._write_flow(
                conn, account_id, code, pos['name'], '部分平仓',
                shares=can_sell_shares, price=price, amount=sell_amount,
                profit=round(net_profit, 2), fees=fees['total'],
                units_before=pos['units'], units_after=new_units,
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
            units_before=pos['units'],
            units_after=new_units,
            message=f"部分平仓: {can_sell_shares}股@{price:.2f} 净利={net_profit:.2f} (锁定{locked_shares}股待下个交易日)",
        )
