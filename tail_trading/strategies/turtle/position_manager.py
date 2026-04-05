"""
海龟交易法 — 持仓管理器
封装 turtle_positions 表的 CRUD 操作
"""

import sqlite3
import logging
from datetime import datetime, timedelta
from core.storage import get_db_connection
from strategies.turtle.atr import calc_stop_price, calc_add_price, calc_unit_size, calc_exit_price

logger = logging.getLogger(__name__)


class PositionManager:
    """海龟交易法持仓管理器"""

    @staticmethod
    def _require_account_id(account_id):
        """校验account_id必须传入"""
        if account_id is None:
            raise ValueError("account_id 不能为空，请传入账户ID")

    # 费率常量
    COMMISSION_RATE = 0.00012    # 佣金费率 万1.2
    COMMISSION_MIN = 5.0         # 佣金最低5元
    STAMP_TAX_RATE = 0.0005      # 印花税 万分之五（仅卖出）
    TRANSFER_FEE_RATE = 0.00001  # 过户费 万分之0.1

    def _calc_fees(self, amount, is_sell=False):
        """
        计算交易费用

        参数:
            amount: 交易金额（价格 × 股数）
            is_sell: 是否为卖出（卖出额外收印花税）

        返回:
            dict: {
                'commission': 佣金,
                'stamp_tax': 印花税,
                'transfer_fee': 过户费,
                'total': 总费用
            }
        """
        # 佣金：万1.2，最低5元
        commission = max(amount * self.COMMISSION_RATE, self.COMMISSION_MIN)

        # 印花税：仅卖出时收取
        stamp_tax = amount * self.STAMP_TAX_RATE if is_sell else 0.0

        # 过户费
        transfer_fee = amount * self.TRANSFER_FEE_RATE

        total = commission + stamp_tax + transfer_fee
        return {
            'commission': round(commission, 2),
            'stamp_tax': round(stamp_tax, 2),
            'transfer_fee': round(transfer_fee, 2),
            'total': round(total, 2),
        }

    def _write_flow(self, conn, account_id, code, name, action, shares=0, price=0,
                    amount=0, profit=0, fees=0, units_before=0, units_after=0,
                    stop_price=0, reason=None):
        """
        写入持仓流水记录

        参数:
            conn: 数据库连接
            account_id: 账户ID
            code: 股票代码
            name: 股票名称
            action: 动作（开仓/加仓/减仓/清仓止损/清仓止盈）
            shares: 本次涉及股数
            price: 本次价格
            amount: 本次金额
            profit: 本次盈亏
            units_before: 操作前单位数
            units_after: 操作后单位数
            stop_price: 止损价
            reason: 原因说明
        """
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute("""
            INSERT INTO turtle_position_flow
            (account_id, code, name, action, shares, price, amount, profit, fees,
             units_before, units_after, stop_price, reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (account_id, code, name, action, shares, price, amount, profit, fees,
              units_before, units_after, stop_price, reason, now))

    def get_active_positions(self, account_id):
        """
        获取指定账户所有持有中的持仓

        参数:
            account_id: 账户ID

        返回:
            list: 持仓记录列表（dict）
        """
        self._require_account_id(account_id)
        conn = get_db_connection()
        try:
            rows = conn.execute(
                "SELECT * FROM turtle_positions WHERE account_id = ? AND status = 'HOLDING' ORDER BY opened_at",
                (account_id,)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_cooling_positions(self, account_id):
        """
        获取指定账户冷却中的持仓

        参数:
            account_id: 账户ID

        返回:
            list: 冷却中持仓列表
        """
        self._require_account_id(account_id)
        conn = get_db_connection()
        try:
            rows = conn.execute(
                "SELECT * FROM turtle_positions WHERE account_id = ? AND status = 'COOLING' ORDER BY cooldown_until",
                (account_id,)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_position(self, account_id, code):
        """
        获取单只持仓详情

        参数:
            account_id: 账户ID
            code: 股票代码

        返回:
            dict or None: 持仓记录
        """
        self._require_account_id(account_id)
        conn = get_db_connection()
        try:
            row = conn.execute(
                "SELECT * FROM turtle_positions WHERE account_id = ? AND code = ? AND status = 'HOLDING'",
                (account_id, code)
            ).fetchone()
            if row:
                return dict(row)
            return None
        finally:
            conn.close()

    def open_position(self, account_id, code, name, price, atr, units=1, account_manager=None, system_type=None):
        """
        开仓

        参数:
            account_id: 账户ID
            code: 股票代码
            name: 股票名称
            price: 入场价
            atr: ATR值
            units: 买入单位数，默认1
            account_manager: AccountManager实例（可选）
            system_type: 'S1'(20日突破) 或 'S2'(55日突破)

        返回:
            dict: 新建的持仓记录
        """
        self._require_account_id(account_id)

        # 计算每单位股数
        if account_manager:
            summary = account_manager.get_summary(account_id)
            capital = summary.get("total", 0) if summary else 100000
        else:
            capital = 100000  # 默认10万

        shares_per_unit = calc_unit_size(capital, atr, price)
        total_shares = shares_per_unit * units
        trade_amount = price * total_shares

        # 计算买入费用
        fees = self._calc_fees(trade_amount, is_sell=False)
        total_cost = trade_amount + fees['total']

        # 检查可用资金（含费用）
        if account_manager and account_manager.get_available(account_id) < total_cost:
            logger.warning(f"[账户{account_id}] 开仓失败：资金不足 (需要{total_cost:.2f})")
            return None

        stop_price = calc_stop_price(price, atr)
        next_add = calc_add_price(price, atr)
        exit_p = 0.0  # 退出价在运行时从K线计算

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        conn = get_db_connection()
        try:
            cursor = conn.execute("""
                INSERT INTO turtle_positions
                (account_id, code, name, status, units, total_shares, avg_cost, entry_price,
                 last_add_price, current_stop, next_add_price, exit_price, atr_value,
                 system_type, opened_at, updated_at)
                VALUES (?, ?, ?, 'HOLDING', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (account_id, code, name, units, total_shares, price, price, price, stop_price, next_add, exit_p, atr, system_type, now, now))
            pos_id = cursor.lastrowid

            # 写持仓流水
            self._write_flow(conn, account_id, code, name, '开仓',
                            shares=total_shares, price=price, amount=trade_amount,
                            profit=0, fees=fees['total'],
                            units_before=0, units_after=units, stop_price=stop_price)

            conn.commit()
            logger.info(f"[账户{account_id}] 开仓 {code} {name} {total_shares}股@{price} 费用={fees['total']:.2f}")
        finally:
            conn.close()

        # 扣减资金（含费用）
        if account_manager:
            account_manager.on_buy(account_id, total_cost)

        return {
            'id': pos_id,
            'code': code,
            'name': name,
            'units': units,
            'total_shares': total_shares,
            'avg_cost': price,
            'entry_price': price,
            'current_stop': stop_price,
            'next_add_price': next_add,
            'atr_value': atr,
            'fees': fees,
        }

    def add_position(self, account_id, code, new_price, atr, account_manager=None):
        """
        加仓（增加1单位）

        参数:
            account_id: 账户ID
            code: 股票代码
            new_price: 当前价格
            atr: ATR值
            account_manager: AccountManager实例（可选）

        返回:
            dict or None: 更新后的持仓记录
        """
        self._require_account_id(account_id)

        pos = self.get_position(account_id, code)
        if not pos:
            return None

        # 计算加仓股数
        if account_manager:
            summary = account_manager.get_summary(account_id)
            capital = summary.get("total", 0) if summary else 100000
        else:
            capital = 100000

        shares_per_unit = calc_unit_size(capital, atr, new_price)
        trade_amount = new_price * shares_per_unit

        # 计算买入费用
        fees = self._calc_fees(trade_amount, is_sell=False)
        total_cost = trade_amount + fees['total']

        # 检查可用资金（含费用）
        if account_manager and account_manager.get_available(account_id) < total_cost:
            logger.warning(f"[账户{account_id}] 加仓失败：资金不足 (需要{total_cost:.2f})")
            return None

        # 更新持仓
        old_units = pos['units']
        new_units = old_units + 1
        new_total = pos['total_shares'] + shares_per_unit
        new_avg = (pos['avg_cost'] * pos['total_shares'] + new_price * shares_per_unit) / new_total
        new_stop = calc_stop_price(new_avg, atr)
        next_add = calc_add_price(new_price, atr)

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = get_db_connection()
        try:
            conn.execute("""
                UPDATE turtle_positions SET
                    units = ?, total_shares = ?, avg_cost = ?,
                    last_add_price = ?, current_stop = ?, next_add_price = ?,
                    atr_value = ?, updated_at = ?
                WHERE account_id = ? AND code = ? AND status = 'HOLDING'
            """, (new_units, new_total, round(new_avg, 2), new_price, new_stop, next_add, atr, now, account_id, code))

            # 写持仓流水
            self._write_flow(conn, account_id, code, pos['name'], '加仓',
                            shares=shares_per_unit, price=new_price, amount=trade_amount,
                            profit=0, fees=fees['total'],
                            units_before=old_units, units_after=new_units, stop_price=new_stop)

            conn.commit()
            logger.info(f"[账户{account_id}] 加仓 {code} {shares_per_unit}股@{new_price} 费用={fees['total']:.2f}")
        finally:
            conn.close()

        # 扣减资金（含费用）
        if account_manager:
            account_manager.on_buy(account_id, total_cost)

        return self.get_position(account_id, code)

    def reduce_position(self, account_id, code, sell_price, account_manager=None):
        """
        减仓（卖出1单位，仅执行一次）

        海龟法则：盈利达1N时减1单位

        参数:
            account_id: 账户ID
            code: 股票代码
            sell_price: 卖出价
            account_manager: AccountManager实例（可选）

        返回:
            dict or None: 减仓记录
        """
        self._require_account_id(account_id)
        pos = self.get_position(account_id, code)
        if not pos:
            return None

        # 已减过仓
        if pos.get('has_reduced', 0):
            logger.warning(f"[{code}] 已减过仓，跳过")
            return None

        # 至少2单位
        if pos['units'] < 2:
            logger.warning(f"[{code}] 仅{pos['units']}单位，无法减仓")
            return None

        # 计算减仓1单位的股数
        shares_per_unit = pos['total_shares'] // pos['units']
        if shares_per_unit <= 0:
            return None

        trade_amount = sell_price * shares_per_unit
        fees = self._calc_fees(trade_amount, is_sell=True)
        net_proceeds = trade_amount - fees['total']
        profit = (sell_price - pos['avg_cost']) * shares_per_unit - fees['total']

        new_units = pos['units'] - 1
        new_total = pos['total_shares'] - shares_per_unit

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = get_db_connection()
        try:
            conn.execute("""
                UPDATE turtle_positions SET
                    units = ?, total_shares = ?, has_reduced = 1,
                    updated_at = ?
                WHERE account_id = ? AND code = ? AND status = 'HOLDING'
            """, (new_units, new_total, now, account_id, code))

            self._write_flow(conn, account_id, code, pos['name'], '减仓',
                            shares=shares_per_unit, price=sell_price, amount=trade_amount,
                            profit=round(profit, 2), fees=fees['total'],
                            units_before=pos['units'], units_after=new_units)

            conn.commit()
            logger.info(f"[{code}] 减仓1单位: {shares_per_unit}股@{sell_price} 净盈亏={profit:.2f}")
        finally:
            conn.close()

        if account_manager:
            account_manager.on_sell(account_id, net_proceeds, profit)

        return self.get_position(account_id, code)

    def close_position(self, account_id, code, reason, sell_price, account_manager=None):
        """
        平仓

        参数:
            account_id: 账户ID
            code: 股票代码
            reason: 平仓原因（'exit'退出 / 'stop_loss'止损）
            sell_price: 卖出价
            account_manager: AccountManager实例（可选）

        返回:
            dict or None: 平仓记录
        """
        self._require_account_id(account_id)
        pos = self.get_position(account_id, code)
        if not pos:
            return None

        trade_amount = sell_price * pos['total_shares']
        gross_profit = (sell_price - pos['avg_cost']) * pos['total_shares']

        # 计算卖出费用（含印花税）
        fees = self._calc_fees(trade_amount, is_sell=True)
        net_proceeds = trade_amount - fees['total']
        net_profit = gross_profit - fees['total']

        # 确定流水动作
        if reason == 'stop_loss':
            flow_action = '清仓止损'
            cooldown_days = 20
        elif net_profit > 0:
            flow_action = '清仓止盈'
            cooldown_days = 10
        else:
            flow_action = '清仓止损'
            cooldown_days = 10

        cooldown_until = (datetime.now() + timedelta(days=cooldown_days)).strftime('%Y-%m-%d')
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        conn = get_db_connection()
        try:
            conn.execute("""
                UPDATE turtle_positions SET
                    status = 'COOLING',
                    cooldown_until = ?,
                    closed_at = ?,
                    updated_at = ?
                WHERE account_id = ? AND code = ? AND status = 'HOLDING'
            """, (cooldown_until, now, now, account_id, code))

            # 写持仓流水
            self._write_flow(conn, account_id, code, pos['name'], flow_action,
                            shares=pos['total_shares'], price=sell_price, amount=trade_amount,
                            profit=round(net_profit, 2), fees=fees['total'],
                            units_before=pos['units'], units_after=0,
                            reason=reason)

            conn.commit()
            logger.info(f"[账户{account_id}] {flow_action} {code} 毛利={gross_profit:.2f} 费用={fees['total']:.2f} 净利={net_profit:.2f}")
        finally:
            conn.close()

        # 增加资金并记录盈亏（扣除费用后的净额）
        if account_manager:
            account_manager.on_sell(account_id, net_proceeds, net_profit)

        return {
            'code': code,
            'name': pos['name'],
            'shares': pos['total_shares'],
            'avg_cost': pos['avg_cost'],
            'sell_price': sell_price,
            'gross_profit': round(gross_profit, 2),
            'fees': fees,
            'net_profit': round(net_profit, 2),
            'reason': reason,
            'cooldown_until': cooldown_until,
        }

    def check_cooldown_release(self, account_id):
        """
        检查并释放到期冷却持仓（状态改为CLOSED）

        参数:
            account_id: 账户ID（必须）

        返回:
            list: 已释放的持仓代码列表
        """
        self._require_account_id(account_id)
        today = datetime.now().strftime('%Y-%m-%d')
        conn = get_db_connection()
        try:
            rows = conn.execute("""
                SELECT code FROM turtle_positions
                WHERE account_id = ? AND status = 'COOLING' AND cooldown_until <= ?
            """, (account_id, today)).fetchall()

            released = [r['code'] for r in rows]

            if released:
                conn.execute("""
                    UPDATE turtle_positions SET status = 'CLOSED', updated_at = ?
                    WHERE account_id = ? AND status = 'COOLING' AND cooldown_until <= ?
                """, (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), account_id, today))
                conn.commit()

            return released
        finally:
            conn.close()

    def get_total_units(self, account_id):
        """
        获取指定账户总持仓单位数

        参数:
            account_id: 账户ID

        返回:
            int: 该账户所有HOLDING持仓的units之和
        """
        self._require_account_id(account_id)
        conn = get_db_connection()
        try:
            row = conn.execute(
                "SELECT COALESCE(SUM(units), 0) as total FROM turtle_positions WHERE account_id = ? AND status = 'HOLDING'",
                (account_id,)
            ).fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()

    def get_position_flow(self, account_id, code=None, limit=20):
        """
        查询持仓流水

        参数:
            account_id: 账户ID
            code: 股票代码（None则查全部）
            limit: 最近N条

        返回:
            list[dict]: 流水记录列表（最新在前）
        """
        self._require_account_id(account_id)
        conn = get_db_connection()
        try:
            conn.row_factory = sqlite3.Row
            if code:
                rows = conn.execute("""
                    SELECT * FROM turtle_position_flow
                    WHERE account_id = ? AND code = ?
                    ORDER BY created_at DESC LIMIT ?
                """, (account_id, code, limit)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT * FROM turtle_position_flow
                    WHERE account_id = ?
                    ORDER BY created_at DESC LIMIT ?
                """, (account_id, limit)).fetchall()

            return [{
                'id': r['id'],
                'account_id': r['account_id'],
                'code': r['code'],
                'name': r['name'],
                'action': r['action'],
                'shares': r['shares'],
                'price': float(r['price']),
                'amount': float(r['amount']),
                'profit': float(r['profit']),
                'fees': float(r['fees']),
                'units_before': r['units_before'],
                'units_after': r['units_after'],
                'stop_price': float(r['stop_price']),
                'reason': r['reason'],
                'created_at': r['created_at'],
            } for r in rows]
        finally:
            conn.close()
