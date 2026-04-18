"""
持仓管理器 — 基础设施层
封装 positions 表的纯CRUD操作，不绑定任何交易策略
策略特有逻辑由上层 TrendTradingPositionManager 处理
"""

import sqlite3
import logging

from datetime import datetime, timedelta

from core.storage import get_db_connection

logger = logging.getLogger(__name__)


class PositionManager:
    """持仓管理器（纯CRUD层）"""

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

    def _write_flow(self, conn, account_id, code, name, action, shares=0, price=0.0,
                    amount=0.0, profit=0.0, fees=0.0, units_before=0, units_after=0,
                    stop_price=0.0, reason=None, target_date=None):
        """
        写入持仓流水记录

        参数:
            conn: 数据库连接
            account_id: 账户ID
            code: 股票代码
            name: 股票名称
            action: 动作（开仓/加仓/清仓止损/清仓止盈）
            shares: 本次涉及股数
            price: 本次价格
            amount: 本次金额
            profit: 本次盈亏
            units_before: 操作前单位数
            units_after: 操作后单位数
            stop_price: 止损价
            reason: 原因说明
            target_date: 业务日期（回测时传入，实盘时为None）
        """
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        operate_date = target_date or datetime.now().strftime('%Y-%m-%d')
        conn.execute("""
            INSERT INTO position_flow
            (account_id, code, name, action, shares, price, amount, profit, fees,
             units_before, units_after, stop_price, reason, created_at, operate_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (account_id, code, name, action, shares, price, amount, profit, fees,
              units_before, units_after, stop_price, reason, now, operate_date))

    def get_active_positions(self, account_id: int) -> list[dict]:
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
                "SELECT * FROM positions WHERE account_id = ? AND status = 'HOLDING' ORDER BY opened_at",
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
                "SELECT * FROM positions WHERE account_id = ? AND status = 'COOLING' ORDER BY cooldown_until",
                (account_id,)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_position(self, account_id: int, code: str) -> dict | None:
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
                "SELECT * FROM positions WHERE account_id = ? AND code = ? AND status = 'HOLDING'",
                (account_id, code)
            ).fetchone()
            if row:
                return dict(row)
            return None
        finally:
            conn.close()

    def open_position(self, account_id, code, name, price, total_shares,
                      stop_price, next_add_price, shares_per_unit,
                      account_manager=None, units=1, atr=0.0, entry_system=None,
                      strategy_ctx=None, target_date=None):
        """
        开仓（纯CRUD，不做任何策略计算）

        参数:
            account_id: 账户ID
            code: 股票代码
            name: 股票名称
            price: 入场价
            total_shares: 总股数（由上层计算好）
            stop_price: 止损价（由上层计算好）
            next_add_price: 下次加仓价（由上层计算好）
            shares_per_unit: 每单位股数
            account_manager: AccountManager实例（可选）
            units: 单位数
            atr: ATR值（原样存储）
            entry_system: 入场系统（原样存储）
            target_date: 业务日期（回测时传入）
        """
        self._require_account_id(account_id)

        trade_amount = price * total_shares
        fees = self._calc_fees(trade_amount, is_sell=False)
        total_cost = trade_amount + fees['total']
        avg_cost = total_cost / total_shares  # 券商逻辑：成本含买入费用

        # 检查可用资金
        if account_manager and account_manager.get_available(account_id) < total_cost:
            logger.warning(f"[账户{account_id}] 开仓失败：资金不足 (需要{total_cost:.2f})")
            return None

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        last_buy_date = target_date[:10] if target_date else datetime.now().strftime('%Y-%m-%d')
        opened_at = f"{target_date} 00:00:00" if target_date else now

        conn = get_db_connection()
        try:
            cursor = conn.execute("""
                INSERT INTO positions
                (account_id, code, name, status, turtle_units, total_shares, avg_cost, entry_price,
                 last_add_price, current_stop, next_add_price, turtle_atr_value,
                 shares_per_unit, turtle_entry_system, last_buy_date, last_buy_shares, opened_at, updated_at)
                VALUES (?, ?, ?, 'HOLDING', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (account_id, code, name, units, total_shares, avg_cost, price, price,
                  stop_price, next_add_price, atr, shares_per_unit, entry_system,
                  last_buy_date, total_shares, opened_at, now))
            pos_id = cursor.lastrowid

            self._write_flow(conn, account_id, code, name, '开仓',
                            shares=total_shares, price=price, amount=trade_amount,
                            profit=0, fees=fees['total'],
                            units_before=0, units_after=units, stop_price=stop_price,
                            target_date=target_date)

            conn.commit()
            logger.info(f"[账户{account_id}] 开仓 {code} {name} {total_shares}股@{price} 费用={fees['total']:.2f}")
        finally:
            conn.close()

        if account_manager:
            account_manager.on_buy(account_id, total_cost)

        return {
            'id': pos_id, 'code': code, 'name': name, 'turtle_units': units,
            'total_shares': total_shares, 'avg_cost': avg_cost, 'entry_price': price,
            'current_stop': stop_price, 'next_add_price': next_add_price,
            'turtle_atr_value': atr, 'shares_per_unit': shares_per_unit, 'fees': fees,
        }

    def add_position(self, account_id, code, new_price, shares_per_unit,
                     new_stop_price, new_next_add_price,
                     account_manager=None, atr=0.0, target_date=None):
        """
        加仓（纯CRUD，接受预计算的止损/加仓价）
        """
        self._require_account_id(account_id)

        pos = self.get_position(account_id, code)
        if not pos:
            return None

        if shares_per_unit <= 0:
            shares_per_unit = pos.get('shares_per_unit', 0)
            if shares_per_unit <= 0:
                shares_per_unit = pos['total_shares'] // pos['turtle_units'] if pos['turtle_units'] > 0 else 0

        trade_amount = new_price * shares_per_unit
        fees = self._calc_fees(trade_amount, is_sell=False)
        total_cost = trade_amount + fees['total']

        if account_manager and account_manager.get_available(account_id) < total_cost:
            logger.warning(f"[账户{account_id}] 加仓失败：资金不足 (需要{total_cost:.2f})")
            return None

        old_units = pos['turtle_units']
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        last_buy_date = target_date[:10] if target_date else datetime.now().strftime('%Y-%m-%d')

        # 新持仓成本 = (旧成本 + 新成交额 + 新买入费用) / 总股数
        # 券商逻辑：每次成本变动都加上交易磨损
        conn = get_db_connection()
        try:
            conn.execute("""
                UPDATE positions SET
                    turtle_units = turtle_units + 1,
                    total_shares = total_shares + ?,
                    avg_cost = (avg_cost * total_shares + ? * ? + ?) / (total_shares + ?),
                    last_add_price = ?,
                    current_stop = ?,
                    next_add_price = ?,
                    turtle_atr_value = ?, last_buy_date = ?, last_buy_shares = ?, updated_at = ?
                WHERE account_id = ? AND code = ? AND status = 'HOLDING'
            """, (
                shares_per_unit, new_price, shares_per_unit, fees['total'], shares_per_unit,
                new_price, new_stop_price, new_next_add_price,
                atr, last_buy_date, shares_per_unit, now, account_id, code
            ))

            new_units = old_units + 1
            self._write_flow(conn, account_id, code, pos['name'], '加仓',
                            shares=shares_per_unit, price=new_price, amount=trade_amount,
                            profit=0, fees=fees['total'],
                            units_before=old_units, units_after=new_units, stop_price=new_stop_price,
                            target_date=target_date)

            conn.commit()
            logger.info(f"[账户{account_id}] 加仓 {code} {shares_per_unit}股@{new_price} 费用={fees['total']:.2f}")
        finally:
            conn.close()

        if account_manager:
            account_manager.on_buy(account_id, total_cost)

        return self.get_position(account_id, code)

                       
        """
        平仓（纯CRUD）

        参数:
            account_id: 账户ID
            code: 股票代码
            reason: 平仓原因
            sell_price: 卖出价
            cooldown_days: 冷却天数（由上层决定）
            account_manager: AccountManager实例
            target_date: 业务日期（回测时传入）
        """
        self._require_account_id(account_id)
        pos = self.get_position(account_id, code)
        if not pos:
            return None

        trade_amount = sell_price * pos['total_shares']
        gross_profit = (sell_price - pos['avg_cost']) * pos['total_shares']
        fees = self._calc_fees(trade_amount, is_sell=True)
        net_proceeds = trade_amount - fees['total']
        net_profit = gross_profit - fees['total']

        # 流水动作
        flow_action = '清仓止损' if reason == 'stop_loss' else '清仓止盈' if net_profit > 0 else '清仓止损'

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        base_date_str = target_date or datetime.now().strftime('%Y-%m-%d')
        base_date = datetime.strptime(base_date_str, '%Y-%m-%d')
        cooldown_until = (base_date + timedelta(days=cooldown_days)).strftime('%Y-%m-%d')

        conn = get_db_connection()
        try:
            conn.execute("""
                UPDATE positions SET
                    status = 'COOLING',
                    cooldown_until = ?,
                    closed_at = ?,
                    updated_at = ?
                WHERE account_id = ? AND code = ? AND status = 'HOLDING'
            """, (cooldown_until, now, now, account_id, code))

            self._write_flow(conn, account_id, code, pos['name'], flow_action,
                            shares=pos['total_shares'], price=sell_price, amount=trade_amount,
                            profit=round(net_profit, 2), fees=fees['total'],
                            units_before=pos['turtle_units'], units_after=0,
                            reason=reason, target_date=target_date)

            conn.commit()
            logger.info(f"[账户{account_id}] {flow_action} {code} 净利={net_profit:.2f}")
        finally:
            conn.close()

        if account_manager:
            account_manager.on_sell(account_id, net_proceeds, net_profit)

        return {
            'code': code, 'name': pos['name'], 'shares': pos['total_shares'],
            'avg_cost': pos['avg_cost'], 'sell_price': sell_price,
            'gross_profit': round(gross_profit, 2), 'fees': fees,
            'net_profit': round(net_profit, 2), 'reason': reason,
            'cooldown_until': cooldown_until,
        }

    def count_today_opens(self, account_id, target_date=None):
        """
        查询今日已开仓标的数

        参数:
            account_id: 账户ID
            target_date: 业务日期（回测时传入）

        返回:
            int: 今日开仓次数
        """
        self._require_account_id(account_id)
        today = target_date or datetime.now().strftime('%Y-%m-%d')
        conn = get_db_connection()
        try:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM position_flow WHERE account_id = ? AND action = '开仓' AND operate_date = ?",
                (account_id, today)
            ).fetchone()
            return int(row['cnt']) if row else 0
        finally:
            conn.close()

    def check_cooldown_release(self, account_id, target_date=None):
        """
        检查并释放到期冷却持仓（状态改为CLOSED）

        参数:
            account_id: 账户ID（必须）
            target_date: 业务日期（回测时传入）

        返回:
            list: 已释放的持仓代码列表
        """
        self._require_account_id(account_id)
        today = target_date or datetime.now().strftime('%Y-%m-%d')
        conn = get_db_connection()
        try:
            rows = conn.execute("""
                SELECT code FROM positions
                WHERE account_id = ? AND status = 'COOLING' AND cooldown_until <= ?
            """, (account_id, today)).fetchall()

            released = [r['code'] for r in rows]

            if released:
                conn.execute("""
                    UPDATE positions SET status = 'CLOSED', updated_at = ?
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
                "SELECT COALESCE(SUM(turtle_units), 0) as total FROM positions WHERE account_id = ? AND status = 'HOLDING'",
                (account_id,)
            ).fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()

    def get_position_status(self, account_id, code, target_date=None):
        """
        获取持仓状态（含T+1锁仓信息）

        参数:
            account_id: 账户ID
            code: 股票代码
            target_date: 业务日期（回测时传入）

        返回:
            dict: {
                'total_shares': 总股数,
                'available_shares': 可卖股数,
                'locked_shares': 锁定股数（今日买入）,
                'last_buy_date': 最近买入日期,
                'last_buy_shares': 最近买入股数,
            }
        """
        pos = self.get_position(account_id, code)
        if not pos:
            return None

        today = target_date or datetime.now().strftime('%Y-%m-%d')
        total = pos.get('total_shares', 0)
        last_buy_date = pos.get('last_buy_date', '')
        last_buy_shares = pos.get('last_buy_shares', 0)

        # 今天买入的部分锁定（T+1）
        if last_buy_date and last_buy_date[:10] == today:
            locked = last_buy_shares
        else:
            locked = 0

        available = total - locked

        return {
            'total_shares': total,
            'available_shares': max(available, 0),
            'locked_shares': locked,
            'last_buy_date': last_buy_date,
            'last_buy_shares': last_buy_shares,
        }

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
                    SELECT * FROM position_flow
                    WHERE account_id = ? AND code = ?
                    ORDER BY operate_date DESC LIMIT ?
                """, (account_id, code, limit)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT * FROM position_flow
                    WHERE account_id = ?
                    ORDER BY operate_date DESC LIMIT ?
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

    def _update_atr_value(self, account_id, code, new_atr):
        """
        更新持仓的ATR值（委托storage）

        参数:
            account_id: 账户ID
            code: 股票代码
            new_atr: 新ATR值
        """
        from core.storage import update_position_atr
        update_position_atr(account_id, code, new_atr)
