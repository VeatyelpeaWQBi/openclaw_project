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

    def get_active_positions(self):
        """
        获取所有持有中的持仓

        返回:
            list: 持仓记录列表（dict）
        """
        conn = get_db_connection()
        rows = conn.execute(
            "SELECT * FROM turtle_positions WHERE status = 'HOLDING' ORDER BY opened_at"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_cooling_positions(self):
        """
        获取冷却中的持仓

        返回:
            list: 冷却中持仓列表
        """
        conn = get_db_connection()
        rows = conn.execute(
            "SELECT * FROM turtle_positions WHERE status = 'COOLING' ORDER BY cooldown_until"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_position(self, code):
        """
        获取单只持仓详情

        参数:
            code: 股票代码

        返回:
            dict or None: 持仓记录
        """
        conn = get_db_connection()
        row = conn.execute(
            "SELECT * FROM turtle_positions WHERE code = ? AND status = 'HOLDING'",
            (code,)
        ).fetchone()
        conn.close()
        if row:
            return dict(row)
        return None

    def open_position(self, code, name, price, atr, units=1, account_manager=None):
        """
        开仓

        参数:
            code: 股票代码
            name: 股票名称
            price: 入场价
            atr: ATR值
            units: 买入单位数，默认1
            account_manager: AccountManager实例（可选）

        返回:
            dict: 新建的持仓记录
        """
        # 计算每单位股数
        if account_manager:
            capital = account_manager.get_summary().get("total", 0)
        else:
            capital = 100000  # 默认10万

        shares_per_unit = calc_unit_size(capital, atr, price)
        total_shares = shares_per_unit * units
        cost = price * total_shares

        # 检查可用资金
        if account_manager and account_manager.get_available() < cost:
            return None

        stop_price = calc_stop_price(price, atr)
        next_add = calc_add_price(price, atr)
        exit_p = 0.0  # 退出价在运行时从K线计算

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        conn = get_db_connection()
        cursor = conn.execute("""
            INSERT INTO turtle_positions
            (code, name, status, units, total_shares, avg_cost, entry_price,
             last_add_price, current_stop, next_add_price, exit_price, atr_value,
             opened_at, updated_at)
            VALUES (?, ?, 'HOLDING', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (code, name, units, total_shares, price, price, price, stop_price, next_add, exit_p, atr, now, now))
        pos_id = cursor.lastrowid
        conn.commit()
        logger.info(f"[持仓] 操作完成")
        conn.close()

        # 扣减资金
        if account_manager:
            account_manager.on_buy(cost)

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
        }

    def add_position(self, code, new_price, atr, account_manager=None):
        """
        加仓（增加1单位）

        参数:
            code: 股票代码
            new_price: 当前价格
            atr: ATR值
            account_manager: AccountManager实例（可选）

        返回:
            dict or None: 更新后的持仓记录
        """
        pos = self.get_position(code)
        if not pos:
            return None

        # 计算加仓股数
        if account_manager:
            capital = account_manager.get_summary().get("total", 0)
        else:
            capital = 100000

        shares_per_unit = calc_unit_size(capital, atr, new_price)
        cost = new_price * shares_per_unit

        # 检查可用资金
        if account_manager and account_manager.get_available() < cost:
            return None

        # 更新持仓
        new_units = pos['units'] + 1
        new_total = pos['total_shares'] + shares_per_unit
        new_avg = (pos['avg_cost'] * pos['total_shares'] + new_price * shares_per_unit) / new_total
        new_stop = calc_stop_price(new_avg, atr)
        next_add = calc_add_price(new_price, atr)

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = get_db_connection()
        conn.execute("""
            UPDATE turtle_positions SET
                units = ?, total_shares = ?, avg_cost = ?,
                last_add_price = ?, current_stop = ?, next_add_price = ?,
                atr_value = ?, updated_at = ?
            WHERE code = ? AND status = 'HOLDING'
        """, (new_units, new_total, round(new_avg, 2), new_price, new_stop, next_add, atr, now, code))
        conn.commit()
        logger.info(f"[持仓] 操作完成")
        conn.close()

        # 扣减资金
        if account_manager:
            account_manager.on_buy(cost)

        return self.get_position(code)

    def close_position(self, code, reason, sell_price, account_manager=None):
        """
        平仓

        参数:
            code: 股票代码
            reason: 平仓原因（'exit'退出 / 'stop_loss'止损）
            sell_price: 卖出价
            account_manager: AccountManager实例（可选）

        返回:
            dict or None: 平仓记录
        """
        pos = self.get_position(code)
        if not pos:
            return None

        proceeds = sell_price * pos['total_shares']
        profit = (sell_price - pos['avg_cost']) * pos['total_shares']

        # 冷却期：止损后20个交易日，退出后10个交易日
        if reason == 'stop_loss':
            cooldown_days = 20
        else:
            cooldown_days = 10

        cooldown_until = (datetime.now() + timedelta(days=cooldown_days)).strftime('%Y-%m-%d')
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        conn = get_db_connection()
        conn.execute("""
            UPDATE turtle_positions SET
                status = 'COOLING',
                cooldown_until = ?,
                closed_at = ?,
                updated_at = ?
            WHERE code = ? AND status = 'HOLDING'
        """, (cooldown_until, now, now, code))
        conn.commit()
        logger.info(f"[持仓] 操作完成")
        conn.close()

        # 增加资金并记录盈亏
        if account_manager:
            account_manager.on_sell(proceeds, profit)

        return {
            'code': code,
            'name': pos['name'],
            'shares': pos['total_shares'],
            'avg_cost': pos['avg_cost'],
            'sell_price': sell_price,
            'profit': round(profit, 2),
            'reason': reason,
            'cooldown_until': cooldown_until,
        }

    def check_cooldown_release(self):
        """
        检查并释放到期冷却持仓（状态改为CLOSED）

        返回:
            list: 已释放的持仓代码列表
        """
        today = datetime.now().strftime('%Y-%m-%d')
        conn = get_db_connection()

        rows = conn.execute("""
            SELECT code FROM turtle_positions
            WHERE status = 'COOLING' AND cooldown_until <= ?
        """, (today,)).fetchall()

        released = [r['code'] for r in rows]

        if released:
            conn.execute("""
                UPDATE turtle_positions SET status = 'CLOSED', updated_at = ?
                WHERE status = 'COOLING' AND cooldown_until <= ?
            """, (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), today))
            conn.commit()
        logger.info(f"[持仓] 操作完成")

        conn.close()
        return released

    def get_total_units(self):
        """
        获取全市场总持仓单位数

        返回:
            int: 所有HOLDING持仓的units之和
        """
        conn = get_db_connection()
        row = conn.execute(
            "SELECT COALESCE(SUM(units), 0) as total FROM turtle_positions WHERE status = 'HOLDING'"
        ).fetchone()
        conn.close()
        return int(row['total']) if row else 0
