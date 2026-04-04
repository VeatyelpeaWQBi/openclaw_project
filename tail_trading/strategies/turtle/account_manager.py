"""
海龟交易法 — 账户管理器
封装 turtle_account 表的 CRUD 操作
"""

import sqlite3
import logging
from datetime import datetime
from core.storage import get_db_connection

logger = logging.getLogger(__name__)


class AccountManager:
    """海龟交易法账户管理器"""

    def init_account(self, capital):
        """
        初始化账户（如已存在则更新总资产）

        参数:
            capital: 初始资金
        """
        conn = get_db_connection()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute("""
            INSERT INTO turtle_account (id, total_capital, available_capital, realized_profit, updated_at, note)
            VALUES (1, ?, ?, 0, ?, '初始化')
            ON CONFLICT(id) DO UPDATE SET
                total_capital = excluded.total_capital,
                updated_at = excluded.updated_at
        """, (capital, capital, now))
        conn.commit()
        logger.info(f"[账户] 操作完成")
        conn.close()

    def get_available(self):
        """
        获取可用资金

        返回:
            float: 可用资金
        """
        conn = get_db_connection()
        row = conn.execute("SELECT available_capital FROM turtle_account WHERE id = 1").fetchone()
        conn.close()
        if row:
            return float(row['available_capital'])
        return 0.0

    def deposit(self, amount):
        """
        入金

        参数:
            amount: 入金金额
        """
        conn = get_db_connection()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute("""
            UPDATE turtle_account SET
                total_capital = total_capital + ?,
                available_capital = available_capital + ?,
                updated_at = ?,
                note = '入金'
            WHERE id = 1
        """, (amount, amount, now))
        conn.commit()
        logger.info(f"[账户] 操作完成")
        conn.close()

    def withdraw(self, amount):
        """
        出金

        参数:
            amount: 出金金额

        返回:
            bool: 是否成功（可用资金不足则失败）
        """
        available = self.get_available()
        if available < amount:
            return False

        conn = get_db_connection()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute("""
            UPDATE turtle_account SET
                total_capital = total_capital - ?,
                available_capital = available_capital - ?,
                updated_at = ?,
                note = '出金'
            WHERE id = 1
        """, (amount, amount, now))
        conn.commit()
        logger.info(f"[账户] 操作完成")
        conn.close()
        return True

    def on_buy(self, cost):
        """
        买入时扣减可用资金（同步更新总资产）

        参数:
            cost: 买入成本 = 价格 × 股数
        """
        conn = get_db_connection()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute("""
            UPDATE turtle_account SET
                available_capital = available_capital - ?,
                updated_at = ?
            WHERE id = 1
        """, (cost, now))
        conn.commit()
        logger.info(f"[账户] 操作完成")
        conn.close()

    def on_sell(self, proceeds, profit):
        """
        卖出时增加可用资金+记录盈亏

        参数:
            proceeds: 卖出所得 = 卖出价 × 股数
            profit: 本笔盈亏（正=盈利，负=亏损）
        """
        conn = get_db_connection()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute("""
            UPDATE turtle_account SET
                total_capital = total_capital + ?,
                available_capital = available_capital + ?,
                realized_profit = realized_profit + ?,
                updated_at = ?
            WHERE id = 1
        """, (profit, proceeds, profit, now))
        conn.commit()
        logger.info(f"[账户] 操作完成")
        conn.close()

    def get_summary(self):
        """
        获取账户摘要

        返回:
            dict: {total, available, realized_profit}
        """
        conn = get_db_connection()
        row = conn.execute(
            "SELECT total_capital, available_capital, realized_profit FROM turtle_account WHERE id = 1"
        ).fetchone()
        conn.close()

        if row:
            return {
                'total': float(row['total_capital']),
                'available': float(row['available_capital']),
                'realized_profit': float(row['realized_profit']),
            }
        return {'total': 0.0, 'available': 0.0, 'realized_profit': 0.0}
