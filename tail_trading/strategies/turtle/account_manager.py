"""
海龟交易法 — 账户管理器
封装 turtle_account 表的 CRUD 操作
"""

import sqlite3
import time
import logging
from datetime import datetime
from core.storage import get_db_connection

logger = logging.getLogger(__name__)


# ==================== 雪花算法 ID 生成器 ====================

class SnowflakeIdGenerator:
    """
    雪花算法ID生成器
    64位结构：1位符号 + 41位时间戳 + 10位机器ID + 12位序列号
    """
    EPOCH = 1704067200000  # 2024-01-01 00:00:00 UTC (毫秒)

    def __init__(self, worker_id=0, datacenter_id=0):
        self.worker_id = worker_id & 0x1F        # 5位
        self.datacenter_id = datacenter_id & 0x1F # 5位
        self.sequence = 0
        self.last_timestamp = -1

    def _current_millis(self):
        return int(time.time() * 1000)

    def generate(self):
        """生成一个雪花ID"""
        timestamp = self._current_millis()

        if timestamp < self.last_timestamp:
            raise Exception(f"时钟回拨，拒绝生成ID (回拨{self.last_timestamp - timestamp}ms)")

        if timestamp == self.last_timestamp:
            self.sequence = (self.sequence + 1) & 0xFFF  # 12位序列号
            if self.sequence == 0:
                # 等待下一毫秒
                while timestamp <= self.last_timestamp:
                    timestamp = self._current_millis()
        else:
            self.sequence = 0

        self.last_timestamp = timestamp

        snowflake_id = (
            ((timestamp - self.EPOCH) << 22) |
            (self.datacenter_id << 17) |
            (self.worker_id << 12) |
            self.sequence
        )
        return snowflake_id


# 全局单例
_snowflake_gen = SnowflakeIdGenerator()


def generate_snowflake_id():
    """生成雪花ID"""
    return _snowflake_gen.generate()


# ==================== 账户管理器 ====================

class AccountManager:
    """海龟交易法账户管理器"""

    def _row_to_dict(self, row):
        """
        将sqlite3.Row转换为dict
        ⚠️ 如果 turtle_account 表新增列，需同步更新此处
        """
        if row is None:
            return None
        return {
            'id': row['id'],
            'total_capital': float(row['total_capital']),
            'available_capital': float(row['available_capital']),
            'realized_profit': float(row['realized_profit']),
            'active': row['active'],
            'bind_id': row['bind_id'],
            'nickname': row['nickname'],
            'simulator': row['simulator'],
            'updated_at': row['updated_at'],
            'note': row['note'],
        }

    def _write_flow(self, conn, account_id, flow_type, amount):
        """
        写入资金流水记录

        参数:
            conn: 数据库连接（复用调用方的连接，保证同一事务）
            account_id: 账户ID
            flow_type: 类型（入金/出金/买入/卖出）
            amount: 金额
        """
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # 查询操作后的余额
        row = conn.execute(
            "SELECT available_capital FROM turtle_account WHERE id = ?",
            (account_id,)
        ).fetchone()
        balance_after = float(row[0]) if row else 0.0

        conn.execute("""
            INSERT INTO turtle_account_flow (account_id, type, amount, balance_after, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (account_id, flow_type, amount, balance_after, now))

        # 更新note
        conn.execute("""
            UPDATE turtle_account SET note = ? WHERE id = ?
        """, (f"{flow_type} {amount} ({now})", account_id))

    def init_account(self, account_id, capital, nickname=None, simulator=1):
        """
        初始化账户（如已存在则更新总资产）

        参数:
            account_id: 账户ID
            capital: 初始资金
            nickname: 用户昵称（可选）
            simulator: 0=机器模拟账户, 1=否(手工账户)
        """
        conn = get_db_connection()
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            conn.execute("""
                INSERT INTO turtle_account (id, total_capital, available_capital, realized_profit, active, nickname, simulator, updated_at, note)
                VALUES (?, ?, ?, 0, 1, ?, ?, ?, '初始化')
                ON CONFLICT(id) DO UPDATE SET
                    total_capital = excluded.total_capital,
                    nickname = COALESCE(excluded.nickname, nickname),
                    simulator = excluded.simulator,
                    updated_at = excluded.updated_at
            """, (account_id, capital, capital, nickname, simulator, now))
            conn.commit()
            logger.info(f"[账户{account_id}] 初始化完成，资金: {capital}")
        finally:
            conn.close()

    def init_account_by_bind_id(self, bind_id, capital, nickname=None, simulator=1):
        """
        通过bind_id初始化账户
        如bind_id已绑定，返回已有账户；否则使用雪花ID自动创建

        参数:
            bind_id: 社交ID（如QQ的sender_id）
            capital: 初始资金
            nickname: 用户昵称（可选）
            simulator: 0=机器模拟账户, 1=否(手工账户)

        返回:
            dict: 账户信息
        """
        # 检查bind_id是否已绑定
        existing = self.get_account_by_bind_id(bind_id)
        if existing:
            logger.info(f"[bind_id={bind_id}] 已绑定账户{existing['id']}，跳过创建")
            return existing

        # 雪花算法生成新ID
        new_id = generate_snowflake_id()

        # 创建账户并绑定
        self.init_account(new_id, capital, nickname, simulator)
        self.bind_social_id(new_id, bind_id)

        logger.info(f"[bind_id={bind_id}] 新建账户{new_id}，资金: {capital}")
        return self.get_summary(new_id)

    def get_available(self, account_id):
        """
        获取可用资金

        参数:
            account_id: 账户ID

        返回:
            float: 可用资金
        """
        conn = get_db_connection()
        try:
            row = conn.execute(
                "SELECT available_capital FROM turtle_account WHERE id = ? AND active = 1",
                (account_id,)
            ).fetchone()
            if row:
                return float(row['available_capital'])
            return 0.0
        finally:
            conn.close()

    def deposit(self, account_id, amount):
        """
        入金

        参数:
            account_id: 账户ID
            amount: 入金金额
        """
        conn = get_db_connection()
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            conn.execute("""
                UPDATE turtle_account SET
                    total_capital = total_capital + ?,
                    available_capital = available_capital + ?,
                    updated_at = ?
                WHERE id = ? AND active = 1
            """, (amount, amount, now, account_id))
            self._write_flow(conn, account_id, '入金', amount)
            conn.commit()
            logger.info(f"[账户{account_id}] 入金 {amount}")
        finally:
            conn.close()

    def withdraw(self, account_id, amount):
        """
        出金（乐观锁扣款）

        参数:
            account_id: 账户ID
            amount: 出金金额

        返回:
            bool: 是否成功（可用资金不足则失败）
        """
        conn = get_db_connection()
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor = conn.execute("""
                UPDATE turtle_account SET
                    total_capital = total_capital - ?,
                    available_capital = available_capital - ?,
                    updated_at = ?
                WHERE id = ? AND active = 1 AND available_capital - ? >= 0
            """, (amount, amount, now, account_id, amount))
            if cursor.rowcount == 0:
                logger.warning(f"[账户{account_id}] 出金失败：可用资金不足")
                return False
            self._write_flow(conn, account_id, '出金', amount)
            conn.commit()
            logger.info(f"[账户{account_id}] 出金 {amount}")
            return True
        finally:
            conn.close()

    def on_buy(self, account_id, cost):
        """
        买入时扣减可用资金（乐观锁扣款）

        参数:
            account_id: 账户ID
            cost: 买入成本 = 价格 × 股数

        返回:
            bool: 是否成功（可用资金不足则失败）
        """
        conn = get_db_connection()
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor = conn.execute("""
                UPDATE turtle_account SET
                    available_capital = available_capital - ?,
                    updated_at = ?
                WHERE id = ? AND active = 1 AND available_capital - ? >= 0
            """, (cost, now, account_id, cost))
            if cursor.rowcount == 0:
                logger.warning(f"[账户{account_id}] 买入失败：可用资金不足")
                return False
            conn.commit()
            logger.info(f"[账户{account_id}] 买入扣减 {cost}")
            return True
        finally:
            conn.close()

    def on_sell(self, account_id, proceeds, profit):
        """
        卖出时增加可用资金+记录盈亏

        参数:
            account_id: 账户ID
            proceeds: 卖出所得 = 卖出价 × 股数
            profit: 本笔盈亏（正=盈利，负=亏损）
        """
        conn = get_db_connection()
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            conn.execute("""
                UPDATE turtle_account SET
                    total_capital = total_capital + ?,
                    available_capital = available_capital + ?,
                    realized_profit = realized_profit + ?,
                    updated_at = ?
                WHERE id = ? AND active = 1
            """, (profit, proceeds, profit, now, account_id))
            conn.commit()
            logger.info(f"[账户{account_id}] 卖出，盈亏: {profit}")
        finally:
            conn.close()

    def get_summary(self, account_id):
        """
        获取账户摘要

        参数:
            account_id: 账户ID

        返回:
            dict: 完整账户信息（含兼容key: total/available/realized_profit）
        """
        conn = get_db_connection()
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM turtle_account WHERE id = ? AND active = 1",
                (account_id,)
            ).fetchone()
            if row is None:
                return None
            d = self._row_to_dict(row)
            # 兼容旧版key
            d['total'] = d['total_capital']
            d['available'] = d['available_capital']
            return d
        finally:
            conn.close()

    def get_account_by_bind_id(self, bind_id):
        """
        通过社交绑定ID查询账户全量信息

        参数:
            bind_id: 社交ID（如QQ的sender_id）

        返回:
            dict or None: 账户全量信息，未找到返回None
        """
        conn = get_db_connection()
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM turtle_account WHERE bind_id = ? AND active = 1",
                (bind_id,)
            ).fetchone()
            return self._row_to_dict(row)
        finally:
            conn.close()

    def get_all_active_accounts(self):
        """
        查询所有活跃账户

        返回:
            list[dict]: 活跃账户列表
        """
        conn = get_db_connection()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM turtle_account WHERE active = 1 ORDER BY id"
            ).fetchall()
            return [self._row_to_dict(r) for r in rows]
        finally:
            conn.close()

    def get_all_inactive_accounts(self):
        """
        查询所有非活跃账户

        返回:
            list[dict]: 非活跃账户列表
        """
        conn = get_db_connection()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM turtle_account WHERE active = 0 ORDER BY id"
            ).fetchall()
            return [self._row_to_dict(r) for r in rows]
        finally:
            conn.close()

    def get_manual_accounts(self):
        """
        查询所有手工账户（simulator=1, active=1）

        返回:
            list[dict]: 手工账户列表
        """
        conn = get_db_connection()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM turtle_account WHERE simulator = 1 AND active = 1 ORDER BY id"
            ).fetchall()
            return [self._row_to_dict(r) for r in rows]
        finally:
            conn.close()

    def get_simulator_accounts(self):
        """
        查询所有机器模拟账户（simulator=0, active=1）

        返回:
            list[dict]: 机器模拟账户列表
        """
        conn = get_db_connection()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM turtle_account WHERE simulator = 0 AND active = 1 ORDER BY id"
            ).fetchall()
            return [self._row_to_dict(r) for r in rows]
        finally:
            conn.close()

    def set_active(self, account_id, active):
        """
        设置账户活跃状态

        参数:
            account_id: 账户ID
            active: 1=活跃, 0=关闭
        """
        conn = get_db_connection()
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            conn.execute("""
                UPDATE turtle_account SET active = ?, updated_at = ? WHERE id = ?
            """, (active, now, account_id))
            conn.commit()
            status = '活跃' if active else '关闭'
            logger.info(f"[账户{account_id}] 状态变更为: {status}")
        finally:
            conn.close()

    def update_nickname(self, account_id, nickname):
        """
        更新用户昵称

        参数:
            account_id: 账户ID
            nickname: 新昵称
        """
        conn = get_db_connection()
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            conn.execute("""
                UPDATE turtle_account SET nickname = ?, updated_at = ? WHERE id = ?
            """, (nickname, now, account_id))
            conn.commit()
            logger.info(f"[账户{account_id}] 昵称更新为: {nickname}")
        finally:
            conn.close()

    def bind_social_id(self, account_id, bind_id):
        """
        绑定社交ID到账户

        参数:
            account_id: 账户ID
            bind_id: 社交ID（如QQ的sender_id）

        返回:
            bool: 是否成功（bind_id已被其他账户绑定则失败）
        """
        conn = get_db_connection()
        try:
            # 检查bind_id是否已被其他账户绑定
            existing = conn.execute(
                "SELECT id FROM turtle_account WHERE bind_id = ? AND id != ?",
                (bind_id, account_id)
            ).fetchone()
            if existing:
                logger.warning(f"[账户{account_id}] bind_id {bind_id} 已被账户{existing[0]}绑定")
                return False

            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            conn.execute("""
                UPDATE turtle_account SET bind_id = ?, updated_at = ? WHERE id = ?
            """, (bind_id, now, account_id))
            conn.commit()
            logger.info(f"[账户{account_id}] 绑定社交ID: {bind_id}")
            return True
        finally:
            conn.close()

    def unbind_social_id(self, account_id):
        """
        解除账户的社交ID绑定

        参数:
            account_id: 账户ID
        """
        conn = get_db_connection()
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            conn.execute("""
                UPDATE turtle_account SET bind_id = NULL, updated_at = ? WHERE id = ?
            """, (now, account_id))
            conn.commit()
            logger.info(f"[账户{account_id}] 解除社交ID绑定")
        finally:
            conn.close()

    def get_flow_records(self, account_id, limit=20):
        """
        查询账户资金流水

        参数:
            account_id: 账户ID
            limit: 最近N条，默认20

        返回:
            list[dict]: 流水记录列表（最新在前）
        """
        conn = get_db_connection()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT * FROM turtle_account_flow
                WHERE account_id = ?
                ORDER BY created_at DESC
                LIMIT ?
            """, (account_id, limit)).fetchall()
            return [{
                'id': r['id'],
                'account_id': r['account_id'],
                'type': r['type'],
                'amount': float(r['amount']),
                'balance_after': float(r['balance_after']),
                'created_at': r['created_at'],
            } for r in rows]
        finally:
            conn.close()

    def set_s1_filter(self, account_id):
        """设置S1过滤（S1交易盈利时调用）"""
        conn = get_db_connection()
        try:
            cursor = conn.execute("""
                UPDATE turtle_account SET s1_filter_active = 1
                WHERE account_id = ? AND s1_filter_active = 0
            """, (account_id,))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"[账户{account_id}] S1过滤已激活")
        finally:
            conn.close()

    def clear_s1_filter(self, account_id):
        """清除S1过滤（S2开仓成功时调用）"""
        conn = get_db_connection()
        try:
            cursor = conn.execute("""
                UPDATE turtle_account SET s1_filter_active = 0
                WHERE account_id = ? AND s1_filter_active = 1
            """, (account_id,))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"[账户{account_id}] S1过滤已清除")
        finally:
            conn.close()

    def is_s1_filtered(self, account_id):
        """查询S1过滤状态"""
        conn = get_db_connection()
        try:
            row = conn.execute("""
                SELECT s1_filter_active FROM turtle_account
                WHERE account_id = ?
            """, (account_id,)).fetchone()
            return row and row['s1_filter_active'] == 1
        finally:
            conn.close()
