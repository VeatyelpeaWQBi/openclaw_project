"""
Robot Executor — 海龟机器人交易执行器
代理 TrendTradingExecutor，在执行前增加机器人特有的逻辑：
  - 资金可购性校验 + 手数降级
  - 涨停买不到 / 跌停卖不掉拦截

职责：
  - 接收信号检测器输出的动作队列
  - 预处理（资金校验、涨跌停拦截）
  - 委托给 TrendTradingExecutor 执行
  - 汇总执行结果
"""

import logging
from infra.account_manager import AccountManager
from strategies.trend_trading.trend_trading_executor import TrendTradingExecutor
from strategies.trend_trading.atr import calc_unit_size

logger = logging.getLogger(__name__)


class RobotExecutor:
    """海龟机器人交易执行器"""

    def __init__(self):
        self.tt_executor = TrendTradingExecutor()
        self.account_manager = AccountManager()

    def set_target_date(self, target_date):
        """传递 target_date 给整个执行链"""
        self.account_manager._target_date = target_date
        self.tt_executor.pm._target_date = target_date
        self.tt_executor.tt_pm._target_date = target_date
        self.tt_executor.account_manager._target_date = target_date
        self.tt_executor.trade_executor._target_date = target_date
        self.tt_executor.trade_executor.position_manager._target_date = target_date
        self.tt_executor.trade_executor.account_manager._target_date = target_date

    def execute_signals(self, account_id, action_queue: list) -> dict:
        """
        执行信号检测器输出的动作队列

        预处理顺序：
          1. 涨跌停拦截（涨停不买，跌停不卖）
          2. 资金校验 + 手数降级
          3. 委托给 TrendTradingExecutor 执行
        """
        if not action_queue:
            return {
                'account_id': account_id, 'total': 0,
                'success': 0, 'failed': 0, 'skipped': 0,
                'results': [], 'summary': '无交易动作',
            }

        # 1. 涨跌停拦截
        action_queue, skipped = self._check_limit_up_down(action_queue)

        # 2. 资金校验 + 手数降级
        action_queue = self._adjust_open_sizes(account_id, action_queue)

        # 3. 执行
        result = self.tt_executor.execute_signals(account_id, action_queue)

        # 合并跳过的计数
        result['skipped'] = result.get('skipped', 0) + skipped
        result['total'] = result.get('total', 0) + skipped

        return result

    def _check_limit_up_down(self, action_queue):
        """
        涨跌停拦截

        买动作（开仓/加仓）+ 涨停 → 跳过
        卖动作（止损/退出/减仓）+ 跌停 → 跳过

        返回: (过滤后的action_queue, 跳过数量)
        """
        from core.storage import get_db_connection

        # 收集需要检查的股票代码
        codes_to_check = set()
        for item in action_queue:
            action = item.get('action', '')
            if action in ('开仓', '加仓', '平仓', '减仓'):
                code = item.get('code', '')
                if code:
                    codes_to_check.add(code)

        if not codes_to_check:
            return action_queue, 0

        # 批量查询最新日K（每只股票取自己的最新一条）
        conn = get_db_connection()
        try:
            placeholders = ','.join(['?'] * len(codes_to_check))
            rows = conn.execute(f"""
                SELECT k.code, k.date, k.open, k.high, k.low, k.close
                FROM daily_kline k
                INNER JOIN (
                    SELECT code, MAX(date) as max_date
                    FROM daily_kline
                    WHERE code IN ({placeholders})
                    GROUP BY code
                ) latest ON k.code = latest.code AND k.date = latest.max_date
                WHERE k.code IN ({placeholders})
            """, list(codes_to_check) + list(codes_to_check)).fetchall()
        finally:
            conn.close()

        # 构建最新K线数据
        latest_kline = {}
        for r in rows:
            latest_kline[r['code']] = {
                'date': str(r['date'])[:10],
                'open': r['open'], 'high': r['high'],
                'low': r['low'], 'close': r['close'],
            }

        # 批量查询前一日收盘价（一次性查出所有code的前一日数据）
        conn = get_db_connection()
        try:
            prev_closes = {}
            for code in codes_to_check:
                kline = latest_kline.get(code)
                if not kline:
                    continue
                latest_date = kline.get('date', '')
                if not latest_date:
                    row = conn.execute("""
                        SELECT close FROM daily_kline
                        WHERE code = ? AND date < (SELECT MAX(date) FROM daily_kline WHERE code = ?)
                        ORDER BY date DESC LIMIT 1
                    """, (code, code)).fetchone()
                else:
                    row = conn.execute("""
                        SELECT close FROM daily_kline
                        WHERE code = ? AND date < ?
                        ORDER BY date DESC LIMIT 1
                    """, (code, latest_date)).fetchone()
                if row:
                    prev_closes[code] = row['close']
        finally:
            conn.close()

        # 逐个检查
        filtered = []
        skipped = 0
        for item in action_queue:
            action = item.get('action', '')
            code = item.get('code', '')

            if action in ('开仓', '加仓') and code in latest_kline:
                kline = latest_kline[code]
                prev_close = prev_closes.get(code)
                if prev_close and self._is_limit_up(code, kline, prev_close):
                    limit_price = self._get_limit_price(code, prev_close, up=True)
                    logger.info(f"[{code}] 涨停({limit_price})，跳过{action}，等待次日")
                    skipped += 1
                    continue

            if action in ('平仓', '减仓') and code in latest_kline:
                kline = latest_kline[code]
                prev_close = prev_closes.get(code)
                if prev_close and self._is_limit_down(code, kline, prev_close):
                    limit_price = self._get_limit_price(code, prev_close, up=False)
                    logger.info(f"[{code}] 跌停({limit_price})，跳过{action}，等待次日")
                    skipped += 1
                    continue

            filtered.append(item)

        return filtered, skipped

    @staticmethod
    def _is_etf(code):
        """是否ETF（价格精确到3位小数）"""
        return code.startswith(('15', '51', '56', '58'))

    @staticmethod
    def _get_price_decimals(code):
        """获取价格精度位数"""
        return 3 if code.startswith(('15', '51', '56', '58')) else 2

    @staticmethod
    def _get_limit_price(code, prev_close, up=True):
        """计算涨跌停价（股票2位小数，ETF3位小数）"""
        pct = 0.20 if code.startswith('3') or code.startswith('688') else 0.10
        decimals = 3 if code.startswith(('15', '51', '56', '58')) else 2
        if up:
            return round(prev_close * (1 + pct), decimals)
        return round(prev_close * (1 - pct), decimals)

    @staticmethod
    def _is_limit_up(code, kline, prev_close):
        """是否涨停（最高价=收盘价且收盘价>=涨停价）"""
        pct = 0.20 if code.startswith('3') or code.startswith('688') else 0.10
        decimals = 3 if code.startswith(('15', '51', '56', '58')) else 2
        tolerance = 10 ** -decimals  # 股票0.01, ETF0.001
        limit_price = round(prev_close * (1 + pct), decimals)
        return kline['high'] == kline['close'] and kline['close'] + tolerance >= limit_price

    @staticmethod
    def _is_limit_down(code, kline, prev_close):
        """是否跌停（最低价=收盘价且收盘价<=跌停价）"""
        pct = 0.20 if code.startswith('3') or code.startswith('688') else 0.10
        decimals = 3 if code.startswith(('15', '51', '56', '58')) else 2
        tolerance = 10 ** -decimals
        limit_price = round(prev_close * (1 - pct), decimals)
        return kline['low'] == kline['close'] and kline['close'] - tolerance <= limit_price

    def _adjust_open_sizes(self, account_id, action_queue):
        """
        对开仓/加仓动作计算手数 + 资金可购性校验

        无论是否降级，都将最终手数写入 item['shares']
        """
        summary = self.account_manager.get_summary(account_id)
        available = summary.get('available', 0) if summary else 0
        capital = summary.get('total', 0) if summary else 0

        if available <= 0 or capital <= 0:
            return action_queue

        for item in action_queue:
            action = item.get('action', '')
            if action not in ('开仓', '加仓'):
                continue

            price = item.get('price', 0)
            atr = item.get('atr', 0)
            if price <= 0 or atr <= 0:
                continue

            # 用 calc_unit_size 计算理论手数
            shares_per_unit = calc_unit_size(capital, atr, price)
            total_shares = shares_per_unit
            estimated_cost = total_shares * price * 1.002

            if available < estimated_cost:
                # 资金不足，降级手数
                affordable_lots = int(available * 0.98 / (price * 100))
                if affordable_lots <= 0:
                    logger.warning(f"[{item.get('code', '')}] 可用资金不足1手(可用{available:.0f}, 单价{price:.2f})")
                    continue
                total_shares = affordable_lots * 100
                code = item.get('code', '')
                logger.info(f"[{code}] 资金降级: {shares_per_unit}股 → {total_shares}股(可用{available:.0f})")

            # 写入手数（无论是否降级）
            item['shares'] = total_shares
            available -= total_shares * price * 1.002

        return action_queue
