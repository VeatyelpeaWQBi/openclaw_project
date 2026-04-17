"""
Robot Executor — 海龟机器人交易执行器
代理 TrendTradingExecutor，在执行前增加机器人特有的逻辑：
  - 资金可购性校验
  - 涨停买不到 / 跌停卖不掉拦截

职责：
  - 接收信号检测器输出的动作队列
  - 预处理（资金校验、涨跌停拦截）
  - 委托给 TrendTradingExecutor 执行
  - 汇总执行结果
"""

import logging
from datetime import datetime

from core.storage import get_recent_trade_dates, get_stocks_daily_kline_on_date
from infra.account_manager import AccountManager
from strategies.trend_trading.trend_trading_executor import TrendTradingExecutor
from strategies.trend_trading.atr import calc_unit_size, calc_stop_price, calc_add_price

logger = logging.getLogger(__name__)


class RobotExecutor:
    """海龟机器人交易执行器"""

    def __init__(self):
        self.tt_executor = TrendTradingExecutor()
        self.account_manager = AccountManager()
        self.target_date = None

    def set_target_date(self, target_date):
        """传递 target_date 给整个执行链"""
        self.target_date = target_date
        self.account_manager.set_target_date(target_date)
        self.tt_executor.pm.set_target_date(target_date)
        self.tt_executor.tt_pm.set_target_date(target_date)
        self.tt_executor.account_manager.set_target_date(target_date)
        self.tt_executor.trade_executor.set_target_date(target_date)
        self.tt_executor.trade_executor.position_manager.set_target_date(target_date)
        self.tt_executor.trade_executor.account_manager.set_target_date(target_date)

    def execute_signals(self, account_id, action_queue: list) -> dict:
        """
        执行信号检测器输出的动作队列

        预处理顺序：
          1. 涨跌停拦截（涨停不买，跌停不卖）
          2. 资金校验
          3. 委托给 TrendTradingExecutor 执行
          4. 每日结算：更新 total_capital = available + 持仓市值
        """
        if not action_queue:
            # 无动作也要结算
            self._daily_settlement(account_id)
            return {
                'account_id': account_id, 'total': 0,
                'success': 0, 'failed': 0, 'skipped': 0,
                'results': [], 'summary': '无交易动作',
            }

        # 1. 涨跌停拦截
        action_queue, skipped = self._check_limit_up_down(action_queue)

        # 2. 资金校验
        action_queue, size_skipped = self._adjust_open_sizes(account_id, action_queue)

        # 3. 执行
        result = self.tt_executor.execute_signals(account_id, action_queue)

        # 合并跳过的计数
        result['skipped'] = result.get('skipped', 0) + skipped + size_skipped
        result['total'] = result.get('total', 0) + skipped + size_skipped

        # 4. 每日结算
        self._daily_settlement(account_id)

        return result

    def _daily_settlement(self, account_id):
        """
        每日结算：更新 total_capital = available_capital + 持仓市值
        """
        # 1. 获取 available_capital
        available = self.account_manager.get_available(account_id)

        # 2. 获取持仓列表
        positions = self.tt_executor.pm.get_active_positions(account_id)

        # 3. 查询当日收盘价，计算持仓市值
        position_value = 0.0
        if positions:
            codes = [pos['code'] for pos in positions]
            base_date = self.target_date or datetime.now().strftime('%Y-%m-%d')
            trade_dates = get_recent_trade_dates(base_date, limit=1, inclusive=True)
            if trade_dates:
                latest_date = trade_dates[0]
                klines = get_stocks_daily_kline_on_date(codes, latest_date)
                for pos in positions:
                    code = pos['code']
                    if code in klines:
                        latest_close = float(klines[code]['close'])
                        position_value += pos['total_shares'] * latest_close

        # 4. 更新 total_capital
        total_capital = available + position_value
        self.account_manager.update_total_capital(account_id, total_capital)
        logger.info(f"[账户{account_id}] 每日结算: available={available:.2f}, 持仓市值={position_value:.2f}, total={total_capital:.2f}")

    def _check_limit_up_down(self, action_queue):
        """
        涨跌停拦截

        买动作（开仓/加仓）+ 涨停 → 跳过
        卖动作（止损/退出/减仓）+ 跌停 → 跳过

        返回: (过滤后的action_queue, 跳过数量)
        """
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

        # 查询 <= target_date 的最后两个交易日
        base_date = self.target_date or datetime.now().strftime('%Y-%m-%d')
        trade_dates = get_recent_trade_dates(base_date, limit=2, inclusive=True)

        if len(trade_dates) < 2:
            return action_queue, 0  # 没有足够的交易日数据

        latest_date = trade_dates[0]

        # 批量查询当日日K
        latest_kline = get_stocks_daily_kline_on_date(list(codes_to_check), latest_date)

        # 批量查询前一日日K，提取收盘价
        prev_klines = get_stocks_daily_kline_on_date(list(codes_to_check), trade_dates[1])
        prev_closes = {code: k['close'] for code, k in prev_klines.items()}

        # 逐个检查
        filtered = []
        skipped = 0
        for item in action_queue:
            action = item.get('action', '')
            code = item.get('code', '')
            name = item.get('name', '')

            if action in ('开仓', '加仓') and code in latest_kline:
                kline = latest_kline[code]
                prev_close = prev_closes.get(code)
                if prev_close and self._is_limit_up(code, kline, prev_close):
                    limit_price = self._get_limit_price(code, prev_close, up=True)
                    logger.info(f"[{code}{name}] 涨停({limit_price})，跳过{action}，等待次日")
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
        pct = 0.20 if code.startswith('3') or code.startswith('688') or code.startswith('689') else 0.10
        decimals = 3 if code.startswith(('15', '51', '56', '58')) else 2
        if up:
            return round(prev_close * (1 + pct), decimals)
        return round(prev_close * (1 - pct), decimals)

    @staticmethod
    def _is_limit_up(code, kline, prev_close):
        """是否涨停（最高价=收盘价且收盘价>=涨停价）"""
        pct = 0.20 if code.startswith('3') or code.startswith('688') or code.startswith('689') else 0.10
        decimals = 3 if code.startswith(('15', '51', '56', '58')) else 2
        tolerance = 10 ** -decimals  # 股票0.01, ETF0.001
        limit_price = round(prev_close * (1 + pct), decimals)
        return kline['high'] == kline['close'] and kline['close'] + tolerance >= limit_price

    @staticmethod
    def _is_limit_down(code, kline, prev_close):
        """是否跌停（最低价=收盘价且收盘价<=跌停价）"""
        pct = 0.20 if code.startswith('3') or code.startswith('688') or code.startswith('689') else 0.10
        decimals = 3 if code.startswith(('15', '51', '56', '58')) else 2
        tolerance = 10 ** -decimals
        limit_price = round(prev_close * (1 - pct), decimals)
        return kline['low'] == kline['close'] and kline['close'] - tolerance <= limit_price

    def _adjust_open_sizes(self, account_id, action_queue):
        """
        对开仓/加仓动作计算手数 + 资金可购性校验

        资金不足时直接跳过（不做手数降级）
        返回: (过滤后的队列, 跳过数量)
        """
        summary = self.account_manager.get_summary(account_id)
        available = summary.get('available_capital', 0) if summary else 0
        capital = summary.get('total_capital', 0) if summary else 0

        if available <= 0 or capital <= 0:
            return action_queue, 0

        # 获取单日开仓上限
        config = self.account_manager.get_position_config(account_id)
        max_daily_open = config.get('max_daily_open', 2)
        today_opens = self.tt_executor.pm.count_today_opens(account_id)
        opens_in_batch = 0  # 本批次已接受的开仓数

        filtered_queue = []
        skipped = 0
        for item in action_queue:
            action = item.get('action', '')
            if action not in ('开仓', '加仓'):
                filtered_queue.append(item)
                continue

            # 开仓数已达上限，直接跳出循环
            if action == '开仓' and (today_opens + opens_in_batch) >= max_daily_open:
                remaining = sum(1 for x in action_queue[action_queue.index(item):] if x.get('action') == '开仓')
                logger.info(f"今日已开仓{today_opens + opens_in_batch}个(上限{max_daily_open})，后续{remaining}个开仓信号全部跳过")
                skipped += remaining
                break

            price = item.get('price', 0)
            atr = item.get('atr', 0)
            if price <= 0 or atr <= 0:
                filtered_queue.append(item)
                continue

            if action == '开仓':
                # 开仓：计算1单位大小（开仓后固定不变）
                shares_per_unit = calc_unit_size(capital, atr, price)

                # 0 = 1手金额超5%仓位上限，不可开仓
                if shares_per_unit <= 0:
                    code = item.get('code', '')
                    logger.info(f"[{code}] 1手金额超5%仓位上限，跳过开仓(资本{capital:,.0f}, 价{price:.2f})")
                    skipped += 1
                    continue

                total_shares = shares_per_unit
                stop_price = calc_stop_price(price, atr)
                next_add_price = calc_add_price(price, atr)

            else:
                # 加仓：使用DB中已有的shares_per_unit（开仓时固定，不重算）
                code = item.get('code', '')
                pos = self.tt_executor.pm.get_position(account_id, code)
                if not pos:
                    logger.warning(f"[{code}] 持仓不存在，跳过加仓")
                    skipped += 1
                    continue
                shares_per_unit = pos.get('shares_per_unit', 0)
                if shares_per_unit <= 0:
                    logger.warning(f"[{code}] shares_per_unit异常，跳过加仓")
                    skipped += 1
                    continue
                total_shares = shares_per_unit
                stop_price = calc_stop_price(price, atr)
                next_add_price = calc_add_price(price, atr)

            estimated_cost = total_shares * price * 1.00013

            if available < estimated_cost:
                # 资金不足，直接跳过
                code = item.get('code', '')
                logger.warning(f"[{code}] 可用资金不足1单位(需{estimated_cost:.0f}, 可用{available:.0f})，跳过{action}")
                skipped += 1
                continue

            # 写入手数
            item['shares'] = total_shares

            # 补充 position_params
            item['position_params'] = {
                'shares_per_unit': shares_per_unit,
                'total_shares': total_shares,
                'stop_price': stop_price,
                'next_add_price': next_add_price,
            }

            available -= total_shares * price * 1.00013
            filtered_queue.append(item)

            # 记录本批次开仓数
            if action == '开仓':
                opens_in_batch += 1

        return filtered_queue, skipped
