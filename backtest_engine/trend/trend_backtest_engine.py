"""
趋势交易回测引擎
基于交易日遍历，完整模拟趋势交易流水线

用法:
    from backtest_engine.trend.trend_backtest_engine import TrendBacktestEngine
    engine = TrendBacktestEngine()
    result = engine.run_backtest(account_id=123, start_date='2026-01-01', end_date='2026-04-16')
"""

import sys
import os
import logging
import time as _time

import pandas as pd

logger = logging.getLogger(__name__)

# quant_trading 项目路径（backtest_engine 与 quant_trading 同级）
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_QT_ROOT = os.path.join(_PROJECT_ROOT, 'quant_trading')
if _QT_ROOT not in sys.path:
    sys.path.insert(0, _QT_ROOT)

from core.storage import get_db_connection, get_all_stocks_daily_data, get_trading_day_offset_from
from strategies.trend_trading.score._base import get_trade_dates
from infra.account_manager import AccountManager
from strategies.trend_trading.trend_trading_position_manager import TrendTradingPositionManager
from strategies.trend_trading.candidate_pool import CandidatePool
from strategies.trend_trading.signal_checker import SignalChecker
from executor.robot_executor import RobotExecutor


class TrendBacktestEngine:
    """趋势交易回测引擎"""

    def run_backtest(self, account_id, start_date, end_date, stock_codes=None):
        """
        执行趋势交易回测

        参数:
            account_id: 模拟账户ID
            start_date: 回测起始日期 'YYYY-MM-DD'
            end_date: 回测结束日期 'YYYY-MM-DD'
            stock_codes: list[str] 需要预加载K线的股票代码列表
                        None则从watchlist候选池获取

        返回:
            dict: {
                'account_id': int,
                'start_date': str,
                'end_date': str,
                'trade_dates': list[str],
                'daily_results': list[dict],
                'monthly_records': list[dict],
            }
        """
        logger.info(f"=== 趋势交易回测 === 账户{account_id} {start_date} ~ {end_date}")
        start_time = _time.time()

        # Step 0: 校验账户为模拟账户
        account = self._get_account_balance(account_id)
        conn = get_db_connection()
        try:
            row = conn.execute("SELECT simulator FROM account WHERE id = ?", (account_id,)).fetchone()
            if not row:
                logger.error(f"账户{account_id}不存在")
                return {'error': '账户不存在'}
            if row['simulator'] != 1:
                logger.error(f"账户{account_id}不是模拟账户(simulator={row['simulator']})")
                return {'error': '非模拟账户，回测需要simulator=1'}
        finally:
            conn.close()

        # Step 1: 获取交易日列表
        trade_dates = get_trade_dates(start_date, end_date)

        if not trade_dates:
            logger.error(f"无交易日数据: {start_date} ~ {end_date}")
            return {'error': '无交易日数据'}

        logger.info(f"交易日: {len(trade_dates)} 天 ({trade_dates[0]} ~ {trade_dates[-1]})")

        # Step 2: 获取需要加载的股票代码（watchlist + 已有持仓）
        if stock_codes is None:
            stock_codes = self._get_watchlist_codes(account_id)
        logger.info(f"预加载股票: {len(stock_codes)} 只")

        # Step 3: 预加载日K数据（含回测前350天历史，供均线/ATR等指标计算）
        data_start = get_trading_day_offset_from(start_date, -350)
        if not data_start:
            data_start = '2020-01-01'  # fallback
        logger.info(f"预加载K线数据: {data_start} ~ {end_date}...")
        all_data = get_all_stocks_daily_data(stock_codes, data_start, end_date)
        logger.info(f"K线数据加载完成: {len(all_data)} 只股票")

        # Step 4: 初始化模块
        account_manager = AccountManager()
        position_manager = TrendTradingPositionManager()
        candidate_pool = CandidatePool()
        signal_checker = SignalChecker()
        robot = RobotExecutor()

        # 候选池只查一次（watchlist在回测期间不变）
        candidate_pool.get_candidate_list()

        # Step 5: 遍历交易日
        daily_results = []
        monthly_acc = {}  # {year_month: {actions_count, start_capital, ...}}

        for i, trade_date in enumerate(trade_dates):
            logger.info(f"进入交易日: {trade_date}")

            # 切片K线数据（截至当日）
            kline_data = {}
            for code, df in all_data.items():
                date_strs = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                sliced = df[date_strs <= trade_date]
                if not sliced.empty:
                    kline_data[code] = sliced

            # 执行单日回测
            result = self._run_single_day(
                account_id, trade_date, kline_data,
                account_manager, position_manager, candidate_pool,
                signal_checker, robot,
            )
            daily_results.append(result)

            # 记录当日动作
            self._accumulate_monthly(account_id, trade_date, result, monthly_acc)

            # 进度日志
            if (i + 1) % 20 == 0 or i == len(trade_dates) - 1:
                logger.info(f"  进度: {i + 1}/{len(trade_dates)} 天")

        # Step 6: 写入月度汇总
        monthly_records = self._save_monthly_records(account_id, monthly_acc)

        # Step 7: 生成报告
        from trend.trend_backtest_report import generate_backtest_report
        report = generate_backtest_report(
            account_id=account_id,
            start_date=start_date,
            end_date=end_date,
            trade_dates=trade_dates,
            daily_results=daily_results,
            monthly_records=monthly_records,
        )

        elapsed = _time.time() - start_time
        logger.info(f"=== 回测完成: {len(trade_dates)}天, 耗时{elapsed:.1f}秒 ===")

        return {
            'account_id': account_id,
            'start_date': start_date,
            'end_date': end_date,
            'trade_dates': trade_dates,
            'daily_results': daily_results,
            'monthly_records': monthly_records,
            'report': report,
            'elapsed': f'{elapsed:.1f}秒',
        }

    def _run_single_day(self, account_id, trade_date, kline_data,
                         account_manager, position_manager, candidate_pool,
                         signal_checker, robot):
        """
        执行单日回测

        参数:
            account_id: 回测账户ID
            trade_date: 目标日期 'YYYY-MM-DD'
            kline_data: {code: DataFrame} 截至当日的K线数据
            account_manager/position_manager/candidate_pool/signal_checker/robot: 复用实例

        返回:
            dict: 该账户当日的执行结果
        """
        # 设置回测日期
        account_manager.set_target_date(trade_date)
        position_manager.set_target_date(trade_date)
        signal_checker.set_target_date(trade_date)
        robot.set_target_date(trade_date)

        # 查询账户
        account = account_manager.get_summary(account_id)
        if not account:
            return {
                'account_id': account_id,
                'date_str': trade_date,
                'has_signal': False,
                'skip_reason': '账户不存在',
                'action_queue': [],
                'robot_result': None,
                'metadata': {},
            }

        nickname = account.get('nickname', f'账户{account_id}')

        if account.get('total_capital', 0) <= 0:
            return {
                'account_id': account_id,
                'nickname': nickname,
                'date_str': trade_date,
                'has_signal': False,
                'skip_reason': '账户资金为0',
                'action_queue': [],
                'robot_result': None,
                'metadata': {'account': account},
            }

        # 冷却释放
        released = position_manager.check_cooldown_release(account_id)

        # 加载持仓
        positions = position_manager.get_active_positions(account_id)

        # 信号检测
        signals = signal_checker.check_all(
            position_manager, account, candidate_pool, kline_data,
        )

        # 模拟账户：signals 即 action_queue
        action_queue = signals
        robot_result = robot.execute_signals(account_id, action_queue)

        return {
            'account_id': account_id,
            'nickname': nickname,
            'date_str': trade_date,
            'action_queue': action_queue,
            'robot_result': robot_result,
            'has_signal': len(action_queue) > 0,
            'metadata': {
                'positions': positions,
                'account': account,
                'released_cooldown': released,
            },
        }

    def _get_watchlist_codes(self, account_id):
        """从watchlist获取需要预加载的股票代码"""
        conn = get_db_connection()
        try:
            rows = conn.execute(
                "SELECT DISTINCT code FROM watchlist WHERE active = 1 AND code IS NOT NULL"
            ).fetchall()
            codes = [r['code'] for r in rows if r['code']]

            # 同时加载已有持仓的股票
            pos_rows = conn.execute(
                "SELECT DISTINCT code FROM positions WHERE account_id = ? AND status = 'HOLDING'",
                (account_id,)
            ).fetchall()
            for r in pos_rows:
                if r['code'] and r['code'] not in codes:
                    codes.append(r['code'])

            return codes
        finally:
            conn.close()

    def _accumulate_monthly(self, account_id, trade_date, result, monthly_acc):
        """累计月度统计数据"""
        year_month = trade_date[:7]  # '2026-04'

        if year_month not in monthly_acc:
            monthly_acc[year_month] = {
                'start_date': trade_date,
                'end_date': trade_date,
                'trade_days': 0,
                'start_capital': None,
                'end_capital': None,
                'open_count': 0,
                'add_count': 0,
                'reduce_count': 0,
                'close_count': 0,
            }

        acc = monthly_acc[year_month]
        acc['end_date'] = trade_date
        acc['trade_days'] += 1

        # 月初资金：第一个交易日的账户资金
        if acc['start_capital'] is None:
            meta = result.get('metadata', {})
            account_info = meta.get('account', {})
            acc['start_capital'] = account_info.get('total_capital', 0)

        # 月末资金：每日从DB获取最新资金
        account_summary = self._get_account_balance(account_id)
        if account_summary:
            acc['end_capital'] = account_summary.get('total_capital', 0)

        # 统计成功执行的动作次数（action字段是中文值）
        robot_result = result.get('robot_result', {})
        for r in robot_result.get('results', []):
            if not r.get('success'):
                continue
            action = r.get('action', '')
            if action == '开仓':
                acc['open_count'] += 1
            elif action == '加仓':
                acc['add_count'] += 1
            elif action == '减仓':
                acc['reduce_count'] += 1
            elif action in ('平仓', '止损平仓', '止盈平仓'):
                acc['close_count'] += 1

    def _get_account_balance(self, account_id):
        """查询账户余额"""
        if not account_id:
            return None
        conn = get_db_connection()
        try:
            row = conn.execute(
                "SELECT total_capital, available_capital, realized_profit FROM account WHERE id = ?",
                (account_id,)
            ).fetchone()
            if row:
                return {
                    'total_capital': float(row['total_capital']),
                    'available_capital': float(row['available_capital']),
                    'realized_profit': float(row['realized_profit']),
                }
            return None
        finally:
            conn.close()

    def _save_monthly_records(self, account_id, monthly_acc):
        """写入月度汇总到 backtest_monthly 表"""
        records = []
        conn = get_db_connection()
        try:
            for year_month, acc in sorted(monthly_acc.items()):
                start_cap = acc['start_capital'] or 0
                end_cap = acc['end_capital'] or start_cap
                profit = end_cap - start_cap
                profit_pct = (profit / start_cap * 100) if start_cap > 0 else 0

                record = {
                    'account_id': account_id,
                    'year_month': year_month,
                    'start_date': acc['start_date'],
                    'end_date': acc['end_date'],
                    'trade_days': acc['trade_days'],
                    'start_capital': round(start_cap, 2),
                    'end_capital': round(end_cap, 2),
                    'profit': round(profit, 2),
                    'profit_pct': round(profit_pct, 2),
                    'open_count': acc['open_count'],
                    'add_count': acc['add_count'],
                    'reduce_count': acc['reduce_count'],
                    'close_count': acc['close_count'],
                }
                records.append(record)

                conn.execute("""
                    INSERT OR REPLACE INTO backtest_monthly
                    (account_id, year_month, start_date, end_date, trade_days,
                     start_capital, end_capital, profit, profit_pct,
                     open_count, add_count, reduce_count, close_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    account_id, year_month, acc['start_date'], acc['end_date'],
                    acc['trade_days'], start_cap, end_cap, profit, profit_pct,
                    acc['open_count'], acc['add_count'], acc['reduce_count'], acc['close_count'],
                ))

            conn.commit()
            logger.info(f"月度汇总已写入: {len(records)} 条")
        finally:
            conn.close()

        return records
