"""
趋势交易策略
基于经典趋势交易则的A股趋势跟踪策略
"""

import logging

from datetime import datetime, timedelta

from strategies.base import BaseStrategy
from core.storage import get_db_connection, get_trading_day_offset_from, is_trade_day
from infra.account_manager import AccountManager
from strategies.trend_trading.trend_trading_position_manager import TrendTradingPositionManager
from strategies.trend_trading.candidate_pool import CandidatePool
from strategies.trend_trading.signal_checker import SignalChecker
from strategies.trend_trading.report import generate_report as _generate_report

logger = logging.getLogger(__name__)


class TrendTradingStrategy(BaseStrategy):
    """趋势交易策略"""

    @property
    def name(self) -> str:
        return 'trend_trading'

    @property
    def status(self) -> str:
        return '开发中，未稳定'

    def __init__(self):
        self.account_manager = AccountManager()
        self.position_manager = TrendTradingPositionManager()
        self.candidate_pool = CandidatePool()
        self.signal_checker = SignalChecker()

    def run(self, target_date=None) -> dict:
        """
        执行趋势交易策略主流程（遍历所有活跃账户）

        参数:
            target_date: 目标日期 'YYYY-MM-DD'，None则使用当天（实盘模式）

        返回:
            dict: {
                'date_str': str,
                'accounts': list,   # 各账户运行结果
                'has_signal': bool, # 是否有任何账户产生信号
            }
        """
        date_str = target_date or datetime.now().strftime('%Y-%m-%d')
        self._target_date = date_str
        logger.info(f"=== 趋势交易策略运行 — {date_str} ===")

        # 实盘模式：检查是否交易日
        if target_date is None:
            if not is_trade_day(date_str):
                logger.info(f"[{date_str}] 非交易日，实盘跳过运行")
                return {
                    'date_str': date_str,
                    'accounts': [],
                    'has_signal': False,
                    'skip_reason': '非交易日',
                }

        # Step 0: 查询所有活跃账户
        active_accounts = self.account_manager.get_all_active_accounts()
        logger.info(f"活跃账户: {len(active_accounts)} 个")

        if not active_accounts:
            logger.warning("无活跃账户，跳过")
            return {
                'date_str': date_str,
                'accounts': [],
                'has_signal': False,
                'skip_reason': '无活跃账户',
            }

        # 逐账户执行
        all_results = []
        for account in active_accounts:
            result = self._run_for_account(account, date_str)
            all_results.append(result)

        has_signal = any(r.get('has_signal', False) for r in all_results)

        return {
            'date_str': date_str,
            'accounts': all_results,
            'has_signal': has_signal,
        }

    def _run_for_account(self, account, date_str):
        """
        单账户执行逻辑

        参数:
            account: 账户信息dict
            date_str: 日期字符串

        返回:
            dict: 该账户的运行结果
        """
        account_id = account['id']
        nickname = account.get('nickname', f'账户{account_id}')
        logger.info(f"[{nickname}({account_id})] 开始运行")

        # Step 1: 检查账户初始化
        if account.get('total_capital', 0) <= 0:
            logger.warning(f"[{nickname}] 账户资金为0，跳过")
            return {
                'account_id': account_id,
                'nickname': nickname,
                'has_signal': False,
                'skip_reason': '账户资金为0',
                'metadata': {},
            }

        # Step 2: 检查冷却释放
        released = self.position_manager.check_cooldown_release(account_id, target_date=date_str)
        if released:
            logger.info(f"[{nickname}] 冷却释放: {released}")

        # Step 3: 加载持仓
        positions = self.position_manager.get_active_positions(account_id)
        logger.info(f"[{nickname}] 当前持仓: {len(positions)} 只")

        # Step 4: 构建候选池
        candidates = self.candidate_pool.get_candidate_list()
        logger.info(f"[{nickname}] 候选池: {len(candidates)} 只")

        # Step 5: 加载K线数据
        kline_data = self._load_kline_data(positions, candidates, target_date=date_str)

        # Step 5.5: 每日更新持仓股ATR值
        if positions:
            atr_updates = self.position_manager.update_atr_values(account_id, kline_data)

        # Step 6: 信号检测
        signals = self.signal_checker.check_all(
            self.position_manager,
            account,
            self.candidate_pool,
            kline_data,
            target_date=self._target_date,
        )

        # Step 7: 汇总结果
        is_simulator = account.get('simulator') == 1

        if is_simulator:
            # 模拟账户：signals 此时是动作队列
            action_queue = signals
            robot_result = self._execute_robot(account_id, nickname, action_queue)
            has_signal = len(action_queue) > 0
            logger.info(f"[{nickname}] 模拟执行: {len(action_queue)} 个动作")
            return {
                'account_id': account_id,
                'nickname': nickname,
                'simulator': account.get('simulator', 0),
                'action_queue': action_queue,
                'robot_result': robot_result,
                'has_signal': has_signal,
                'skip_reason': '' if has_signal else '无交易信号',
                'metadata': {
                    'positions': positions,
                    'account': account,
                    'released_cooldown': released,
                },
            }
        else:
            # 手工账户：signals 是信号列表
            has_signal = len(signals) > 0
            critical_count = sum(1 for s in signals if s.get('urgency') == 'critical')
            logger.info(f"[{nickname}] 完成: 持仓{len(positions)}只, 信号{len(signals)}个")
            return {
                'account_id': account_id,
                'nickname': nickname,
                'simulator': account.get('simulator', 0),
                'candidates': [
                    {'code': c.get('code', ''), 'name': c.get('name', ''), 'source': c.get('source', '')}
                    for c in candidates
                ],
                'signals': signals,
                'has_signal': has_signal,
                'skip_reason': '' if has_signal else '无交易信号',
                'metadata': {
                    'positions': positions,
                    'account': account,
                    'candidates': candidates,
                    'holding_count': len(positions),
                    'candidate_count': len(candidates),
                    'signal_count': len(signals),
                    'critical_count': critical_count,
                    'released_cooldown': released,
                },
            }

    def _load_kline_data(self, positions, candidates, target_date=None):
        """从SQLite批量加载K线数据（不调API，获取不到的跳过）"""
        import pandas as pd
        from collections import defaultdict

        holding_codes = [p['code'] for p in positions]
        candidate_codes = [c['code'] for c in candidates if c.get('code')]
        all_codes = list(set(holding_codes + candidate_codes))

        if not all_codes:
            return {}

        # 从交易日历获取350个交易日前的日期（基于 target_date 或今天）
        base_date = target_date or datetime.now().strftime('%Y-%m-%d')
        start_date = get_trading_day_offset_from(base_date, -350)
        if not start_date:
            # fallback：使用base_date而非now()，避免回测时日期穿越
            start_date = (datetime.strptime(base_date, '%Y-%m-%d') - timedelta(days=500)).strftime('%Y-%m-%d')

        # 批量SQL查询
        conn = get_db_connection()
        try:
            placeholders = ','.join(['?'] * len(all_codes))
            rows = conn.execute(f"""
                SELECT code, date, open, high, low, close, volume, amount,
                       turnover, change_pct, mktcap, nmc
                FROM daily_kline
                WHERE code IN ({placeholders}) AND date >= ?
                ORDER BY code, date
            """, all_codes + [start_date]).fetchall()
        finally:
            conn.close()

        # 按code分组构建DataFrame
        grouped = defaultdict(list)
        for r in rows:
            grouped[r['code']].append(dict(r))

        kline_data = {}
        for code, code_rows in grouped.items():
            df = pd.DataFrame(code_rows)
            df['date'] = df['date'].astype(str)
            df = df.sort_values('date').reset_index(drop=True)
            if not df.empty:
                kline_data[code] = df

        skipped = len(all_codes) - len(kline_data)
        logger.info(f"批量加载K线: {len(kline_data)}只 (跳过{skipped}只无数据)")
        return kline_data

    def _execute_robot(self, account_id, nickname, action_queue):
        """
        机器人账户：执行交易动作队列

        通过 RobotExecutor 执行：
          - 自动按优先级排序（平仓 > 减仓 > 加仓 > 开仓）
          - 调用 TradeExecutor 进行底层交易执行
          - T+1锁定处理
          - 执行结果记录

        参数:
            account_id: 账户ID
            nickname: 账户昵称
            action_queue: 动作队列（来自 SignalChecker._to_action_queue）

        返回:
            dict: 执行结果汇总
        """
        from executor.robot_executor import RobotExecutor
        robot = RobotExecutor()
        result = robot.execute_signals(account_id, action_queue, target_date=self._target_date)
        logger.info(f"[{nickname}] 机器人执行完成: {result['summary']}")
        return result

    def generate_report(self, result: dict) -> str:
        """
        生成趋势信号日报（多账户）

        参数:
            result: run() 的返回值

        返回:
            str: 日报文本
        """
        date_str = result.get('date_str', '')
        accounts = result.get('accounts', [])

        if not accounts:
            return f"📊 趋势交易 — {date_str}\n无活跃账户"

        lines = [f"📊 趋势交易 — {date_str}", ""]

        for acc_result in accounts:
            nickname = acc_result.get('nickname', '')
            account_id = acc_result.get('account_id', '')
            metadata = acc_result.get('metadata', {})

            lines.append(f"=== {nickname} ===")

            # 获取今日开仓数（用于报告）
            try:
                today_opens = self.position_manager.count_today_opens(account_id, target_date=date_str)
            except Exception:
                today_opens = None

            report = _generate_report(
                signals=acc_result.get('signals', []),
                positions=metadata.get('positions', []),
                account=metadata.get('account', {}),
                candidates=acc_result.get('candidates', []),
                today_opens=today_opens,
            )
            lines.append(report)
            lines.append("")

        return '\n'.join(lines)
