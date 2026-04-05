"""
海龟交易法策略
基于经典海龟交易法则的A股趋势跟踪策略
"""

import logging
from datetime import datetime
from strategies.base import BaseStrategy
from core.storage import get_daily_data_from_sqlite
from strategies.turtle.account_manager import AccountManager
from strategies.turtle.position_manager import PositionManager
from strategies.turtle.candidate_pool import CandidatePool
from strategies.turtle.signal_checker import SignalChecker
from strategies.turtle.report import generate_report as _generate_report

logger = logging.getLogger(__name__)


class TurtleStrategy(BaseStrategy):
    """海龟交易法策略"""

    @property
    def name(self) -> str:
        return 'turtle'

    def __init__(self, account_id=None):
        """
        参数:
            account_id: 账户ID（None则遍历所有活跃账户）
        """
        self.account_manager = AccountManager()
        self.position_manager = PositionManager()
        self.candidate_pool = CandidatePool()
        self.signal_checker = SignalChecker()
        self.account_id = account_id

    def _get_target_accounts(self):
        """获取目标账户列表"""
        if self.account_id is not None:
            account = self.account_manager.get_summary(self.account_id)
            if account:
                return [account]
            return []
        return self.account_manager.get_all_active_accounts()

    def run(self, account_id=None) -> dict:
        """
        执行海龟交易法策略主流程

        参数:
            account_id: 覆盖实例的account_id（可选）

        返回:
            dict: {
                'date_str': str,
                'accounts': list,  # 各账户运行结果
                'has_signal': bool,
            }
        """
        target_id = account_id or self.account_id
        date_str = datetime.now().strftime('%Y-%m-%d')
        logger.info(f"=== 海龟交易法策略运行 — {date_str} ===")

        # 确定目标账户
        if target_id is not None:
            accounts = [self.account_manager.get_summary(target_id)]
            accounts = [a for a in accounts if a is not None]
        else:
            accounts = self.account_manager.get_all_active_accounts()

        if not accounts:
            logger.warning("无活跃账户，跳过")
            return {
                "date_str": date_str,
                "accounts": [],
                "has_signal": False,
                "skip_reason": "无活跃账户",
            }

        # 逐账户执行
        all_results = []
        for account in accounts:
            aid = account['id']
            result = self._run_for_account(aid, date_str)
            all_results.append(result)

        has_signal = any(r.get('has_signal', False) for r in all_results)

        return {
            'date_str': date_str,
            'accounts': all_results,
            'has_signal': has_signal,
        }

    def _run_for_account(self, account_id, date_str):
        """单账户执行逻辑"""
        logger.info(f"[账户{account_id}] 开始运行")

        # 1. 检查冷却释放
        released = self.position_manager.check_cooldown_release(account_id)
        if released:
            logger.info(f"[账户{account_id}] 冷却释放: {released}")

        # 2. 检查账户
        account_summary = self.account_manager.get_summary(account_id)
        if not account_summary or account_summary.get('total', 0) <= 0:
            logger.error(f"[账户{account_id}] 账户异常")
            return {
                "account_id": account_id,
                "has_signal": False,
                "skip_reason": "账户异常",
                "metadata": {},
            }

        # 3. 加载持仓
        positions = self.position_manager.get_active_positions(account_id)
        logger.info(f"[账户{account_id}] 当前持仓: {len(positions)} 只")

        # 4. 构建候选池
        candidates = self.candidate_pool.get_candidate_list()
        logger.info(f"候选池: {len(candidates)} 只")

        # 5. 加载K线数据
        kline_data = {}
        holding_codes = [p['code'] for p in positions]
        candidate_codes = [c['code'] for c in candidates if c.get('code')]
        all_codes = list(set(holding_codes + candidate_codes))

        from core.data_access import get_stock_daily_kline_range
        from core.storage import save_daily_kline_to_sqlite, get_trading_day_offset

        prev_trade_date = get_trading_day_offset(1)
        if prev_trade_date:
            start_date = prev_trade_date.replace('-', '')
            end_date = datetime.now().strftime('%Y%m%d')
        else:
            from datetime import timedelta
            start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
            end_date = datetime.now().strftime('%Y%m%d')

        for code in all_codes:
            try:
                market = 'sh' if code.startswith(('6',)) else 'sz'
                recent_df = get_stock_daily_kline_range(code, market=market, start_date=start_date, end_date=end_date)
                if not recent_df.empty:
                    name = ''
                    for c in candidates:
                        if c.get('code') == code:
                            name = c.get('name', '')
                            break
                    save_daily_kline_to_sqlite(code, name, recent_df)
            except Exception as e:
                logger.debug(f"[{code}] 更新日K失败: {e}")

            df = get_daily_data_from_sqlite(code, days=350)
            if not df.empty:
                kline_data[code] = df

        logger.info(f"[账户{account_id}] 加载K线数据: {len(kline_data)} 只")

        # 6. 信号检测
        signals = self.signal_checker.check_all(
            self.position_manager,
            self.account_manager,
            self.candidate_pool,
            kline_data,
            account_id,
        )

        # 7. 汇总结果
        has_signal = len(signals) > 0
        critical_count = sum(1 for s in signals if s.get('urgency') == 'critical')

        return {
            'account_id': account_id,
            'nickname': account_summary.get('nickname', ''),
            'candidates': [
                {'code': c.get('code', ''), 'name': c.get('name', ''), 'source': c.get('source', '')}
                for c in candidates
            ],
            'signals': signals,
            'has_signal': has_signal,
            'skip_reason': '' if has_signal else '无交易信号',
            'metadata': {
                'positions': positions,
                'account': account_summary,
                'candidates': candidates,
                'holding_count': len(positions),
                'candidate_count': len(candidates),
                'signal_count': len(signals),
                'critical_count': critical_count,
                'released_cooldown': released,
            },
        }

    def generate_report(self, result: dict) -> str:
        """
        生成海龟交易法日报

        参数:
            result: run() 的返回值

        返回:
            str: 日报文本
        """
        accounts = result.get('accounts', [])
        if not accounts:
            return f"📊 海龟交易法 — {result.get('date_str', '')}\n无活跃账户"

        # 多账户报告
        lines = [f"📊 海龟交易法 — {result.get('date_str', '')}"]
        lines.append("")

        for acc_result in accounts:
            aid = acc_result.get('account_id', '')
            nickname = acc_result.get('nickname', f'账户{aid}')
            metadata = acc_result.get('metadata', {})

            lines.append(f"=== {nickname} (ID:{aid}) ===")

            report = _generate_report(
                signals=acc_result.get('signals', []),
                positions=metadata.get('positions', []),
                account=metadata.get('account', {}),
                candidates=acc_result.get('candidates', []),
            )
            lines.append(report)
            lines.append("")

        return '\n'.join(lines)
