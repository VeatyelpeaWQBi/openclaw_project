"""
海龟交易法策略
基于经典海龟交易法则的A股趋势跟踪策略
"""

import logging
import random
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

    def __init__(self):
        self.account_manager = AccountManager()
        self.position_manager = PositionManager()
        self.candidate_pool = CandidatePool()
        self.signal_checker = SignalChecker()

    def run(self) -> dict:
        """
        执行海龟交易法策略主流程

        返回:
            dict: {
                'date_str': str,
                'candidates': list,
                'signals': list,
                'has_signal': bool,
                'skip_reason': str,
                'metadata': dict,
            }
        """
        date_str = datetime.now().strftime('%Y-%m-%d')
        logger.info(f"=== 海龟交易法策略运行 — {date_str} ===")

        # 1. 检查冷却释放
        released = self.position_manager.check_cooldown_release()
        if released:
            logger.info(f"冷却释放: {released}")

        # 2. 检查账户是否已初始化
        account_summary = self.account_manager.get_summary()
        if account_summary.get('total', 0) <= 0:
            logger.error("账户未初始化，请先设置资金（如: 账户24万）")
            return {
                "date_str": date_str,
                "candidates": [],
                "signals": [],
                "has_signal": False,
                "skip_reason": "账户未初始化，请先发送 账户24万 设置资金",
                "metadata": {},
            }


        # 3. 加载持仓
        positions = self.position_manager.get_active_positions()
        logger.info(f"当前持仓: {len(positions)} 只")

        # 4. 构建候选池
        candidates = self.candidate_pool.get_candidate_list()
        logger.info(f"候选池: {len(candidates)} 只")

        # 5. 加载K线数据
        kline_data = {}
        holding_codes = [p['code'] for p in positions]
        candidate_codes = [c['code'] for c in candidates if c.get('code')]
        all_codes = list(set(holding_codes + candidate_codes))

        # 判断今天是否交易日
        import sqlite3 as _sqlite3
        from core.paths import DB_PATH as _DB_PATH
        today_str = datetime.now().strftime('%Y-%m-%d')
        _conn_check = _sqlite3.connect(_DB_PATH)
        _row = _conn_check.execute("SELECT trade_status FROM trade_calendar WHERE trade_date = ?", (today_str,)).fetchone()
        _conn_check.close()
        is_trading_day = bool(_row and _row[0] == 1)

        if is_trading_day:
            # 交易日：直接调akshare获取实时数据（跳过封装层的sleep）
            import akshare as ak
            from core.storage import save_daily_kline_to_sqlite, get_trading_day_offset

            prev_trade_date = get_trading_day_offset(1)
            start_date = prev_trade_date.replace('-', '') if prev_trade_date else (datetime.now() - __import__('datetime').timedelta(days=5)).strftime('%Y%m%d')
            end_date = datetime.now().strftime('%Y%m%d')

            logger.info(f"交易日：从API获取{len(all_codes)}只股票数据...")
            for code in all_codes:
                try:
                    market = 'sh' if code.startswith(('6',)) else 'sz'
                    sina_code = f'{market}{code}'
                    df_api = ak.stock_zh_a_daily(symbol=sina_code, start_date=start_date, end_date=end_date, adjust='qfq')
                    if df_api is not None and not df_api.empty:
                        import pandas as _pd
                        df_api['date'] = _pd.to_datetime(df_api['date'])
                        name = next((c.get('name', '') for c in candidates if c.get('code') == code), '')
                        save_daily_kline_to_sqlite(code, name, df_api)
                except Exception as e:
                    logger.debug(f"[{code}] API获取失败: {e}")
                import time as _time
                _time.sleep(random.uniform(0.6, 1.2))  # 防新浪限流
        else:
            logger.info("非交易日：直接从DB读取")

        # 从DB加载完整日K
        for code in all_codes:
            df = get_daily_data_from_sqlite(code, days=350)
            if not df.empty:
                kline_data[code] = df

        logger.info(f"加载K线数据: {len(kline_data)} 只")
        # 6. 信号检测
        signals = self.signal_checker.check_all(
            self.position_manager,
            self.account_manager,
            self.candidate_pool,
            kline_data,
        )

        # 7. 汇总结果
        has_signal = len(signals) > 0
        critical_count = sum(1 for s in signals if s.get('urgency') == 'critical')

        result = {
            'date_str': date_str,
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

        logger.info(f"策略运行完成: 持仓{len(positions)}只, 候选{len(candidates)}只, 信号{len(signals)}个")
        return result

    def generate_report(self, result: dict) -> str:
        """
        生成海龟交易法日报

        参数:
            result: run() 的返回值

        返回:
            str: 日报文本
        """
        metadata = result.get('metadata', {})

        report = _generate_report(
            signals=result.get('signals', []),
            positions=metadata.get('positions', []),
            account=metadata.get('account', {}),
            candidates=result.get('candidates', []),
        )

        return report
