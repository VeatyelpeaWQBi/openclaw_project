"""
海龟交易法 — 信号检测器
检查止损、退出、加仓、预警、入场等信号
"""

import logging
from strategies.turtle.breakout import check_entry_signal, check_exit_signal
from strategies.turtle.filters import trend_filter, is_eligible
from strategies.turtle.atr import get_atr_value

logger = logging.getLogger(__name__)


class SignalChecker:
    """海龟交易法信号检测器"""

    def check_all(self, position_manager, account_manager, candidate_pool, kline_data):
        """
        主入口：检查所有信号

        参数:
            position_manager: PositionManager实例
            account_manager: AccountManager实例
            candidate_pool: CandidatePool实例
            kline_data: dict，{code: DataFrame} 日K数据字典

        返回:
            list: 信号列表 [{type, code, name, detail, urgency}]
        """
        signals = []

        # 1. 检查现有持仓
        positions = position_manager.get_active_positions()
        for pos in positions:
            code = pos['code']
            df = kline_data.get(code)

            if df is None or df.empty:
                logger.warning(f"[{code}] 无K线数据，跳过信号检测")
                continue

            latest_price = float(df['close'].iloc[-1])

            # 止损检查（最高优先级）
            sl = self.check_stop_loss(pos, latest_price)
            if sl:
                signals.append(sl)
                continue  # 止损后不再检查其他信号

            # 预警检查
            warn = self.check_risk_warning(pos, latest_price)
            if warn:
                signals.append(warn)

            # 退出检查
            exit_sig = self.check_exit(pos, df)
            if exit_sig:
                signals.append(exit_sig)
                continue

            # 加仓检查
            atr = pos.get('atr_value', 0)
            if not atr:
                atr = get_atr_value(df)
            add_sig = self.check_add(pos, latest_price, atr)
            if add_sig:
                signals.append(add_sig)

        # 2. 检查候选池入场信号
        holding_codes = {p['code'] for p in positions}
        for stock in (candidate_pool.merged_pool if hasattr(candidate_pool, 'merged_pool') else []):
            code = stock.get('code', '')
            if not code or code in holding_codes:
                continue

            df = kline_data.get(code)
            if df is None or df.empty:
                continue

            entry_sig = self.check_entry(stock, df)
            if entry_sig:
                signals.append(entry_sig)

        # 按紧急度排序
        urgency_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        signals.sort(key=lambda s: urgency_order.get(s.get('urgency', 'low'), 3))

        logger.info(f"信号检测完成: 共{len(signals)}个信号")
        return signals

    def check_stop_loss(self, position, latest_price):
        """
        止损检查：当前价 ≤ 止损价

        参数:
            position: 持仓记录
            latest_price: 最新价格

        返回:
            dict or None: 止损信号
        """
        stop_price = position.get('current_stop', 0)
        if stop_price <= 0:
            return None

        if latest_price <= stop_price:
            logger.warning(f"[{position['code']}] 触发止损! 现价{latest_price} ≤ 止损价{stop_price}")
            return {
                'type': 'stop_loss',
                'code': position['code'],
                'name': position.get('name', ''),
                'detail': f"现价{latest_price:.2f} 触及止损价{stop_price:.2f}，需立即卖出",
                'urgency': 'critical',
                'price': latest_price,
            }
        return None

    def check_exit(self, position, df):
        """
        退出检查：反向突破唐奇安通道下轨

        参数:
            position: 持仓记录
            df: 日K数据

        返回:
            dict or None: 退出信号
        """
        exit_sig = check_exit_signal(df, short=10, long=20)
        if exit_sig['signal']:
            logger.info(f"[{position['code']}] 触发退出信号: {exit_sig['type']}")
            return {
                'type': 'exit',
                'code': position['code'],
                'name': position.get('name', ''),
                'detail': f"收盘价{exit_sig['exit_price']:.2f} 跌破{exit_sig['type']}通道下轨{exit_sig['channel_low']:.2f}",
                'urgency': 'high',
                'price': exit_sig['exit_price'],
            }
        return None

    def check_add(self, position, latest_price, atr):
        """
        加仓检查：当前价 ≥ 加仓触发价

        参数:
            position: 持仓记录
            latest_price: 最新价格
            atr: ATR值

        返回:
            dict or None: 加仓信号
        """
        next_add = position.get('next_add_price', 0)
        max_units = 4  # 最大4单位

        if next_add <= 0:
            return None

        if position.get('units', 0) >= max_units:
            return None

        if latest_price >= next_add:
            logger.info(f"[{position['code']}] 触发加仓信号! 现价{latest_price} ≥ 加仓价{next_add}")
            return {
                'type': 'add',
                'code': position['code'],
                'name': position.get('name', ''),
                'detail': f"现价{latest_price:.2f} 达到加仓价{next_add:.2f}，当前{position.get('units', 0)}单位",
                'urgency': 'medium',
                'price': latest_price,
            }
        return None

    def check_risk_warning(self, position, latest_price):
        """
        预警检查：距止损 < 3%

        参数:
            position: 持仓记录
            latest_price: 最新价格

        返回:
            dict or None: 预警信号
        """
        stop_price = position.get('current_stop', 0)
        if stop_price <= 0 or latest_price <= 0:
            return None

        distance_pct = (latest_price - stop_price) / latest_price * 100

        if 0 < distance_pct < 3:
            logger.info(f"[{position['code']}] 风险预警! 距止损仅{distance_pct:.1f}%")
            return {
                'type': 'warning',
                'code': position['code'],
                'name': position.get('name', ''),
                'detail': f"现价{latest_price:.2f} 距止损价{stop_price:.2f}仅{distance_pct:.1f}%",
                'urgency': 'high',
                'price': latest_price,
            }
        return None

    def check_entry(self, stock, df):
        """
        建仓检查：突破信号 + 趋势过滤

        参数:
            stock: 候选股票 {code, name, ...}
            df: 日K数据

        返回:
            dict or None: 入场信号
        """
        code = stock.get('code', '')

        # 基础过滤
        eligible, reason = is_eligible(stock)
        if not eligible:
            return None

        # 突破信号
        entry = check_entry_signal(df, short=20, long=55)
        if not entry['signal']:
            return None

        # 趋势过滤（非强制，但记录）
        trend = trend_filter(df)

        # ATR计算
        atr = get_atr_value(df)
        if atr <= 0:
            return None

        logger.info(f"[{code}] 入场信号! 突破类型={entry['type']}，趋势={trend}")
        return {
            'type': 'entry',
            'code': code,
            'name': stock.get('name', ''),
            'detail': f"{entry['type']}突破，收盘{entry['break_price']:.2f} 突破通道{entry['channel_high']:.2f}，趋势{trend}，ATR={atr:.2f}",
            'urgency': 'medium',
            'price': entry['break_price'],
            'atr': atr,
            'trend': trend,
            'breakout_type': entry['type'],
        }
