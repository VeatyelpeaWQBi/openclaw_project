"""
海龟交易法 — 信号检测器
检查止损、退出、加仓、预警、入场等信号

海龟交易法信号优先级（从高到低）：
  1. 止损：收盘价 ≤ 入场价 - 2×ATR → 立即退出
  2. 退出：收盘价 < N日唐奇安通道下轨 → 趋势结束退出
  3. 加仓：收盘价 ≥ 上次加仓价 + 0.5×ATR → 加1单位（最多4单位）
  4. 预警：距止损 < 3% → 提醒主人关注
  5. 建仓：空仓 + N日突破 + 均线多头 → 入场信号
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
        主入口：按优先级检查所有信号

        海龟法则：止损优先于一切，退出优先于加仓
        代码实现：用 continue 跳过后续低优先级检查
        """
        signals = []

        # === 第一部分：检查现有持仓 ===
        positions = position_manager.get_active_positions()
        for pos in positions:
            code = pos['code']
            df = kline_data.get(code)

            if df is None or df.empty:
                logger.warning(f"[{code}] 无K线数据，跳过信号检测")
                continue

            latest_price = float(df['close'].iloc[-1])

            # ① 止损检查（最高优先级）
            # 海龟法则：价格触及止损线必须立即退出，不考虑其他信号
            sl = self.check_stop_loss(pos, latest_price)
            if sl:
                signals.append(sl)
                continue  # 止损后不再检查退出/加仓

            # ② 预警检查
            # 海龟增强：A股适配，距止损<3%时提前预警
            # 不 continue，预警可以和退出/加仓同时存在
            warn = self.check_risk_warning(pos, latest_price)
            if warn:
                signals.append(warn)

            # ③ 退出检查
            # 海龟法则：System1用10日反向突破，System2用20日反向突破
            exit_sig = self.check_exit(pos, df)
            if exit_sig:
                signals.append(exit_sig)
                continue  # 退出后不再检查加仓

            # ④ 加仓检查
            # 海龟法则：价格每涨0.5×ATR加1单位，最多4单位
            atr = pos.get('atr_value', 0)
            if not atr:
                atr = get_atr_value(df)
            add_sig = self.check_add(pos, latest_price, atr)
            if add_sig:
                signals.append(add_sig)

        # === 第二部分：检查候选池入场信号 ===
        holding_codes = {p['code'] for p in positions}
        for stock in (candidate_pool.merged_pool if hasattr(candidate_pool, 'merged_pool') else []):
            code = stock.get('code', '')
            if not code or code in holding_codes:
                continue

            df = kline_data.get(code)
            if df is None or df.empty:
                continue

            # ⑤ 建仓检查
            # 海龟法则：突破 + 均线多头过滤 → 入场
            entry_sig = self.check_entry(stock, df)
            if entry_sig:
                signals.append(entry_sig)

        # 按紧急度排序：critical > high > medium > low
        urgency_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        signals.sort(key=lambda s: urgency_order.get(s.get('urgency', 'low'), 3))

        logger.info(f"信号检测完成: 共{len(signals)}个信号")
        return signals

    def check_stop_loss(self, position, latest_price):
        """
        止损检查

        海龟法则：
          止损价 = 入场价 - 2 × ATR
          收盘价 ≤ 止损价 → 立即退出全部持仓

        设计依据：
          原版海龟用 2N（2倍ATR）作为止损距离
          1N = ATR(20)，代表日均波动幅度
          2N 给价格留了约2天的正常波动空间
        """
        stop_price = position.get('current_stop', 0)
        if stop_price <= 0:
            return None

        # 海龟条件：现价 ≤ 止损价（入场价 - 2×ATR）
        if latest_price <= stop_price:
            logger.warning(f"[{position['code']}] 触及止损! 现价{latest_price} ≤ 止损价{stop_price}")
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
        趋势退出检查

        海龟法则：
          System1: 收盘价 < 过去10日最低价（10日唐奇安通道下轨）→ 退出
          System2: 收盘价 < 过去20日最低价（20日唐奇安通道下轨）→ 退出

        设计依据：
          反向突破 = 趋势可能反转的信号
          用通道下轨（而非固定百分比）让退出点随价格波动自适应
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

    def check_risk_warning(self, position, latest_price):
        """
        预警检查（A股增强）

        海龟法则：原版无此规则
        A股适配：距止损 < 3% 时预警，让主人提前关注

        条件：
          distance_pct = (现价 - 止损价) / 现价 × 100
          0 < distance_pct < 3% → 预警
          distance_pct ≤ 0 → 已跌破，由止损处理，此处不报预警
        """
        stop_price = position.get('current_stop', 0)
        if stop_price <= 0 or latest_price <= 0:
            return None

        distance_pct = (latest_price - stop_price) / latest_price * 100

        # 海龟增强：距止损不到3%且尚未跌破
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

    def check_add(self, position, latest_price, atr):
        """
        加仓检查

        海龟法则：
          加仓触发价 = 上次加仓价 + 0.5 × ATR
          收盘价 ≥ 加仓触发价 → 加1单位
          最大持仓：单市场4单位

        设计依据：
          0.5N 是原版海龟的加仓间距
          每涨0.5N加1单位，确保在趋势中逐步加码
          4单位上限 = 最大风险4%（4×1%）
        """
        next_add = position.get('next_add_price', 0)
        max_units = 4

        if next_add <= 0:
            return None

        # 海龟条件：已满4单位，不再加仓
        if position.get('units', 0) >= max_units:
            return None

        # 海龟条件：现价 ≥ 加仓触发价（上次加仓价 + 0.5×ATR）
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

    def check_entry(self, stock, df):
        """
        建仓检查

        海龟法则（System1 + System2合并）：
          ① 基础过滤：非ST、非涨停、非北交所
          ② 突破信号：
             System1: 收盘价 > 20日最高价（20日唐奇安通道上轨）
             System2: 收盘价 > 55日最高价（55日唐奇安通道上轨）
             System1有过滤器：上次20日突破盈利则跳过，等55日突破
          ③ 趋势过滤：350日均线↑ 且 25日均线↑ → 多头趋势
          ④ ATR计算：用于后续仓位大小计算

        设计依据：
          突破 = 新的价格区间 = 趋势启动信号
          均线过滤 = 确认大趋势方向，减少假突破
          20/55日 = 原版海龟的两个系统参数
        """
        code = stock.get('code', '')

        # 海龟条件①：基础过滤（A股适配）
        eligible, reason = is_eligible(stock)
        if not eligible:
            return None

        # 海龟条件②：突破信号（20日/55日唐奇安通道上轨）
        entry = check_entry_signal(df, short=20, long=55)
        if not entry['signal']:
            return None

        # 海龟条件③：趋势过滤（350日+25日均线方向）
        # 非强制阻止，记录趋势方向供参考
        trend = trend_filter(df)

        # 海龟条件④：ATR计算（用于仓位管理）
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
