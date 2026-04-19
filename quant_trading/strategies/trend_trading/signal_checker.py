"""
趋势交易 — 信号检测器
检查止损、退出、加仓、预警、入场等信号

趋势交易信号优先级（从高到低）：
  1. 止损：收盘价 ≤ 止损价 → 立即退出（无条件）
  2. 退出：收盘价 < 20日唐奇安通道下轨 → 趋势反转退出
     🔒 统一使用20日反向突破（S2），避免10日退出被洗盘干扰
  3. 加仓：收盘价 ≥ 上次加仓价 + 0.5×ATR → 加1单位（最多4单位）
  4. 预警：距止损 < 3% → 提醒主人关注
  5. 建仓：空仓 + N日突破 + 均线多头 → 入场信号

改良逻辑（2026-04-19）：
  - 统一S2退出：所有持仓（S1/S2入场）统一使用20日反向突破退出
  - 避免洗盘干扰：20日最低价给强势股更多回调空间
  - 设计依据：强势股回调通常3-7天，不会持续20天以上
  - 新易盛案例：S1(10日)5月27退出亏损-828，S2(20日)11月14退出盈利+68448
"""

import logging

from strategies.trend_trading.breakout import check_entry_signal, check_exit_signal
from strategies.trend_trading.filters import trend_filter, is_eligible
from strategies.trend_trading.atr import get_atr_value
from core.indicators import is_supertrend_bullish
from config.entry_filter import ENTRY_FILTER_CONFIG
from strategies.trend_trading.score.composite_score import rank_signals
from datetime import datetime as _dt

logger = logging.getLogger(__name__)


class SignalChecker:
    """趋势交易信号检测器"""

    def check_all(self, position_manager, account, candidate_pool, kline_data, target_date=None):
        """
        主入口：按优先级检查所有信号

        参数:
            position_manager: 持仓管理器
            account: 账户对象（含id/simulator/filter_active等）
            candidate_pool: 候选池
            kline_data: K线数据
            target_date: 目标日期 'YYYY-MM-DD'（回测模式必填，实盘模式可默认取当日）

        返回:
            手工账户(simulator=0): list[dict] 信号列表
            模拟账户(simulator=1): list[dict] 交易动作队列
        """
        account_id = account['id']
        is_simulator = account.get('simulator') == 1
        signals = []

        # === 第一部分：检查现有持仓 ===
        positions = position_manager.get_active_positions(account_id)
        for pos in positions:
            code = pos['code']
            df = kline_data.get(code)

            if df is None or df.empty:
                logger.warning(f"[{code}] 无K线数据，跳过信号检测")
                continue

            latest_price = float(df['close'].iloc[-1])

            # ① 止损检查（最高优先级）
            # 趋势法则：价格触及止损线必须立即退出，不考虑其他信号
            sl = self.check_stop_loss(pos, latest_price, target_date=target_date)
            if sl:
                signals.append(sl)
                continue  # 止损后不再检查退出/加仓

            # ② 预警检查
            # 趋势增强：A股适配，距止损<3%时提前预警
            # 不 continue，预警可以和退出/加仓同时存在
            warn = self.check_risk_warning(pos, latest_price)
            if warn:
                signals.append(warn)

            # ③ 退出检查（统一S2）
            # 改良逻辑：所有持仓统一使用20日反向突破退出，避免洗盘干扰
            exit_sig = self.check_exit(pos, df, target_date=target_date)
            if exit_sig:
                signals.append(exit_sig)
                continue  # 退出后不再检查加仓

            # ④ 加仓检查
            # 趋势法则：价格每涨0.5×ATR加1单位，最多4单位
            atr = pos.get('turtle_atr_value', 0)
            if not atr:
                atr = get_atr_value(df)
            add_sig = self.check_add(pos, latest_price, atr)
            if add_sig:
                signals.append(add_sig)

        # === 第二部分：检查候选池入场信号 ===
        # 持仓数已达上限，跳过入场检测
        max_holdings = account.get('max_holdings', 5)
        if len(positions) >= max_holdings:
            logger.info(f"持仓数已达上限({len(positions)}/{max_holdings})，跳过入场信号检测")
            entry_signals = []
        else:
            # 排除：持仓中 + 冷却中
            holding_codes = {p['code'] for p in positions}
            cooling_positions = position_manager.get_cooling_positions(account_id)
            cooling_codes = {p['code'] for p in cooling_positions}
            exclude_codes = holding_codes | cooling_codes

            entry_signals = []
            for stock in (candidate_pool.merged_pool if hasattr(candidate_pool, 'merged_pool') else []):
                code = stock.get('code', '')
                if not code or code in exclude_codes:
                    continue

                df = kline_data.get(code)
                if df is None or df.empty:
                    continue

                # ⑤ 建仓检查
                # 趋势法则：突破 + 均线多头过滤 → 入场
                entry_sig = self.check_entry(stock, df, account)
                if entry_sig:
                    entry_signals.append(entry_sig)

        # 入场信号按综合评分排序（所有信号都需要评分）
        if entry_signals:
            # 回测模式：使用传入的target_date
            # 实盘模式：未传入时默认取当日
            signal_date = target_date if target_date else _dt.now().strftime('%Y-%m-%d')
            entry_signals = rank_signals(entry_signals, signal_date)
            logger.info(f"入场信号按综合评分排序: "
                        f"{[(s['code'], s.get('composite_score', '?')) for s in entry_signals]}")

            # 二次筛选：基于当日平均分动态过滤低质量信号
            entry_signals = filter_entry_signals(entry_signals)

        signals.extend(entry_signals)

        # 按紧急度排序：critical > high > medium > low
        urgency_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        signals.sort(key=lambda s: urgency_order.get(s.get('urgency', 'low'), 3))

        logger.info(f"信号检测完成: 共{len(signals)}个信号")

        # 模拟账户：转换为交易动作队列
        if is_simulator:
            return self._to_action_queue(signals)

        # 手工账户：返回信号列表
        return signals

    def _to_action_queue(self, signals):
        """
        将信号列表转换为模拟账户的交易动作队列

        信号类型 → 动作映射:
          stop_loss → 平仓(止损)
          exit      → 平仓(退出)
          add       → 加仓
          entry     → 开仓
          warning   → 忽略

        返回:
            list[dict]: 交易动作队列
        """
        action_map = {
            'stop_loss': '平仓',
            'exit': '平仓',
            'add': '加仓',
            'entry': '开仓',
        }

        queue = []
        for sig in signals:
            sig_type = sig.get('type', '')
            action = action_map.get(sig_type)

            if action is None:
                # warning 等不需要执行的信号，跳过
                continue

            queue.append({
                'action': action,
                'code': sig.get('code', ''),
                'name': sig.get('name', ''),
                'price': sig.get('price', 0),
                'atr': sig.get('atr', 0),
                'reason': sig_type if action == '平仓' else None,
                'signal_type': sig_type,
                'urgency': sig.get('urgency', ''),
                'turtle_entry_system': self._to_entry_system(sig.get('breakout_type', '')),
            })

        logger.info(f"交易动作队列: {len(queue)} 个动作")
        return queue

    @staticmethod
    def _clean_name(name):
        """清除名称中的HTML/Markdown标记和来源标签，只保留纯股票名"""
        import re
        # 去掉 <font ...>...</font>
        name = re.sub(r'<font[^>]*>', '', name)
        name = re.sub(r'</font>', '', name)
        # 去掉 **
        name = name.replace('**', '')
        # 去掉来源标签（A500核心池、自选、热点池等）
        name = re.sub(r'[（(][^)）]*[)）]', '', name)
        return name.strip()

    @staticmethod
    def _to_entry_system(breakout_type):
        """将突破类型转换为系统类型"""
        if '55' in str(breakout_type):
            return 'S2'
        return 'S1'

    def _format_shares_status(self, position, target_date=None):
        """格式化持仓状态（可卖/锁定）"""
        from datetime import datetime
        # 回测模式：使用传入的target_date
        # 实盘模式：使用当前日期
        today = target_date if target_date else datetime.now().strftime('%Y-%m-%d')
        total = position.get('total_shares', 0)
        last_buy_date = position.get('last_buy_date', '')
        last_buy_shares = position.get('last_buy_shares', 0)
        if last_buy_date and str(last_buy_date)[:10] == today:
            locked = last_buy_shares
        else:
            locked = 0
        available = total - locked
        if locked > 0:
            return f"持仓{total}股(可卖{available}/锁定{locked})"
        return f"持仓{total}股"

    def check_stop_loss(self, position, latest_price, target_date=None):
        """
        止损检查

        趋势法则：
          止损价 = 入场价 - 2 × ATR
          收盘价 ≤ 止损价 → 立即退出全部持仓

        设计依据：
          原版趋势用 2N（2倍ATR）作为止损距离
          1N = ATR(20)，代表日均波动幅度
          2N 给价格留了约2天的正常波动空间
        """
        stop_price = position.get('current_stop', 0)
        if stop_price <= 0:
            return None

        # 趋势条件：现价 ≤ 止损价（入场价 - 2×ATR）
        if latest_price <= stop_price:
            logger.warning(f"[{position['code']}] 触及止损! 现价{latest_price} ≤ 止损价{stop_price}")
            return {
                'type': 'stop_loss',
                'code': position['code'],
                'name': self._clean_name(position.get('name', '')),
                'detail': f"现价{latest_price:.2f} 触及止损价{stop_price:.2f}，需立即卖出 [{self._format_shares_status(position, target_date=target_date)}]",
                'urgency': 'critical',
                'price': latest_price,
            }
        return None

    def check_exit(self, position, df, target_date=None):
        """
        趋势退出检查（统一S2）

        改良逻辑（2026-04-19）：
          统一使用20日反向突破退出（无论S1/S2入场）
          避免10日退出被洗盘干扰，捕获完整趋势

        设计依据：
          - 强势股回调通常3-7天，不会持续20天以上
          - 20日最低价给强势股更多回调空间
          - 新易盛案例：10月14日S1触发后股价反弹，S2继续持有吃到高点

        参数:
            position: 持仓信息（含turtle_entry_system）
            df: 日K DataFrame

        返回:
            dict: 退出信号 或 None
        """
        # 统一使用20日反向突破退出
        exit_point = 20

        # 记录原系统类型（用于日志）
        turtle_entry_system = position.get('turtle_entry_system', 'S1')

        exit_sig = check_exit_signal(df, exit_point=exit_point)
        if exit_sig['signal']:
            logger.info(f"[{position['code']}] S2退出触发(原{turtle_entry_system}): {exit_sig['type']}")
            return {
                'type': 'exit',
                'code': position['code'],
                'name': self._clean_name(position.get('name', '')),
                'detail': f"收盘价{exit_sig['exit_price']:.2f} 跌破20日通道下轨{exit_sig['channel_low']:.2f} [{self._format_shares_status(position, target_date=target_date)}]",
                'urgency': 'high',
                'price': exit_sig['exit_price'],
            }
        return None

    def check_risk_warning(self, position, latest_price):
        """
        预警检查（A股增强）

        趋势法则：原版无此规则
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

        # 趋势增强：距止损不到3%且尚未跌破
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

        趋势法则：
          加仓触发价 = 上次加仓价 + 0.5 × ATR
          收盘价 ≥ 加仓触发价 → 加1单位
          最大持仓：单市场4单位

        设计依据：
          0.5N 是原版趋势的加仓间距
          每涨0.5N加1单位，确保在趋势中逐步加码
          4单位上限 = 最大风险4%（4×1%）
        """
        next_add = position.get('next_add_price', 0)
        max_units = 4

        if next_add <= 0:
            return None

        # 趋势条件：已满4单位，不再加仓
        if position.get('turtle_units', 0) >= max_units:
            return None

        # 趋势条件：现价 ≥ 加仓触发价（上次加仓价 + 0.5×ATR）
        if latest_price >= next_add:
            logger.info(f"[{position['code']}] 触发加仓信号! 现价{latest_price} ≥ 加仓价{next_add}")
            return {
                'type': 'add',
                'code': position['code'],
                'name': position.get('name', ''),
                'detail': f"现价{latest_price:.2f} 达到加仓价{next_add:.2f}，当前{position.get('turtle_units', 0)}单位",
                'urgency': 'medium',
                'price': latest_price,
                'atr': atr,
            }
        return None

    def check_entry(self, stock, df, account):
        """
        建仓检查

        趋势法则（System1 + System2合并）：
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
          20/55日 = 原版趋势的两个系统参数
        """
        code = stock.get('code', '')

        # 趋势条件①：基础过滤（A股适配）
        eligible, reason = is_eligible(stock)
        if not eligible:
            return None

        # 趋势条件②：突破信号（20日/55日唐奇安通道上轨）
        entry = check_entry_signal(df, short=20, long=55, s1_filtered=account['turtle_s1_filter_active'])
        if not entry['signal']:
            return None

        # 趋势条件③：趋势过滤（350日+25日均线方向）
        # 非强制阻止，记录趋势方向供参考
        trend = trend_filter(df)

        # 趋势条件④：SuperTrend状态（日线级别）
        st_bullish = is_supertrend_bullish(df)
        st_status = 'SuperTrend多头状态' if st_bullish else 'SuperTrend空头状态'

        # 趋势条件⑤：ATR计算（用于仓位管理）
        atr = get_atr_value(df)
        if atr <= 0:
            return None

        # 纯名称（不带HTML格式化，格式化由report层处理）
        name = stock.get('name', '')
        source = stock.get('source', '')
        nickname = stock.get('account_nickname', '')

        # 信号格式化
        sys_label = 'S2' if '55' in entry['type'] else 'S1'
        trend_label = {'多头': '均线偏多', '空头': '均线偏空'}.get(trend, '均线不明')
        st_label = 'SuperTrend多头' if st_bullish else 'SuperTrend空头'

        logger.debug(f"[{code}] 入场信号! {sys_label}，{trend_label}，{st_label}")
        return {
            'type': 'entry',
            'code': code,
            'name': name,
            'detail': f"{sys_label}突破，收{entry['break_price']:.2f}(>{entry['channel_high']:.2f})，{trend_label}，{st_label}",
            'urgency': 'medium',
            'price': entry['break_price'],
            'atr': atr,
            'trend': trend,
            'supertrend': '多头' if st_bullish else '空头',
            'breakout_type': entry['type'],
            'source': source,
            'account_nickname': nickname,
        }


def filter_entry_signals(signals: list, config: dict = None) -> list:
    """
    开仓信号二次筛选
    
    基于当日信号平均分动态设定个股入场门槛：
    - 弱势市场（平均分<50）：个股分>80才开仓
    - 正常市场（平均分50~55）：个股分>平均分+10才开仓
    - 强势市场（平均分>55）：个股分>平均分才开仓
    
    参数:
        signals: 当日所有开仓信号列表，每个信号包含 {'code', 'composite_score', ...}
        config: 配置项（可选，默认使用全局配置）
    
    返回:
        list: 通过筛选的信号列表（按综合分降序排列）
    """
    if not signals:
        return []
    
    # 使用传入配置或默认配置
    cfg = config or ENTRY_FILTER_CONFIG
    
    # 计算当日平均分
    daily_avg_score = sum(s.get('composite_score', 0.0) for s in signals) / len(signals)
    
    # 从配置读取阈值
    weak_threshold = cfg['weak_market_threshold']
    strong_threshold = cfg['strong_market_threshold']
    weak_score_limit = cfg['weak_market_score_limit']
    normal_bonus = cfg['normal_market_bonus']
    
    # 确定个股入场门槛
    if daily_avg_score < weak_threshold:
        # 弱势市场：只跟踪最强抱团股
        threshold = weak_score_limit
        market_status = '弱势'
    elif daily_avg_score <= strong_threshold:
        # 正常市场：要求适度超额
        threshold = daily_avg_score + normal_bonus
        market_status = '正常'
    else:
        # 强势市场：跟随趋势
        threshold = daily_avg_score
        market_status = '强势'
    
    logger.info(f"[二次筛选] 当日平均分{daily_avg_score:.2f}，市场状态[{market_status}]，"
                f"个股门槛>{threshold:.2f}")
    
    # 筛选达标信号
    filtered = [s for s in signals if s.get('composite_score', 0.0) > threshold]
    
    # 按综合分降序排列（开仓顺序）
    filtered.sort(key=lambda x: x.get('composite_score', 0), reverse=True)
    
    # 记录筛选结果
    if len(signals) != len(filtered):
        filtered_codes = [s['code'] for s in filtered]
        rejected = [s for s in signals if s['code'] not in filtered_codes]
        logger.info(f"[二次筛选] 筛选前{len(signals)}个，筛选后{len(filtered)}个，"
                    f"通过{len(filtered_codes)}个: {[(s['code'], s.get('composite_score', 0)) for s in filtered_codes]}")
    
    return filtered
