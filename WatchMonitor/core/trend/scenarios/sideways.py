"""
震荡场景分析器

功能：分析震荡趋势下的三个子场景
1. 震荡区间分析
2. 向上突破分析
3. 向下突破分析
"""

import logging
from typing import List, Dict
from pandas import DataFrame

from .base import BaseScenario

logger = logging.getLogger(__name__)


class SidewaysScenario(BaseScenario):
    """震荡场景分析器"""

    scenario_type = "sideways"
    scenario_name = "横盘震荡"

    def analyze(self) -> None:
        """执行震荡分析（只负责分析计算，不处理信号和报告）"""
        # 1. 震荡区间分析
        self._analyze_range()

        # 2. 向上突破分析
        self._analyze_upward_breakout()

        # 3. 向下突破分析
        self._analyze_downward_breakout()

    def _analyze_range(self):
        """分析震荡区间（只计算，不生成信号或报告）"""
        df = self.df
        if len(df) < 20:
            return

        try:
            import numpy as np

            # 取最近20-30天数据
            lookback_days = min(30, len(df))
            recent = df.tail(lookback_days)

            # 使用分位数计算区间（避免单点极值影响）
            bottom_low = np.percentile(recent['low'], 10)    # 底部区间下界
            bottom_high = np.percentile(recent['low'], 25)   # 底部区间上界
            top_low = np.percentile(recent['high'], 75)      # 顶部区间下界
            top_high = np.percentile(recent['high'], 90)     # 顶部区间上界
            current_price = df['close'].iloc[-1]

            # 区间宽度（基于核心震荡区域 25%-75%）
            core_high = np.percentile(recent['high'], 75)
            core_low = np.percentile(recent['low'], 25)
            range_width = (core_high - core_low) / core_low * 100 if core_low > 0 else 0

            # 价格在核心区间的位置
            price_position = 50 if core_high <= core_low else (current_price - core_low) / (core_high - core_low) * 100

            # 只存储分析结果，不生成报告
            self._analysis_result['range'] = {
                'bottom_low': round(bottom_low, 2),
                'bottom_high': round(bottom_high, 2),
                'top_low': round(top_low, 2),
                'top_high': round(top_high, 2),
                'core_high': round(core_high, 2),
                'core_low': round(core_low, 2),
                'width_pct': round(range_width, 2),
                'price_position': round(price_position, 1),
                'in_range_days': len(recent),
            }

        except Exception as e:
            logger.warning(f"[{self.context.get('code')}] 震荡区间分析失败: {e}")

    def _analyze_upward_breakout(self):
        """分析向上突破信号（只计算，不生成信号或报告）"""
        df = self.df
        if len(df) < 10:
            return

        try:
            analysis_data = {}

            # 获取震荡区间
            range_data = self._analysis_result.get('range', {})
            if not range_data:
                return

            top_low = range_data.get('top_low')
            top_high = range_data.get('top_high')
            current_price = df['close'].iloc[-1]

            if top_low is None or top_high is None:
                return

            # 1. 突破上沿（价格超过顶部区间下界）
            if current_price > top_low:
                analysis_data['breakout_up'] = {
                    'top_low': top_low,
                    'current_price': current_price,
                }

            # 2. 检查放量
            if len(df) >= 5:
                recent_volume = df['volume'].tail(3).mean()
                prev_volume = df['volume'].iloc[-10:-3].mean() if len(df) >= 13 else df['volume'].head(3).mean()

                if prev_volume > 0:
                    volume_ratio = recent_volume / prev_volume
                    analysis_data['upward_volume'] = {
                        'volume_ratio': volume_ratio,
                    }

            # 3. 检查回踩不破
            if len(df) >= 3:
                pullback_days = 0
                for i in range(1, min(4, len(df))):
                    if df['close'].iloc[-i] < top_high and df['close'].iloc[-i] > top_low * 0.97:
                        pullback_days = i
                        break
                analysis_data['pullback'] = {
                    'days': pullback_days,
                }

            # 4. 均线多头排列
            try:
                from core.indicator_funcs import calculate_ma
                mas = calculate_ma(df, [5, 10, 20])
                ma5, ma10, ma20 = mas.get('ma5'), mas.get('ma10'), mas.get('ma20')

                if all([ma5, ma10, ma20]) and ma5 > ma10 > ma20:
                    analysis_data['ma_bullish_arrangement'] = True
            except:
                pass

            self._analysis_result['upward_breakout'] = analysis_data

        except Exception as e:
            logger.warning(f"[{self.context.get('code')}] 向上突破分析失败: {e}")

    def _analyze_downward_breakout(self):
        """分析向下突破信号（只计算，不生成信号或报告）"""
        df = self.df
        if len(df) < 10:
            return

        try:
            analysis_data = {}

            # 获取震荡区间
            range_data = self._analysis_result.get('range', {})
            if not range_data:
                return

            bottom_low = range_data.get('bottom_low')
            bottom_high = range_data.get('bottom_high')
            current_price = df['close'].iloc[-1]

            if bottom_low is None or bottom_high is None:
                return

            # 1. 跌破下沿（价格低于底部区间上界）
            if current_price < bottom_high:
                analysis_data['breakout_down'] = {
                    'bottom_high': bottom_high,
                    'current_price': current_price,
                }

            # 2. 检查放量
            if len(df) >= 5:
                recent_volume = df['volume'].tail(3).mean()
                prev_volume = df['volume'].iloc[-10:-3].mean() if len(df) >= 13 else df['volume'].head(3).mean()

                if prev_volume > 0:
                    volume_ratio = recent_volume / prev_volume
                    analysis_data['downward_volume'] = {
                        'volume_ratio': volume_ratio,
                    }

            # 3. 检查反弹受阻
            if len(df) >= 3:
                blocked_days = 0
                for i in range(1, min(4, len(df))):
                    if df['close'].iloc[-i] > bottom_low and df['close'].iloc[-i] < bottom_high * 1.03:
                        blocked_days = i
                        break
                analysis_data['rebound'] = {
                    'days': blocked_days,
                }

            # 4. 均线空头排列
            try:
                from core.indicator_funcs import calculate_ma
                mas = calculate_ma(df, [5, 10, 20])
                ma5, ma10, ma20 = mas.get('ma5'), mas.get('ma10'), mas.get('ma20')

                if all([ma5, ma10, ma20]) and ma5 < ma10 < ma20:
                    analysis_data['ma_bearish_arrangement'] = True
            except:
                pass

            self._analysis_result['downward_breakout'] = analysis_data

        except Exception as e:
            logger.warning(f"[{self.context.get('code')}] 向下突破分析失败: {e}")

    def detect_signals(self) -> None:
        """
        检测震荡趋势下的所有信号

        基于analyze()中计算的分析数据生成信号
        """
        signals = []

        # 1. 向上突破信号
        upward_data = self._analysis_result.get('upward_breakout', {})

        # 突破上沿
        if 'breakout_up' in upward_data:
            top_low = upward_data['breakout_up'].get('top_low', 0)
            signals.append({
                'type': 'breakout_up',
                'severity': 'medium',
                'message': f"突破顶部区间 {top_low:.2f}"
            })

        # 放量突破
        if 'upward_volume' in upward_data:
            uv = upward_data['upward_volume']
            if uv['volume_ratio'] >= 1.5:
                signals.append({
                    'type': 'breakout_up_volume',
                    'severity': 'positive',
                    'message': f"放量突破（量比{uv['volume_ratio']:.2f}）"
                })

        # 回踩不破
        if 'pullback' in upward_data:
            pb = upward_data['pullback']
            if pb['days'] > 0:
                signals.append({
                    'type': 'pullback_hold',
                    'severity': 'info',
                    'message': f"回踩{pb['days']}日不破"
                })

        # 均线多头排列
        if upward_data.get('ma_bullish_arrangement'):
            signals.append({
                'type': 'ma_bullish_arrangement',
                'severity': 'medium',
                'message': "均线多头排列"
            })

        # 2. 向下突破信号
        downward_data = self._analysis_result.get('downward_breakout', {})

        # 跌破下沿
        if 'breakout_down' in downward_data:
            bottom_high = downward_data['breakout_down'].get('bottom_high', 0)
            signals.append({
                'type': 'breakout_down',
                'severity': 'medium',
                'message': f"跌破底部区间 {bottom_high:.2f}"
            })

        # 放量下跌
        if 'downward_volume' in downward_data:
            dv = downward_data['downward_volume']
            if dv['volume_ratio'] >= 1.2:
                signals.append({
                    'type': 'breakout_down_volume',
                    'severity': 'high',
                    'message': f"放量下跌（量比{dv['volume_ratio']:.2f}）"
                })

        # 反弹受阻
        if 'rebound' in downward_data:
            rb = downward_data['rebound']
            if rb['days'] > 0:
                signals.append({
                    'type': 'rebound_blocked',
                    'severity': 'info',
                    'message': f"反弹{rb['days']}日受阻"
                })

        # 均线空头排列
        if downward_data.get('ma_bearish_arrangement'):
            signals.append({
                'type': 'ma_bearish_arrangement',
                'severity': 'medium',
                'message': "均线空头排列"
            })

        self._signals = signals

    def generate_report(self) -> None:
        """
        生成格式化报告

        基于analyze()中计算的分析数据和detect_signals()中生成的信号生成报告
        """
        # 震荡区间报告
        range_data = self._analysis_result.get('range', {})
        if range_data:
            bottom_low = range_data.get('bottom_low', 0)
            bottom_high = range_data.get('bottom_high', 0)
            top_low = range_data.get('top_low', 0)
            top_high = range_data.get('top_high', 0)
            width_pct = range_data.get('width_pct', 0)
            price_pos = range_data.get('price_position', 0)
            in_range_days = range_data.get('in_range_days', 0)
            self._report_lines.append(
                f"    - 震荡区间: 底部[{bottom_low:.2f}~{bottom_high:.2f}] "
                f"顶部[{top_low:.2f}~{top_high:.2f}]，宽度{width_pct:.1f}%，"
                f"价格位置{price_pos:.0f}%，震荡{in_range_days}天"
            )

        # 向上突破报告
        upward_signals = [s for s in self._signals if any(k in s.get('type', '') for k in ['breakout_up', 'breakout_up_volume', 'pullback_hold', 'ma_bullish_arrangement'])]
        if upward_signals:
            self._report_lines.append(f"    - 向上突破: 检测到{len(upward_signals)}个信号")
            for sig in upward_signals:
                severity_mark = {
                    'positive': '✅',
                    'medium': '●',
                    'info': '○',
                    'high': '⚠️',
                    'critical': '🔴'
                }.get(sig['severity'], '•')
                self._report_lines.append(f"      {severity_mark} {sig['message']}")
        else:
            self._report_lines.append("    - 向上突破: 未检测到明显信号")

        # 向下突破报告
        downward_signals = [s for s in self._signals if any(k in s.get('type', '') for k in ['breakout_down', 'breakout_down_volume', 'rebound_blocked', 'ma_bearish_arrangement'])]
        if downward_signals:
            self._report_lines.append(f"    - 向下突破: 检测到{len(downward_signals)}个信号")
            for sig in downward_signals:
                severity_mark = {
                    'positive': '✅',
                    'medium': '●',
                    'info': '○',
                    'high': '⚠️',
                    'critical': '🔴'
                }.get(sig['severity'], '•')
                self._report_lines.append(f"      {severity_mark} {sig['message']}")
        else:
            self._report_lines.append("    - 向下突破: 未检测到明显信号")
