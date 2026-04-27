"""
MA均线技术指标模块

完全独立封装：
- 初始化时传入df，内部完成所有计算
- 提供信号、报告、评分输出
"""

from typing import Dict, List
from pandas import DataFrame

from .base import BaseIndicator


class MAIndicator(BaseIndicator):
    """MA均线指标 - 多周期支撑压力"""

    name = "ma"
    display_name = "均线"

    def _calculate(self) -> None:
        """计算MA均线指标"""
        if self.df is None or self.df.empty:
            return

        periods = self.params.get('periods', [5, 10, 20, 60, 120, 250])
        slope_periods = self.params.get('slope_periods', [5, 10, 20])

        from core.indicator_funcs import calculate_ma, calculate_ma_slope

        ma_values = calculate_ma(self.df, periods)
        ma_slopes = calculate_ma_slope(self.df, slope_periods)

        self._data = {}
        for period in periods:
            self._data[f'ma{period}'] = ma_values.get(f'ma{period}')

        for period in slope_periods:
            self._data[f'ma{period}_slope'] = ma_slopes.get(f'ma{period}_slope', 0)

    def _detect_signals(self) -> None:
        """检测MA均线信号"""
        is_position = self.context.get('is_position', False)
        current_price = self.context.get('current_price')

        if not is_position:
            return

        # 破位检测
        if current_price:
            ma5 = self._data.get('ma5')
            ma10 = self._data.get('ma10')
            ma20 = self._data.get('ma20')

            if ma5 and current_price < ma5:
                self._signals.append({
                    'type': 'ma5_breakdown',
                    'severity': 'medium',
                    'message': f"跌破MA5（{ma5:.2f}）"
                })

            if ma10 and current_price < ma10:
                self._signals.append({
                    'type': 'ma10_breakdown',
                    'severity': 'high',
                    'message': f"跌破MA10（{ma10:.2f}）"
                })

            if ma20 and current_price < ma20:
                self._signals.append({
                    'type': 'ma20_breakdown',
                    'severity': 'high',
                    'message': f"跌破MA20（{ma20:.2f}），支撑失效"
                })

        # MA5拐头向下预警
        ma5_slope = self._data.get('ma5_slope', 0)
        if ma5_slope == -1 and len(self.df) >= 5:
            deduct_price = self.df['close'].iloc[-5]
            current_close = self.df['close'].iloc[-1]
            required_change_pct = ((deduct_price - current_close) / current_close * 100) if current_close > 0 else 0

            self._signals.append({
                'type': 'ma5_turning_down',
                'severity': 'warning',
                'message': f"MA5拐头向下，抵扣价{deduct_price:.2f}（需涨{required_change_pct:+.1f}%）"
            })

    def _generate_report(self) -> None:
        """生成MA均线报告内容"""
        current_price = self.context.get('current_price')

        # 均线位置
        ma_status = []
        for period in [5, 10, 20, 60]:
            ma_val = self._data.get(f'ma{period}')
            if ma_val and current_price:
                ma_status.append(f'MA{period}⤴' if current_price > ma_val else f'MA{period}⤵')

        if ma_status:
            self._report_lines.append(f"    - 均线位置: {' '.join(ma_status)}")

        # 均线斜率（趋势）
        slope_status = []
        for period in [5, 10]:
            slope = self._data.get(f'ma{period}_slope', 0)
            if slope == 1:
                slope_status.append(f'MA{period}⤴')
            elif slope == -1:
                slope_status.append(f'MA{period}⤵')
            elif slope == 0:
                slope_status.append(f'MA{period}→')

        if slope_status:
            self._report_lines.append(f"    - 均线趋势: {' '.join(slope_status)}")

    def _calculate_score(self) -> None:
        """计算MA均线评分"""
        current_price = self.context.get('current_price')

        if current_price:
            ma5 = self._data.get('ma5')
            ma10 = self._data.get('ma10')

            if ma5:
                if current_price > ma5:
                    self._score += 1.0
                    self._score_reasons.append('价>MA5')
                else:
                    self._score -= 1.0
                    self._score_reasons.append('价<MA5')

            if ma10:
                if current_price > ma10:
                    self._score += 1.0
                    self._score_reasons.append('价>MA10')
                else:
                    self._score -= 0.5
                    self._score_reasons.append('价<MA10')

        # 斜率评分
        ma5_slope = self._data.get('ma5_slope', 0)
        ma10_slope = self._data.get('ma10_slope', 0)

        if ma5_slope == 1:
            self._score += 1.0
            self._score_reasons.append('MA5⤴')
        elif ma5_slope == -1:
            self._score -= 1.0
            self._score_reasons.append('MA5⤵')

        if ma10_slope == 1:
            self._score += 0.5
            self._score_reasons.append('MA10⤴')
        elif ma10_slope == -1:
            self._score -= 0.5
            self._score_reasons.append('MA10⤵')