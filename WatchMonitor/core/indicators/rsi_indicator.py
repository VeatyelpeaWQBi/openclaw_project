"""
RSI技术指标模块

完全独立封装：
- 初始化时传入df，内部完成所有计算
- 提供信号、报告、评分输出
"""

from typing import Dict, List
from pandas import DataFrame

from .base import BaseIndicator


class RSIIndicator(BaseIndicator):
    """RSI指标 - 超买超卖判断"""

    name = "rsi"
    display_name = "RSI"

    def _calculate(self) -> None:
        """计算RSI指标"""
        if self.df is None or self.df.empty:
            return

        period = self.params.get('period', 14)

        from core.indicator_funcs import calculate_rsi

        rsi_val = calculate_rsi(self.df, period)

        self._data = {
            'rsi_14': rsi_val,
        }

    def _detect_signals(self) -> None:
        """检测RSI信号"""
        is_position = self.context.get('is_position', False)

        if not is_position:
            return

        from core.indicator_funcs import check_divergence

        rsi_div = check_divergence(self.df, indicator='rsi')
        if rsi_div and rsi_div.get('type') == 'top_divergence':
            self._signals.append({
                'type': 'rsi_top_divergence',
                'severity': 'high',
                'message': 'RSI顶背离'
            })

    def _generate_report(self) -> None:
        """生成RSI报告内容"""
        rsi = self._data.get('rsi_14')

        if rsi is None:
            return

        # 状态判断
        if rsi > 70:
            status = '(超买)'
        elif rsi < 30:
            status = '(超卖)'
        else:
            status = '(中性)'

        self._report_lines.append(f"    - {self.display_name}: {rsi:.1f}{status}")

    def _calculate_score(self) -> None:
        """计算RSI评分"""
        rsi = self._data.get('rsi_14')
        is_candidate = self.context.get('is_candidate', False)

        if rsi is None:
            return

        if is_candidate:
            # 抄底场景
            if rsi < 30:
                self._score = 2.0
                self._score_reasons.append('RSI超卖')
            elif rsi < 50:
                self._score = 1.0
                self._score_reasons.append('RSI偏低')
            elif rsi > 70:
                self._score = -2.0
                self._score_reasons.append('RSI超买')
        else:
            # 持仓场景
            if rsi > 70:
                self._score = -1.0
                self._score_reasons.append('RSI超买')
            elif rsi < 30:
                self._score = 1.0
                self._score_reasons.append('RSI超卖反转')