"""
K线形态技术指标模块

完全独立封装：
- 初始化时传入df，内部完成所有计算
- 提供信号、报告、评分输出
"""

from typing import Dict, List
from pandas import DataFrame

from .base import BaseIndicator


class CandleIndicator(BaseIndicator):
    """K线形态指标 - 影线、阴阳线判断"""

    name = "candle"
    display_name = "K线形态"

    def _calculate(self) -> None:
        """识别K线形态"""
        if self.df is None or self.df.empty:
            return

        from core.indicator_funcs import identify_candle_patterns

        self._data = identify_candle_patterns(self.df) or {}

    def _detect_signals(self) -> None:
        """检测K线形态信号"""
        is_position = self.context.get('is_position', False)
        is_candidate = self.context.get('is_candidate', False)

        if is_position:
            from core.indicator_funcs import (
                check_high_long_upper_shadow,
                check_breakdown_big_bull_candle,
                check_breakdown_medium_bull_candle
            )

            # 高位长上影线
            long_upper = check_high_long_upper_shadow(self.df)
            if long_upper:
                self._signals.append({
                    'type': 'high_long_upper_shadow',
                    'severity': 'medium',
                    'message': '高位长上影线'
                })

            # 跌破大阳线
            breakdown_big = check_breakdown_big_bull_candle(self.df)
            if breakdown_big:
                self._signals.append({
                    'type': 'breakdown_big_bull_candle',
                    'severity': 'high',
                    'message': f"跌破大阳线开盘价{breakdown_big['open_price']:.2f}"
                })

            # 跌破中阳线
            breakdown_medium = check_breakdown_medium_bull_candle(self.df)
            if breakdown_medium:
                self._signals.append({
                    'type': 'breakdown_medium_bull_candle',
                    'severity': 'medium',
                    'message': f"跌破中阳线开盘价{breakdown_medium['open_price']:.2f}"
                })

        if is_candidate:
            is_long_lower = self._data.get('is_long_lower_shadow', 0)
            if is_long_lower:
                self._signals.append({
                    'type': 'long_lower_shadow',
                    'severity': 'positive',
                    'message': '长下影线（底部有支撑）'
                })

    def _generate_report(self) -> None:
        """生成K线形态报告内容（不单独展示）"""
        pass

    def _calculate_score(self) -> None:
        """计算K线形态评分"""
        is_candidate = self.context.get('is_candidate', False)

        if not is_candidate:
            return

        is_long_lower = self._data.get('is_long_lower_shadow', 0)
        if is_long_lower:
            self._score = 0.5
            self._score_reasons.append('长下影线')