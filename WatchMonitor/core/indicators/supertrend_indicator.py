"""
SuperTrend技术指标模块

完全独立封装：
- 初始化时传入df，内部完成所有计算
- 提供信号、报告、评分输出
"""

from typing import Dict, List
from pandas import DataFrame

from .base import BaseIndicator


class SuperTrendIndicator(BaseIndicator):
    """SuperTrend指标 - 趋势方向判断"""

    name = "supertrend"
    display_name = "SuTd"

    def _calculate(self) -> None:
        """计算SuperTrend指标"""
        if self.df is None or self.df.empty:
            return

        atr_period = self.params.get('atr_period', 10)
        multiplier = self.params.get('multiplier', 3.0)

        # 调用原有计算函数
        from core.indicator_funcs import calculate_supertrend, calculate_atr

        st_df = calculate_supertrend(self.df, atr_period, multiplier)
        atr_val = calculate_atr(self.df, atr_period)

        if st_df.empty:
            return

        # 方向判断：supertrend=True表示多头
        is_bullish = st_df['supertrend'].iloc[-1]
        direction = 1 if is_bullish else -1

        self._data = {
            'st_direction': direction,
            'st_upper_band': float(st_df['upper_band'].iloc[-1]) if not st_df['upper_band'].empty else None,
            'st_lower_band': float(st_df['lower_band'].iloc[-1]) if not st_df['lower_band'].empty else None,
            'st_atr': float(atr_val.iloc[-1]) if not atr_val.empty else None,
        }

    def _detect_signals(self) -> None:
        """检测SuperTrend信号"""
        is_position = self.context.get('is_position', False)

        if not is_position:
            return

        # 调用翻空检测函数
        from core.indicator_funcs import check_supertrend_flip, get_weekly_kline

        st_flip = check_supertrend_flip(self.df)
        if st_flip and st_flip['type'] == 'flip_to_bear':
            severity = 'high'

            # 周线SuperTrend检测
            weekly_df = get_weekly_kline(self.df)
            if not weekly_df.empty:
                weekly_st_flip = check_supertrend_flip(weekly_df)
                if weekly_st_flip and weekly_st_flip['type'] == 'flip_to_bear':
                    severity = 'critical'
                    self._signals.append({
                        'type': 'weekly_st_flip_bear',
                        'severity': 'critical',
                        'message': '⚠️ 周线SuperTrend翻空（极高风险）'
                    })

            self._signals.append({
                'type': 'st_flip_bear',
                'severity': severity,
                'message': '日线SuperTrend翻空',
                'upper_band': st_flip.get('upper_band')
            })

    def _generate_report(self) -> None:
        """生成SuperTrend报告内容"""
        st_dir = self._data.get('st_direction')
        st_upper = self._data.get('st_upper_band')
        st_lower = self._data.get('st_lower_band')
        current_price = self.context.get('current_price')

        if st_dir is None:
            return

        # 方向文本
        st_dir_text = '多头⬆' if st_dir == 1 else '空头⬇' if st_dir == -1 else 'N/A'

        # 反转预警
        warning = ''
        if st_dir == 1 and st_lower and current_price:
            gap_pct = (current_price - st_lower) / st_lower * 100
            if gap_pct > 0:
                warning = f"（多→空切换点{st_lower:.2f}，距-{gap_pct:.1f}%）"
            else:
                warning = f"（⚠️已跌破多→空切换{st_lower:.2f}）"
        elif st_dir == -1 and st_upper and current_price:
            gap_pct = (current_price - st_upper) / st_upper * 100
            if gap_pct < 0:
                warning = f"（空→多切换点{st_upper:.2f}，距+{-gap_pct:.1f}%）"
            else:
                warning = f"（⚠️已突破空→多切换点{st_upper:.2f}）"

        self._report_lines.append(f"    - {self.display_name}: {st_dir_text} {warning}")

    def _calculate_score(self) -> None:
        """计算SuperTrend评分"""
        st_dir = self._data.get('st_direction')

        if st_dir == 1:
            self._score = 2.0
            self._score_reasons.append('SuTd多头')
        elif st_dir == -1:
            self._score = -2.0
            self._score_reasons.append('SuTd空头')