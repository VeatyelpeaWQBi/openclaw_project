"""
MACD技术指标模块

完全独立封装：
- 初始化时传入df，内部完成所有计算
- 提供信号、报告、评分输出
"""

from typing import Dict, List
from pandas import DataFrame

from .base import BaseIndicator


class MACDIndicator(BaseIndicator):
    """MACD指标 - 趋势强度判断"""

    name = "macd"
    display_name = "MACD"

    def _calculate(self) -> None:
        """计算MACD指标"""
        if self.df is None or self.df.empty:
            return

        fast_period = self.params.get('fast_period', 12)
        slow_period = self.params.get('slow_period', 26)
        signal_period = self.params.get('signal_period', 9)

        from core.indicator_funcs import calculate_macd, calculate_macd_slope

        macd_data = calculate_macd(self.df, fast_period, slow_period, signal_period)
        slope_data = calculate_macd_slope(self.df)

        self._data = {
            'macd_dif': macd_data.get('dif'),
            'macd_dea': macd_data.get('dea'),
            'macd_histogram': macd_data.get('histogram'),
            'macd_histogram_slope': slope_data.get('histogram_slope', 0),
            'macd_dif_slope': slope_data.get('dif_slope', 0),
            'macd_dea_slope': slope_data.get('dea_slope', 0),
            'macd_slope_summary': slope_data.get('slope_summary', '→震荡'),
        }

    def _detect_signals(self) -> None:
        """检测MACD信号"""
        is_position = self.context.get('is_position', False)

        if not is_position:
            return

        from core.indicator_funcs import check_divergence, get_weekly_kline

        macd_div = check_divergence(self.df, indicator='macd')
        if macd_div and macd_div.get('type') == 'top_divergence':
            severity = 'high'

            # 周线顶背离检测
            weekly_df = get_weekly_kline(self.df)
            if not weekly_df.empty:
                weekly_div = check_divergence(weekly_df, indicator='macd')
                if weekly_div and weekly_div.get('type') == 'top_divergence':
                    severity = 'critical'
                    self._signals.append({
                        'type': 'weekly_macd_top_divergence',
                        'severity': 'critical',
                        'message': '⚠️ 周线MACD顶背离（极高风险）'
                    })

            self._signals.append({
                'type': 'macd_top_divergence',
                'severity': severity,
                'message': 'MACD顶背离'
            })

    def _generate_report(self) -> None:
        """生成MACD报告内容"""
        macd_dif = self._data.get('macd_dif')
        macd_dea = self._data.get('macd_dea')
        hist_slope = self._data.get('macd_histogram_slope', 0)
        dif_slope = self._data.get('macd_dif_slope', 0)
        dea_slope = self._data.get('macd_dea_slope', 0)
        slope_summary = self._data.get('macd_slope_summary', '→震荡')

        if macd_dif is None or macd_dea is None:
            return

        # 斜率方向图标
        hist_slope_text = '🚀' if hist_slope == 1 else ('🪂' if hist_slope == -1 else '→')
        dif_slope_text = '🚀' if dif_slope == 1 else ('🪂' if dif_slope == -1 else '→')
        dea_slope_text = '🚀' if dea_slope == 1 else ('🪂' if dea_slope == -1 else '→')

        self._report_lines.append(f"    - {self.display_name}: 柱{hist_slope_text} DIF{dif_slope_text} DEA{dea_slope_text} {slope_summary}")

    def _calculate_score(self) -> None:
        """计算MACD评分"""
        hist_slope = self._data.get('macd_histogram_slope', 0)

        if hist_slope == 1:
            self._score = 1.0
            self._score_reasons.append('MACD柱🚀')
        elif hist_slope == -1:
            self._score = -1.0
            self._score_reasons.append('MACD柱🪂')