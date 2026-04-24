"""
量比技术指标模块

完全独立封装：
- 初始化时传入df，内部完成所有计算
- 提供信号、报告、评分输出
"""

from typing import Dict, List
from pandas import DataFrame

from .base import BaseIndicator


class VolumeIndicator(BaseIndicator):
    """量比指标 - 放量缩量判断"""

    name = "volume"
    display_name = "量比"

    def _calculate(self) -> None:
        """计算量比指标"""
        if self.df is None or self.df.empty:
            return

        periods = self.params.get('periods', [5, 20])

        from core.indicator_funcs import calculate_volume_ratio

        self._data = {}
        for period in periods:
            self._data[f'volume_ratio_{period}'] = calculate_volume_ratio(self.df, period)

    def _detect_signals(self) -> None:
        """检测量比信号"""
        is_position = self.context.get('is_position', False)
        is_candidate = self.context.get('is_candidate', False)

        if is_position:
            from core.indicator_funcs import check_volume_stagnation
            stagnation = check_volume_stagnation(self.df)
            if stagnation:
                self._signals.append({
                    'type': 'volume_stagnation',
                    'severity': 'medium',
                    'message': f"放量滞涨（量比{stagnation['volume_ratio']:.2f}，涨幅{stagnation['change_pct']:.1f}%）"
                })

        if is_candidate:
            volume_ratio_5 = self._data.get('volume_ratio_5', 1.0)
            if volume_ratio_5 > 1.5 and len(self.df) >= 5:
                prev_volumes = self.df['volume'].iloc[-5:-1]
                avg_prev = prev_volumes.mean()
                today_vol = self.df['volume'].iloc[-1]
                if avg_prev > 0 and today_vol > avg_prev * 1.5:
                    self._signals.append({
                        'type': 'volume_breakout',
                        'severity': 'positive',
                        'message': '缩量后放量异动'
                    })

    def _generate_report(self) -> None:
        """生成量比报告内容（不单独展示）"""
        pass

    def _calculate_score(self) -> None:
        """计算量比评分"""
        is_candidate = self.context.get('is_candidate', False)

        if not is_candidate:
            return

        volume_ratio_5 = self._data.get('volume_ratio_5', 1.0)
        if volume_ratio_5 and volume_ratio_5 > 1.5:
            self._score = 0.5
            self._score_reasons.append('放量异动')