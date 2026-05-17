"""
量比技术指标模块

完全独立封装：
- 初始化时传入df，内部完成所有计算
- 提供信号、报告、评分输出
"""

from datetime import datetime
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

        code = self.context.get('code')
        if not code:
            # 无代码，直接计算
            self._calculate_realtime()
            return

        # 优先从DB读取
        from core.storage import get_technical_indicators
        today = datetime.now().strftime('%Y-%m-%d')
        db_data = get_technical_indicators(code, today)

        if db_data:
            periods = self.params.get('periods', [5, 20])
            self._data = {}
            for period in periods:
                self._data[f'volume_ratio_{period}'] = db_data.get(f'volume_ratio_{period}')
            self._data['obv'] = db_data.get('obv')
            self._data['ma_obv'] = db_data.get('ma_obv')
            return

        # 回退到实时计算
        self._calculate_realtime()

    def _calculate_realtime(self) -> None:
        """实时计算量比指标"""
        periods = self.params.get('periods', [5, 20])

        from core.indicator_funcs import calculate_volume_ratio

        self._data = {}
        for period in periods:
            self._data[f'volume_ratio_{period}'] = calculate_volume_ratio(self.df, period)

        # OBV计算
        obv, ma_obv = self._calculate_obv()
        self._data['obv'] = obv
        self._data['ma_obv'] = ma_obv

    def _calculate_obv(self) -> tuple:
        """
        计算OBV能量潮及其均线

        返回:
            tuple: (obv值, ma_obv值)
        """
        if self.df.empty or len(self.df) < 2:
            return 0, None

        if 'close' not in self.df.columns or 'volume' not in self.df.columns:
            return 0, None

        close = self.df['close'].values
        volume = self.df['volume'].values

        # 计算OBV序列
        obv_values = [0]
        for i in range(1, len(close)):
            prev_obv = obv_values[-1]
            if close[i] > close[i-1]:
                obv_values.append(prev_obv + volume[i])
            elif close[i] < close[i-1]:
                obv_values.append(prev_obv - volume[i])
            else:
                obv_values.append(prev_obv)

        obv = int(obv_values[-1])

        # 计算OBV的30日均线
        ma_obv = None
        if len(obv_values) >= 30:
            import pandas as pd
            ma_obv = round(pd.Series(obv_values).rolling(30).mean().iloc[-1], 2)

        return obv, ma_obv

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