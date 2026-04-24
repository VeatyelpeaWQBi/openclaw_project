"""
ADX技术指标模块

完全独立封装：
- 数据来自外部adx_score表
- 提供信号、报告、评分输出
"""

from typing import Dict, List
from pandas import DataFrame

from .base import BaseIndicator


class ADXIndicator(BaseIndicator):
    """ADX指标 - 趋势强度判断"""

    name = "adx"
    display_name = "ADX"

    def _calculate(self) -> None:
        """查询ADX数据（来自外部表）"""
        code = self.context.get('code')

        if not code:
            return

        from core.adx_analyzer import get_stock_adx

        adx_info = get_stock_adx(code)
        if adx_info:
            self._data = {
                'adx': adx_info.get('adx'),
                'trend_type': adx_info.get('trend_type'),
                'display': adx_info.get('display'),
            }

    def _detect_signals(self) -> None:
        """ADX信号检测（暂无独立信号，评分体现）"""
        pass

    def _generate_report(self) -> None:
        """生成ADX报告内容"""
        display = self._data.get('display')

        if display:
            self._report_lines.append(f"    - {display}")

    def _calculate_score(self) -> None:
        """计算ADX评分"""
        adx = self._data.get('adx')
        trend_type = self._data.get('trend_type')

        if adx is None:
            return

        if adx >= 25:
            if trend_type in ('强多头', '中等多头', '弱多头'):
                self._score = 2.0
                self._score_reasons.append('ADX多头')
            elif trend_type in ('强空头', '中等空头', '弱空头'):
                self._score = -2.0
                self._score_reasons.append('ADX空头')
            elif trend_type == '趋势不明':
                self._score_reasons.append('ADX不明')
            else:
                self._score_reasons.append('ADX趋势弱')
        elif adx < 15:
            self._score_reasons.append('ADX无趋势')
        else:
            # 15-25区间
            self._score_reasons.append('ADX趋势萌芽')