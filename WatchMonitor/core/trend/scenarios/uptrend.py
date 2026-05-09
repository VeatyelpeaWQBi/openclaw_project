"""
上升趋势场景分析器

功能：分析上升趋势下的三个子场景
1. 上升趋势分析
2. 顶部结构分析
3. 上升拐点分析
"""

import logging
from typing import List, Dict
from pandas import DataFrame

from .base import BaseScenario

logger = logging.getLogger(__name__)


class UptrendScenario(BaseScenario):
    """上升趋势场景分析器"""

    scenario_type = "uptrend"
    scenario_name = "上升趋势"

    def analyze(self) -> None:
        """执行上升趋势分析（只负责分析计算，不处理信号和报告）"""
        # 1. 上升趋势分析
        self._analyze_uptrend_strength()

        # 2. 顶部结构分析
        self._analyze_top_structure()

        # 3. 上升拐点分析
        self._analyze_turning_point()

    def _analyze_uptrend_strength(self):
        """分析上升趋势强度（只计算，不生成信号或报告）"""
        df = self.df
        if len(df) < 20:
            return

        try:
            from core.indicator_funcs import calculate_ma, calculate_supertrend
            import numpy as np

            # 获取均线
            mas = calculate_ma(df, [5, 10, 20, 60])

            # 计算均线紧密度
            ma5 = mas.get('ma5')
            ma10 = mas.get('ma10')
            ma20 = mas.get('ma20')
            ma60 = mas.get('ma60')

            if all([ma5, ma10, ma20, ma60]):
                # 均线间距（越小越紧密）
                spreads = [
                    abs(ma5 - ma10) / ma10 * 100,
                    abs(ma10 - ma20) / ma20 * 100,
                    abs(ma20 - ma60) / ma60 * 100,
                ]
                avg_spread = np.mean(spreads)

                # 价格上涨角度（最近20天）
                if len(df) >= 20:
                    start_price = df['close'].iloc[-20]
                    end_price = df['close'].iloc[-1]
                    angle = (end_price - start_price) / start_price * 100
                else:
                    angle = 0

                # 判断强度
                if avg_spread < 3 and angle > 10:
                    strength = "强势"
                elif avg_spread < 5 and angle > 5:
                    strength = "中等"
                else:
                    strength = "弱势"

                # 只存储分析结果，不生成报告
                self._analysis_result['uptrend_strength'] = {
                    'strength': strength,
                    'avg_spread': round(avg_spread, 2),
                    'angle': round(angle, 2),
                }

        except Exception as e:
            logger.warning(f"[{self.context.get('code')}] 上升趋势强度分析失败: {e}")

    def _analyze_top_structure(self):
        """分析顶部结构特征（只计算，不生成信号或报告）"""
        df = self.df
        if len(df) < 20:
            return

        try:
            analysis_data = {}

            # 1. 量价背离检测（量增价滞）
            recent = df.tail(10)
            if len(recent) >= 3:
                avg_volume_early = recent['volume'].iloc[:-3].mean()
                avg_volume_late = recent['volume'].iloc[-3:].mean()
                volume_increase = (avg_volume_late - avg_volume_early) / avg_volume_early * 100

                price_change = (recent['close'].iloc[-1] - recent['close'].iloc[-4]) / recent['close'].iloc[-4] * 100

                analysis_data['volume_divergence'] = {
                    'volume_increase': volume_increase,
                    'price_change': price_change,
                }

            # 2. 高位横盘震荡
            high = df['high'].tail(20).max()
            current_price = df['close'].iloc[-1]
            if current_price > high * 0.95:
                # 检查是否横盘
                recent_closes = df['close'].tail(10)
                price_std = recent_closes.std() / recent_closes.mean() * 100
                analysis_data['high_sideways'] = {
                    'high': high,
                    'current_price': current_price,
                    'price_std': price_std,
                }

            # 3. MACD顶背离
            try:
                from core.indicator_funcs import calculate_macd
                macd_data = calculate_macd(df)
                if len(macd_data) > 20:
                    recent_macd = macd_data.tail(20)
                    price_highs = df['high'].tail(20)
                    macd_highs = recent_macd['macd_dif']

                    if len(price_highs) > 5 and len(macd_highs) > 5:
                        price_recent_high = price_highs.tail(5).max()
                        price_prev_high = price_highs.iloc[:-5].max()
                        macd_recent_max = macd_highs.tail(5).max()
                        macd_prev_max = macd_highs.iloc[:-5].max()

                        analysis_data['macd_divergence'] = {
                            'price_recent_high': price_recent_high,
                            'price_prev_high': price_prev_high,
                            'macd_recent_max': macd_recent_max,
                            'macd_prev_max': macd_prev_max,
                        }
            except:
                pass

            self._analysis_result['top_structure'] = analysis_data

        except Exception as e:
            logger.warning(f"[{self.context.get('code')}] 顶部结构分析失败: {e}")

    def _analyze_turning_point(self):
        """分析上升拐点信号（只计算，不生成信号或报告）"""
        df = self.df
        if len(df) < 10:
            return

        try:
            analysis_data = {}
            code = self.context.get('code', '')

            # 1. MA5拐头向下检测
            try:
                from core.indicator_funcs import calculate_ma_slope
                slopes = calculate_ma_slope(df, [5])
                ma5_slope = slopes.get('ma5_slope', 0)

                if ma5_slope == -1:
                    # 计算抵扣价
                    deduct_price = df['close'].iloc[-5]
                    current_close = df['close'].iloc[-1]
                    required_change_pct = ((deduct_price - current_close) / current_close * 100)

                    analysis_data['ma5_turning'] = {
                        'slope': ma5_slope,
                        'required_change_pct': required_change_pct,
                    }
            except:
                pass

            # 2. 跌破关键均线
            current_price = df['close'].iloc[-1]
            try:
                from core.indicator_funcs import calculate_ma
                mas = calculate_ma(df, [10, 20])

                ma10 = mas.get('ma10')
                ma20 = mas.get('ma20')

                prev_close = df['close'].iloc[-2] if len(df) > 1 else current_price

                if ma10 and prev_close > ma10 and current_price < ma10:
                    analysis_data['ma10_breakdown'] = {
                        'ma10': ma10,
                        'prev_close': prev_close,
                        'current_price': current_price,
                    }

                if ma20 and prev_close > ma20 and current_price < ma20:
                    analysis_data['ma20_breakdown'] = {
                        'ma20': ma20,
                        'prev_close': prev_close,
                        'current_price': current_price,
                    }
            except:
                pass

            # 3. 高位长上影线
            if len(df) >= 5:
                recent = df.tail(5)
                for i, row in recent.iterrows():
                    body_size = abs(row['close'] - row['open'])
                    upper_shadow = row['high'] - max(row['open'], row['close'])
                    lower_shadow = min(row['open'], row['close']) - row['low']

                    if upper_shadow > body_size * 2 and row['close'] > row['open']:
                        analysis_data['high_upper_shadow'] = True
                        break

            # 4. SuperTrend翻空
            try:
                from core.indicator_funcs import check_supertrend_flip
                st_flip = check_supertrend_flip(df)
                if st_flip and st_flip.get('type') == 'flip_to_bear':
                    analysis_data['st_flip_bear'] = True
            except:
                pass

            self._analysis_result['turning_point'] = analysis_data

        except Exception as e:
            logger.warning(f"[{self.context.get('code')}] 拐点分析失败: {e}")

    def detect_signals(self) -> None:
        """
        检测上升趋势下的所有信号

        基于analyze()中计算的分析数据生成信号
        """
        signals = []

        # 1. 顶部结构信号
        top_data = self._analysis_result.get('top_structure', {})

        # 量价背离
        if top_data and 'volume_divergence' in top_data:
            vol_div = top_data['volume_divergence']
            if vol_div['volume_increase'] > 30 and vol_div['price_change'] < 3:
                signals.append({
                    'type': 'top_divergence_volume',
                    'severity': 'medium',
                    'message': f"量价背离：放量{vol_div['volume_increase']:.1f}%但涨幅仅{vol_div['price_change']:.1f}%"
                })

        # 高位横盘
        if top_data and 'high_sideways' in top_data:
            hs = top_data['high_sideways']
            if hs['price_std'] < 2:
                signals.append({
                    'type': 'high_sideways',
                    'severity': 'medium',
                    'message': "高位横盘震荡"
                })

        # MACD顶背离
        if top_data and 'macd_divergence' in top_data:
            md_div = top_data['macd_divergence']
            if md_div['price_recent_high'] > md_div['price_prev_high'] and md_div['macd_recent_max'] < md_div['macd_prev_max']:
                signals.append({
                    'type': 'macd_top_divergence',
                    'severity': 'high',
                    'message': "MACD顶背离：价格创新高但指标未新高"
                })

        # 2. 上升拐点信号
        turn_data = self._analysis_result.get('turning_point', {})

        # MA5拐头向下
        if turn_data and 'ma5_turning' in turn_data:
            turning = turn_data['ma5_turning']
            if turning['slope'] == -1:
                signals.append({
                    'type': 'ma5_turning_down',
                    'severity': 'medium',
                    'message': f"MA5拐头向下（需涨{turning['required_change_pct']:+.1f}%维持）"
                })

        # 跌破MA10
        if turn_data and 'ma10_breakdown' in turn_data:
            breakdown = turn_data['ma10_breakdown']
            signals.append({
                'type': 'ma10_breakdown',
                'severity': 'high',
                'message': f"跌破MA10（{breakdown['ma10']:.2f}）"
            })

        # 跌破MA20
        if turn_data and 'ma20_breakdown' in turn_data:
            breakdown = turn_data['ma20_breakdown']
            signals.append({
                'type': 'ma20_breakdown',
                'severity': 'high',
                'message': f"跌破MA20（{breakdown['ma20']:.2f}）"
            })

        # 高位长上影线
        if turn_data and turn_data.get('high_upper_shadow'):
            signals.append({
                'type': 'high_upper_shadow',
                'severity': 'medium',
                'message': "高位长上影线"
            })

        # SuperTrend翻空
        if turn_data and turn_data.get('st_flip_bear'):
            signals.append({
                'type': 'st_flip_bear',
                'severity': 'high',
                'message': "SuperTrend翻空"
            })

        self._signals = signals

    def generate_report(self) -> None:
        """
        生成格式化报告

        基于analyze()中计算的分析数据和detect_signals()中生成的信号生成报告
        """
        # 上升强度报告
        strength_data = self._analysis_result.get('uptrend_strength', {})
        if strength_data:
            strength = strength_data.get('strength', '')
            avg_spread = strength_data.get('avg_spread', 0)
            angle = strength_data.get('angle', 0)
            self._report_lines.append(f"    - 上升强度: {strength}（均线间距{avg_spread:.1f}%，涨幅{angle:.1f}%）")

        # 顶部结构报告
        top_data = self._analysis_result.get('top_structure', {})
        top_signals = [s for s in self._signals if 'top' in s.get('type', '')]
        if top_signals:
            self._report_lines.append(f"    - 顶部结构: 检测到{len(top_signals)}个信号")
            for sig in top_signals:
                self._report_lines.append(f"      - {sig['message']}")
        else:
            self._report_lines.append("    - 顶部结构: 未检测到明显信号")

        # 拐点信号报告
        turn_signals = [s for s in self._signals if 'turning' in s.get('type', '') or 'breakdown' in s.get('type', '') or 'upper_shadow' in s.get('type', '') or 'st_flip' in s.get('type', '')]
        if turn_signals:
            self._report_lines.append(f"    - 拐点信号: 检测到{len(turn_signals)}个信号")
            for sig in turn_signals:
                severity_mark = {
                    'high': '⚠️',
                    'medium': '●',
                    'info': '○',
                    'critical': '🔴',
                    'positive': '✅'
                }.get(sig['severity'], '•')
                self._report_lines.append(f"      {severity_mark} {sig['message']}")
        else:
            self._report_lines.append("    - 拐点信号: 未检测到明显信号")
