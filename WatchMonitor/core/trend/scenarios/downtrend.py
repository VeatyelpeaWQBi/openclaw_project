"""
下跌趋势场景分析器

功能：分析下跌趋势下的三个子场景
1. 下跌趋势分析
2. 底部结构分析
3. 底部企稳分析
"""

import logging
from typing import List, Dict
from pandas import DataFrame

from .base import BaseScenario

logger = logging.getLogger(__name__)


class DowntrendScenario(BaseScenario):
    """下跌趋势场景分析器"""

    scenario_type = "downtrend"
    scenario_name = "下降趋势"

    def analyze(self) -> None:
        """执行下跌趋势分析（只负责分析计算，不处理信号和报告）"""
        # 1. 下跌趋势分析
        self._analyze_downtrend_strength()

        # 2. 底部结构分析
        self._analyze_bottom_structure()

        # 3. 底部企稳分析
        self._analyze_bottom_stabilization()

    def _analyze_downtrend_strength(self):
        """分析下跌趋势强度（只计算，不生成信号或报告）"""
        df = self.df
        if len(df) < 20:
            return

        try:
            from core.indicator_funcs import calculate_ma
            import numpy as np

            # 获取均线
            mas = calculate_ma(df, [5, 10, 20, 60])

            # 计算均线紧密度
            ma5 = mas.get('ma5')
            ma10 = mas.get('ma10')
            ma20 = mas.get('ma20')
            ma60 = mas.get('ma60')

            if all([ma5, ma10, ma20, ma60]):
                # 均线间距
                spreads = [
                    abs(ma5 - ma10) / ma10 * 100,
                    abs(ma10 - ma20) / ma20 * 100,
                    abs(ma20 - ma60) / ma60 * 100,
                ]
                avg_spread = np.mean(spreads)

                # 价格下跌角度（最近20天）
                if len(df) >= 20:
                    start_price = df['close'].iloc[-20]
                    end_price = df['close'].iloc[-1]
                    angle = (end_price - start_price) / start_price * 100
                else:
                    angle = 0

                # 阴线占比
                recent = df.tail(10)
                bearish_count = (recent['close'] < recent['open']).sum()
                bearish_ratio = bearish_count / len(recent) * 100

                # 判断强度
                if avg_spread < 3 and angle < -10 and bearish_ratio > 60:
                    strength = "强势"
                elif avg_spread < 5 and angle < -5 and bearish_ratio > 50:
                    strength = "中等"
                else:
                    strength = "弱势"

                # 只存储分析结果，不生成报告
                self._analysis_result['downtrend_strength'] = {
                    'strength': strength,
                    'avg_spread': round(avg_spread, 2),
                    'angle': round(angle, 2),
                    'bearish_ratio': round(bearish_ratio, 1),
                }

        except Exception as e:
            logger.warning(f"[{self.context.get('code')}] 下跌趋势强度分析失败: {e}")

    def _analyze_bottom_structure(self):
        """分析底部结构特征（只计算，不生成信号或报告）"""
        df = self.df
        if len(df) < 30:
            return

        try:
            analysis_data = {}

            # 1. 下跌速度放缓
            if len(df) >= 20:
                early_10 = df['close'].iloc[-20:-10]
                late_10 = df['close'].iloc[-10:]

                early_decline = (early_10.iloc[-1] - early_10.iloc[0]) / early_10.iloc[0] * 100
                late_decline = (late_10.iloc[-1] - late_10.iloc[0]) / late_10.iloc[0] * 100

                analysis_data['decline_slowdown'] = {
                    'early_decline': early_decline,
                    'late_decline': late_decline,
                }

            # 2. 成交量萎缩
            recent_volumes = df['volume'].tail(10)
            avg_volume = df['volume'].tail(20).mean() if len(df) >= 20 else recent_volumes.mean()
            recent_avg = recent_volumes.mean()

            if avg_volume > 0:
                volume_ratio = recent_avg / avg_volume
                analysis_data['volume_shrink'] = {
                    'volume_ratio': volume_ratio,
                }

            # 3. 长下影线增多
            recent = df.tail(10)
            long_lower_count = 0
            for _, row in recent.iterrows():
                body_size = abs(row['close'] - row['open'])
                lower_shadow = min(row['open'], row['close']) - row['low']

                if lower_shadow > body_size * 2:
                    long_lower_count += 1

            analysis_data['long_lower_shadows'] = {
                'count': long_lower_count,
            }

            # 4. MACD底背离
            try:
                from core.indicator_funcs import calculate_macd
                macd_data = calculate_macd(df)
                if len(macd_data) > 20:
                    recent_macd = macd_data.tail(20)
                    price_lows = df['low'].tail(20)
                    macd_lows = recent_macd['macd_dif']

                    if len(price_lows) > 5 and len(macd_lows) > 5:
                        price_recent_low = price_lows.tail(5).min()
                        price_prev_low = price_lows.iloc[:-5].min()
                        macd_recent_min = macd_lows.tail(5).min()
                        macd_prev_min = macd_lows.iloc[:-5].min()

                        analysis_data['macd_divergence'] = {
                            'price_recent_low': price_recent_low,
                            'price_prev_low': price_prev_low,
                            'macd_recent_min': macd_recent_min,
                            'macd_prev_min': macd_prev_min,
                        }
            except:
                pass

            self._analysis_result['bottom_structure'] = analysis_data

        except Exception as e:
            logger.warning(f"[{self.context.get('code')}] 底部结构分析失败: {e}")

    def _analyze_bottom_stabilization(self):
        """分析底部企稳信号（只计算，不生成信号或报告）"""
        df = self.df
        if len(df) < 10:
            return

        try:
            analysis_data = {}

            # 1. 多日不创新低
            today_low = df['low'].iloc[-1]

            # 3日不新低
            if len(df) >= 5:
                recent_lows_3d = df['low'].iloc[-4:-1].min()
                analysis_data['no_new_low_3d'] = today_low > recent_lows_3d

            # 5日不新低
            if len(df) >= 7:
                recent_lows_5d = df['low'].iloc[-6:-1].min()
                analysis_data['no_new_low_5d'] = today_low > recent_lows_5d

            # 2. 均线走平/拐头
            try:
                from core.indicator_funcs import calculate_ma_slope
                slopes = calculate_ma_slope(df, [5, 10, 20])

                flat_count = sum([1 for s in slopes.values() if s == 0])
                up_count = sum([1 for s in slopes.values() if s == 1])

                analysis_data['ma_state'] = {
                    'flat_count': flat_count,
                    'up_count': up_count,
                }
            except:
                pass

            # 3. 温和放量
            if len(df) >= 20:
                recent_volume = df['volume'].tail(5).mean()
                prev_volume = df['volume'].iloc[-20:-5].mean() if len(df) >= 25 else df['volume'].head(5).mean()

                if prev_volume > 0:
                    volume_ratio = recent_volume / prev_volume
                    analysis_data['moderate_volume'] = {
                        'volume_ratio': volume_ratio,
                    }

            # 4. 金叉检测
            try:
                from core.indicator_funcs import check_ma_cross
                cross_signals = []
                for fast, slow in [(5, 10), (5, 20)]:
                    cross = check_ma_cross(df, fast, slow)
                    if cross and cross.get('type') == 'golden_cross':
                        cross_signals.append({
                            'fast': fast,
                            'slow': slow,
                            'cross': cross,
                        })
                analysis_data['golden_cross'] = cross_signals

            except:
                pass

            self._analysis_result['bottom_stabilization'] = analysis_data

        except Exception as e:
            logger.warning(f"[{self.context.get('code')}] 企稳分析失败: {e}")

    def detect_signals(self) -> None:
        """
        检测下跌趋势下的所有信号

        基于analyze()中计算的分析数据生成信号
        """
        signals = []

        # 1. 底部结构信号
        bottom_data = self._analysis_result.get('bottom_structure', {})

        # 下跌速度放缓
        if 'decline_slowdown' in bottom_data:
            slowdown = bottom_data['decline_slowdown']
            if slowdown['early_decline'] < slowdown['late_decline'] * 0.5:
                signals.append({
                    'type': 'decline_slowdown',
                    'severity': 'info',
                    'message': f"下跌速度放缓（前10日{slowdown['early_decline']:.1f}%，后10日{slowdown['late_decline']:.1f}%）"
                })

        # 成交量萎缩
        if 'volume_shrink' in bottom_data:
            shrink = bottom_data['volume_shrink']
            if shrink['volume_ratio'] < 0.6:
                signals.append({
                    'type': 'volume_shrink',
                    'severity': 'info',
                    'message': f"成交量萎缩（量比{shrink['volume_ratio']:.2f}）"
                })

        # 长下影线增多
        if 'long_lower_shadows' in bottom_data:
            lower = bottom_data['long_lower_shadows']
            if lower['count'] >= 3:
                signals.append({
                    'type': 'more_long_lower_shadows',
                    'severity': 'info',
                    'message': f"近期{lower['count']}根长下影线"
                })

        # MACD底背离
        if 'macd_divergence' in bottom_data:
            md_div = bottom_data['macd_divergence']
            if md_div['price_recent_low'] < md_div['price_prev_low'] and md_div['macd_recent_min'] > md_div['macd_prev_min']:
                signals.append({
                    'type': 'macd_bottom_divergence',
                    'severity': 'medium',
                    'message': "MACD底背离：价格创新低但指标未新低"
                })

        # 2. 底部企稳信号
        stab_data = self._analysis_result.get('bottom_stabilization', {})

        # 多日不创新低
        if stab_data.get('no_new_low_3d'):
            signals.append({
                'type': 'no_new_low_3d',
                'severity': 'info',
                'message': "3日不创新低"
            })

        # 5日不创新低
        if stab_data.get('no_new_low_5d'):
            signals.append({
                'type': 'no_new_low_5d',
                'severity': 'medium',
                'message': "5日不创新低"
            })

        # 均线走平/拐头
        if 'ma_state' in stab_data:
            ma_state = stab_data['ma_state']
            if ma_state['up_count'] >= 2:
                signals.append({
                    'type': 'ma_turning_up',
                    'severity': 'medium',
                    'message': "MA多根拐头向上"
                })
            elif ma_state['flat_count'] >= 2:
                signals.append({
                    'type': 'ma_flat',
                    'severity': 'info',
                    'message': "MA多根走平"
                })

        # 温和放量
        if 'moderate_volume' in stab_data:
            vol = stab_data['moderate_volume']
            if 1.2 <= vol['volume_ratio'] <= 1.8:
                signals.append({
                    'type': 'moderate_volume',
                    'severity': 'medium',
                    'message': f"温和放量（量比{vol['volume_ratio']:.2f}）"
                })

        # 金叉检测
        if 'golden_cross' in stab_data:
            for gc in stab_data['golden_cross']:
                signals.append({
                    'type': 'golden_cross',
                    'severity': 'positive',
                    'message': f"MA{gc['fast']}金叉MA{gc['slow']}"
                })

        self._signals = signals

    def generate_report(self) -> None:
        """
        生成格式化报告

        基于analyze()中计算的分析数据和detect_signals()中生成的信号生成报告
        """
        # 下跌强度报告
        strength_data = self._analysis_result.get('downtrend_strength', {})
        if strength_data:
            strength = strength_data.get('strength', '')
            avg_spread = strength_data.get('avg_spread', 0)
            angle = strength_data.get('angle', 0)
            bearish_ratio = strength_data.get('bearish_ratio', 0)
            self._report_lines.append(
                f"    - 下跌强度: {strength}（均线间距{avg_spread:.1f}%，跌幅{abs(angle):.1f}%，阴线{bearish_ratio:.0f}%）"
            )

        # 底部结构报告
        bottom_signals = [s for s in self._signals if any(k in s.get('type', '') for k in ['decline', 'volume', 'lower', 'macd_bottom'])]
        if bottom_signals:
            self._report_lines.append(f"    - 底部结构: 检测到{len(bottom_signals)}个信号")
            for sig in bottom_signals:
                self._report_lines.append(f"      - {sig['message']}")
        else:
            self._report_lines.append("    - 底部结构: 未检测到明显信号")

        # 企稳信号报告
        stab_signals = [s for s in self._signals if any(k in s.get('type', '') for k in ['no_new', 'ma_flat', 'ma_turning', 'moderate_volume', 'golden_cross'])]
        if stab_signals:
            self._report_lines.append(f"    - 企稳信号: 检测到{len(stab_signals)}个信号")
            for sig in stab_signals:
                severity_mark = {
                    'positive': '✅',
                    'medium': '●',
                    'info': '○',
                    'high': '⚠️',
                    'critical': '🔴'
                }.get(sig['severity'], '•')
                self._report_lines.append(f"      {severity_mark} {sig['message']}")
        else:
            self._report_lines.append("    - 企稳信号: 未检测到明显信号")
