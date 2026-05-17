"""
趋势检测器

功能：基于日K数据检测趋势类型（上升/下降/横盘震荡）

检测算法（多因子综合）：
- 均线排列 (30%): MA5/10/20/60 排列状态
- 均线斜率 (25%): MA5/10/20 斜率方向
- SuperTrend (25%): ST 方向和稳定性
- 价格相对位置 (20%): 收盘价相对均线位置

趋势分类：
- 上升趋势: score >= 60
- 下降趋势: score <= 40
- 横盘震荡: 40 < score < 60
"""

import logging
import os
from typing import Dict
from pandas import DataFrame

logger = logging.getLogger(__name__)

# 配置文件路径（统一使用根目录config）
# detector.py在 WatchMonitor/core/trend/，向上4级到达根目录
_ROOT_CONFIG = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), 'config')
CONFIG_PATH = os.path.join(_ROOT_CONFIG, 'trend_scenarios.yaml')


class TrendDetector:
    """趋势检测器"""

    def __init__(self, config: Dict = None):
        """
        初始化趋势检测器

        参数:
            config: 检测参数配置（可选，不传则从配置文件加载）
        """
        if config is not None:
            self.config = config
        else:
            self.config = self._load_config()

    def _load_config(self) -> Dict:
        """从配置文件加载配置，失败则使用默认配置"""
        try:
            import yaml
            if os.path.exists(CONFIG_PATH):
                with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                    config_data = yaml.safe_load(f)
                return config_data.get('trend_detection', self._default_config())
        except Exception as e:
            logger.warning(f"加载配置文件失败，使用默认配置: {e}")
        return self._default_config()

    def _default_config(self) -> Dict:
        """默认配置"""
        return {
            # 因子权重
            'ma_arrangement_weight': 0.30,
            'ma_slope_weight': 0.25,
            'supertrend_weight': 0.25,
            'price_position_weight': 0.20,
            # 趋势分类阈值
            'uptrend_threshold': 60,
            'downtrend_threshold': 40,
        }

    def detect_trend(self, df: DataFrame, code: str) -> Dict:
        """
        检测趋势类型

        参数:
            df: 日K数据
            code: 股票代码

        返回:
            dict: {
                'code': str,
                'trend_type': str,      # 'uptrend', 'downtrend', 'sideways'
                'trend_name': str,      # '上升趋势', '下降趋势', '横盘震荡'
                'score': float,         # 0-100，越高越多头
                'strength': str,        # 'strong', 'medium', 'weak'
                'details': dict,        # 各因子得分详情
            }
        """
        if df is None or df.empty or len(df) < 60:
            return self._empty_result(code)

        # 计算各因子得分
        ma_arrangement_score, ma_arrangement_details = self._calculate_ma_arrangement(df)
        ma_slope_score, ma_slope_details = self._calculate_ma_slope(df)
        st_score, st_details = self._calculate_supertrend(df)
        price_position_score, price_position_details = self._calculate_price_position(df)

        # 加权总分
        w = self.config
        total_score = (
            ma_arrangement_score * w['ma_arrangement_weight'] +
            ma_slope_score * w['ma_slope_weight'] +
            st_score * w['supertrend_weight'] +
            price_position_score * w['price_position_weight']
        )

        total_score = round(total_score, 1)

        # 确定趋势类型
        if total_score >= w['uptrend_threshold']:
            trend_type = 'uptrend'
            trend_name = '上升趋势'
        elif total_score <= w['downtrend_threshold']:
            trend_type = 'downtrend'
            trend_name = '下降趋势'
        else:
            trend_type = 'sideways'
            trend_name = '横盘震荡'

        # 确定强度
        if total_score >= 70 or total_score <= 30:
            strength = 'strong'
        elif total_score >= 60 or total_score <= 40:
            strength = 'medium'
        else:
            strength = 'weak'

        return {
            'code': code,
            'trend_type': trend_type,
            'trend_name': trend_name,
            'score': total_score,
            'strength': strength,
            'details': {
                'ma_arrangement': {
                    'score': ma_arrangement_score,
                    'details': ma_arrangement_details,
                },
                'ma_slope': {
                    'score': ma_slope_score,
                    'details': ma_slope_details,
                },
                'supertrend': {
                    'score': st_score,
                    'details': st_details,
                },
                'price_position': {
                    'score': price_position_score,
                    'details': price_position_details,
                },
            },
        }

    def _calculate_ma_arrangement(self, df: DataFrame) -> tuple:
        """
        计算均线排列得分

        判断标准：
        - 完美多头排列（MA5>MA10>MA20>MA60且价格在上）：100分
        - 部分多头排列：60-90分
        - 完美空头排列：0分
        - 部分空头排列：10-40分
        - 交织震荡：50分左右
        """
        if len(df) < 60:
            return 50, {'reason': '数据不足'}

        try:
            from core.indicator_funcs import calculate_ma
            mas = calculate_ma(df, [5, 10, 20, 60])

            ma5 = mas.get('ma5')
            ma10 = mas.get('ma10')
            ma20 = mas.get('ma20')
            ma60 = mas.get('ma60')
            current_price = df['close'].iloc[-1]

            if None in [ma5, ma10, ma20, ma60, current_price]:
                return 50, {'reason': '指标计算缺失'}

            # 检查多头排列
            is_bullish = (ma5 > ma10 > ma20 > ma60)
            is_above = current_price > ma5

            # 检查空头排列
            is_bearish = (ma5 < ma10 < ma20 < ma60)
            is_below = current_price < ma5

            # 计算排列得分
            if is_bullish and is_above:
                score = 100
                details = '完美多头排列，价格在所有均线上方'
            elif is_bullish:
                score = 75
                details = '部分多头排列'
            elif is_bearish and is_below:
                score = 0
                details = '完美空头排列，价格在所有均线下方'
            elif is_bearish:
                score = 25
                details = '部分空头排列'
            else:
                # 交织情况，计算多头比例
                bull_count = sum([
                    ma5 > ma10,
                    ma10 > ma20,
                    ma20 > ma60,
                    current_price > ma5,
                ])
                score = 40 + bull_count * 12  # 40-100
                details = f'均线交织，多头指标{bull_count}/4'

            return score, {'arrangement': details}

        except Exception as e:
            logger.warning(f"均线排列计算失败: {e}")
            return 50, {'reason': str(e)}

    def _calculate_ma_slope(self, df: DataFrame) -> tuple:
        """
        计算均线斜率得分

        判断标准：
        - MA5/10/20全部向上：100分
        - MA5/10/20全部向下：0分
        - 混合：50分左右
        """
        if len(df) < 20:
            return 50, {'reason': '数据不足'}

        try:
            from core.indicator_funcs import calculate_ma_slope
            slopes = calculate_ma_slope(df, [5, 10, 20])

            slope5 = slopes.get('ma5_slope', 0)
            slope10 = slopes.get('ma10_slope', 0)
            slope20 = slopes.get('ma20_slope', 0)

            # 统计向上数量
            up_count = sum([1 for s in [slope5, slope10, slope20] if s == 1])
            down_count = sum([1 for s in [slope5, slope10, slope20] if s == -1])

            if up_count == 3:
                score = 100
                details = 'MA5/10/20全部向上'
            elif up_count == 2:
                score = 75
                details = 'MA多数向上'
            elif down_count == 2:
                score = 25
                details = 'MA多数向下'
            elif down_count == 3:
                score = 0
                details = 'MA5/10/20全部向下'
            else:
                score = 50
                details = 'MA方向不明'

            return score, {'slope': details}

        except Exception as e:
            logger.warning(f"均线斜率计算失败: {e}")
            return 50, {'reason': str(e)}

    def _calculate_supertrend(self, df: DataFrame) -> tuple:
        """
        计算SuperTrend得分

        判断标准：
        - SuperTrend多头且稳定：100分
        - SuperTrend空头且稳定：0分
        - 频繁切换：50分
        """
        if len(df) < 100:
            return 50, {'reason': '数据不足'}

        try:
            from core.indicator_funcs import calculate_supertrend, check_supertrend_flip
            st_df = calculate_supertrend(df, atr_period=90, multiplier=3.0)

            if st_df is None or st_df.empty:
                return 50, {'reason': 'SuperTrend计算失败'}

            # 最近20天的方向统计
            recent_st = st_df.iloc[-20:]
            direction_counts = recent_st['supertrend'].value_counts()
            bull_count = direction_counts.get(True, 0)   # True = 多头
            bear_count = direction_counts.get(False, 0)  # False = 空头

            # 最近10天的情况（更近权重更高）
            recent_10 = st_df.iloc[-10:]
            recent_10_bull = (recent_10['supertrend'] == True).sum()
            recent_10_bear = (recent_10['supertrend'] == False).sum()

            # 计算得分
            if recent_10_bull >= 8:
                score = 100
                details = 'SuperTrend强势多头（近10天9-10天多头）'
            elif recent_10_bear >= 8:
                score = 0
                details = 'SuperTrend强势空头（近10天9-10天空头）'
            elif recent_10_bull > recent_10_bear:
                score = 60 + recent_10_bull * 4  # 64-100
                details = f'SuperTrend偏多（近10天多头{recent_10_bull}天）'
            elif recent_10_bear > recent_10_bull:
                score = 20 + recent_10_bear * 4  # 24-60
                details = f'SuperTrend偏空（近10天空头{recent_10_bear}天）'
            else:
                score = 50
                details = 'SuperTrend方向不明'

            return score, {'supertrend': details}

        except Exception as e:
            logger.warning(f"SuperTrend计算失败: {e}")
            return 50, {'reason': str(e)}

    def _calculate_price_position(self, df: DataFrame) -> tuple:
        """
        计算价格相对位置得分

        判断标准：
        - 价格在所有均线之上：100分
        - 价格在所有均线之下：0分
        - 中间：根据均线分布计算
        """
        if len(df) < 60:
            return 50, {'reason': '数据不足'}

        try:
            from core.indicator_funcs import calculate_ma
            mas = calculate_ma(df, [5, 10, 20, 60])

            current_price = df['close'].iloc[-1]
            mas_above = sum([1 for ma in mas.values() if current_price > ma])
            mas_below = sum([1 for ma in mas.values() if current_price < ma])

            if mas_above == 4:
                score = 100
                details = '价格在所有均线之上'
            elif mas_below == 4:
                score = 0
                details = '价格在所有均线之下'
            else:
                score = mas_above * 25
                details = f'价格在{mas_above}/4条均线之上'

            return score, {'position': details}

        except Exception as e:
            logger.warning(f"价格位置计算失败: {e}")
            return 50, {'reason': str(e)}

    def _empty_result(self, code: str) -> Dict:
        """返回空结果"""
        return {
            'code': code,
            'trend_type': 'sideways',
            'trend_name': '数据不足',
            'score': 50.0,
            'strength': 'weak',
            'details': {'reason': '数据不足，无法判断趋势'},
        }
