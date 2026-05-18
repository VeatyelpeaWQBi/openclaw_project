"""
金叉级别计算模块

计算MACD金叉级别（底部圆弧积分面积）
"""

from typing import Optional

import pandas as pd


def calculate_macd_cross_rank(df: pd.DataFrame, cross_date: str) -> float:
    """
    计算MACD金叉级别（底部圆弧积分面积）

    从DIF开始低于DEA的起点，到金叉点，计算histogram负值区域的积分面积
    面积越大 = 级别越高

    参数:
        df: 日K数据（需包含 dif, dea, histogram 列）
        cross_date: 金叉日期

    返回:
        float: 底部圆弧积分面积，如果计算失败返回0
    """
    if df.empty or 'dif' not in df.columns or 'dea' not in df.columns:
        return 0.0

    # 找到cross_date在df中的索引
    if 'date' in df.columns:
        date_matches = df[df['date'] == cross_date]
        if date_matches.empty:
            return 0.0
        cross_idx = date_matches.index[0]
    else:
        # 如果没有date列，假设最后一天是金叉日
        cross_idx = len(df) - 1

    if cross_idx < 1:
        return 0.0

    # 向前回溯，找到DIF开始低于DEA的位置
    start_idx = cross_idx
    for i in range(cross_idx - 1, -1, -1):
        dif = df['dif'].iloc[i]
        dea = df['dea'].iloc[i]

        # 找到DIF>=DEA的位置（不再低于DEA的前一天）
        if dif >= dea:
            start_idx = i + 1
            break

    if start_idx >= cross_idx:
        return 0.0

    # 计算histogram负值区域积分面积
    area = 0.0
    has_histogram = 'histogram' in df.columns

    for i in range(start_idx, cross_idx + 1):
        dif = df['dif'].iloc[i]
        dea = df['dea'].iloc[i]

        if has_histogram:
            hist = df['histogram'].iloc[i]
            if pd.notna(hist) and hist < 0:
                area += abs(hist)
        else:
            # 如果没有histogram列，用dif-dea*2计算
            hist = (dif - dea) * 2
            if pd.notna(hist) and hist < 0:
                area += abs(hist)

    return area


def calculate_signal_score(macd_area: float, obv_slope: float) -> float:
    """
    综合评分 = MACD金叉级别权重 + OBV斜率权重

    参数:
        macd_area: 底部圆弧积分面积
        obv_slope: OBV斜率（可能需要归一化）

    返回:
        float: 综合评分
    """
    macd_weight = 0.7
    obv_weight = 0.3

    # OBV斜率归一化（假设合理范围为-1000~1000）
    normalized_obv_slope = max(0, min(1, (obv_slope + 1000) / 2000))

    return macd_area * macd_weight + normalized_obv_slope * 1000 * obv_weight


class CrossRanker:
    """金叉级别计算器"""

    def calculate_rank(self, df: pd.DataFrame, cross_date: str) -> float:
        """
        计算金叉级别

        参数:
            df: 日K数据
            cross_date: 金叉日期

        返回:
            float: 金叉级别（底部圆弧积分面积）
        """
        return calculate_macd_cross_rank(df, cross_date)

    def calculate_score(self, macd_area: float, obv_slope: float) -> float:
        """
        计算综合评分（用于排序）

        参数:
            macd_area: MACD金叉级别
            obv_slope: OBV斜率

        返回:
            float: 综合评分
        """
        return calculate_signal_score(macd_area, obv_slope)