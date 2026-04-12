"""
VCP 方案A: pytafast.ZIGZAG + 实体极值 + 噪点过滤
当前方案的优化版，作为基准对比
"""

import logging
import numpy as np
import pytafast

from strategies.trend_trading.vcp_core import (
    analyze_vcp_common, align_pairs, filter_noise_pairs,
)

logger = logging.getLogger(__name__)


def detect_swings(df, deviation_pct=7.0):
    """
    pytafast.ZIGZAG 波段识别

    参数:
        df: 日K DataFrame
        deviation_pct: 价格反转偏差百分比

    返回:
        tuple: (peaks, troughs)
    """
    peaks = []
    troughs = []

    opens = df['open'].values.astype(float)
    closes = df['close'].values.astype(float)
    body_high = np.maximum(opens, closes)
    body_low = np.minimum(opens, closes)

    out = pytafast.ZIGZAG(body_high, body_low, change=deviation_pct, percent=True)

    nonzero = [(i, out[i]) for i in range(len(out)) if out[i] != 0]
    if len(nonzero) < 3:
        return peaks, troughs

    for j in range(1, len(nonzero) - 1):
        _, v_prev = nonzero[j - 1]
        i_curr, v_curr = nonzero[j]
        _, v_next = nonzero[j + 1]
        date = str(df['date'].iloc[i_curr])

        if v_curr > v_prev and v_curr > v_next:
            peaks.append({"date": date, "price": float(v_curr)})
            logger.debug(f'  [A] 波峰: {date} @ {v_curr:.2f}')
        elif v_curr < v_prev and v_curr < v_next:
            troughs.append({"date": date, "price": float(v_curr)})
            logger.debug(f'  [A] 波谷: {date} @ {v_curr:.2f}')

    # 前置波谷丢弃
    if troughs and peaks and troughs[0]['date'] < peaks[0]['date']:
        troughs.pop(0)

    # 噪点过滤
    peaks, troughs = filter_noise_pairs(peaks, troughs)

    # 对齐
    peaks, troughs = align_pairs(peaks, troughs)

    return peaks, troughs


def analyze_vcp(df, deviation_pct=5.0):
    """方案A 统一入口"""
    peaks, troughs = detect_swings(df, deviation_pct)
    return analyze_vcp_common(peaks, troughs, df)
