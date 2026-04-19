"""
趋势交易综合评分模块

三维度加权评分，用于S1/S2开仓信号排序：
  RS（动量）  × 0.40 — 这只股票强不强？
  VCP（形态） × 0.35 — 买点好不好？
  ADX（趋势） × 0.25 — 趋势环境好不好？

关键设计：
  RS / ADX 取信号触发日当天评分（实时动量和趋势环境）
  VCP 取信号触发日前一个交易日的评分（突破前的预备形态质量）

所有数据库操作通过 core.storage 进行。
"""

import logging

from core.storage import (
    get_rs_score_by_code,
    get_vcp_score_by_code,
    get_adx_score_by_code,
)
from strategies.trend_trading.score._base import get_trade_dates

logger = logging.getLogger(__name__)

# 综合评分权重
WEIGHT_RS = 0.40
WEIGHT_VCP = 0.35
WEIGHT_ADX = 0.25

# 基准指数（RS计算用）
DEFAULT_BENCHMARK = '000510'


def _get_prev_trade_date(signal_date):
    """
    获取信号日的前一个交易日

    逻辑：取交易日历中 signal_date 之前（含）的倒数第二个交易日。
    无论 signal_date 本身是否为交易日，都返回"最后一个交易日的前一天"。

    参数:
        signal_date: 信号触发日 'YYYY-MM-DD'

    返回:
        str or None: 前一个交易日
    """
    dates = get_trade_dates(end_date=signal_date)
    if len(dates) < 2:
        return None
    return dates[-2]

def get_composite_score(code, signal_date, benchmark_code=DEFAULT_BENCHMARK,
                        prev_trade_date=None):
    """
    获取单只股票在信号日的综合评分

    评分规则：
      - RS：取信号日当天评分（实时动量强度）
      - VCP：取信号日前一个交易日评分（突破前预备形态）
      - ADX：取信号日当天评分（实时趋势环境）

    参数:
        code: 股票代码
        signal_date: 信号触发日 'YYYY-MM-DD'
        benchmark_code: RS基准指数代码
        prev_trade_date: 前一个交易日（可选，批量调用时传入避免重复查询）

    返回:
        dict: {
            'code': str,
            'signal_date': str,
            'composite_score': float,   # 综合评分 0-100
            'rs_score': float or None,  # RS评分 0-100
            'vcp_score': float or None, # VCP评分（归一化后）0-100
            'adx_score': float or None, # ADX评分 0-100
            'rs_raw': float or None,    # RS原始值
            'vcp_raw': float or None,   # VCP原始值（-40 ~ +95）
            'adx_raw': float or None,   # ADX原始值
            'vcp_date': str or None,    # VCP实际取值日期
            'missing': list[str],       # 缺失的维度
        }
    """
    missing = []
    actual_dates = {}

    # RS：取信号日当天数据，无数据则标记缺失
    rs_row = get_rs_score_by_code(code, signal_date, benchmark_code)
    rs_raw = rs_row['rs_score'] if rs_row else None
    rs_score = rs_raw if rs_raw is not None else 0.0
    if rs_raw is None:
        missing.append('RS')
    else:
        actual_dates['rs'] = rs_row.get('calc_date')

    # VCP：取前一个交易日（突破前形态）数据，无数据则标记缺失
    if prev_trade_date is None:
        prev_trade_date = _get_prev_trade_date(signal_date)
    vcp_raw = None
    vcp_date = None
    if prev_trade_date:
        vcp_row = get_vcp_score_by_code(code, prev_trade_date)
        if vcp_row:
            vcp_raw = vcp_row['score']
            vcp_date = prev_trade_date
    if vcp_raw is None:
        missing.append('VCP')

    # VCP原始范围：-40 ~ +95，归一化到 0-100
    vcp_score = vcp_raw if vcp_raw is not None else 0.0

    # ADX：取信号日当天数据，无数据则标记缺失
    adx_row = get_adx_score_by_code(code, signal_date)
    adx_raw = adx_row['adx_score_val'] if adx_row else None
    adx_score = adx_raw if adx_raw is not None else 0.0
    if adx_raw is None:
        missing.append('ADX')

    # 综合评分
    composite = (
        rs_score * WEIGHT_RS
        + vcp_score * WEIGHT_VCP
        + adx_score * WEIGHT_ADX
    )

    # 缺失维度惩罚：每缺一个维度，综合分乘以 (1 - 该维度权重)
    if missing:
        penalty = 1.0
        if 'RS' in missing:
            penalty -= WEIGHT_RS
        if 'VCP' in missing:
            penalty -= WEIGHT_VCP
        if 'ADX' in missing:
            penalty -= WEIGHT_ADX
        composite *= penalty

    return {
        'code': code,
        'signal_date': signal_date,
        'composite_score': round(composite, 2),
        'rs_score': round(rs_score, 2) if rs_raw is not None else None,
        'vcp_score': round(vcp_score, 2) if vcp_raw is not None else None,
        'adx_score': round(adx_score, 2) if adx_raw is not None else None,
        'rs_raw': rs_raw,
        'vcp_raw': vcp_raw,
        'adx_raw': adx_raw,
        'vcp_date': vcp_date,
        'missing': missing,
    }


def rank_signals(signals, signal_date, benchmark_code=DEFAULT_BENCHMARK):
    """
    对S1/S2开仓信号列表按综合评分排序

    参数:
        signals: 信号列表，每个 dict 至少包含 'code' 字段
        signal_date: 信号触发日 'YYYY-MM-DD'
        benchmark_code: RS基准指数代码

    返回:
        list[dict]: 按综合评分降序排列的信号列表（附带评分信息）
    """
    if not signals:
        return []

    # 预计算前一个交易日，避免每个信号重复查DB
    prev_trade_date = _get_prev_trade_date(signal_date)

    scored = []
    for sig in signals:
        code = sig.get('code', '')
        if not code:
            continue

        result = get_composite_score(code, signal_date, benchmark_code,
                                     prev_trade_date=prev_trade_date)
        merged = {**sig, **result}
        scored.append(merged)

    # 按综合评分降序
    scored.sort(key=lambda x: x['composite_score'], reverse=True)

    # 添加排名
    for i, item in enumerate(scored, 1):
        item['rank'] = i

    return scored


def format_score_label(result):
    """
    将综合评分结果格式化为简短标签（用于信号通知）

    参数:
        result: get_composite_score() 返回的 dict

    返回:
        str: 格式化标签，如 "⭐⭐⭐ 72.5分"
    """
    score = result['composite_score']
    missing = result.get('missing', [])

    if score >= 70:
        stars = '⭐⭐⭐'
    elif score >= 50:
        stars = '⭐⭐'
    elif score >= 30:
        stars = '⭐'
    else:
        stars = ''

    label = f"{stars} {score:.1f}分" if stars else f"{score:.1f}分"

    if missing:
        label += f" (缺{'/'.join(missing)})"

    return label
