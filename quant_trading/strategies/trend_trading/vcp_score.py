"""
趋势交易 — VCP波段收缩度评分模块
Volatility Contraction Pattern (VCP) 检测与评分

VCP 核心思想：
  股价在蓄力阶段，每次回调的波幅递减（向右收缩），
  最终形成极度紧凑的枢轴点，预示即将突破。

流程：
  1. ZigZag 提取波峰/波谷序列
  2. 验证是否符合 VCP 形态（波幅递减 + 顶部水平 + 量能枯竭）
  3. 对符合形态的股票进行 0-100 纯度评分
"""

import logging
import pandas as pd
logger = logging.getLogger(__name__)


# ==================== 核心：VCP 形态检测 + 评分（统一入口） ====================

def analyze_vcp(df, deviation_pct=0.05):
    """
    对个股日K数据进行 VCP 形态检测与评分（统一入口）

    参数:
        df: 日K DataFrame，需包含 date, high, low, close, volume 列
        deviation_pct: ZigZag 确认反转的最小回撤幅度，默认 5%

    返回:
        dict: {
            'is_vcp': bool,          # 是否符合 VCP 形态
            'score': int,            # 纯度评分 0-100
            'peaks': list,           # 波峰列表 [{"date", "price"}]
            'troughs': list,         # 波谷列表 [{"date", "price"}]
            'drawdowns': list,       # 各段回撤幅度序列
            'details': dict,         # 各维度评分明细
            'reject_reason': str,    # 不符合形态的原因（空字符串=符合）
        }
    """
    result = {
        'is_vcp': False,
        'score': 0,
        'peaks': [],
        'troughs': [],
        'drawdowns': [],
        'details': {},
        'reject_reason': '',
    }

    if df is None or len(df) < 90:
        result['reject_reason'] = '数据不足（<30条日K）'
        return result

    # Step 1: ZigZag 提取波峰波谷
    peaks, troughs = calculate_zigzag_extrema(
        df['high'].values, df['low'].values, df['date'].values,
        deviation_pct=deviation_pct,
    )

    result['peaks'] = peaks
    result['troughs'] = troughs

    n = len(peaks)
    logger.debug(f'ZigZag 提取完成: 波峰={n}组, 波谷={len(troughs)}组')

    # 打印每个波段详情
    for i in range(min(n, len(troughs))):
        peak = peaks[i]
        trough = troughs[i]
        change_pct = (peak['price'] - trough['price']) / trough['price'] * 100
        logger.debug(f'  波段{i+1}: {trough["date"]}~{peak["date"]}, '
                     f'涨幅{change_pct:.1f}%, 拐点({trough["price"]:.2f}→{peak["price"]:.2f})')

    if n < 2 or n != len(troughs):
        result['reject_reason'] = f'波峰数={n}, 波谷数={len(troughs)}，不足2组或数量不匹配'
        return result

    # Step 2: 计算回撤序列
    drawdowns = [(peaks[i]['price'] - troughs[i]['price']) / peaks[i]['price'] for i in range(n)]
    result['drawdowns'] = [round(d, 4) for d in drawdowns]

    for i in range(n):
        logger.debug(f'  回撤{i+1}: 波峰{peaks[i]["price"]:.2f}→波谷{troughs[i]["price"]:.2f}, '
                     f'回撤幅度{drawdowns[i]*100:.1f}%')

    # Step 3: 验证 VCP 形态
    reject = _validate_vcp(peaks, troughs, drawdowns, df)
    if reject:
        result['reject_reason'] = reject
        logger.debug(f'VCP 验证不通过: {reject}')
        return result

    # Step 4: 计算纯度评分
    score, details = calculate_vcp_score(peaks, troughs, drawdowns, df)

    result['is_vcp'] = True
    result['score'] = score
    result['details'] = details

    logger.info(f"VCP 形态确认! 评分={score}/100, 波峰={n}组, 回撤={[round(d*100,1) for d in drawdowns]}%")
    return result


# ==================== VCP 形态验证 ====================

def _validate_vcp(peaks, troughs, drawdowns, df):
    """
    验证是否符合 VCP 形态

    检查项：
      1. 波幅递减（D_i < D_{i-1} * 0.7）
      2. 枢轴点紧凑度（最后回撤 <= 5%）
      3. 顶部水平（峰顶偏差 <= 10%）
      4. 成交量枯竭

    返回:
        str: 不符合原因（空字符串=符合）
    """
    n = len(peaks)
    D = drawdowns

    # ① 波幅递减
    for i in range(1, n):
        if D[i] >= D[i - 1] * 0.7:
            return f'波幅未递减: D[{i}]={D[i]*100:.1f}% >= D[{i-1}]={D[i-1]*100:.1f}%*0.7'

    # ② 枢轴点紧凑度
    if D[-1] > 0.05:
        return f'最后回撤过大: {D[-1]*100:.1f}% > 5%'

    # ③ 顶部水平
    peak_prices = [p['price'] for p in peaks]
    max_p, min_p = max(peak_prices), min(peak_prices)
    if (max_p - min_p) / max_p > 0.10:
        return f'顶部不平: 峰顶偏差{(max_p-min_p)/max_p*100:.1f}% > 10%'

    # ④ 成交量枯竭
    pivot_date = troughs[-1]['date']
    avg_vol_50 = _calc_ma_volume(df, 50)
    pivot_vol = _calc_avg_vol_in_period(df, pivot_date)
    if avg_vol_50 > 0 and pivot_vol >= avg_vol_50 * 0.5:
        return f'量能未枯竭: 枢轴成交量比={pivot_vol/avg_vol_50:.2f} >= 0.5'

    return ''


# ==================== VCP 纯度评分 ====================

def calculate_vcp_score(peaks, troughs, drawdowns, df):
    """
    计算 VCP 蓄力纯度评分 (0-100分)

    四个维度：
      1. 收缩段数 (T-Count)      — 3-4段最优, 2段/5段次之
      2. 枢轴点紧凑度 (Pivot)    — 最后回撤越小越好
      3. 量能枯竭度 (Volume)     — 枢轴成交量越低越好
      4. 顶部水平度 (Flatness)   — 峰顶越接近越好

    参数:
        peaks: 波峰列表 [{"date", "price"}]
        troughs: 波谷列表 [{"date", "price"}]
        drawdowns: 各段回撤幅度序列
        df: 日K DataFrame

    返回:
        tuple: (score: int, details: dict)
    """
    n = len(peaks)
    if n < 2:
        return 0, {}

    D = drawdowns
    score = 0
    details = {}

    # ① 收缩段数 (T-Count)，满分 20
    if n in (3, 4):
        t_score = 20
    elif n == 2:
        t_score = 15
    else:
        t_score = 10
    score += t_score
    details['t_count'] = {'段数': n, '得分': t_score}

    # ② 枢轴点紧凑度，满分 35
    last_d = D[-1]
    if last_d < 0.02:
        p_score = 35
    elif last_d < 0.04:
        p_score = 25
    elif last_d <= 0.06:
        p_score = 15
    else:
        p_score = 0
    score += p_score
    details['pivot_tightness'] = {'最后回撤': f'{last_d*100:.1f}%', '得分': p_score}

    # ③ 量能枯竭度，满分 35
    pivot_date = troughs[-1]['date']
    v_ratio = _get_volume_ratio(df, pivot_date)
    if v_ratio < 0.3:
        v_score = 35
    elif v_ratio < 0.5:
        v_score = 25
    elif v_ratio < 0.7:
        v_score = 15
    else:
        v_score = 0
    score += v_score
    details['volume_dryup'] = {'量比': f'{v_ratio:.2f}', '得分': v_score}

    # ④ 顶部水平度，满分 10
    peak_prices = [p['price'] for p in peaks]
    delta_p = (max(peak_prices) - min(peak_prices)) / max(peak_prices)
    if delta_p < 0.03:
        f_score = 10
    elif delta_p < 0.06:
        f_score = 5
    else:
        f_score = 0
    score += f_score
    details['top_flatness'] = {'峰顶偏差': f'{delta_p*100:.1f}%', '得分': f_score}

    logger.debug(f'VCP 评分: 段数={n}({t_score}) + 紧凑度={p_score} + 量能枯竭={v_score} + 顶部水平={f_score} = {score}/100')
    return score, details


# ==================== ZigZag 波峰波谷提取 ====================

def calculate_zigzag_extrema(highs, lows, dates, deviation_pct=0.05):
    """
    基于回撤幅度的 ZigZag 算法，提取波峰和波谷序列

    参数:
        highs: 每日最高价序列
        lows: 每日最低价序列
        dates: 对应的交易日序列
        deviation_pct: 确认反转所需的最小回撤幅度，默认 5%

    返回:
        tuple: (peaks, troughs)
            peaks: [{"date": date, "price": price}, ...]
            troughs: [{"date": date, "price": price}, ...]
    """
    peaks = []
    troughs = []

    if len(highs) < 2:
        return peaks, troughs

    trend = 1  # 1=上涨段, -1=下跌段
    last_extreme_price = float(highs[0])
    last_extreme_date = dates[0]

    for i in range(1, len(highs)):
        current_high = float(highs[i])
        current_low = float(lows[i])
        current_date = dates[i]

        if trend == 1:
            # 上涨趋势中，寻找波峰
            if current_high >= last_extreme_price:
                last_extreme_price = current_high
                last_extreme_date = current_date
            elif (last_extreme_price - current_low) / last_extreme_price >= deviation_pct:
                peaks.append({"date": last_extreme_date, "price": last_extreme_price})
                logger.debug(f'  [ZigZag] 确认波峰: {last_extreme_date} @ {last_extreme_price:.2f}')
                trend = -1
                last_extreme_price = current_low
                last_extreme_date = current_date
        else:
            # 下跌趋势中，寻找波谷
            if current_low <= last_extreme_price:
                last_extreme_price = current_low
                last_extreme_date = current_date
            elif (current_high - last_extreme_price) / last_extreme_price >= deviation_pct:
                troughs.append({"date": last_extreme_date, "price": last_extreme_price})
                logger.debug(f'  [ZigZag] 确认波谷: {last_extreme_date} @ {last_extreme_price:.2f}')
                trend = 1
                last_extreme_price = current_high
                last_extreme_date = current_date

    return peaks, troughs


# ==================== 内部辅助函数 ====================

def _calc_ma_volume(df, period):
    """计算最近 period 日均成交量"""
    if df is None or len(df) < period:
        return 0.0
    return float(df['volume'].iloc[-period:].mean())


def _calc_avg_vol_in_period(df, start_date, end_date=None):
    """
    计算指定日期范围内的平均成交量

    参数:
        df: 日K DataFrame
        start_date: 起始日期
        end_date: 截止日期（None=最新）
    """
    if df is None or df.empty:
        return 0.0

    mask = pd.to_datetime(df['date']) >= pd.to_datetime(start_date)
    if end_date:
        mask &= pd.to_datetime(df['date']) <= pd.to_datetime(end_date)

    subset = df.loc[mask]
    if subset.empty:
        return 0.0
    return float(subset['volume'].mean())


def _get_volume_ratio(df, pivot_date, ma_period=50):
    """
    计算枢轴期成交量与长期均量的比值

    参数:
        df: 日K DataFrame
        pivot_date: 枢轴点（最后波谷）日期
        ma_period: 均量周期

    返回:
        float: 枢轴期成交量 / 长期均量
    """
    avg_vol = _calc_ma_volume(df, ma_period)
    if avg_vol <= 0:
        return 1.0

    pivot_vol = _calc_avg_vol_in_period(df, pivot_date)
    return pivot_vol / avg_vol
