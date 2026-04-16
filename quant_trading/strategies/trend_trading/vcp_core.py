"""
VCP 共享核心模块
包含 VCP 评分、验证、ATR计算、Prominence计算、噪点过滤等所有方案共用的逻辑
"""

import logging
import numpy as np
import pandas as pd
import pytafast

logger = logging.getLogger(__name__)


# ==================== 统一入口模板 ====================

def analyze_vcp_common(peaks, troughs, df):
    """
    各方案通用的 VCP 分析入口
    接收已识别的 peaks/troughs，执行验证+评分

    参数:
        peaks: [{"date": str, "price": float}, ...]
        troughs: [{"date": str, "price": float}, ...]
        df: 原始日K DataFrame

    返回:
        dict: {is_vcp, score, peaks, troughs, drawdowns, details, reject_reason}
    """
    result = {
        'is_vcp': False,
        'score': 0,
        'peaks': list(peaks),
        'troughs': list(troughs),
        'drawdowns': [],
        'details': {},
        'reject_reason': '',
    }

    # 配对对齐
    peaks, troughs = align_pairs(peaks, troughs)
    n = min(len(peaks), len(troughs))
    peaks = peaks[:n]
    troughs = troughs[:n]

    result['peaks'] = peaks
    result['troughs'] = troughs

    if n < 2:
        result['reject_reason'] = f'配对不足2组（波峰{len(peaks)}个, 波谷{len(troughs)}个）'
        return result

    # 打印波段详情
    for i in range(n):
        drop_pct = (peaks[i]['price'] - troughs[i]['price']) / peaks[i]['price'] * 100
        logger.debug(f'  波段{i+1}: {peaks[i]["date"]}~{troughs[i]["date"]}, '
                     f'跌幅{drop_pct:.1f}%, 拐点({peaks[i]["price"]:.2f}→{troughs[i]["price"]:.2f})')

    # 计算回撤序列
    drawdowns = [(peaks[i]['price'] - troughs[i]['price']) / peaks[i]['price'] for i in range(n)]
    result['drawdowns'] = [round(d, 4) for d in drawdowns]

    for i in range(n):
        logger.debug(f'  回撤{i+1}: 波峰{peaks[i]["price"]:.2f}→波谷{troughs[i]["price"]:.2f}, '
                     f'回撤幅度{drawdowns[i]*100:.1f}%')

    # 三角回归分析（在评分之前执行，评分需要使用三角数据）
    tri = triangle_analysis(peaks, troughs)
    result['triangle'] = tri

    # 评分（无过滤，每只股都出分，可负分）
    score, details = calculate_vcp_score(peaks, troughs, drawdowns, df, tri)
    result['score'] = score
    result['details'] = details
    result['is_vcp'] = True  # 所有股票都视为可评分

    logger.debug(f"VCP 评分={score}/ 波峰={n}组, 回撤={[round(d*100,1) for d in drawdowns]}%, "
                f"三角={tri['pattern_description']}")
    return result


# ==================== 配对对齐 ====================

def align_pairs(peaks, troughs):
    """确保 peaks[0] 先于 troughs[0]，截断到配对完整"""
    if not peaks or not troughs:
        return peaks, troughs

    # 如果第一个极值是波谷，丢弃
    if troughs[0]['date'] < peaks[0]['date']:
        logger.debug(f'  [对齐] 丢弃前置波谷: {troughs[0]["date"]}')
        troughs = troughs[1:]

    n = min(len(peaks), len(troughs))
    return peaks[:n], troughs[:n]


# ==================== 噪点对过滤 ====================

def filter_noise_pairs(peaks, troughs, min_gap_days=5):
    """
    二次过滤噪点配对
    如果前后两组配对之间间隔 < min_gap_days，删除波幅较小的那组
    """
    from datetime import datetime

    if len(peaks) < 2 or len(troughs) < 2:
        return peaks, troughs

    peaks, troughs = list(peaks), list(troughs)

    while True:
        pair_len = min(len(peaks), len(troughs))
        if pair_len < 2:
            break

        removed = False
        for i in range(pair_len - 1):
            try:
                t_date = datetime.strptime(str(troughs[i]['date'])[:10], '%Y-%m-%d')
                p_next_date = datetime.strptime(str(peaks[i + 1]['date'])[:10], '%Y-%m-%d')
                gap_days = (p_next_date - t_date).days
            except (ValueError, TypeError):
                continue

            if gap_days < min_gap_days:
                dd_curr = peaks[i]['price'] - troughs[i]['price']
                dd_next = peaks[i + 1]['price'] - troughs[i + 1]['price']

                if dd_curr <= dd_next:
                    logger.debug(f'  [噪点过滤] 删除配对{i+1}: {peaks[i]["date"]}→{troughs[i]["date"]} '
                                 f'(间隔{gap_days}天, 波幅{dd_curr:.2f})')
                    peaks.pop(i)
                    troughs.pop(i)
                else:
                    logger.debug(f'  [噪点过滤] 删除配对{i+2}: {peaks[i+1]["date"]}→{troughs[i+1]["date"]} '
                                 f'(间隔{gap_days}天, 波幅{dd_next:.2f})')
                    peaks.pop(i + 1)
                    troughs.pop(i + 1)
                removed = True
                break

        if not removed:
            break

    return peaks, troughs


# ==================== ATR 计算 ====================

def calc_atr(df, period=14):
    """
    用 Wilder RMA 计算 ATR

    返回:
        numpy.ndarray: ATR值数组，与 df 等长
    """
    high = df['high'].values.astype(float)
    low = df['low'].values.astype(float)
    close = df['close'].values.astype(float)

    n = len(close)
    tr = np.zeros(n)
    tr[0] = high[0] - low[0]
    for i in range(1, n):
        tr[i] = max(high[i] - low[i],
                    abs(high[i] - close[i - 1]),
                    abs(low[i] - close[i - 1]))

    atr = np.full(n, np.nan)
    if n >= period:
        atr[period - 1] = np.mean(tr[:period])
        for i in range(period, n):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

    return atr


# ==================== Prominence 计算 ====================

def calc_prominence(idx, series, window=30):
    """
    计算 series[idx] 处极值的 prominence（突出度）

    prominence = val - max(left_base, right_base)
    left_base: 向左扫描，遇到 >= val 的位置停止，期间最低值
    right_base: 向右扫描，遇到 >= val 的位置停止，期间最低值

    返回:
        float: prominence 值
    """
    n = len(series)
    val = series[idx]

    # 向左
    left_base = val
    for j in range(idx - 1, max(-1, idx - window - 1), -1):
        if j < 0:
            break
        if series[j] >= val:
            left_base = val
            break
        left_base = min(left_base, series[j])

    # 向右
    right_base = val
    for j in range(idx + 1, min(n, idx + window + 1)):
        if series[j] >= val:
            right_base = val
            break
        right_base = min(right_base, series[j])

    return val - max(left_base, right_base)


def filter_by_prominence(extremes, series, atr_values, min_prom_mult=0.5):
    """
    用 prominence 过滤不够突出的极值

    参数:
        extremes: [(idx, price), ...]
        series: 价格序列
        atr_values: ATR数组
        min_prom_mult: prominence >= ATR * 此系数才保留

    返回:
        list: [(idx, price), ...]
    """
    filtered = []
    for idx, price in extremes:
        prom = calc_prominence(idx, series)
        threshold = atr_values[idx] * min_prom_mult if not np.isnan(atr_values[idx]) else 0
        if prom >= threshold:
            filtered.append((idx, price))
        else:
            logger.debug(f'  [Prominence] 过滤极值 idx={idx} 价格={price:.2f} prom={prom:.2f} < 阈值{threshold:.2f}')
    return filtered


# ==================== 最小持续天数过滤 ====================

def filter_min_duration(peaks, troughs, min_days=3):
    """
    过滤持续天数不足 min_days 的波段
    波段持续天数 = 波峰到波谷的天数
    """
    if not peaks or not troughs:
        return peaks, troughs

    from datetime import datetime

    peaks_filtered = []
    troughs_filtered = []
    n = min(len(peaks), len(troughs))

    for i in range(n):
        try:
            p_date = datetime.strptime(str(peaks[i]['date'])[:10], '%Y-%m-%d')
            t_date = datetime.strptime(str(troughs[i]['date'])[:10], '%Y-%m-%d')
            duration = abs((t_date - p_date).days)
        except (ValueError, TypeError):
            peaks_filtered.append(peaks[i])
            troughs_filtered.append(troughs[i])
            continue

        if duration >= min_days:
            peaks_filtered.append(peaks[i])
            troughs_filtered.append(troughs[i])
        else:
            logger.debug(f'  [持续天数] 过滤波段: {peaks[i]["date"]}→{troughs[i]["date"]} ({duration}天 < {min_days})')

    return peaks_filtered, troughs_filtered


# ==================== VCP 评分系统 ====================

def calculate_vcp_score(peaks, troughs, drawdowns, df, tri):
    """
    VCP 弹簧蓄力评分系统（无过滤，每只股都出分，可负分）

    核心比喻：日K波动如同弹簧，宽幅震荡→逐渐压缩→突破释放。
    A股做多获利，需要判断弹簧即将反弹的时机。

    6个维度（允许负分）：
    ① 弹簧压缩度（收敛程度）  — +25 ~ -10 — 弹簧压了多紧
    ② 收缩质量（回撤递减）    — +20 ~ -10 — 压缩是否在加速
    ③ 三角形态可信度（R²）    — +15 ~ -5  — 弹簧形状是否规则
    ④ 波段数量               — +10 ~ -5   — 2~6段有效
    ⑤ 量能枯竭               — +15 ~ 0    — 多空都已疲惫
    ⑥ 三角类型               — +10 ~ -10  — 做多胜率方向

    理论范围: -40 ~ +95
    返回: (score, details)
    """
    n = len(peaks)
    if n < 2:
        return 0, {'error': {'信息': '配对不足2组', '得分': 0}}

    D = drawdowns
    score = 0
    details = {}
    pct = tri.get('convergence_pct', 0)
    speed = tri.get('convergence_speed', 0)
    is_contracting = tri.get('is_contracting', False)

    # ===== ① 弹簧压缩度 (+25 ~ -10) =====
    if pct >= 0.85:
        c_score = 25
    elif pct >= 0.70:
        c_score = 20
    elif pct >= 0.50:
        c_score = 14
    elif pct >= 0.30:
        c_score = 7
    elif pct >= 0.10:
        c_score = 2
    elif pct >= -0.10:
        c_score = 0 if speed < 0.1 else 1
    elif pct >= -0.30:
        c_score = -4
    else:
        c_score = -10
    score += c_score
    details['compression'] = {'收敛程度': f'{pct:.0%}', '速度': f'{speed:.2f}', '得分': c_score}

    # ===== ② 收缩质量 (+20 ~ -10) =====
    overall_ratio = D[-1] / D[0] if D[0] > 0 else 1.0
    decreasing_count = sum(1 for i in range(1, n) if D[i] < D[i - 1])
    decreasing_pct = decreasing_count / (n - 1) if n >= 2 else 0

    if not is_contracting:
        trend_score = 0 if overall_ratio < 0.5 else -5
        step_score = 0 if decreasing_pct >= 0.5 else -5
    else:
        if overall_ratio < 0.2:
            trend_score = 12
        elif overall_ratio < 0.35:
            trend_score = 9
        elif overall_ratio < 0.5:
            trend_score = 6
        elif overall_ratio < 0.7:
            trend_score = 3
        elif overall_ratio < 1.0:
            trend_score = 0
        else:
            trend_score = -5

        step_score = round(decreasing_pct * 8)  # 0~8
        if decreasing_pct < 0.3:
            step_score = -5
        elif decreasing_pct < 0.5:
            step_score = 0

    q_score = trend_score + step_score
    score += q_score
    details['contraction_quality'] = {'末/首比': f'{overall_ratio:.2f}', '递减占比': f'{decreasing_pct:.0%}', '得分': q_score}

    # ===== ③ 三角形态可信度 (+15 ~ -5) =====
    r2_upper = tri.get('r_squared_upper', 0)
    r2_lower = tri.get('r_squared_lower', 0)
    avg_r2 = (r2_upper + r2_lower) / 2
    dev = tri.get('median_deviation', 1.0)

    if avg_r2 >= 0.9 and dev < 0.02:
        r_score = 15
    elif avg_r2 >= 0.8 and dev < 0.03:
        r_score = 12
    elif avg_r2 >= 0.7 and dev < 0.04:
        r_score = 9
    elif avg_r2 >= 0.5 and dev < 0.06:
        r_score = 4
    elif avg_r2 >= 0.3:
        r_score = 0
    else:
        r_score = -5
    score += r_score
    details['shape_credibility'] = {'R²均值': f'{avg_r2:.3f}', '偏离率': f'{dev:.3f}', '得分': r_score}

    # ===== ④ 波段数量 (+10 ~ -5) =====
    # 2~6段是VCP有效区间。3~4段最佳（充分压缩且不杂乱）。
    if n in (3, 4):
        s_score = 10
    elif n in (5, 6):
        s_score = 6
    elif n == 2:
        s_score = 3
    else:
        s_score = -5
    score += s_score
    details['swing_count'] = {'波段数': n, '得分': s_score}

    # ===== ⑤ 量能枯竭 (+15 ~ 0) =====
    pivot_date = troughs[-1]['date']
    v_ratio = get_volume_ratio(df, pivot_date)
    if v_ratio < 0.2:
        v_score = 15
    elif v_ratio < 0.35:
        v_score = 12
    elif v_ratio < 0.5:
        v_score = 8
    elif v_ratio < 0.7:
        v_score = 4
    else:
        v_score = 0
    score += v_score
    details['volume_dryup'] = {'量比': f'{v_ratio:.2f}', '得分': v_score}

    # ===== ⑥ 三角类型 (+10 ~ -10) =====
    tri_type = tri.get('triangle_type', 'unknown')
    if tri_type == '上升三角':
        b_score = 10
    elif tri_type == '对称三角':
        b_score = 7
    elif tri_type == '收缩楔形':
        b_score = 5
    elif tri_type == '下降三角':
        b_score = 0
    elif tri_type == '发散':
        b_score = -10
    else:
        b_score = 0
    score += b_score
    details['triangle_bonus'] = {'类型': tri_type, '得分': b_score}

    logger.debug(f'VCP 评分: 压缩={c_score} + 收缩={q_score} + 可信={r_score} + 波段={s_score} + 量能={v_score} + 类型={b_score} = {score}')
    return score, details


# ==================== 成交量辅助函数 ====================

def calc_ma_volume(df, period):
    """计算最近 period 日均成交量"""
    if df is None or len(df) < period:
        return 0.0
    return float(df['volume'].iloc[-period:].mean())


def calc_avg_vol_in_period(df, start_date, end_date=None):
    """计算指定日期范围内的平均成交量"""
    if df is None or df.empty:
        return 0.0

    mask = pd.to_datetime(df['date']) >= pd.to_datetime(start_date)
    if end_date:
        mask &= pd.to_datetime(df['date']) <= pd.to_datetime(end_date)

    subset = df.loc[mask]
    if subset.empty:
        return 0.0
    return float(subset['volume'].mean())


def get_volume_ratio(df, pivot_date, ma_period=50):
    """计算枢轴期成交量与长期均量的比值"""
    avg_vol = calc_ma_volume(df, ma_period)
    if avg_vol <= 0:
        return 1.0

    pivot_vol = calc_avg_vol_in_period(df, pivot_date)
    return pivot_vol / avg_vol


# ==================== 三角回归分析 ====================

def triangle_analysis(peaks, troughs):
    """
    线性回归三角形态分析（所有方案共用）

    对波峰序列做OLS回归 → 上沿
    对波谷序列做OLS回归 → 下沿
    分析收缩率、收敛速度、收敛程度、偏离率、拟合优度

    返回:
        dict: {
            'is_contracting': bool,        # 是否收缩
            'contraction_rate': float,     # 收缩率 (p_slope - t_slope)，负值=收缩
            'convergence_speed': float,    # 收敛速度 (0~1)
            'convergence_pct': float,      # 相对收敛程度 (0~1)
            'initial_gap': float,          # 初始间距（价格单位）
            'current_gap': float,          # 当前间距（价格单位）
            'median_deviation': float,     # 偏离率中位数
            'r_squared_upper': float,      # 上沿R²
            'r_squared_lower': float,      # 下沿R²
            'apex_idx': float,             # 两条回归线交汇的索引位置（天数）
            'apex_date': str,              # 理论交汇日期
            'triangle_type': str,          # 三角类型
            'pattern_description': str,    # 形态描述文本
        }
    """
    result = {
        'is_contracting': False,
        'contraction_rate': 0,
        'convergence_speed': 0,
        'convergence_pct': 0,
        'initial_gap': 0,
        'current_gap': 0,
        'median_deviation': 1.0,
        'r_squared_upper': 0,
        'r_squared_lower': 0,
        'apex_idx': float('inf'),
        'apex_date': '',
        'triangle_type': 'unknown',
        'pattern_description': '',
    }

    if len(peaks) < 2 or len(troughs) < 2:
        result['pattern_description'] = '数据不足'
        return result

    # 提取索引和价格：用日期距离第一个点的天数作为x轴
    from datetime import datetime
    try:
        base_date = datetime.strptime(str(peaks[0]['date'])[:10], '%Y-%m-%d')
        peak_x = np.array([(datetime.strptime(str(p['date'])[:10], '%Y-%m-%d') - base_date).days for p in peaks], dtype=float)
        peak_y = np.array([p['price'] for p in peaks], dtype=float)
        trough_x = np.array([(datetime.strptime(str(t['date'])[:10], '%Y-%m-%d') - base_date).days for t in troughs], dtype=float)
        trough_y = np.array([t['price'] for t in troughs], dtype=float)
    except (ValueError, TypeError):
        result['pattern_description'] = '日期解析失败'
        return result

    # OLS 回归（含R²）
    def ols(x, y):
        n = len(x)
        if n < 2:
            return 0, 0, 0
        x_mean, y_mean = np.mean(x), np.mean(y)
        ss_xy = np.sum((x - x_mean) * (y - y_mean))
        ss_xx = np.sum((x - x_mean) ** 2)
        if ss_xx == 0:
            return 0, y_mean, 0
        slope = ss_xy / ss_xx
        intercept = y_mean - slope * x_mean
        y_pred = slope * x + intercept
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - y_mean) ** 2)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0
        return slope, intercept, r_squared

    peak_slope, peak_intercept, r2_upper = ols(peak_x, peak_y)
    trough_slope, trough_intercept, r2_lower = ols(trough_x, trough_y)

    # 基础指标
    contraction_rate = peak_slope - trough_slope
    is_contracting = contraction_rate < 0

    # 三角顶点
    if abs(contraction_rate) > 1e-9:
        apex_x = (trough_intercept - peak_intercept) / contraction_rate
    else:
        apex_x = float('inf')

    # 交汇日期
    if apex_x != float('inf') and apex_x > 0:
        try:
            apex_date = (base_date + __import__('datetime').timedelta(days=int(round(apex_x)))).strftime('%Y-%m-%d')
        except (OverflowError, ValueError):
            apex_date = ''
    else:
        apex_date = ''

    # ===== 收敛速度 =====
    # 两线靠拢速度相对于价格波动幅度的比率
    # 值越大 → 收缩越快（三角形越"尖"）
    denom = max(abs(peak_slope), abs(trough_slope))
    if denom > 1e-9:
        convergence_speed = abs(contraction_rate) / denom
    else:
        convergence_speed = 0

    # ===== 相对收敛程度 =====
    # 用事件中最早和最晚的日期来计算初始间距和当前间距
    all_event_dates = sorted(set([str(p['date'])[:10] for p in peaks] + [str(t['date'])[:10] for t in troughs]))
    if len(all_event_dates) >= 2:
        start_day = (datetime.strptime(all_event_dates[0], '%Y-%m-%d') - base_date).days
        end_day = (datetime.strptime(all_event_dates[-1], '%Y-%m-%d') - base_date).days

        upper_start = peak_slope * start_day + peak_intercept
        lower_start = trough_slope * start_day + trough_intercept
        upper_end = peak_slope * end_day + peak_intercept
        lower_end = trough_slope * end_day + trough_intercept

        initial_gap = abs(upper_start - lower_start)
        current_gap = abs(upper_end - lower_end)

        if initial_gap > 1e-9:
            convergence_pct = 1 - current_gap / initial_gap
        else:
            convergence_pct = 0
    else:
        initial_gap = 0
        current_gap = 0
        convergence_pct = 0

    # ===== 偏离率中位数 =====
    deviations = []
    for i, p in enumerate(peaks):
        fitted = peak_slope * peak_x[i] + peak_intercept
        if fitted > 0:
            deviations.append(abs(p['price'] - fitted) / fitted)
    for i, t in enumerate(troughs):
        fitted = trough_slope * trough_x[i] + trough_intercept
        if fitted > 0:
            deviations.append(abs(t['price'] - fitted) / fitted)
    median_dev = float(np.median(deviations)) if deviations else 1.0

    # ===== 三角类型 =====
    if abs(peak_slope) < 0.01 and trough_slope > 0:
        tri_type = '上升三角'
    elif peak_slope < -0.01 and abs(trough_slope) < 0.01:
        tri_type = '下降三角'
    elif peak_slope < 0 and trough_slope > 0:
        tri_type = '对称三角'
    elif is_contracting:
        tri_type = '收缩楔形'
    else:
        tri_type = '发散'

    # ===== 形态描述 =====
    if not is_contracting:
        pattern_description = f"发散三角形（速度{convergence_speed:.2f}）"
    elif tri_type == '上升三角':
        pattern_description = f"收敛上升三角形（程度{convergence_pct:.0%}，速度{convergence_speed:.2f}）"
    elif tri_type == '下降三角':
        pattern_description = f"收敛下降三角形（程度{convergence_pct:.0%}，速度{convergence_speed:.2f}）"
    elif tri_type == '对称三角':
        pattern_description = f"收敛对称三角形（程度{convergence_pct:.0%}，速度{convergence_speed:.2f}）"
    else:
        pattern_description = f"收敛{tri_type}（程度{convergence_pct:.0%}，速度{convergence_speed:.2f}）"

    result.update({
        'is_contracting': is_contracting,
        'contraction_rate': round(contraction_rate, 6),
        'convergence_speed': round(convergence_speed, 4),
        'convergence_pct': round(convergence_pct, 4),
        'initial_gap': round(initial_gap, 2),
        'current_gap': round(current_gap, 2),
        'median_deviation': round(median_dev, 4),
        'r_squared_upper': round(r2_upper, 4),
        'r_squared_lower': round(r2_lower, 4),
        'apex_idx': round(apex_x, 1),
        'apex_date': apex_date,
        'triangle_type': tri_type,
        'pattern_description': pattern_description,
    })

    logger.debug(f'  [三角] {pattern_description} | 收缩率={contraction_rate:.4f} '
                 f'偏离率={median_dev:.3f} R²上={r2_upper:.3f} 下={r2_lower:.3f} '
                 f'顶点={apex_date} 初始间距={initial_gap:.2f} 当前间距={current_gap:.2f}')

    return result


# ==================== Zigzag 方案（原 vcp_zigzag.py） ====================

def detect_swings(df, deviation_pct=7.0):
    """
    pytafast.ZIGZAG 波段识别

    参数:
        df: 日K DataFrame
        deviation_pct: 价格反转偏差百分比

    返回:
        tuple: (peaks, troughs)
    """
    if pytafast is None:
        raise ImportError("pytafast 未安装，无法使用 zigzag 方案")

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
    """Zigzag方案 统一入口"""
    peaks, troughs = detect_swings(df, deviation_pct)
    return analyze_vcp_common(peaks, troughs, df)
