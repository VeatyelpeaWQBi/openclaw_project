"""
VCP (波动率收缩形态) 核心计算模块

从 strategies/trend_trading/vcp_core.py + vcp_zigzag.py 提取，
DB访问复用 core.storage

功能：
- ZIGZAG波段识别
- VCP评分（6维度弹簧蓄力模型）
- 三角回归分析
- 批量历史计算

用法：
  from strategies.trend_trading.score.vcp_core import analyze_vcp, calc_vcp_batch

  # 单只股票VCP分析
  result = analyze_vcp(df)

  # 批量计算全市场
  calc_vcp_batch()
"""

import logging
import time
import numpy as np
import pandas as pd

from core.storage import get_db_connection, save_vcp_score, get_daily_data_from_sqlite, get_daily_data_range
from strategies.trend_trading.score._base import get_all_stock_codes

logger = logging.getLogger(__name__)

WINDOW = 104  # 90天分析窗口 + 14天ATR计算


# ==================== ZIGZAG 波段识别 ====================

def detect_swings_zigzag(df, deviation_pct=7.0):
    """
    pytafast.ZIGZAG 波段识别

    参数:
        df: 日K DataFrame
        deviation_pct: 价格反转偏差百分比

    返回:
        tuple: (peaks, troughs)
    """
    try:
        import pytafast
    except ImportError:
        logger.error("pytafast 未安装，ZIGZAG不可用")
        return [], []

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
        elif v_curr < v_prev and v_curr < v_next:
            troughs.append({"date": date, "price": float(v_curr)})

    # 前置波谷丢弃
    if troughs and peaks and troughs[0]['date'] < peaks[0]['date']:
        troughs.pop(0)

    # 噪点过滤
    peaks, troughs = filter_noise_pairs(peaks, troughs)

    # 对齐
    peaks, troughs = align_pairs(peaks, troughs)

    return peaks, troughs


# ==================== 配对对齐 ====================

def align_pairs(peaks, troughs):
    """确保 peaks[0] 先于 troughs[0]，截断到配对完整"""
    if not peaks or not troughs:
        return peaks, troughs
    if troughs[0]['date'] < peaks[0]['date']:
        troughs = troughs[1:]
    n = min(len(peaks), len(troughs))
    return peaks[:n], troughs[:n]


# ==================== 噪点对过滤 ====================

def filter_noise_pairs(peaks, troughs, min_gap_days=5):
    """
    二次过滤噪点配对
    如果前后两组配对之间间隔 < min_gap_days，删除波幅较小的那组
    """
    from datetime import datetime as dt

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
                t_date = dt.strptime(str(troughs[i]['date'])[:10], '%Y-%m-%d')
                p_next_date = dt.strptime(str(peaks[i + 1]['date'])[:10], '%Y-%m-%d')
                gap_days = (p_next_date - t_date).days
            except (ValueError, TypeError):
                continue

            if gap_days < min_gap_days:
                dd_curr = peaks[i]['price'] - troughs[i]['price']
                dd_next = peaks[i + 1]['price'] - troughs[i + 1]['price']

                if dd_curr <= dd_next:
                    peaks.pop(i)
                    troughs.pop(i)
                else:
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


# ==================== 三角回归分析 ====================

def triangle_analysis(peaks, troughs):
    """
    线性回归三角形态分析

    返回:
        dict: 收缩率、收敛速度、收敛程度、偏离率、拟合优度、三角类型等
    """
    from datetime import datetime as dt, timedelta

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

    try:
        base_date = dt.strptime(str(peaks[0]['date'])[:10], '%Y-%m-%d')
        peak_x = np.array([(dt.strptime(str(p['date'])[:10], '%Y-%m-%d') - base_date).days for p in peaks], dtype=float)
        peak_y = np.array([p['price'] for p in peaks], dtype=float)
        trough_x = np.array([(dt.strptime(str(t['date'])[:10], '%Y-%m-%d') - base_date).days for t in troughs], dtype=float)
        trough_y = np.array([t['price'] for t in troughs], dtype=float)
    except (ValueError, TypeError):
        result['pattern_description'] = '日期解析失败'
        return result

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

    contraction_rate = peak_slope - trough_slope
    is_contracting = contraction_rate < 0

    if abs(contraction_rate) > 1e-9:
        apex_x = (trough_intercept - peak_intercept) / contraction_rate
    else:
        apex_x = float('inf')

    if apex_x != float('inf') and apex_x > 0:
        try:
            apex_date = (base_date + timedelta(days=int(round(apex_x)))).strftime('%Y-%m-%d')
        except (OverflowError, ValueError):
            apex_date = ''
    else:
        apex_date = ''

    denom = max(abs(peak_slope), abs(trough_slope))
    convergence_speed = abs(contraction_rate) / denom if denom > 1e-9 else 0

    all_event_dates = sorted(set(
        [str(p['date'])[:10] for p in peaks] +
        [str(t['date'])[:10] for t in troughs]
    ))
    if len(all_event_dates) >= 2:
        start_day = (dt.strptime(all_event_dates[0], '%Y-%m-%d') - base_date).days
        end_day = (dt.strptime(all_event_dates[-1], '%Y-%m-%d') - base_date).days
        upper_start = peak_slope * start_day + peak_intercept
        lower_start = trough_slope * start_day + trough_intercept
        upper_end = peak_slope * end_day + peak_intercept
        lower_end = trough_slope * end_day + trough_intercept
        initial_gap = abs(upper_start - lower_start)
        current_gap = abs(upper_end - lower_end)
        convergence_pct = 1 - current_gap / initial_gap if initial_gap > 1e-9 else 0
    else:
        initial_gap = current_gap = convergence_pct = 0

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

    if not is_contracting:
        pattern_description = f"发散三角形（速度{convergence_speed:.2f}）"
    elif tri_type == '上升三角':
        pattern_description = f"收敛上升三角形（程度{convergence_pct:.0%}，速度{convergence_speed:.2f}）"
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

    return result


# ==================== 成交量辅助 ====================

def calc_ma_volume(df, period):
    """计算最近 period 日均成交量"""
    if df is None or len(df) < period:
        return 0.0
    return float(df['volume'].iloc[-period:].mean())


def get_volume_ratio(df, pivot_date, ma_period=50):
    """计算枢轴期成交量与长期均量的比值"""
    avg_vol = calc_ma_volume(df, ma_period)
    if avg_vol <= 0:
        return 1.0

    mask = pd.to_datetime(df['date']) >= pd.to_datetime(pivot_date)
    subset = df.loc[mask]
    if subset.empty:
        return 1.0
    pivot_vol = float(subset['volume'].mean())
    return pivot_vol / avg_vol


# ==================== VCP 评分系统 ====================

def calculate_vcp_score(peaks, troughs, drawdowns, df, tri):
    """
    VCP 弹簧蓄力评分系统（6维度）

    ① 弹簧压缩度（收敛程度）  — +25 ~ -10
    ② 收缩质量（回撤递减）    — +20 ~ -10
    ③ 三角形态可信度（R²）    — +15 ~ -5
    ④ 波段数量               — +10 ~ -5
    ⑤ 量能枯竭               — +15 ~ 0
    ⑥ 三角类型               — +10 ~ -10

    理论范围: -40 ~ +95
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

    # ① 弹簧压缩度 (+25 ~ -10)
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

    # ② 收缩质量 (+20 ~ -10)
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

        step_score = round(decreasing_pct * 8)
        if decreasing_pct < 0.3:
            step_score = -5
        elif decreasing_pct < 0.5:
            step_score = 0

    q_score = trend_score + step_score
    score += q_score
    details['contraction_quality'] = {'末/首比': f'{overall_ratio:.2f}', '递减占比': f'{decreasing_pct:.0%}', '得分': q_score}

    # ③ 三角形态可信度 (+15 ~ -5)
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

    # ④ 波段数量 (+10 ~ -5)
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

    # ⑤ 量能枯竭 (+15 ~ 0)
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

    # ⑥ 三角类型 (+10 ~ -10)
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

    return score, details


# ==================== 统一入口 ====================

def analyze_vcp(df, deviation_pct=5.0):
    """
    VCP 分析统一入口

    参数:
        df: 日K DataFrame，包含 date, open, high, low, close, volume
        deviation_pct: ZIGZAG反转偏差百分比

    返回:
        dict: {is_vcp, score, peaks, troughs, drawdowns, details, reject_reason}
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

    peaks, troughs = detect_swings_zigzag(df, deviation_pct)

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

    # 计算回撤序列
    drawdowns = [(peaks[i]['price'] - troughs[i]['price']) / peaks[i]['price'] for i in range(n)]
    result['drawdowns'] = [round(d, 4) for d in drawdowns]

    # 三角回归分析
    tri = triangle_analysis(peaks, troughs)
    result['triangle'] = tri

    # 评分
    score, details = calculate_vcp_score(peaks, troughs, drawdowns, df, tri)
    result['score'] = score
    result['details'] = details
    result['is_vcp'] = True

    return result



# ==================== 批量计算 ====================


def _calc_vcp_for_stock_df(code, df):
    """
    对单只股票的DataFrame计算VCP评分（增量/全量共用）

    返回:
        list[dict]: 评分记录列表
    """
    n = len(df)
    if n < WINDOW:
        return []

    records = []
    for i in range(WINDOW - 1, n):
        df_window = df.iloc[i - WINDOW + 1: i + 1]
        try:
            result = analyze_vcp(df_window)
        except Exception:
            continue

        d = result.get('details', {})
        records.append({
            'code': code,
            'calc_date': str(df_window['date'].iloc[-1])[:10],
            'score': result['score'],
            'score_compression': d.get('compression', {}).get('得分'),
            'score_contraction': d.get('contraction_quality', {}).get('得分'),
            'score_credibility': d.get('shape_credibility', {}).get('得分'),
            'score_swing_count': d.get('swing_count', {}).get('得分'),
            'score_volume': d.get('volume_dryup', {}).get('得分'),
            'score_triangle_type': d.get('triangle_bonus', {}).get('得分'),
            'data_start': str(df_window['date'].iloc[0])[:10],
            'data_end': str(df_window['date'].iloc[-1])[:10],
        })

    return records


def calc_vcp_batch():
    """
    全量刷新：批量计算全市场VCP评分

    返回:
        int: 总写入条数
    """
    logger.info(f"=== VCP 全量刷新 ===")
    logger.info(f"窗口大小: {WINDOW}天")

    codes = get_all_stock_codes()
    logger.info(f"共 {len(codes)} 只股票")

    total_records = 0
    start_time = time.time()

    for idx, code in enumerate(codes):
        df = get_daily_data_from_sqlite(code)
        if df.empty:
            continue

        records = _calc_vcp_for_stock_df(code, df)
        if records:
            save_vcp_score(records)
            total_records += len(records)

        if (idx + 1) % 100 == 0 or idx == len(codes) - 1:
            elapsed = time.time() - start_time
            speed = (idx + 1) / elapsed if elapsed > 0 else 0
            logger.info(f"  进度: {idx + 1}/{len(codes)} 只, "
                        f"已写入 {total_records} 条, "
                        f"速度: {speed:.1f}只/秒")

    elapsed = time.time() - start_time
    logger.info(f"[VCP] 全量完成: {len(codes)} 只股票, {total_records} 条记录, 耗时 {elapsed:.0f}秒")
    return total_records


def calc_vcp_recent(end_date, days=30):
    """
    近日增量刷新：计算最近N个交易日的VCP评分

    VCP需要104天的滑动窗口（WINDOW），所以实际加载数据需往前多取104天。
    只计算最近 days 个交易日的评分。

    参数:
        end_date: 结束日期 'YYYY-MM-DD'（通常是最新交易日）
        days: 要刷新的交易日数（默认30）

    返回:
        int: 写入条数
    """
    from core.storage import get_trading_day_offset_from

    logger.info(f"[VCP] 近日刷新: 最近{days}天到 {end_date}")

    codes = get_all_stock_codes()
    if not codes:
        logger.error("未找到股票代码")
        return 0
    logger.info(f"共 {len(codes)} 只股票")

    # 获取 end_date 前 days 个交易日起始日
    # VCP滑动窗口需要额外 WINDOW-1 天预热
    start_date = get_trading_day_offset_from(end_date, -(days + WINDOW - 1))
    if not start_date:
        logger.error(f"无法获取预热起始日")
        return 0

    # 获取需要计算的最后几天日期范围
    # 从 start_date + WINDOW - 1 到 end_date 的每一天都要计算
    from strategies.trend_trading.score._base import get_trade_dates
    calc_dates = get_trade_dates(start_date, end_date)
    # 实际要计算的日期：从第 WINDOW 个交易日开始
    calc_dates = calc_dates[WINDOW - 1:]
    calc_dates_set = set(calc_dates)
    logger.info(f"预热{WINDOW - 1}天, 计算{len(calc_dates)}天, 数据范围{start_date}~{end_date}")

    # 删除旧数据
    from core.storage import get_db_connection
    conn = get_db_connection()
    try:
        for d in calc_dates:
            conn.execute("DELETE FROM vcp_score WHERE calc_date = ?", (d,))
        conn.commit()
    finally:
        conn.close()

    total_records = 0
    start_time = time.time()

    for idx, code in enumerate(codes):
        df = get_daily_data_range(code, start_date, end_date)
        if df.empty or len(df) < WINDOW:
            continue

        records = _calc_vcp_for_stock_df(code, df)
        if records:
            records = [r for r in records if r['calc_date'] in calc_dates_set]
            if records:
                save_vcp_score(records)
                total_records += len(records)

        if (idx + 1) % 100 == 0 or idx == len(codes) - 1:
            elapsed = time.time() - start_time
            speed = (idx + 1) / elapsed if elapsed > 0 else 0
            logger.info(f"  进度: {idx + 1}/{len(codes)} 只, "
                        f"已写入 {total_records} 条, "
                        f"速度: {speed:.1f}只/秒")

    elapsed = time.time() - start_time
    logger.info(f"[VCP] 近日完成: {total_records} 条, 耗时 {elapsed:.0f}秒")
    return total_records


def calc_vcp_from_data(stock_data, all_dates, days):
    """
    从预加载数据计算VCP评分（由 calc_scores.py 统一调度调用）

    参数:
        stock_data: {code: DataFrame} 预加载的日K数据
        all_dates: list[str] 指数日期列表（升序）
        days: 计算天数

    返回:
        int: 写入条数
    """
    if len(all_dates) < WINDOW + days - 1:
        logger.error(f"[VCP] 日期不足: 需要{WINDOW + days - 1}天，实际{len(all_dates)}天")
        return 0

    calc_dates = all_dates[-days:]
    calc_dates_set = set(calc_dates)

    logger.info(f"[VCP] 从预加载数据计算: {len(stock_data)}只股票, {len(calc_dates)}天")

    # 删除旧数据
    conn = get_db_connection()
    try:
        for d in calc_dates:
            conn.execute("DELETE FROM vcp_score WHERE calc_date = ?", (d,))
        conn.commit()
    finally:
        conn.close()

    total_records = 0
    start_time = time.time()

    for idx, (code, df) in enumerate(stock_data.items()):
        if len(df) < WINDOW:
            continue

        # 只取最后 WINDOW + days - 1 行，避免对全部历史做滑动窗口
        need_rows = WINDOW + days - 1
        if len(df) > need_rows:
            df = df.iloc[-need_rows:].reset_index(drop=True)

        records = _calc_vcp_for_stock_df(code, df)
        if records:
            records = [r for r in records if r['calc_date'] in calc_dates_set]
            if records:
                save_vcp_score(records)
                total_records += len(records)

        if (idx + 1) % 500 == 0 or idx == len(stock_data) - 1:
            elapsed = time.time() - start_time
            speed = (idx + 1) / elapsed if elapsed > 0 else 0
            logger.info(f"  进度: {idx + 1}/{len(stock_data)} 只, "
                        f"已写入 {total_records} 条, "
                        f"速度: {speed:.1f}只/秒")

    elapsed = time.time() - start_time
    logger.info(f"[VCP] 完成: {total_records} 条, 耗时 {elapsed:.0f}秒")
    return total_records
