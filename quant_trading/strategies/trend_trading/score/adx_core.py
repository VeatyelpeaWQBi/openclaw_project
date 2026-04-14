"""
ADX (平均趋向指数) 核心计算模块

算法严格遵循 J. Welles Wilder Jr. 1978年《New Concepts in Technical Trading Systems》
融合四源调研成果：
  - 算法精确性（豆包：SMA初始值+RMA递推）
  - 代码安全性（Claude Code：除零保护）
  - 评分体系（虾虾子：分段线性+方向加权）
  - 实战逻辑（Gemini：VCP/RS联动）

本模块职责：纯算分，不给交易建议

用法：
  from strategies.trend_trading.score.adx_core import (
      calculate_adx, adx_score, calc_adx_for_stock, calc_adx_batch, get_adx_score
  )
"""

import logging
import time
import numpy as np
import pandas as pd

from core.storage import get_db_connection, get_daily_data_from_sqlite, get_daily_data_range, save_adx_score
from strategies.trend_trading.score._base import get_all_stock_codes

logger = logging.getLogger(__name__)

DEFAULT_PERIOD = 14
ADX_WARMUP = 27  # ADX预热期：14(TR/DM) + 13(DX) = 27根K线NaN


# ==================== Wilder RMA (怀尔德平滑法) ====================

def wilder_rma(series: pd.Series, period: int) -> pd.Series:
    """
    怀尔德平滑法（RMA），精确实现：
    - 初始值：前period个数据的SMA
    - 递推：RMA[i] = RMA[i-1] * (period-1)/period + X[i] / period
    - 数学上等价于 ewm(alpha=1/period, adjust=False)，但初始值处理更精确

    参数:
        series: pandas Series
        period: 平滑周期（通常为14）

    返回:
        pandas Series（前period-1个值为NaN）
    """
    result = pd.Series(np.nan, index=series.index, dtype=float)

    if len(series) < period:
        return result

    # 初始值：前period个数据的SMA
    first_sma = series.iloc[:period].mean()
    result.iloc[period - 1] = first_sma

    # 向量化递推（从period开始）
    values = series.values
    rma_val = first_sma
    result_arr = np.full(len(values), np.nan)
    result_arr[period - 1] = first_sma
    alpha = 1.0 / period

    for i in range(period, len(values)):
        rma_val = rma_val * (1.0 - alpha) + values[i] * alpha
        result_arr[i] = rma_val

    return pd.Series(result_arr, index=series.index)


# ==================== ADX 6步计算 ====================

def calculate_adx(df: pd.DataFrame, period: int = DEFAULT_PERIOD) -> pd.DataFrame:
    """
    计算完整ADX系统（+DI, -DI, DX, ADX）
    严格遵循Wilder原著，带初始值SMA处理和除零保护

    参数:
        df: DataFrame，必须包含 high, low, close 列
        period: 计算周期（默认14）

    返回:
        DataFrame，新增 plus_di, minus_di, dx, adx 列
    """
    high = df['high']
    low = df['low']
    close = df['close']
    prev_close = close.shift(1)

    # Step 1: True Range（真实波幅）
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)

    # Step 2: Directional Movement（方向变动值）
    up_move = high - high.shift(1)
    down_move = low.shift(1) - low

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    # Step 3: Wilder Smoothing（精确RMA平滑）
    str_smooth = wilder_rma(tr, period)
    plus_dm_smooth = wilder_rma(pd.Series(plus_dm, index=df.index), period)
    minus_dm_smooth = wilder_rma(pd.Series(minus_dm, index=df.index), period)

    # Step 4: Directional Indicators（方向指标，含除零保护）
    di_denom = str_smooth.replace(0, np.nan)
    plus_di = 100.0 * plus_dm_smooth / di_denom
    minus_di = 100.0 * minus_dm_smooth / di_denom

    # Step 5: DX（方向性变动指数，含除零保护）
    di_diff = (plus_di - minus_di).abs()
    di_sum = (plus_di + minus_di).replace(0, np.nan)
    dx = 100.0 * di_diff / di_sum

    # Step 6: ADX（DX的Wilder平滑）
    adx = wilder_rma(dx, period)

    # 预热期掩码：ADX需要2*period-1根K线才能有效（14+13=27）
    # 前2*period-1个索引的ADX设为NaN
    warmup = 2 * period - 1
    if len(adx) >= warmup:
        adx.iloc[:warmup] = np.nan

    result = df.copy()
    result['plus_di'] = plus_di
    result['minus_di'] = minus_di
    result['dx'] = dx
    result['adx'] = adx

    return result


# ==================== 评分映射 ====================

def adx_score(adx_value: float, plus_di: float, minus_di: float) -> float:
    """
    ADX趋势强度方向综合评分（-100~+100分）
    分段线性映射 + 方向正负号

    设计：
    - 分段线性映射（虾虾子方案），A股适配阈值（豆包方案）
    - 方向正负号：+DI>-DI → 正分（多头），+DI<-DI → 负分（空头）
    - 绝对值 = 趋势强度，正负号 = 方向，信息零损失
    - A股只做多：空头趋势负分，综合评分自然被拉低，无需额外惩罚

    参数:
        adx_value: ADX原始值
        plus_di: +DI值
        minus_di: -DI值

    返回:
        float: -100~+100分（正=多头趋势，负=空头趋势）
    """
    # 分段线性基础分（0-100）
    if adx_value < 15:
        base_score = adx_value / 15 * 15              # 0-15 → 0-15分
    elif adx_value < 25:
        base_score = 15 + (adx_value - 15) / 10 * 25  # 15-25 → 15-40分
    elif adx_value < 30:
        base_score = 40 + (adx_value - 25) / 5 * 15   # 25-30 → 40-55分
    elif adx_value < 40:
        base_score = 55 + (adx_value - 30) / 10 * 25   # 30-40 → 55-80分
    elif adx_value < 60:
        base_score = 80 + (adx_value - 40) / 20 * 15   # 40-60 → 80-95分
    else:
        base_score = 95 + min(5.0, (adx_value - 60) / 40 * 5)  # 60+ → 95-100分

    # 空头趋势 → 负分（绝对值=base，不加direction_bonus避免双重计算）
    if plus_di < minus_di:
        return round(-base_score, 2)

    # 多头趋势 → 正分 + 方向加成
    di_sum = plus_di + minus_di
    if di_sum > 0:
        direction_ratio = (plus_di - minus_di) / di_sum  # 0 到 +1
        direction_bonus = direction_ratio * 10             # +0~10分
    else:
        direction_bonus = 0

    return round(base_score + direction_bonus, 2)



# ==================== 单只股票计算 ====================


def _extract_adx_records(code, df, period):
    """
    从ADX计算结果中提取记录列表（全量/增量共用）

    返回:
        list[dict]: 评分记录列表
    """
    if df.empty or len(df) < period * 2:
        return []

    required_cols = ['high', 'low', 'close']
    for col in required_cols:
        if col not in df.columns:
            logger.debug(f"[{code}] 缺少列 {col}，跳过")
            return []

    try:
        result_df = calculate_adx(df, period)
    except Exception as e:
        logger.debug(f"[{code}] ADX计算异常: {e}")
        return []

    records = []
    for i in range(len(result_df)):
        adx_val = result_df['adx'].iloc[i]
        if pd.isna(adx_val):
            continue

        plus_di = result_df['plus_di'].iloc[i]
        minus_di = result_df['minus_di'].iloc[i]
        dx_val = result_df['dx'].iloc[i]
        date_str = str(df['date'].iloc[i])[:10]

        records.append({
            'code': code,
            'calc_date': date_str,
            'period': period,
            'adx': round(float(adx_val), 4),
            'plus_di': round(float(plus_di), 4),
            'minus_di': round(float(minus_di), 4),
            'dx': round(float(dx_val), 4),
            'adx_score_val': adx_score(float(adx_val), float(plus_di), float(minus_di)),
        })

    return records


# ==================== 全量批量计算 ====================

def calc_adx_batch(period: int = DEFAULT_PERIOD) -> int:
    """
    全量刷新：批量计算全市场ADX评分

    返回:
        int: 总写入条数
    """
    import time
    logger.info(f"[ADX] 全量刷新 (period={period})")

    codes = get_all_stock_codes()
    logger.info(f"共 {len(codes)} 只股票")

    total_records = 0
    start_time = time.time()

    for idx, code in enumerate(codes):
        df = get_daily_data_from_sqlite(code)
        records = _extract_adx_records(code, df, period)
        if records:
            save_adx_score(records)
            total_records += len(records)

        if (idx + 1) % 100 == 0 or idx == len(codes) - 1:
            elapsed = time.time() - start_time
            speed = (idx + 1) / elapsed if elapsed > 0 else 0
            logger.info(f"  进度: {idx + 1}/{len(codes)} 只, "
                        f"已写入 {total_records} 条, "
                        f"速度: {speed:.1f}只/秒")

    elapsed = time.time() - start_time
    logger.info(f"[ADX] 全量完成: {total_records} 条, 耗时 {elapsed:.0f}秒")
    return total_records


# ==================== 近日增量刷新 ====================

def calc_adx_recent(end_date, days=30, period: int = DEFAULT_PERIOD) -> int:
    """
    近日增量刷新：计算最近N个交易日的ADX评分

    ADX需要 2*period-1 天预热期，所以实际加载数据需往前多取预热天数。

    参数:
        end_date: 结束日期 'YYYY-MM-DD'
        days: 要刷新的交易日数（默认30）
        period: ADX计算周期

    返回:
        int: 写入条数
    """
    import time
    from core.storage import save_adx_score, get_trading_day_offset_from

    logger.info(f"[ADX] 近日刷新: 最近{days}天到 {end_date} (period={period})")

    codes = get_all_stock_codes()
    if not codes:
        logger.error("未找到股票代码")
        return 0
    logger.info(f"共 {len(codes)} 只股票")

    warmup = 2 * period - 1  # ADX预热期
    # 加载数据范围：end_date前 days + warmup 天
    data_start = get_trading_day_offset_from(end_date, -(days + warmup))
    if not data_start:
        logger.error(f"无法获取预热起始日")
        return 0

    logger.info(f"预热{warmup}天, 计算{days}天, 数据范围{data_start}~{end_date}")

    # 删除旧数据
    from strategies.trend_trading.score._base import get_trade_dates
    calc_dates = get_trade_dates(data_start, end_date)
    # 实际要保存的日期：跳过预热期
    save_dates = calc_dates[warmup:]

    conn = get_db_connection()
    try:
        for d in save_dates:
            conn.execute("DELETE FROM adx_score WHERE calc_date = ?", (d,))
        conn.commit()
    finally:
        conn.close()

    total_records = 0
    start_time = time.time()

    for idx, code in enumerate(codes):
        df = get_daily_data_range(code, data_start, end_date)
        records = _extract_adx_records(code, df, period)
        if records:
            save_adx_score(records)
            total_records += len(records)

        if (idx + 1) % 100 == 0 or idx == len(codes) - 1:
            elapsed = time.time() - start_time
            speed = (idx + 1) / elapsed if elapsed > 0 else 0
            logger.info(f"  进度: {idx + 1}/{len(codes)} 只, "
                        f"已写入 {total_records} 条, "
                        f"速度: {speed:.1f}只/秒")

    elapsed = time.time() - start_time
    logger.info(f"[ADX] 近日完成: {total_records} 条, 耗时 {elapsed:.0f}秒")
    return total_records


def calc_adx_from_data(stock_data, all_dates, days, period: int = DEFAULT_PERIOD) -> int:
    """
    从预加载数据计算ADX评分（由 calc_scores.py 统一调度调用）

    参数:
        stock_data: {code: DataFrame} 预加载的日K数据
        all_dates: list[str] 指数日期列表（升序）
        days: 计算天数
        period: ADX计算周期

    返回:
        int: 写入条数
    """
    warmup = 2 * period - 1
    if len(all_dates) < warmup + days:
        logger.error(f"[ADX] 日期不足: 需要{warmup + days}天，实际{len(all_dates)}天")
        return 0

    calc_dates = all_dates[-days:]
    save_dates_set = set(calc_dates)

    logger.info(f"[ADX] 从预加载数据计算: {len(stock_data)}只股票, {len(calc_dates)}天, period={period}")

    # 删除旧数据
    conn = get_db_connection()
    try:
        for d in calc_dates:
            conn.execute("DELETE FROM adx_score WHERE calc_date = ?", (d,))
        conn.commit()
    finally:
        conn.close()

    total_records = 0
    start_time = time.time()

    for idx, (code, df) in enumerate(stock_data.items()):
        records = _extract_adx_records(code, df, period)
        if records:
            records = [r for r in records if r['calc_date'] in save_dates_set]
            if records:
                save_adx_score(records)
                total_records += len(records)

        if (idx + 1) % 500 == 0 or idx == len(stock_data) - 1:
            elapsed = time.time() - start_time
            speed = (idx + 1) / elapsed if elapsed > 0 else 0
            logger.info(f"  进度: {idx + 1}/{len(stock_data)} 只, "
                        f"已写入 {total_records} 条, "
                        f"速度: {speed:.1f}只/秒")

    elapsed = time.time() - start_time
    logger.info(f"[ADX] 完成: {total_records} 条, 耗时 {elapsed:.0f}秒")
    return total_records


# ==================== 实时查询 ====================

def get_adx_score(code: str, calc_date: str, period: int = DEFAULT_PERIOD) -> dict | None:
    """
    查询单只股票在某日的ADX评分

    优先从DB缓存查询，miss时实时计算

    返回:
        dict or None: {adx, plus_di, minus_di, dx, adx_score_val}
    """
    # 1. 尝试DB查询
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT adx, plus_di, minus_di, dx, adx_score_val
            FROM adx_score
            WHERE code = ? AND calc_date = ? AND period = ?
        """, (code, calc_date, period)).fetchone()

        if row:
            return {
                'adx': row['adx'],
                'plus_di': row['plus_di'],
                'minus_di': row['minus_di'],
                'dx': row['dx'],
                'adx_score_val': row['adx_score_val'],
            }
    finally:
        conn.close()

    # 2. DB miss，实时计算
    df = get_daily_data_from_sqlite(code)
    if df.empty or len(df) < period * 2:
        return None

    try:
        result_df = calculate_adx(df, period)
    except Exception:
        return None

    mask = df['date'].astype(str).str[:10] == calc_date
    if not mask.any():
        return None

    idx = mask.idxmax()
    adx_val = result_df['adx'].iloc[idx]
    if pd.isna(adx_val):
        return None

    plus_di = result_df['plus_di'].iloc[idx]
    minus_di = result_df['minus_di'].iloc[idx]
    dx_val = result_df['dx'].iloc[idx]

    return {
        'adx': round(float(adx_val), 4),
        'plus_di': round(float(plus_di), 4),
        'minus_di': round(float(minus_di), 4),
        'dx': round(float(dx_val), 4),
        'adx_score_val': adx_score(float(adx_val), float(plus_di), float(minus_di)),
    }
