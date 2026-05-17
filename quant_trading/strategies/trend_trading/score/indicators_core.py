"""
技术指标全量历史刷新核心模块

支持指标：
  - MA (均线): ma5, ma10, ma20, ma60, ma120, ma250, ma5_slope, ma10_slope, ma20_slope
  - SuperTrend: st_upper_band, st_lower_band, st_direction, st_atr
  - MACD: macd_dif, macd_dea, macd_histogram, macd_histogram_slope, macd_dif_slope, macd_dea_slope, macd_slope_summary
  - RSI: rsi_14
  - 量比: volume_ratio_5, volume_ratio_20
  - OBV: obv, ma_obv
  - K线形态: is_long_upper_shadow, is_long_lower_shadow, is_bullish_candle, is_bearish_candle

用法：
  from strategies.trend_trading.score.indicators_core import calc_indicators_batch

  # 全量刷新
  calc_indicators_batch()

  # 增量刷新
  calc_indicators_recent(end_date, days=30)
"""

import logging
import time
import numpy as np
import pandas as pd
from datetime import datetime

from core.storage import get_db_connection, get_daily_data_from_sqlite, get_daily_data_range

logger = logging.getLogger(__name__)

# ==================== 辅助函数 ====================

def calculate_ema(series, period):
    """计算EMA（指数移动平均）"""
    alpha = 2 / (period + 1)
    return series.ewm(alpha=alpha, adjust=False).mean()


def calculate_atr(df, period=14):
    """计算ATR (Average True Range) - 使用RMA（Wilder平滑）"""
    high = df['high']
    low = df['low']
    close = df['close']

    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(close.shift() - low)

    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = pd.Series(np.nan, index=true_range.index, dtype=float)
    if len(true_range) >= period:
        rma = true_range.iloc[:period].mean()
        atr.iloc[period - 1] = rma
        for i in range(period, len(true_range)):
            rma = (rma * (period - 1) + true_range.iloc[i]) / period
            atr.iloc[i] = rma

    return atr


def calculate_supertrend(df, atr_period=90, multiplier=3.0):
    """
    计算SuperTrend指标

    返回:
        DataFrame: 包含 supertrend(布尔值), upper_band, lower_band, atr
    """
    n = len(df)
    if n == 0:
        return pd.DataFrame({'supertrend': [], 'upper_band': [], 'lower_band': [], 'atr': []})

    high = df['high']
    low = df['low']
    close = df['close']

    atr = calculate_atr(df, atr_period)

    hl2 = (high + low) / 2
    basic_upper = hl2 + (multiplier * atr)
    basic_lower = hl2 - (multiplier * atr)

    supertrend = [True] * n
    final_upper = basic_upper.copy()
    final_lower = basic_lower.copy()

    first_valid = atr_period - 1
    if first_valid >= n:
        return pd.DataFrame({'supertrend': supertrend, 'upper_band': final_upper, 'lower_band': final_lower, 'atr': atr})

    if close.iloc[first_valid] > basic_upper.iloc[first_valid]:
        direction = -1
    elif close.iloc[first_valid] < basic_lower.iloc[first_valid]:
        direction = 1
    else:
        direction = -1

    for i in range(first_valid + 1, n):
        prev_fu = final_upper.iloc[i - 1]
        prev_fl = final_lower.iloc[i - 1]
        prev_close = close.iloc[i - 1]

        if basic_upper.iloc[i] < prev_fu or prev_close > prev_fu:
            pass
        else:
            final_upper.iloc[i] = prev_fu

        if basic_lower.iloc[i] > prev_fl or prev_close < prev_fl:
            pass
        else:
            final_lower.iloc[i] = prev_fl

        if direction == -1:
            if close.iloc[i] < final_lower.iloc[i]:
                direction = 1
        else:
            if close.iloc[i] > final_upper.iloc[i]:
                direction = -1

        supertrend[i] = (direction == -1)

    return pd.DataFrame({
        'supertrend': supertrend,
        'upper_band': final_upper,
        'lower_band': final_lower,
        'atr': atr
    })


def calculate_macd(df, fast=12, slow=26, signal=9):
    """
    计算MACD指标

    返回:
        dict: {'dif_series', 'dea_series', 'histogram_series'}
    """
    close = df['close']
    ema_fast = calculate_ema(close, fast)
    ema_slow = calculate_ema(close, slow)

    dif = ema_fast - ema_slow
    dea = calculate_ema(dif, signal)
    histogram = (dif - dea) * 2

    return {
        'dif_series': dif,
        'dea_series': dea,
        'histogram_series': histogram
    }


def calculate_rsi_series(df, period=14):
    """
    计算RSI序列

    返回:
        pandas.Series: RSI序列
    """
    if df.empty or 'close' not in df.columns:
        return None

    close = df['close']
    delta = close.diff()

    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)

    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


def calculate_volume_ratio(df, days=5):
    """
    计算量比序列（当日成交量 / 近N日平均成交量）
    """
    if len(df) < days + 1:
        return pd.Series([1.0] * len(df), index=df.index)

    volume = df['volume']
    ratio = pd.Series([1.0] * len(df), index=df.index)

    for i in range(days, len(df)):
        today_volume = volume.iloc[i]
        avg_volume = volume.iloc[i-days:i].mean()
        if pd.notna(today_volume) and pd.notna(avg_volume) and avg_volume > 0:
            ratio.iloc[i] = round(today_volume / avg_volume, 2)

    return ratio


def calculate_obv(df):
    """
    计算OBV能量潮序列

    返回:
        pd.Series: OBV序列
    """
    if df.empty:
        return pd.Series([], dtype=float)

    close = df['close'].values
    volume = df['volume'].values

    obv_values = [0]
    for i in range(1, len(close)):
        prev_obv = obv_values[-1]
        if close[i] > close[i-1]:
            obv_values.append(prev_obv + volume[i])
        elif close[i] < close[i-1]:
            obv_values.append(prev_obv - volume[i])
        else:
            obv_values.append(prev_obv)

    return pd.Series(obv_values, index=df.index)


def calculate_ma_series(df, periods=[5, 10, 20, 60, 120, 250]):
    """
    计算多周期均线序列

    返回:
        DataFrame: 各周期均线序列
    """
    if df.empty or 'close' not in df.columns:
        return pd.DataFrame()

    result = pd.DataFrame(index=df.index)
    for period in periods:
        result[f'ma{period}'] = df['close'].rolling(window=period).mean()

    return result


def calculate_ma_slope(df, periods=[5, 10, 20]):
    """
    计算均线斜率方向序列

    返回:
        dict: {f'ma{period}_slope': Series}
    """
    if df.empty or len(df) < 2:
        return {}

    ma_series = calculate_ma_series(df, periods)
    result = {}

    for period in periods:
        col = f'ma{period}'
        if col in ma_series.columns and len(ma_series) >= 2:
            slope = pd.Series([0] * len(ma_series), index=ma_series.index)

            for i in range(1, len(ma_series)):
                today = ma_series[col].iloc[i]
                yesterday = ma_series[col].iloc[i-1]

                if pd.notna(today) and pd.notna(yesterday):
                    diff = today - yesterday
                    threshold = today * 0.001  # 0.1%变化视为走平
                    if diff > threshold:
                        slope.iloc[i] = 1  # 向上
                    elif diff < -threshold:
                        slope.iloc[i] = -1  # 向下
                    else:
                        slope.iloc[i] = 0  # 走平

            result[f'ma{period}_slope'] = slope

    return result


def identify_candle_patterns_series(df):
    """
    识别K线形态序列

    返回:
        dict: {pattern_name: Series}
    """
    if df.empty:
        return {}

    n = len(df)
    is_bullish_candle = pd.Series([0] * n, index=df.index)
    is_bearish_candle = pd.Series([0] * n, index=df.index)
    is_long_upper_shadow = pd.Series([0] * n, index=df.index)
    is_long_lower_shadow = pd.Series([0] * n, index=df.index)

    for i in range(n):
        open_price = df['open'].iloc[i]
        high_price = df['high'].iloc[i]
        low_price = df['low'].iloc[i]
        close_price = df['close'].iloc[i]

        body = abs(close_price - open_price)
        upper_shadow = high_price - max(open_price, close_price)
        lower_shadow = min(open_price, close_price) - low_price

        is_bullish_candle.iloc[i] = 1 if close_price > open_price else 0
        is_bearish_candle.iloc[i] = 1 if close_price < open_price else 0

        # 长上影线：上影线 > 实体×2
        is_long_upper_shadow.iloc[i] = 1 if body > 0 and upper_shadow > body * 2 else 0
        # 长下影线：下影线 > 实体×2
        is_long_lower_shadow.iloc[i] = 1 if body > 0 and lower_shadow > body * 2 else 0

    return {
        'is_bullish_candle': is_bullish_candle,
        'is_bearish_candle': is_bearish_candle,
        'is_long_upper_shadow': is_long_upper_shadow,
        'is_long_lower_shadow': is_long_lower_shadow,
    }


def calculate_macd_slope(df, threshold=0.01):
    """
    计算MACD三线斜率序列

    返回:
        dict: {histogram_slope: Series, dif_slope: Series, dea_slope: Series, slope_summary: dict}
    """
    if df.empty or len(df) < 2:
        return {
            'histogram_slope': pd.Series([0] * len(df), index=df.index),
            'dif_slope': pd.Series([0] * len(df), index=df.index),
            'dea_slope': pd.Series([0] * len(df), index=df.index),
        }

    macd_data = calculate_macd(df)
    dif_series = macd_data.get('dif_series')
    dea_series = macd_data.get('dea_series')
    histogram_series = macd_data.get('histogram_series')

    if dif_series is None or len(dif_series) < 2:
        return {
            'histogram_slope': pd.Series([0] * len(df), index=df.index),
            'dif_slope': pd.Series([0] * len(df), index=df.index),
            'dea_slope': pd.Series([0] * len(df), index=df.index),
        }

    n = len(dif_series)
    hist_slope = pd.Series([0] * n, index=dif_series.index)
    dif_slope = pd.Series([0] * n, index=dif_series.index)
    dea_slope = pd.Series([0] * n, index=dif_series.index)

    for i in range(1, n):
        # MACD柱状图斜率
        today_hist = histogram_series.iloc[i]
        yesterday_hist = histogram_series.iloc[i-1]
        hist_change = today_hist - yesterday_hist
        hist_slope.iloc[i] = 1 if hist_change > threshold else (-1 if hist_change < -threshold else 0)

        # DIF线斜率
        today_dif = dif_series.iloc[i]
        yesterday_dif = dif_series.iloc[i-1]
        dif_change = today_dif - yesterday_dif
        dif_slope.iloc[i] = 1 if dif_change > threshold else (-1 if dif_change < -threshold else 0)

        # DEA线斜率
        if dea_series is not None:
            today_dea = dea_series.iloc[i]
            yesterday_dea = dea_series.iloc[i-1]
            dea_change = today_dea - yesterday_dea
            dea_slope.iloc[i] = 1 if dea_change > threshold else (-1 if dea_change < -threshold else 0)

    return {
        'histogram_slope': hist_slope,
        'dif_slope': dif_slope,
        'dea_slope': dea_slope,
    }


def _get_macd_slope_summary(hist_slope, dif_slope, dea_slope):
    """根据三线斜率综合判断MACD趋势状态"""
    if hist_slope == 1 and dif_slope == 1 and dea_slope == 1:
        return '🚀向上加速'
    elif hist_slope == 1 and dif_slope == 1 and dea_slope == 0:
        return '🚀向上延续'
    elif hist_slope == 1 and dif_slope == 0 and dea_slope == 0:
        return '🚀整理蓄势'
    elif hist_slope == 0 and dif_slope == 1 and dea_slope == 1:
        return '→走平中'
    elif hist_slope == 0 and dif_slope == 0 and dea_slope == 0:
        return '→无方向'
    elif hist_slope == -1 and dif_slope == -1 and dea_slope == -1:
        return '🪂向下加速'
    elif hist_slope == -1 and dif_slope == -1 and dea_slope == 0:
        return '🪂向下延续'
    elif hist_slope == -1 and dif_slope == 0 and dea_slope == 0:
        return '🪂下跌趋缓'
    elif hist_slope == 1 and dif_slope == 0 and dea_slope == -1:
        return '→震荡'
    elif hist_slope == -1 and dif_slope == 1 and dea_slope == 1:
        return '→反转中'
    else:
        return '→震荡'


# ==================== 单只股票计算 ====================

def _extract_indicator_records(code, df):
    """
    从日K数据中提取所有技术指标记录

    返回:
        list[dict]: 技术指标记录列表
    """
    if df.empty or len(df) < 250:
        return []

    required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
    for col in required_cols:
        if col not in df.columns:
            logger.debug(f"[{code}] 缺少列 {col}，跳过")
            return []

    try:
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        records = []

        # MA序列
        ma_series = calculate_ma_series(df, [5, 10, 20, 60, 120, 250])

        # MA斜率
        ma_slopes = calculate_ma_slope(df, [5, 10, 20])

        # SuperTrend
        st = calculate_supertrend(df, atr_period=90, multiplier=3.0)

        # MACD
        macd = calculate_macd(df, fast=12, slow=26, signal=9)
        macd_slopes = calculate_macd_slope(df)

        # RSI
        rsi_series = calculate_rsi_series(df, period=14)

        # 量比
        vol_ratio_5 = calculate_volume_ratio(df, days=5)
        vol_ratio_20 = calculate_volume_ratio(df, days=20)

        # OBV
        obv_series = calculate_obv(df)
        ma_obv_series = obv_series.rolling(30).mean()

        # K线形态
        candle_patterns = identify_candle_patterns_series(df)

        # 遍历每个交易日生成记录
        for i in range(len(df)):
            date_str = str(df['date'].iloc[i])[:10]

            # 只有当有足够的历史数据时才生成记录
            # MA250需要至少250天，但可以从第250天开始记录
            if i < 250:
                continue

            record = {
                'code': code,
                'calc_date': date_str,
                'created_at': now_str,
            }

            # MA
            for period in [5, 10, 20, 60, 120, 250]:
                ma_key = f'ma{period}'
                if ma_key in ma_series.columns:
                    record[ma_key] = float(ma_series[ma_key].iloc[i]) if pd.notna(ma_series[ma_key].iloc[i]) else None

            # MA斜率
            for period in [5, 10, 20]:
                slope_key = f'ma{period}_slope'
                if slope_key in ma_slopes:
                    record[slope_key] = int(ma_slopes[slope_key].iloc[i]) if pd.notna(ma_slopes[slope_key].iloc[i]) else None

            # SuperTrend
            if len(st) > i:
                record['st_upper_band'] = float(st['upper_band'].iloc[i]) if pd.notna(st['upper_band'].iloc[i]) else None
                record['st_lower_band'] = float(st['lower_band'].iloc[i]) if pd.notna(st['lower_band'].iloc[i]) else None
                record['st_direction'] = 1 if st['supertrend'].iloc[i] else -1
                record['st_atr'] = float(st['atr'].iloc[i]) if pd.notna(st['atr'].iloc[i]) else None

            # MACD
            record['macd_dif'] = float(macd['dif_series'].iloc[i]) if pd.notna(macd['dif_series'].iloc[i]) else None
            record['macd_dea'] = float(macd['dea_series'].iloc[i]) if pd.notna(macd['dea_series'].iloc[i]) else None
            record['macd_histogram'] = float(macd['histogram_series'].iloc[i]) if pd.notna(macd['histogram_series'].iloc[i]) else None

            # MACD斜率
            record['macd_histogram_slope'] = int(macd_slopes['histogram_slope'].iloc[i]) if pd.notna(macd_slopes['histogram_slope'].iloc[i]) else 0
            record['macd_dif_slope'] = int(macd_slopes['dif_slope'].iloc[i]) if pd.notna(macd_slopes['dif_slope'].iloc[i]) else 0
            record['macd_dea_slope'] = int(macd_slopes['dea_slope'].iloc[i]) if pd.notna(macd_slopes['dea_slope'].iloc[i]) else 0

            # MACD综合判断
            hist_slope = record['macd_histogram_slope']
            dif_slope = record['macd_dif_slope']
            dea_slope = record['macd_dea_slope']
            record['macd_slope_summary'] = _get_macd_slope_summary(hist_slope, dif_slope, dea_slope)

            # RSI
            if rsi_series is not None and i < len(rsi_series):
                record['rsi_14'] = float(rsi_series.iloc[i]) if pd.notna(rsi_series.iloc[i]) else None

            # 量比
            record['volume_ratio_5'] = float(vol_ratio_5.iloc[i]) if i < len(vol_ratio_5) and pd.notna(vol_ratio_5.iloc[i]) else None
            record['volume_ratio_20'] = float(vol_ratio_20.iloc[i]) if i < len(vol_ratio_20) and pd.notna(vol_ratio_20.iloc[i]) else None

            # OBV
            if i < len(obv_series):
                record['obv'] = int(obv_series.iloc[i]) if pd.notna(obv_series.iloc[i]) else None
                record['ma_obv'] = float(ma_obv_series.iloc[i]) if i >= 30 and pd.notna(ma_obv_series.iloc[i]) else None

            # K线形态
            record['is_bullish_candle'] = int(candle_patterns['is_bullish_candle'].iloc[i])
            record['is_bearish_candle'] = int(candle_patterns['is_bearish_candle'].iloc[i])
            record['is_long_upper_shadow'] = int(candle_patterns['is_long_upper_shadow'].iloc[i])
            record['is_long_lower_shadow'] = int(candle_patterns['is_long_lower_shadow'].iloc[i])

            records.append(record)

        return records

    except Exception as e:
        logger.debug(f"[{code}] 技术指标计算异常: {e}")
        return []


def _save_indicators_batch(records):
    """批量保存技术指标记录"""
    if not records:
        return 0

    conn = get_db_connection()
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        rows = []

        for r in records:
            rows.append((
                r['code'],
                r['calc_date'],
                r.get('ma5'), r.get('ma10'), r.get('ma20'), r.get('ma60'), r.get('ma120'), r.get('ma250'),
                r.get('ma5_slope'), r.get('ma10_slope'), r.get('ma20_slope'),
                r.get('st_upper_band'), r.get('st_lower_band'), r.get('st_direction'), r.get('st_atr'),
                r.get('macd_dif'), r.get('macd_dea'), r.get('macd_histogram'),
                r.get('macd_histogram_slope', 0), r.get('macd_dif_slope', 0), r.get('macd_dea_slope', 0),
                r.get('macd_slope_summary', '→震荡'),
                r.get('rsi_14'),
                r.get('volume_ratio_5'), r.get('volume_ratio_20'),
                r.get('obv'), r.get('ma_obv'),
                r.get('is_long_upper_shadow', 0), r.get('is_long_lower_shadow', 0),
                r.get('is_bullish_candle', 0), r.get('is_bearish_candle', 0),
                r.get('created_at', now),
            ))

        conn.executemany("""
            INSERT OR REPLACE INTO technical_indicators
            (code, calc_date, ma5, ma10, ma20, ma60, ma120, ma250,
             ma5_slope, ma10_slope, ma20_slope,
             st_upper_band, st_lower_band, st_direction, st_atr,
             macd_dif, macd_dea, macd_histogram,
             macd_histogram_slope, macd_dif_slope, macd_dea_slope, macd_slope_summary,
             rsi_14, volume_ratio_5, volume_ratio_20, obv, ma_obv,
             is_long_upper_shadow, is_long_lower_shadow,
             is_bullish_candle, is_bearish_candle,
             created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        return len(rows)
    except Exception as e:
        logger.error(f"批量写入技术指标失败: {e}")
        return 0
    finally:
        conn.close()


# ==================== 全量批量计算 ====================

def calc_indicators_batch():
    """
    全量刷新：批量计算全市场技术指标

    返回:
        int: 总写入条数
    """
    from strategies.trend_trading.score._base import get_all_stock_codes

    logger.info("[技术指标] 全量刷新开始")

    codes = get_all_stock_codes()
    logger.info(f"共 {len(codes)} 只股票")

    total_records = 0
    start_time = time.time()

    for idx, code in enumerate(codes):
        df = get_daily_data_from_sqlite(code)
        records = _extract_indicator_records(code, df)
        if records:
            _save_indicators_batch(records)
            total_records += len(records)

        if (idx + 1) % 100 == 0 or idx == len(codes) - 1:
            elapsed = time.time() - start_time
            speed = (idx + 1) / elapsed if elapsed > 0 else 0
            logger.info(f"  进度: {idx + 1}/{len(codes)} 只, "
                        f"已写入 {total_records} 条, "
                        f"速度: {speed:.1f}只/秒")

    elapsed = time.time() - start_time
    logger.info(f"[技术指标] 全量完成: {total_records} 条, 耗时 {elapsed:.0f}秒")
    return total_records


# ==================== 近日增量刷新 ====================

def calc_indicators_recent(end_date, days=30):
    """
    近日增量刷新：计算最近N个交易日的技术指标

    参数:
        end_date: 结束日期 'YYYY-MM-DD'
        days: 要刷新的交易日数（默认30）

    返回:
        int: 写入条数
    """
    from strategies.trend_trading.score._base import get_all_stock_codes, get_trade_dates

    logger.info(f"[技术指标] 近日刷新: 最近{days}天到 {end_date}")

    codes = get_all_stock_codes()
    if not codes:
        logger.error("未找到股票代码")
        return 0

    # 需要预热数据：MA250需要250天，SuperTrend需要90天，MACD需要35天左右
    warmup = 260
    # 加载数据范围：end_date前 days + warmup 天
    from core.storage import get_trading_day_offset_from
    data_start = get_trading_day_offset_from(end_date, -(days + warmup))
    if not data_start:
        logger.error(f"无法获取预热起始日")
        return 0

    logger.info(f"预热{warmup}天, 计算{days}天, 数据范围{data_start}~{end_date}")

    # 删除旧数据
    calc_dates = get_trade_dates(data_start, end_date)
    # 实际要保存的日期：跳过预热期
    save_dates = calc_dates[warmup:]

    conn = get_db_connection()
    try:
        for d in save_dates:
            conn.execute("DELETE FROM technical_indicators WHERE calc_date = ?", (d,))
        conn.commit()
    finally:
        conn.close()

    total_records = 0
    start_time = time.time()

    for idx, code in enumerate(codes):
        df = get_daily_data_range(code, data_start, end_date)
        records = _extract_indicator_records(code, df)
        if records:
            # 只保留需要保存的日期
            records = [r for r in records if r['calc_date'] in save_dates]
            if records:
                _save_indicators_batch(records)
                total_records += len(records)

        if (idx + 1) % 100 == 0 or idx == len(codes) - 1:
            elapsed = time.time() - start_time
            speed = (idx + 1) / elapsed if elapsed > 0 else 0
            logger.info(f"  进度: {idx + 1}/{len(codes)} 只, "
                        f"已写入 {total_records} 条, "
                        f"速度: {speed:.1f}只/秒")

    elapsed = time.time() - start_time
    logger.info(f"[技术指标] 近日完成: {total_records} 条, 耗时 {elapsed:.0f}秒")
    return total_records