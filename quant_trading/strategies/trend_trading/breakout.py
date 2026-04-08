"""
趋势交易 — 突破检测模块
包含：唐奇安通道突破检测、入场/退出信号
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def detect_donchian_high(df, period):
    """
    计算N日唐奇安通道上轨（N日最高价）

    参数:
        df: 日K DataFrame，需包含 high 列
        period: 回看周期

    返回:
        float: N日最高价
    """
    if df is None or len(df) < period:
        return 0.0

    # 取最近period根K线的最高价（排除当前K线）
    lookback = df.iloc[-(period + 1):-1]
    if lookback.empty:
        lookback = df.iloc[-period:]

    return round(float(lookback['high'].max()), 2)


def detect_donchian_low(df, period):
    """
    计算N日唐奇安通道下轨（N日最低价）

    参数:
        df: 日K DataFrame，需包含 low 列
        period: 回看周期

    返回:
        float: N日最低价
    """
    if df is None or len(df) < period:
        return 0.0

    # 取最近period根K线的最低价（排除当前K线）
    lookback = df.iloc[-(period + 1):-1]
    if lookback.empty:
        lookback = df.iloc[-period:]

    return round(float(lookback['low'].min()), 2)


def detect_breakout(df, period=20):
    """
    检测突破信号（当日收盘价突破N日最高/最低）

    参数:
        df: 日K DataFrame，需包含 high, low, close 列
        period: 唐奇安通道周期

    返回:
        str: 'up'（向上突破）/ 'down'（向下突破）/ None（无突破）
    """
    if df is None or len(df) < period + 1:
        return None

    close_now = float(df['close'].iloc[-1])
    high_prev = detect_donchian_high(df, period)
    low_prev = detect_donchian_low(df, period)

    if close_now > high_prev:
        logger.info(f'[突破检测] 向上突破, 现价{close:.2f} > 高点{high:.2f}')
        return 'up'
    elif close_now < low_prev:
        logger.info(f'[突破检测] 向下突破, 现价{close:.2f} < 低点{low:.2f}')
        return 'down'

    return None


def check_entry_signal(df, short=20, long=55, s1_filtered=1):
    """
    检查入场突破信号

    短期（20日）或长期（55日）突破均可入场
    S1过滤激活时跳过短期突破，只检查长期突破

    参数:
        df: 日K DataFrame
        short: 短期突破周期，默认20
        long: 长期突破周期，默认55
        s1_filtered: S1过滤是否激活（True则跳过20日突破）

    返回:
        dict: {
            'signal': bool,         # 是否有入场信号
            'type': str,            # '55日突破' / '20日突破' / None
            'break_price': float,   # 突破价格
            'channel_high': float,  # 通道上轨
        }
    """
    result = {
        'signal': False,
        'type': None,
        'break_price': 0.0,
        'channel_high': 0.0,
    }

    if df is None or len(df) < long + 1:
        return result

    close_now = float(df['close'].iloc[-1])

    # 先检查长期突破（优先级更高）
    long_high = detect_donchian_high(df, long)
    if close_now > long_high and long_high > 0:
        result['signal'] = True
        result['type'] = '55日突破'
        result['break_price'] = close_now
        result['channel_high'] = long_high
        return result

    # 再检查短期突破
    # S1过滤：上次S1盈利则跳过，等待55日突破
    if s1_filtered == 0:
        return result

    short_high = detect_donchian_high(df, short)
    if close_now > short_high and short_high > 0:
        result['signal'] = True
        result['type'] = '20日突破'
        result['break_price'] = close_now
        result['channel_high'] = short_high
        return result

    return result


def check_exit_signal(df, exit_point=10):
    """
    检查退出信号（反向突破）

    退出条件：收盘价跌破N日唐奇安下轨

    参数:
        df: 日K DataFrame
        exit_point: 退出周期，10=S1退出，20=S2退出

    返回:
        dict: {
            'signal': bool,
            'type': str,
            'exit_price': float,
            'channel_low': float,
        }
    """
    result = {
        'signal': False,
        'type': None,
        'exit_price': 0.0,
        'channel_low': 0.0,
    }

    if df is None or len(df) < exit_point + 1:
        return result

    close_now = float(df['close'].iloc[-1])
    channel_low = detect_donchian_low(df, exit_point)

    if close_now < channel_low and channel_low > 0:
        result['signal'] = True
        result['type'] = f'{exit_point}日退出'
        result['exit_price'] = close_now
        result['channel_low'] = channel_low

    return result
