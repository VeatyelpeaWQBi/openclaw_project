"""
海龟交易法 — ATR相关计算模块
包含：单位头寸计算、止损价、加仓价、退出信号价
"""

import pandas as pd
import logging
from core.indicators import calculate_atr

logger = logging.getLogger(__name__)


def calc_unit_size(capital, atr, price):
    """
    计算1单位头寸的股数（取整100股）

    参数:
        capital: 账户总资产
        atr: 当前ATR值
        price: 当前价格

    返回:
        int: 股数（100的整数倍），最小100
    """
    if atr <= 0 or price <= 0:
        return 100

    # 海龟公式：1单位 = 账户的1% / (ATR × 每股波动)
    # A股简化：unit_risk = ATR（绝对值波动），每股1%
    unit_risk = atr
    dollar_volatility = unit_risk
    # 1单位头寸 = 总资金的1% / 每股波动
    raw_shares = (capital * 0.01) / dollar_volatility

    # 取整到100股（A股最小买入单位）
    shares = int(raw_shares / 100) * 100
    if shares < 100:
        shares = 100

    # 额外检查：买入金额不应超过总资金的25%
    max_shares_by_cap = int((capital * 0.25) / price / 100) * 100
    if max_shares_by_cap < shares:
        shares = max(max_shares_by_cap, 100)

    logger.debug(f'[仓位计算] 1单位={shares}股 (资本{capital:,.0f}, ATR{atr:.2f}, 价{price:.2f})')
    return shares


def calc_stop_price(entry_price, atr, multiplier=2.0):
    """
    计算止损价（入场价 - 2×ATR）

    参数:
        entry_price: 入场价
        atr: ATR值
        multiplier: ATR倍数，默认2.0

    返回:
        float: 止损价
    """
    return round(entry_price - multiplier * atr, 2)


def calc_add_price(last_price, atr, threshold=0.5):
    """
    计算加仓触发价（上次买入价 + 0.5×ATR）

    参数:
        last_price: 上次买入价
        atr: ATR值
        threshold: ATR阈值倍数，默认0.5

    返回:
        float: 加仓触发价
    """
    return round(last_price + threshold * atr, 2)


def calc_exit_price(df, period=10):
    """
    计算退出信号价（N日最低价）

    参数:
        df: 日K DataFrame，需包含 low 列
        period: 回看周期，默认10日

    返回:
        float: 退出信号价
    """
    if df is None or len(df) < period:
        return 0.0

    # 排除最后一根K线，取前N日的最低价
    lookback = df.iloc[-(period):]
    return round(float(lookback['low'].min()), 2)


def get_atr_value(df, period=14):
    """
    获取最新ATR值

    参数:
        df: 日K DataFrame
        period: ATR周期

    返回:
        float: 最新ATR值
    """
    if df is None or len(df) < period:
        return 0.0

    atr_series = calculate_atr(df, period)
    return round(float(atr_series.iloc[-1]), 4)
