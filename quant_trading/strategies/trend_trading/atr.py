"""
趋势交易 — ATR相关计算模块
包含：单位头寸计算、止损价、加仓价、退出信号价
"""

import logging

import pandas as pd

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
        int: 股数（100的整数倍），最小100；0=不可开仓（1手超5%仓位）
    """
    if atr <= 0 or price <= 0:
        return 100

    # 趋势公式：1单位 = 账户的1% / (ATR × 每股波动)
    # A股简化：unit_risk = ATR（绝对值波动），每股1%
    unit_risk = atr
    dollar_volatility = unit_risk
    # 1单位头寸 = 总资金的1% / 每股波动
    raw_shares = (capital * 0.01) / dollar_volatility

    # 取整到100股（A股最小买入单位）
    shares = int(raw_shares / 100) * 100
    if shares < 100:
        shares = 100

    # 5%仓位上限
    max_shares_by_cap = int((capital * 0.05) / price / 100) * 100
    if max_shares_by_cap < shares:
        shares = max_shares_by_cap

    # 如果5%上限取整后为0，说明1手金额超过5%，不可开仓
    if shares <= 0:
        logger.info(f'[仓位计算] 跳过: 1手({price * 100:.0f}元) > 5%仓位({capital * 0.05:.0f}元), 资本{capital:,.0f}, 价{price:.2f}')
        return 0

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


def calc_avg_cost_after_add(old_avg_cost, old_shares, add_price, add_shares, fees=0):
    """
    计算加仓后的平均成本
    
    海龟改良：基于平均成本计算止损线，而非加仓价
    
    参数:
        old_avg_cost: 加仓前的平均成本
        old_shares: 加仓前的总股数
        add_price: 加仓价
        add_shares: 加仓股数
        fees: 加仓手续费
    
    返回:
        float: 新的平均成本
    """
    total_cost = old_avg_cost * old_shares + add_price * add_shares + fees
    total_shares = old_shares + add_shares
    return round(total_cost / total_shares, 2)


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
    lookback = df.iloc[-(period + 1):-1]
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
