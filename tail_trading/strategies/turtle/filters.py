"""
海龟交易法 — 过滤器模块
包含：趋势过滤、可交易性检查、流动性检查
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def trend_filter(df, ma_long=350, ma_short=25):
    """
    趋势过滤器（基于均线方向判断大趋势）

    参数:
        df: 日K DataFrame，需包含 close 列
        ma_long: 长期均线周期，默认350日
        ma_short: 短期均线周期，默认25日

    返回:
        str: '多头'/ '空头'/ '不明'
    """
    if df is None or len(df) < ma_long:
        return '不明'

    close = df['close']
    ma_l = close.rolling(window=ma_long).mean()
    ma_s = close.rolling(window=ma_short).mean()

    if ma_l.isna().iloc[-1] or ma_s.isna().iloc[-1]:
        return '不明'

    current_ma_l = ma_l.iloc[-1]
    current_ma_s = ma_s.iloc[-1]
    prev_ma_l = ma_l.iloc[-2] if not ma_l.isna().iloc[-2] else current_ma_l

    # 短期均线在长期均线之上，且长期均线上升 → 多头
    if current_ma_s > current_ma_l and current_ma_l > prev_ma_l:
        logger.debug(f'[趋势过滤] 双均线多头 → 多头')
        return '多头'
    # 短期均线在长期均线之下 → 空头
    elif current_ma_s < current_ma_l:
        logger.debug(f'[趋势过滤] 双均线空头 → 空头')
        return '空头'

    return '不明'


def is_eligible(stock):
    """
    检查股票是否可交易

    排除：ST股、北交所（8开头）、新股（上市不足60日）

    参数:
        stock: dict，需包含 code, name 字段

    返回:
        tuple: (bool, str) — (是否可交易, 排除原因)
    """
    code = stock.get('code', '')
    name = stock.get('name', '')

    # ST股
    if 'ST' in name.upper() or '退' in name:
        logger.debug(f'[标的过滤] 不合格: ') 
        return False, 'ST或退市'

    # 北交所（8开头的6位代码）
    if code.startswith('8') and len(code) == 6:
        logger.debug(f'[标的过滤] 不合格: ') 
        return False, '北交所'

    # 科创板/创业板不限制，但排除异常代码
    if len(code) != 6:
        logger.debug(f'[标的过滤] 不合格: ') 
        return False, '代码异常'

    return True, ''


def is_liquid_enough(float_mv, turnover_amount, volume_ratio):
    """
    流动性检查

    条件：换手额/流通市值 ≥ 0.05% 且 量比 ≥ 0.5

    参数:
        float_mv: 流通市值（元）
        turnover_amount: 当日成交额（元）
        volume_ratio: 量比

    返回:
        bool: 是否满足流动性要求
    """
    if float_mv <= 0:
        return False

    # 换手率（金额维度）
    turnover_rate = turnover_amount / float_mv

    if turnover_rate < 0.0005:  # 0.05%
        return False

    if volume_ratio < 0.5:
        return False

    return True
