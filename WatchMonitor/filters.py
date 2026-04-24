"""
选股过滤模块
实现游牧型T+1选股逻辑
"""

import logging

import numpy as np
import pandas as pd

from core.data_access import get_etf_daily_kline
from core.storage import get_daily_data_from_sqlite
from core.indicator_funcs import is_supertrend_bullish, get_weekly_kline, calculate_volume_ratio

logger = logging.getLogger(__name__)


def filter_etf_candidates(sector_name):
    """
    筛选板块对应的ETF

    参数:
        sector_name: 板块名称

    返回:
        list: ETF列表
    """
    # 板块到ETF的映射
    etf_mapping = {
        '半导体': ['512480', '159995'],
        '芯片': ['512480', '159995'],
        '人工智能': ['515070', '159819'],
        'AI': ['515070', '159819'],
        '券商': ['512000', '159842'],
        '证券': ['512000', '159842'],
        '军工': ['512660', '512810'],
        '光伏': ['515790', '159857'],
        '新能源': ['516160', '159806'],
        '锂电': ['516660', '159840'],
        '医药': ['512010', '159929'],
        '创新药': ['516060', '159858'],
        '机器人': ['562500', '159770'],
    }

    etf_codes = []
    for key, codes in etf_mapping.items():
        if key in sector_name:
            etf_codes.extend(codes)

    if not etf_codes:
        return []

    # 去重
    etf_codes = list(set(etf_codes))

    # 获取ETF数据
    etf_candidates = []
    for code in etf_codes:
        df = get_etf_daily_kline(code)  # 使用ETF专用接口

        if df.empty or len(df) < 30:
            continue

        # 检查SuperTrend（仅日线）
        daily_bullish = is_supertrend_bullish(df)

        if daily_bullish:
            latest = df.iloc[-1]
            etf_candidates.append({
                'code': code,
                'name': f'{sector_name}ETF',
                'sector': sector_name,
                'change_pct': latest['change_pct'],
                'turnover': 0,
                'volume_ratio': round(calculate_volume_ratio(df), 2),
                'price': latest['close'],
                'daily_supertrend': '多头',
                'target_profit': '5-10%',
                'stop_loss': '-3%',
                'risk_level': '较低',
                'is_etf': True
            })

    return etf_candidates
