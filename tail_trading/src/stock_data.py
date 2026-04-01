"""
个股数据获取模块（薄封装层）
实际数据获取逻辑在 data_source.py 中
调用者无需关心数据源配置
"""

import logging

from data_source import get_stock_daily_kline, get_stock_daily_kline_range, get_stock_realtime

logger = logging.getLogger(__name__)

__all__ = ['get_stock_daily_kline', 'get_stock_daily_kline_range', 'get_stock_realtime']

if __name__ == '__main__':
    logger.info("测试个股数据获取...")
    df = get_stock_daily_kline('002409', market='sz', days=30)
    if not df.empty:
        logger.info(f"获取到 {len(df)} 条日K数据")
        logger.debug(f"\n{df.tail()}")
    else:
        logger.warning("未获取到数据")
