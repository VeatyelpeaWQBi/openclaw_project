"""
板块数据获取模块（薄封装层）
实际数据获取逻辑在 data_source.py 中
调用者无需关心数据源配置
"""

import logging

from data_source import get_sector_ranking, get_sector_stocks

logger = logging.getLogger(__name__)

__all__ = ['get_sector_ranking', 'get_sector_stocks']

if __name__ == '__main__':
    logger.info("测试板块数据获取...")
    sectors = get_sector_ranking(sector_type=2, limit=10)
    logger.info(f"获取到 {len(sectors)} 个概念板块")
    for s in sectors[:5]:
        logger.info(f"  {s['name']}: {s['change_percent']}%")
