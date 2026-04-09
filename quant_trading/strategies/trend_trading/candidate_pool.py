"""
趋势交易 — 候选股池构建
从 watchlist 表读取全部候选股（不再调用外部API）
"""

import logging
from core.storage import get_db_connection

logger = logging.getLogger(__name__)


class CandidatePool:
    """候选股池管理器"""

    def __init__(self):
        self.merged_pool = []

    def get_candidate_list(self):
        """
        从 watchlist 表获取全部候选股

        返回:
            list: [{code, name, source, pool_type, item_type, keyword}]
            - source: 来源标识（pool_type, 如 core_pool/theme_pool/personal_pool）
            - pool_type: 关注池类型
            - item_type: 标的类型（stock/index/sector_index/theme_index）
        """
        try:
            conn = get_db_connection()
            rows = conn.execute(
                "SELECT w.code, w.name, w.keyword, w.type, w.pool_type, a.nickname "
                "FROM watchlist w "
                "LEFT JOIN account a ON w.account_id = a.id "
                "WHERE w.active = 1"
            ).fetchall()
            conn.close()

            pool = []
            for r in rows:
                r = dict(r)
                if r.get('code'):
                    pool.append({
                        'code': r['code'],
                        'name': r.get('name', ''),
                        'source': r.get('pool_type', 'manual'),
                        'pool_type': r.get('pool_type', ''),
                        'item_type': r.get('type', 'stock'),
                        'keyword': r.get('keyword', ''),
                        'account_nickname': r.get('nickname', ''),
                    })

            self.merged_pool = pool
            logger.info(f"候选池: {len(pool)} 只")

            # 按来源统计
            sources = {}
            for s in pool:
                src = s['source']
                sources[src] = sources.get(src, 0) + 1
            logger.info(f"来源分布: {sources}")

        except Exception as e:
            logger.error(f"构建候选池失败: {e}")
            self.merged_pool = []

        return self.merged_pool
