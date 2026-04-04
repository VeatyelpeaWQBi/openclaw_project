"""
海龟交易法 — 候选股池构建
从 turtle_watchlist 表读取全部候选股（不再调用外部API）
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
        从 turtle_watchlist 表获取全部候选股

        返回:
            list: [{code, name, source, keyword, pool_type}]
        """
        try:
            conn = get_db_connection()
            rows = conn.execute(
                "SELECT code, name, keyword, pool_type FROM turtle_watchlist WHERE active = 1"
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
                        'keyword': r.get('keyword', ''),
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
