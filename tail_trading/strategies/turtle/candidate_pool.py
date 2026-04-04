"""
海龟交易法 — 候选股池构建
包含：核心池（A500）、热点池、自选池的构建与合并
"""

import os
import csv
import sqlite3
import logging
import subprocess
from io import StringIO
from datetime import datetime
from core.storage import get_db_connection

logger = logging.getLogger(__name__)

# mx-xuangu 脚本路径
MX_SCRIPT = os.path.expanduser("~/.npm-global/lib/node_modules/openclaw/skills/mx-xuangu/mx_xuangu.py")
MX_OUTPUT = "/home/drizztbi/.openclaw/workspace/mx_data/output"


def call_mx_xuangu(query):
    """
    调用 mx-xuangu 选股脚本

    参数:
        query: 选股查询语句

    返回:
        list: 解析后的股票列表 [{code, name, ...}]
    """
    env = os.environ.copy()
    env['MX_APIKEY'] = 'mkt_VuyJ8Ys9p6kVYRSXQb7mD58zXZ65AE8wjEfVZHuUIpA'

    try:
        logger.info(f"[mx-xuangu] 查询: {query}")
        result = subprocess.run(
            ['python3', MX_SCRIPT, query],
            capture_output=True, text=True, env=env, timeout=60
        )

        if result.returncode != 0:
            logger.warning(f"[mx-xuangu] 返回码非0: {result.stderr[:200]}")
            return []

        # 解析输出（CSV格式或文本）
        output = result.stdout.strip()
        if not output:
            logger.warning("[mx-xuangu] 输出为空")
            return []

        stocks = []
        # 尝试CSV解析
        try:
            reader = csv.DictReader(StringIO(output))
            for row in reader:
                code = row.get('代码', row.get('code', ''))
                name = row.get('名称', row.get('name', ''))
                if code:
                    stocks.append({
                        'code': str(code).strip(),
                        'name': str(name).strip() if name else '',
                        'row': dict(row),
                    })
        except Exception:
            # 回退：逐行解析
            lines = output.split('\n')
            for line in lines[1:]:  # 跳过表头
                parts = line.split(',')
                if len(parts) >= 2:
                    code = parts[0].strip()
                    name = parts[1].strip()
                    if code and code.isdigit():
                        stocks.append({'code': code, 'name': name})

        logger.info(f"[mx-xuangu] 返回 {len(stocks)} 只股票")
        return stocks

    except subprocess.TimeoutExpired:
        logger.error("[mx-xuangu] 调用超时(60s)")
        return []
    except Exception as e:
        logger.error(f"[mx-xuangu] 调用异常: {type(e).__name__}: {e}")
        return []


class CandidatePool:
    """候选股池管理器"""

    def __init__(self):
        self.core_pool = []
        self.hotspot_pool = []
        self.watchlist_pool = []
        self.merged_pool = []

    def build_core_pool(self):
        """
        从中证A500成分股构建核心池

        返回:
            list: [{code, name}]
        """
        try:
            stocks = call_mx_xuangu("中证A500成分股")
            self.core_pool = [
                {'code': s['code'], 'name': s['name'], 'source': 'a500'}
                for s in stocks if s.get('code')
            ]
            logger.info(f"核心池(A500): {len(self.core_pool)} 只")
        except Exception as e:
            logger.error(f"构建核心池失败: {e}")
            self.core_pool = []

        return self.core_pool

    def build_hotspot_pool(self):
        """
        构建热点池：查最近1月涨幅+成交额TOP题材的成分股

        返回:
            list: [{code, name, theme}]
        """
        try:
            # 查近期热门板块
            hot_sectors = call_mx_xuangu("最近1月涨幅前5的板块")
            logger.info(f"热点板块: {len(hot_sectors)} 个")

            all_stocks = []
            seen_codes = set()

            for sector in hot_sectors[:5]:
                sector_name = sector.get('name', '')
                if not sector_name:
                    continue

                # 查板块成分股
                member_stocks = call_mx_xuangu(f"{sector_name}板块成分股 成交额前5")
                for s in member_stocks[:5]:
                    code = s.get('code', '')
                    if code and code not in seen_codes:
                        seen_codes.add(code)
                        all_stocks.append({
                            'code': code,
                            'name': s.get('name', ''),
                            'source': 'hotspot',
                            'theme': sector_name,
                        })

            self.hotspot_pool = all_stocks
            logger.info(f"热点池: {len(self.hotspot_pool)} 只")

        except Exception as e:
            logger.error(f"构建热点池失败: {e}")
            self.hotspot_pool = []

        return self.hotspot_pool

    def build_watchlist_pool(self):
        """
        从 turtle_watchlist 表构建自选池

        返回:
            list: [{code, name, keyword}]
        """
        try:
            conn = get_db_connection()
            rows = conn.execute(
                "SELECT * FROM turtle_watchlist WHERE active = 1"
            ).fetchall()
            conn.close()

            self.watchlist_pool = []
            for r in rows:
                r_dict = dict(r)
                # 有具体代码的直接加入
                if r_dict.get('code'):
                    self.watchlist_pool.append({
                        'code': r_dict['code'],
                        'name': r_dict.get('name', ''),
                        'source': 'watchlist',
                        'keyword': r_dict.get('keyword', ''),
                    })
                # 有关键词的需要搜索
                elif r_dict.get('keyword'):
                    keyword = r_dict['keyword']
                    results = call_mx_xuangu(f"{keyword}相关股票 成交额前3")
                    for s in results[:3]:
                        self.watchlist_pool.append({
                            'code': s.get('code', ''),
                            'name': s.get('name', ''),
                            'source': 'watchlist',
                            'keyword': keyword,
                        })

            logger.info(f"自选池: {len(self.watchlist_pool)} 只")

        except Exception as e:
            logger.error(f"构建自选池失败: {e}")
            self.watchlist_pool = []

        return self.watchlist_pool

    def merge_pools(self):
        """
        三池合并去重

        返回:
            list: 合并后的候选池
        """
        seen = set()
        merged = []

        for pool in [self.core_pool, self.hotspot_pool, self.watchlist_pool]:
            for stock in pool:
                code = stock.get('code', '')
                if code and code not in seen:
                    seen.add(code)
                    merged.append(stock)

        self.merged_pool = merged
        logger.info(f"合并候选池: {len(merged)} 只（核心{len(self.core_pool)} + 热点{len(self.hotspot_pool)} + 自选{len(self.watchlist_pool)}）")
        return merged

    def get_candidate_list(self):
        """
        获取最终候选池（构建+合并）

        返回:
            list: [{code, name, source, ...}]
        """
        self.build_core_pool()
        self.build_hotspot_pool()
        self.build_watchlist_pool()
        return self.merge_pools()
