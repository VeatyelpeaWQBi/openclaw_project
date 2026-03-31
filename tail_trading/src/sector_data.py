"""
板块数据获取模块
使用AKShare获取热门板块排名

注意：AKShare底层也调用东方财富API，可能需要代理配置。
代理配置在 config/local_config.json 中。
"""

import akshare as ak
import time
import random
import os
import json
import requests as _requests

# 读取代理配置
_PROXY = {}
_config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'local_config.json')
if os.path.exists(_config_path):
    try:
        with open(_config_path, 'r', encoding='utf-8') as _f:
            _config = json.load(_f)
            _PROXY = _config.get('proxy', {})
    except Exception:
        pass

# 如果配置了代理，设置环境变量让AKShare的requests库使用代理
if _PROXY:
    os.environ.setdefault('HTTP_PROXY', _PROXY.get('http', ''))
    os.environ.setdefault('HTTPS_PROXY', _PROXY.get('https', ''))


def get_sector_ranking(sector_type=2, limit=20):
    """
    获取板块涨幅排名

    参数:
        sector_type: 1=行业板块, 2=概念板块
        limit: 返回数量

    返回:
        list: 板块列表，每个包含 code, name, change_percent, volume, amount
        注意：AKShare板块排名接口不返回成交量/成交额，volume和amount固定为0
    """
    try:
        if sector_type == 1:
            df = ak.stock_board_industry_name_em()
        else:
            df = ak.stock_board_concept_name_em()

        # 按涨跌幅排序
        df = df.sort_values('涨跌幅', ascending=False).head(limit)

        sectors = []
        for _, row in df.iterrows():
            sectors.append({
                'code': row.get('板块代码', ''),
                'name': row.get('板块名称', ''),
                'change_percent': float(row.get('涨跌幅', 0)),
                'volume': 0,  # AKShare板块排名接口无此字段
                'amount': 0,  # AKShare板块排名接口无此字段
            })

        time.sleep(random.uniform(0.5, 1.5))
        return sectors

    except Exception as e:
        print(f"获取板块数据失败: {e}")
        return []


def get_sector_stocks(sector_code, sector_type=2, limit=50):
    """
    获取板块内的个股列表

    参数:
        sector_code: 板块名称或板块代码（如 "人工智能" 或 "BK0800"）
        sector_type: 1=行业, 2=概念
        limit: 返回数量

    返回:
        list: 个股列表
    """
    try:
        if sector_type == 1:
            df = ak.stock_board_industry_cons_em(symbol=sector_code)
        else:
            df = ak.stock_board_concept_cons_em(symbol=sector_code)

        stocks = []
        for _, row in df.head(limit).iterrows():
            stocks.append({
                'code': str(row.get('代码', '')),
                'name': row.get('名称', ''),
                'price': float(row.get('最新价', 0)),
                'change_pct': float(row.get('涨跌幅', 0)),
                'change_percent': float(row.get('涨跌幅', 0)),
                'volume': float(row.get('成交量', 0)),
                'amount': float(row.get('成交额', 0)),
                'turnover': float(row.get('换手率', 0)),
                'high': float(row.get('最高', 0)),
                'low': float(row.get('最低', 0)),
                'open': float(row.get('今开', 0)),
            })

        time.sleep(random.uniform(0.5, 1.5))
        return stocks

    except Exception as e:
        print(f"获取板块个股失败: {e}")
        return []


if __name__ == '__main__':
    print("测试板块数据获取...")
    sectors = get_sector_ranking(sector_type=2, limit=10)
    print(f"获取到 {len(sectors)} 个概念板块")
    for s in sectors[:5]:
        print(f"  {s['name']}: {s['change_percent']}%")
