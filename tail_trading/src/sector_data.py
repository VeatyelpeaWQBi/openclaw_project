"""
板块数据获取模块
使用东方财富API获取热门板块排名
"""

import requests
import json
import os
from datetime import datetime

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'http://quote.eastmoney.com/'
}

# 加载本地配置（代理/转发服务）
RELAY_BASE = None  # 转发服务地址
PROXIES = {}
_config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'local_config.json')
if os.path.exists(_config_path):
    try:
        with open(_config_path, 'r', encoding='utf-8') as _f:
            _config = json.load(_f)
            PROXIES = _config.get('proxy', {})
            RELAY_BASE = _config.get('relay', None)
    except Exception:
        pass


def _make_request(url, params=None):
    """统一请求方法，支持转发服务"""
    if RELAY_BASE:
        # 通过转发服务请求
        relay_url = f"{RELAY_BASE}/relay?url={url}"
        if params:
            query = '&'.join(f"{k}={v}" for k, v in params.items())
            relay_url = f"{RELAY_BASE}/relay?url={url}?{query}"
        return requests.get(relay_url, headers=HEADERS, timeout=10)
    else:
        # 直接请求
        return requests.get(url, params=params, headers=HEADERS, timeout=10, proxies=PROXIES)

def get_sector_ranking(sector_type=2, limit=20):
    """
    获取板块涨幅排名
    
    参数:
        sector_type: 1=行业板块, 2=概念板块, 3=地域板块
        limit: 返回数量
    
    返回:
        list: 板块列表，每个包含 code, name, change_percent, volume, amount
    """
    fs_map = {
        1: "m:90+t:2",  # 行业板块
        2: "m:90+t:3",  # 概念板块
        3: "m:90+t:4",  # 地域板块
    }
    
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    params = {
        'pn': 1,
        'pz': limit,
        'po': 1,
        'np': 1,
        'fltt': 2,
        'invt': 2,
        'fid': 'f3',  # 按涨跌幅排序
        'fs': fs_map.get(sector_type, "m:90+t:3"),
        'fields': 'f12,f14,f2,f3,f5,f6,f62,f184'
    }
    
    try:
        resp = _make_request(url, params)
        data = resp.json()
        
        if data.get('data') and data['data'].get('diff'):
            sectors = []
            for item in data['data']['diff']:
                sectors.append({
                    'code': item.get('f12', ''),
                    'name': item.get('f14', ''),
                    'change_percent': item.get('f3', 0),
                    'volume': item.get('f5', 0),
                    'amount': item.get('f6', 0)
                })
            return sectors
        return []
    except Exception as e:
        print(f"获取板块数据失败: {e}")
        return []

def get_sector_stocks(sector_code, sector_type=2, limit=50):
    """
    获取板块内的个股列表
    
    参数:
        sector_code: 板块代码（如 BK0899）
        sector_type: 1=行业, 2=概念, 3=地域
        limit: 返回数量
    
    返回:
        list: 个股列表
    """
    # 概念板块使用 b:BKxxxx 格式
    if sector_code.startswith('BK'):
        fs = f"b:{sector_code}"
    else:
        fs_map = {
            1: "m:90+t:2",
            2: "m:90+t:3",
            3: "m:90+t:4",
        }
        fs = f"{fs_map.get(sector_type)}+b:{sector_code}"
    
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    params = {
        'pn': 1,
        'pz': limit,
        'po': 1,
        'np': 1,
        'fltt': 2,
        'invt': 2,
        'fid': 'f3',
        'fs': fs,
        'fields': 'f2,f3,f5,f6,f8,f12,f14,f15,f16,f17,f18,f20'
    }
    
    try:
        resp = _make_request(url, params)
        data = resp.json()
        
        if data.get('data') and data['data'].get('diff'):
            stocks = []
            for item in data['data']['diff']:
                stocks.append({
                    'code': item.get('f12', ''),
                    'name': item.get('f14', ''),
                    'price': item.get('f2', 0),
                    'change_percent': item.get('f3', 0),
                    'volume': item.get('f5', 0),
                    'amount': item.get('f6', 0),
                    'turnover': item.get('f8', 0),
                    'high': item.get('f15', 0),
                    'low': item.get('f16', 0),
                    'open': item.get('f17', 0),
                    'yesterday_close': item.get('f18', 0),
                    'market_cap': item.get('f20', 0)
                })
            return stocks
        return []
    except Exception as e:
        print(f"获取板块个股失败: {e}")
        return []

if __name__ == '__main__':
    print("测试板块数据获取...")
    sectors = get_sector_ranking(sector_type=2, limit=10)
    print(f"获取到 {len(sectors)} 个概念板块")
    for s in sectors[:5]:
        print(f"  {s['name']}: {s['change_percent']}%")
