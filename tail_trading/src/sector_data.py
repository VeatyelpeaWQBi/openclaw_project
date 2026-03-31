"""
板块数据获取模块
使用AKShare获取热门板块排名

数据源策略：AKShare内部路由，不指定具体后端
优先东方财富(_em)，失败时自动切换同花顺(_ths)
"""

import akshare as ak
import time
import random
import os

# 禁止代理干扰（AKShare内部管理连接）
for _k in ('HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy'):
    os.environ.pop(_k, None)


def _try_em_then_ths(em_func, ths_func, *args, **kwargs):
    """
    通用路由：先试东方财富(_em)，失败自动切同花顺(_ths)

    参数:
        em_func: 东方财富API函数
        ths_func: 同花顺API函数
        *args, **kwargs: 传给函数的参数

    返回:
        (dataframe, source_name)
    """
    try:
        result = em_func(*args, **kwargs)
        if result is not None and not result.empty:
            return result, 'eastmoney'
    except Exception as e:
        print(f"  ⚠️ 东方财富失败: {e}")

    try:
        result = ths_func(*args, **kwargs)
        if result is not None and not result.empty:
            return result, 'tonghuashun'
    except Exception as e:
        print(f"  ⚠️ 同花顺失败: {e}")

    return None, None


def get_sector_ranking(sector_type=2, limit=20):
    """
    获取板块涨幅排名

    参数:
        sector_type: 1=行业板块, 2=概念板块
        limit: 返回数量

    返回:
        list: 板块列表，每个包含 code, name, change_percent, volume, amount
    """
    try:
        if sector_type == 1:
            em_func = ak.stock_board_industry_name_em
            ths_func = ak.stock_board_industry_name_ths
        else:
            em_func = ak.stock_board_concept_name_em
            ths_func = ak.stock_board_concept_name_ths

        df, source = _try_em_then_ths(em_func, ths_func)

        if df is None or df.empty:
            print("获取板块数据失败: 两个数据源均不可用")
            return []

        print(f"  ✅ 板块数据来源: {source} ({len(df)}个板块)")

        # 标准化列名（东方财富和同花顺列名不同）
        if source == 'eastmoney':
            sort_col = '涨跌幅'
            code_col = '板块代码'
            name_col = '板块名称'
            vol_col = '成交量'
            amt_col = '成交额'
        else:
            # 同花顺只返回 name, code（无涨跌幅列）
            # 需要额外获取涨跌幅数据
            sort_col = None
            code_col = 'code'
            name_col = 'name'
            vol_col = None
            amt_col = None

        if sort_col and sort_col in df.columns:
            # 东方财富：直接按涨跌幅排序
            df = df.sort_values(sort_col, ascending=False).head(limit)
            sectors = []
            for _, row in df.iterrows():
                sectors.append({
                    'code': str(row.get(code_col, '')),
                    'name': str(row.get(name_col, '')),
                    'change_percent': float(row.get(sort_col, 0)),
                    'volume': float(row.get(vol_col, 0) or 0),
                    'amount': float(row.get(amt_col, 0) or 0),
                })
        else:
            # 同花顺：返回板块列表，不带涨跌幅（由main.py自行处理）
            # 只返回前limit个板块名称和代码
            df = df.head(limit)
            sectors = []
            for _, row in df.iterrows():
                sectors.append({
                    'code': str(row.get(code_col, '')),
                    'name': str(row.get(name_col, '')),
                    'change_percent': 0,  # 同花顺基础接口无涨跌幅
                    'volume': 0,
                    'amount': 0,
                })

        time.sleep(random.uniform(0.3, 1.0))
        return sectors

    except Exception as e:
        print(f"获取板块数据失败: {e}")
        return []


def get_sector_stocks(sector_code, sector_type=2, limit=50):
    """
    获取板块内的个股列表

    参数:
        sector_code: 板块名称（如 "人工智能"）
        sector_type: 1=行业, 2=概念
        limit: 返回数量

    返回:
        list: 个股列表
    """
    try:
        if sector_type == 1:
            em_func = ak.stock_board_industry_cons_em
            # 同花顺行业成分股暂无对应函数，仅用东方财富
            df = em_func(symbol=sector_code)
            source = 'eastmoney'
        else:
            em_func = ak.stock_board_concept_cons_em
            # 同花顺概念成分股暂无对应函数，仅用东方财富
            df = em_func(symbol=sector_code)
            source = 'eastmoney'

        if df is None or df.empty:
            print(f"  ⚠️ 获取板块[{sector_code}]成分股失败")
            return []

        stocks = []
        for _, row in df.head(limit).iterrows():
            stocks.append({
                'code': str(row.get('代码', '')),
                'name': str(row.get('名称', '')),
                'price': float(row.get('最新价', 0) or 0),
                'change_pct': float(row.get('涨跌幅', 0) or 0),
                'change_percent': float(row.get('涨跌幅', 0) or 0),
                'volume': float(row.get('成交量', 0) or 0),
                'amount': float(row.get('成交额', 0) or 0),
                'turnover': float(row.get('换手率', 0) or 0),
                'high': float(row.get('最高', 0) or 0),
                'low': float(row.get('最低', 0) or 0),
                'open': float(row.get('今开', 0) or 0),
            })

        time.sleep(random.uniform(0.3, 1.0))
        return stocks

    except Exception as e:
        print(f"获取板块[{sector_code}]个股失败: {e}")
        return []


if __name__ == '__main__':
    print("测试板块数据获取...")
    sectors = get_sector_ranking(sector_type=2, limit=10)
    print(f"获取到 {len(sectors)} 个概念板块")
    for s in sectors[:5]:
        print(f"  {s['name']}: {s['change_percent']}%")
