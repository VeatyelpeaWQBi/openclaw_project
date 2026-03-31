"""
统一数据源路由器
对所有在线A股数据获取进行统一包装，支持多数据源轮询和自动降级

数据源优先级：
1. AKShare 东方财富 (_em)
2. AKShare 同花顺 (_ths)
3. adata 同花顺 (ths)
4. adata 东方财富 (east)
5. adata 新浪 (sina)
6. adata 腾讯 (qq)
7. adata 百度 (baidu)
"""

import akshare as ak
import adata
import pandas as pd
import time
import random
import os
import traceback

# 禁止代理干扰
for _k in ('HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy'):
    os.environ.pop(_k, None)


# ==================== 核心路由引擎 ====================

def _try_sources(sources, *args, **kwargs):
    """
    通用多数据源轮询路由

    参数:
        sources: list of (source_name, func) 按优先级排列
        *args, **kwargs: 传给每个数据源函数的参数

    返回:
        (result, source_name) 或 (None, None)
    """
    for name, func in sources:
        try:
            result = func(*args, **kwargs)
            if result is not None:
                if hasattr(result, 'empty') and result.empty:
                    continue
                if isinstance(result, list) and len(result) == 0:
                    continue
                return result, name
        except Exception as e:
            print(f"  ⚠️ [{name}] 失败: {e}")
            continue
    return None, None


def _safe_float(val, default=0.0):
    """安全转换为float，处理字符串和None"""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# ==================== 1. 板块排名 ====================

def _ak_em_sector_ranking(sector_type):
    """AKShare 东方财富板块排名"""
    if sector_type == 1:
        df = ak.stock_board_industry_name_em()
    else:
        df = ak.stock_board_concept_name_em()
    # 标准化
    df = df.rename(columns={'涨跌幅': 'change_pct', '板块代码': 'code', '板块名称': 'name',
                             '成交量': 'volume', '成交额': 'amount'})
    return df


def _ak_ths_sector_ranking(sector_type):
    """AKShare 同花顺板块列表（无涨跌幅）"""
    if sector_type == 1:
        df = ak.stock_board_industry_name_ths()
    else:
        df = ak.stock_board_concept_name_ths()
    return df


def _adata_ths_sector_ranking(sector_type):
    """adata 同花顺概念板块列表"""
    if sector_type == 2:
        df = adata.stock.info.all_concept_code_ths()
        return df.rename(columns={'index_code': 'code', 'name': 'name'})
    # adata 行业板块暂无
    return None


def get_sector_ranking(sector_type=2, limit=20):
    """
    获取板块涨幅排名（多数据源）

    参数:
        sector_type: 1=行业板块, 2=概念板块
        limit: 返回数量

    返回:
        list: [{code, name, change_percent, volume, amount}]
    """
    sources = [
        ('AKShare-EM', lambda st=sector_type: _ak_em_sector_ranking(st)),
        ('AKShare-THS', lambda st=sector_type: _ak_ths_sector_ranking(st)),
    ]
    if sector_type == 2:
        sources.append(('adata-THS', lambda st=sector_type: _adata_ths_sector_ranking(st)))

    df, source = _try_sources(sources)

    if df is None or (hasattr(df, 'empty') and df.empty):
        print("获取板块数据失败: 所有数据源均不可用")
        return []

    print(f"  ✅ 板块数据来源: {source} ({len(df)}个板块)")

    sectors = []
    if source == 'AKShare-EM':
        df = df.sort_values('change_pct', ascending=False).head(limit)
        for _, row in df.iterrows():
            sectors.append({
                'code': str(row.get('code', '')),
                'name': str(row.get('name', '')),
                'change_percent': _safe_float(row.get('change_pct')),
                'volume': _safe_float(row.get('volume')),
                'amount': _safe_float(row.get('amount')),
            })
    else:
        # THS/adata 无涨跌幅，返回列表供后续使用
        df = df.head(limit)
        for _, row in df.iterrows():
            sectors.append({
                'code': str(row.get('code', row.get('index_code', ''))),
                'name': str(row.get('name', '')),
                'change_percent': 0,
                'volume': 0,
                'amount': 0,
            })

    time.sleep(random.uniform(0.3, 1.0))
    return sectors


# ==================== 2. 板块成分股 ====================

def _ak_em_sector_stocks_concept(sector_code):
    """AKShare 东方财富概念板块成分股"""
    df = ak.stock_board_concept_cons_em(symbol=sector_code)
    return _normalize_stock_list(df)


def _ak_em_sector_stocks_industry(sector_code):
    """AKShare 东方财富行业板块成分股"""
    df = ak.stock_board_industry_cons_em(symbol=sector_code)
    return _normalize_stock_list(df)


def _adata_ths_sector_stocks(sector_code, index_code=None):
    """adata 同花顺概念板块成分股"""
    df = adata.stock.info.concept_constituent_ths(index_code=index_code, name=sector_code)
    if df is None or df.empty:
        return []
    stocks = []
    for _, row in df.iterrows():
        stocks.append({
            'code': str(row.get('stock_code', '')),
            'name': str(row.get('short_name', '')),
            'price': 0,
            'change_pct': 0,
            'change_percent': 0,
            'volume': 0,
            'amount': 0,
            'turnover': 0,
            'high': 0,
            'low': 0,
            'open': 0,
        })
    return stocks


def _normalize_stock_list(df):
    """标准化东方财富成分股DataFrame为统一格式"""
    if df is None or df.empty:
        return []
    stocks = []
    for _, row in df.iterrows():
        stocks.append({
            'code': str(row.get('代码', '')),
            'name': str(row.get('名称', '')),
            'price': _safe_float(row.get('最新价')),
            'change_pct': _safe_float(row.get('涨跌幅')),
            'change_percent': _safe_float(row.get('涨跌幅')),
            'volume': _safe_float(row.get('成交量')),
            'amount': _safe_float(row.get('成交额')),
            'turnover': _safe_float(row.get('换手率')),
            'high': _safe_float(row.get('最高')),
            'low': _safe_float(row.get('最低')),
            'open': _safe_float(row.get('今开')),
        })
    return stocks


def get_sector_stocks(sector_code, sector_type=2, limit=50, index_code=None):
    """
    获取板块内的个股列表（多数据源）

    参数:
        sector_code: 板块名称（如 "人工智能"）
        sector_type: 1=行业, 2=概念
        limit: 返回数量
        index_code: 同花顺板块代码（可选，用于adata降级）

    返回:
        list: [{code, name, price, change_pct, ...}]
    """
    if sector_type == 1:
        sources = [
            ('AKShare-EM', lambda: _ak_em_sector_stocks_industry(sector_code)),
        ]
    else:
        sources = [
            ('AKShare-EM', lambda: _ak_em_sector_stocks_concept(sector_code)),
            ('adata-THS', lambda: _adata_ths_sector_stocks(sector_code, index_code=index_code)),
        ]

    stocks, source = _try_sources(sources)

    if stocks is None or not stocks:
        print(f"  ⚠️ 获取板块[{sector_code}]成分股失败: 所有数据源均不可用")
        return []

    print(f"  ✅ 板块[{sector_code}]成分股来源: {source} ({len(stocks)}只)")
    time.sleep(random.uniform(0.3, 1.0))
    return stocks[:limit]


# ==================== 3. 个股日K线 ====================

def _ak_hist(stock_code, start_date, end_date):
    """AKShare 东方财富个股日K"""
    df = ak.stock_zh_a_hist(
        symbol=stock_code, period="daily",
        start_date=start_date, end_date=end_date, adjust="qfq"
    )
    return _normalize_kline_akshare(df)


def _ak_tx_hist(stock_code, start_date, end_date):
    """AKShare 腾讯数据源个股日K（Tencent）"""
    # 腾讯数据源需要市场前缀
    if stock_code.startswith('6'):
        symbol = f'sh{stock_code}'
    else:
        symbol = f'sz{stock_code}'
    df = ak.stock_zh_a_hist_tx(symbol=symbol, start_date=start_date, end_date=end_date, adjust='qfq')
    return _normalize_kline_tx(df)


def _adata_sina_hist(stock_code, start_date, end_date):
    """adata 新浪个股日K"""
    df = adata.stock.market.sina_market.get_market(
        stock_code=stock_code, start_date=start_date, end_date=end_date
    )
    return _normalize_kline_adata(df)


def _adata_qq_hist(stock_code, start_date, end_date):
    """adata 腾讯个股日K"""
    df = adata.stock.market.qq_market.get_market(
        stock_code=stock_code, start_date=start_date, end_date=end_date
    )
    return _normalize_kline_adata(df)


def _adata_baidu_hist(stock_code, start_date, end_date):
    """adata 百度个股日K"""
    df = adata.stock.market.baidu_market.get_market(
        stock_code=stock_code, start_date=start_date, end_date=end_date
    )
    return _normalize_kline_adata(df)


def _normalize_kline_akshare(df):
    """标准化AKShare日K DataFrame"""
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns={
        '日期': 'date', '开盘': 'open', '收盘': 'close',
        '最高': 'high', '最低': 'low', '成交量': 'volume',
        '成交额': 'amount', '振幅': 'amplitude', '涨跌幅': 'change_pct',
        '涨跌额': 'change_amt', '换手率': 'turnover',
    })
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    return df


def _normalize_kline_adata(df):
    """标准化adata日K DataFrame"""
    if df is None or df.empty:
        return pd.DataFrame()
    # adata返回的列名可能不同，统一处理
    col_map = {
        'trade_time': 'date', 'trade_date': 'date',
        'open': 'open', 'close': 'close', 'high': 'high', 'low': 'low',
        'volume': 'volume', 'amount': 'amount',
        'change_pct': 'change_pct', 'change': 'change_amt',
        'turnover_ratio': 'turnover',
    }
    for old, new in col_map.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})

    # 转换数值列
    for col in ['open', 'close', 'high', 'low', 'volume', 'amount', 'change_pct']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)

    return df


def _normalize_kline_tx(df):
    """标准化腾讯数据源日K DataFrame（stock_zh_a_hist_tx）"""
    if df is None or df.empty:
        return pd.DataFrame()
    df['date'] = pd.to_datetime(df['date'])
    # 腾讯数据源返回：date, open, close, high, low, amount
    # 补充缺失列
    if 'volume' not in df.columns:
        df['volume'] = 0
    if 'change_pct' not in df.columns:
        df['change_pct'] = df['close'].pct_change() * 100
    if 'change_amt' not in df.columns:
        df['change_amt'] = df['close'].diff()
    if 'amplitude' not in df.columns:
        df['amplitude'] = (df['high'] - df['low']) / df['close'].shift(1) * 100
    if 'turnover' not in df.columns:
        df['turnover'] = 0
    df = df.sort_values('date').reset_index(drop=True)
    return df


def get_stock_daily_kline_range(stock_code, market='sh', start_date=None, end_date=None):
    """
    获取个股日K数据（多数据源）

    参数:
        stock_code: 股票代码
        market: 市场（保留兼容，AKShare自动识别）
        start_date: 'YYYYMMDD'
        end_date: 'YYYYMMDD'

    返回:
        DataFrame: 标准化日K数据
    """
    from datetime import datetime, timedelta

    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')

    sources = [
        ('AKShare-EM', lambda: _ak_hist(stock_code, start_date, end_date)),
        ('AKShare-TX', lambda: _ak_tx_hist(stock_code, start_date, end_date)),
        ('adata-sina', lambda: _adata_sina_hist(stock_code, start_date, end_date)),
        ('adata-qq', lambda: _adata_qq_hist(stock_code, start_date, end_date)),
        ('adata-baidu', lambda: _adata_baidu_hist(stock_code, start_date, end_date)),
    ]

    df, source = _try_sources(sources)

    if df is None or (hasattr(df, 'empty') and df.empty):
        print(f"获取个股日K数据失败 ({stock_code}): 所有数据源均不可用")
        return pd.DataFrame()

    time.sleep(random.uniform(0.2, 0.8))
    return df


def get_stock_daily_kline(stock_code, market='sh', days=60):
    """获取个股日K数据（最近N天）"""
    from datetime import datetime, timedelta
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
    return get_stock_daily_kline_range(stock_code, market=market,
                                       start_date=start_date, end_date=end_date)


# ==================== 4. 个股实时行情 ====================

def _ak_spot(stock_code):
    """AKShare 东方财富全市场实时行情，筛选目标股"""
    df = ak.stock_zh_a_spot_em()
    if df is None or df.empty:
        return {}
    row = df[df['代码'] == stock_code]
    if row.empty:
        return {}
    d = row.iloc[0]
    return {
        'code': stock_code,
        'price': _safe_float(d.get('最新价')),
        'open': _safe_float(d.get('今开')),
        'high': _safe_float(d.get('最高')),
        'low': _safe_float(d.get('最低')),
        'yesterday_close': _safe_float(d.get('昨收')),
        'change_pct': _safe_float(d.get('涨跌幅')),
        'volume': _safe_float(d.get('成交量')),
        'amount': _safe_float(d.get('成交额')),
    }


def get_stock_realtime(stock_code):
    """
    获取个股实时行情（多数据源）
    """
    sources = [
        ('AKShare-EM', lambda: _ak_spot(stock_code)),
    ]

    result, source = _try_sources(sources)

    if result is None or not result:
        print(f"获取实时行情失败 ({stock_code}): 所有数据源均不可用")
        return {}

    return result


# ==================== 测试入口 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("测试统一数据源路由器")
    print("=" * 50)

    print("\n--- 1. 板块排名 ---")
    sectors = get_sector_ranking(sector_type=2, limit=5)
    print(f"获取到 {len(sectors)} 个板块")
    for s in sectors[:3]:
        print(f"  {s['name']}: {s['change_percent']}%")

    time.sleep(2)

    print("\n--- 2. 个股日K ---")
    df = get_stock_daily_kline('002409', market='sz', days=30)
    print(f"获取到 {len(df)} 条日K数据")
    if not df.empty:
        print(df.tail(3))
