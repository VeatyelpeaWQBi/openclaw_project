"""
个股数据获取模块
使用AKShare获取个股K线和实时行情

数据源策略：AKShare内部路由，不指定具体后端
"""

import akshare as ak
import pandas as pd
import time
import random
import os

# 禁止代理干扰（AKShare内部管理连接）
for _k in ('HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy'):
    os.environ.pop(_k, None)


def get_stock_daily_kline(stock_code, market='sh', days=60):
    """
    获取个股日K数据（最近N天）

    参数:
        stock_code: 股票代码，如 '002409'
        market: 市场，'sh'=上海，'sz'=深圳
        days: 获取最近多少天的数据

    返回:
        pandas.DataFrame: 日K数据，包含 date, open, high, low, close, volume, amount
    """
    from datetime import datetime, timedelta
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
    return get_stock_daily_kline_range(stock_code, market=market,
                                       start_date=start_date, end_date=end_date)


def get_stock_daily_kline_range(stock_code, market='sh', start_date=None, end_date=None):
    """
    获取指定日期范围的个股日K数据
    使用AKShare通用接口（自动选择数据源）

    参数:
        stock_code: 股票代码，如 '002409'
        market: 市场，'sh'=上海，'sz'=深圳（此参数保留兼容，AKShare自动识别）
        start_date: 开始日期 'YYYYMMDD' 格式
        end_date: 结束日期 'YYYYMMDD' 格式

    返回:
        pandas.DataFrame: 日K数据
    """
    from datetime import datetime, timedelta

    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')

    try:
        df = ak.stock_zh_a_hist(
            symbol=stock_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"  # 前复权
        )

        if df is None or df.empty:
            return pd.DataFrame()

        # 标准化列名：AKShare返回中文列名
        df = df.rename(columns={
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '涨跌幅': 'change_pct',
            '涨跌额': 'change_amt',
            '换手率': 'turnover',
        })

        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)

        time.sleep(random.uniform(0.2, 0.8))
        return df

    except Exception as e:
        print(f"获取个股日K数据失败 ({stock_code}): {e}")
        return pd.DataFrame()


def get_stock_realtime(stock_code):
    """
    获取个股实时行情
    使用AKShare通用接口

    参数:
        stock_code: 股票代码

    返回:
        dict: 实时行情数据
    """
    try:
        df = ak.stock_zh_a_spot_em()

        if df is None or df.empty:
            return {}

        # 筛选目标股票
        row = df[df['代码'] == stock_code]
        if row.empty:
            return {}

        d = row.iloc[0]
        return {
            'code': stock_code,
            'price': float(d.get('最新价', 0) or 0),
            'open': float(d.get('今开', 0) or 0),
            'high': float(d.get('最高', 0) or 0),
            'low': float(d.get('最低', 0) or 0),
            'yesterday_close': float(d.get('昨收', 0) or 0),
            'change_pct': float(d.get('涨跌幅', 0) or 0),
            'volume': float(d.get('成交量', 0) or 0),
            'amount': float(d.get('成交额', 0) or 0),
        }

    except Exception as e:
        print(f"获取实时行情失败 ({stock_code}): {e}")
        return {}


if __name__ == '__main__':
    print("测试个股数据获取...")
    df = get_stock_daily_kline('002409', market='sz', days=30)
    if not df.empty:
        print(f"获取到 {len(df)} 条日K数据")
        print(df.tail())
    else:
        print("未获取到数据")
