"""
个股日K数据获取模块
使用东方财富API获取个股历史K线数据
"""

import requests
import pandas as pd
from datetime import datetime, timedelta

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'http://quote.eastmoney.com/'
}

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
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
    return get_stock_daily_kline_range(stock_code, market=market, start_date=start_date, end_date=end_date)


def get_stock_daily_kline_range(stock_code, market='sh', start_date=None, end_date=None):
    """
    获取指定日期范围的个股日K数据

    参数:
        stock_code: 股票代码，如 '002409'
        market: 市场，'sh'=上海，'sz'=深圳
        start_date: 开始日期 'YYYYMMDD' 格式
        end_date: 结束日期 'YYYYMMDD' 格式

    返回:
        pandas.DataFrame: 日K数据
    """
    if stock_code.startswith('6'):
        secid = f"1.{stock_code}"
    else:
        secid = f"0.{stock_code}"

    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')

    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
        'klt': '101',  # 日K
        'fqt': '1',    # 前复权
        'secid': secid,
        'beg': start_date,
        'end': end_date
    }

    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
        data = resp.json()

        if data.get('data') and data['data'].get('klines'):
            records = []
            for line in data['data']['klines']:
                fields = line.split(',')
                records.append({
                    'date': fields[0],
                    'open': float(fields[1]),
                    'close': float(fields[2]),
                    'high': float(fields[3]),
                    'low': float(fields[4]),
                    'volume': int(fields[5]),
                    'amount': float(fields[6]),
                    'amplitude': float(fields[7]),
                    'change_pct': float(fields[9]),
                    'change_amt': float(fields[10])
                })

            df = pd.DataFrame(records)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            return df
        return pd.DataFrame()
    except Exception as e:
        print(f"获取个股日K数据失败 ({stock_code}): {e}")
        return pd.DataFrame()


def get_stock_realtime(stock_code):
    """
    获取个股实时行情

    参数:
        stock_code: 股票代码

    返回:
        dict: 实时行情数据
    """
    if stock_code.startswith('6'):
        secid = f"1.{stock_code}"
    else:
        secid = f"0.{stock_code}"

    url = "http://push2.eastmoney.com/api/qt/stock/get"
    params = {
        'secid': secid,
        'fields': 'f43,f44,f45,f46,f47,f48,f50,f51,f52,f55,f57,f58,f60,f116,f117,f162,f170'
    }

    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
        data = resp.json()

        if data.get('data'):
            d = data['data']
            return {
                'code': stock_code,
                'price': d.get('f43', 0) / 100 if d.get('f43') else 0,
                'open': d.get('f46', 0) / 100 if d.get('f46') else 0,
                'high': d.get('f44', 0) / 100 if d.get('f44') else 0,
                'low': d.get('f45', 0) / 100 if d.get('f45') else 0,
                'yesterday_close': d.get('f60', 0) / 100 if d.get('f60') else 0,
                'change_pct': d.get('f170', 0) / 100 if d.get('f170') else 0,
                'volume': d.get('f47', 0),
                'amount': d.get('f48', 0)
            }
        return {}
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
