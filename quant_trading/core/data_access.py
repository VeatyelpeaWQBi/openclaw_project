"""
统一数据源路由器
对所有在线A股数据获取进行统一包装，支持新浪数据源

数据源：
- 板块排名：新浪 (ak.stock_sector_spot)
- 板块成分股：新浪 (ak.stock_sector_detail)
- 个股日K：新浪 (ak.stock_zh_a_daily)
- ETF日K：新浪 (ak.fund_etf_hist_sina)
"""

import akshare as ak
import pandas as pd
import time
import random
import os
import traceback
import logging

logger = logging.getLogger(__name__)

# 禁止代理干扰
for _k in ('HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy'):
    os.environ.pop(_k, None)


# ==================== 数据源配置 ====================

SECTOR_RANKING_SOURCE = 'sina'
SECTOR_STOCKS_SOURCE = 'sina'
STOCK_KLINE_SOURCE = 'sina'


# ==================== 工具函数 ====================

def _safe_float(val, default=0.0):
    """安全转换为float，处理字符串和None"""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# ==================== 1. 板块排名（新浪） ====================

def _sina_sector_ranking(sector_type):
    """
    新浪板块排名（一次性获取全部板块，含涨跌幅）

    参数:
        sector_type: 1=行业板块, 2=概念板块

    返回:
        list: [{code(label), name, change_percent, volume, amount}]
    """
    indicator = '行业' if sector_type == 1 else '概念'
    type_name = '行业' if sector_type == 1 else '概念'

    try:
        logger.info(f"[新浪] 获取{type_name}板块排名...")
        df = ak.stock_sector_spot(indicator=indicator)

        if df is None or df.empty:
            logger.warning(f"[新浪] {type_name}板块排名返回空数据")
            return []

        logger.info(f"[新浪] {type_name}板块: 获取 {len(df)} 个板块")

        sectors = []
        for _, row in df.iterrows():
            label = str(row.get('label', ''))
            name = str(row.get('板块', ''))
            if not label or not name:
                continue
            sectors.append({
                'code': label,  # label 作为唯一标识
                'name': name,
                'change_percent': _safe_float(row.get('涨跌幅')),
                'volume': _safe_float(row.get('总成交量')),
                'amount': _safe_float(row.get('总成交额')),
            })

        # 按涨幅降序排序
        sectors.sort(key=lambda x: x.get('change_percent', 0), reverse=True)

        logger.debug(f"[新浪] {type_name}板块TOP5: {[(s['name'], s['change_percent']) for s in sectors[:5]]}")
        return sectors

    except Exception as e:
        logger.error(f"[新浪] {type_name}板块排名获取失败: {type(e).__name__}: {e}")
        return []


def get_sector_ranking(sector_type=2, limit=20):
    """
    获取板块涨幅排名

    参数:
        sector_type: 1=行业板块, 2=概念板块
        limit: 返回数量

    返回:
        list: [{code(label), name, change_percent, volume, amount}]
    """
    type_name = '行业' if sector_type == 1 else '概念'
    logger.info(f"获取{type_name}板块排名(limit={limit})...")

    sectors = _sina_sector_ranking(sector_type)

    if not sectors:
        logger.warning(f"{type_name}板块排名获取失败")
        return []

    result = sectors[:limit]
    logger.info(f"{type_name}板块排名: 返回 {len(result)} 个, TOP5: {[(s['name'], s['change_percent']) for s in result[:5]]}")
    return result


# ==================== 2. 板块成分股（新浪） ====================

def _sina_sector_stocks(sector_label):
    """
    新浪板块成分股

    参数:
        sector_label: 板块标签（来自 stock_sector_spot 的 label 字段，如 'gn_hwqc'）

    返回:
        list: [{code, name, price, change_pct, change_percent, turnover, volume, amount}]
    """
    try:
        logger.info(f"[新浪] 获取板块[{sector_label}]成分股...")
        df = ak.stock_sector_detail(sector=sector_label)

        if df is None or df.empty:
            logger.warning(f"[新浪] 板块[{sector_label}]成分股为空")
            return []

        stocks = []
        for _, row in df.iterrows():
            code = str(row.get('code', ''))
            name = str(row.get('name', ''))
            if not code:
                continue
            change_pct = _safe_float(row.get('changepercent'))
            stocks.append({
                'code': code,
                'name': name,
                'price': _safe_float(row.get('trade')),
                'change_pct': change_pct,
                'change_percent': change_pct,
                'turnover': _safe_float(row.get('turnoverratio')),
                'volume': _safe_float(row.get('volume')),
                'amount': _safe_float(row.get('amount')),
                'high': _safe_float(row.get('high')),
                'low': _safe_float(row.get('low')),
                'open': _safe_float(row.get('open')),
            })

        logger.info(f"[新浪] 板块[{sector_label}]成分股: {len(stocks)} 只")
        return stocks

    except Exception as e:
        logger.error(f"[新浪] 板块[{sector_label}]成分股获取失败: {type(e).__name__}: {e}")
        return []


def get_sector_stocks(sector_label, limit=50):
    """
    获取板块内的个股列表

    参数:
        sector_label: 新浪板块标识（label字段，如 'gn_hwqc'）
        limit: 返回数量

    返回:
        list: [{code, name, price, change_pct, turnover, volume, amount}]
    """
    logger.info(f"获取板块[{sector_label}]成分股(limit={limit})...")

    stocks = _sina_sector_stocks(sector_label)

    if not stocks:
        logger.warning(f"板块[{sector_label}]成分股获取失败")
        return []

    time.sleep(random.uniform(0.6, 1.2))
    return stocks[:limit]


# ==================== 3. 个股日K线（新浪） ====================

def _sina_daily_kline(stock_code, market='sh', start_date=None, end_date=None):
    """
    新浪个股日K数据

    参数:
        stock_code: 股票代码（纯数字，如 '002409'）
        market: 市场前缀（'sh' 或 'sz'）
        start_date: 起始日期 'YYYYMMDD'
        end_date: 截止日期 'YYYYMMDD'

    返回:
        DataFrame: 标准化日K数据
    """
    from datetime import datetime, timedelta

    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')

    sina_code = f'{market}{stock_code}'

    try:
        logger.debug(f"[新浪] 获取日K: {sina_code} ({start_date}~{end_date})")
        df = ak.stock_zh_a_daily(symbol=sina_code, start_date=start_date, end_date=end_date)

        if df is None or df.empty:
            logger.warning(f"[新浪] {sina_code} 日K返回空数据（可能已退市或不存在）")
            return pd.DataFrame()

        # 检查是否有date列（新浪对某些股票/ETF返回异常数据）
        if 'date' not in df.columns and 'Date' not in df.columns:
            logger.warning(f"[新浪] {sina_code} 返回数据无date列（可能已退市/北交所/ETF）")
            return pd.DataFrame()

        # 标准化列名
        if 'Date' in df.columns:
            df = df.rename(columns={'Date': 'date'})
        df['date'] = pd.to_datetime(df['date'])

        # 确保必要的数值列存在
        for col in ['open', 'close', 'high', 'low', 'volume', 'amount']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 补充缺失列
        if 'change_pct' not in df.columns and 'close' in df.columns:
            df['change_pct'] = df['close'].pct_change() * 100
        if 'turnover' not in df.columns:
            df['turnover'] = 0

        df = df.sort_values('date').reset_index(drop=True)

        logger.debug(f"[新浪] {sina_code} 日K: {len(df)} 条")
        return df

    except Exception as e:
        if 'JSONDecodeError' in type(e).__name__ or 'No value' in str(e):
            logger.warning(f"[新浪] {sina_code} 日K返回空数据（可能已退市或不存在）: {e}")
        else:
            logger.error(f"[新浪] {sina_code} 日K获取失败: {type(e).__name__}: {e}")
        return pd.DataFrame()


def _sina_etf_daily_kline(etf_code, start_date=None, end_date=None):
    """
    新浪ETF日K数据

    参数:
        etf_code: ETF代码（如 'sh512010' 或 '512010'，自动识别市场）
        start_date: 起始日期 'YYYYMMDD'（新浪接口不支持范围，返回全部后截取）
        end_date: 截止日期 'YYYYMMDD'

    返回:
        DataFrame: 标准化日K数据
    """
    from datetime import datetime, timedelta

    # 自动识别市场前缀
    if not etf_code.startswith(('sh', 'sz')):
        code_num = etf_code
        if code_num.startswith(('5', '9')):
            sina_code = f'sh{code_num}'
        elif code_num.startswith(('1', '3')):
            sina_code = f'sz{code_num}'
        else:
            sina_code = f'sh{code_num}'
    else:
        sina_code = etf_code

    try:
        logger.debug(f"[新浪] 获取ETF日K: {sina_code}")
        df = ak.fund_etf_hist_sina(symbol=sina_code)

        if df is None or df.empty:
            logger.warning(f"[新浪] ETF {sina_code} 日K返回空数据")
            return pd.DataFrame()

        if 'date' not in df.columns:
            logger.warning(f"[新浪] ETF {sina_code} 返回数据无date列")
            return pd.DataFrame()

        df['date'] = pd.to_datetime(df['date'])
        for col in ['open', 'close', 'high', 'low', 'volume', 'amount']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 补充缺失列
        if 'change_pct' not in df.columns and 'close' in df.columns:
            df['change_pct'] = df['close'].pct_change() * 100

        df = df.sort_values('date').reset_index(drop=True)

        # 截取日期范围
        if start_date:
            start_dt = pd.to_datetime(start_date)
            df = df[df['date'] >= start_dt]
        if end_date:
            end_dt = pd.to_datetime(end_date)
            df = df[df['date'] <= end_dt]
        df = df.reset_index(drop=True)

        logger.debug(f"[新浪] ETF {sina_code} 日K: {len(df)} 条")
        return df

    except Exception as e:
        logger.error(f"[新浪] ETF {sina_code} 日K获取失败: {type(e).__name__}: {e}")
        return pd.DataFrame()


def get_etf_daily_kline(etf_code, start_date=None, end_date=None):
    """
    获取ETF日K数据（公开接口）

    参数:
        etf_code: ETF代码（如 '512010'，自动识别市场）
        start_date: 起始日期 'YYYYMMDD'
        end_date: 截止日期 'YYYYMMDD'

    返回:
        DataFrame: 标准化日K数据
    """
    logger.info(f"获取ETF日K: {etf_code} ({start_date or '全部'}~{end_date or '全部'})")
    df = _sina_etf_daily_kline(etf_code, start_date=start_date, end_date=end_date)
    if df.empty:
        logger.warning(f"[ETF {etf_code}] 日K数据获取为空")
    time.sleep(random.uniform(0.6, 1.2))  # 防限流
    return df


def get_stock_daily_kline(stock_code, market='sh', days=120):
    """
    获取个股日K数据（最近N天）

    参数:
        stock_code: 股票代码（纯数字，如 '002409'）
        market: 市场前缀（'sh' 或 'sz'）
        days: 回溯天数

    返回:
        DataFrame: 标准化日K数据
    """
    from datetime import datetime, timedelta
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')

    logger.info(f"获取日K: {market}{stock_code} (最近{days}天)")

    df = _sina_daily_kline(stock_code, market=market, start_date=start_date, end_date=end_date)

    if df.empty:
        logger.error(f"[{stock_code}] 日K数据获取失败")

    time.sleep(random.uniform(0.6, 1.2))  # 防限流
    return df


def get_stock_daily_kline_range(stock_code, market='sh', start_date=None, end_date=None):
    """
    获取个股日K数据（指定日期范围）

    参数:
        stock_code: 股票代码（纯数字，如 '002409'）
        market: 市场前缀（'sh' 或 'sz'）
        start_date: 起始日期 'YYYYMMDD'
        end_date: 截止日期 'YYYYMMDD'

    返回:
        DataFrame: 标准化日K数据
    """
    from datetime import datetime, timedelta

    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')

    logger.info(f"获取日K: {market}{stock_code} ({start_date}~{end_date})")

    df = _sina_daily_kline(stock_code, market=market, start_date=start_date, end_date=end_date)

    if df.empty:
        logger.error(f"[{stock_code}] 日K数据获取失败 (start={start_date}, end={end_date})")

    time.sleep(random.uniform(0.6, 1.2))  # 防限流
    return df


# ==================== 4. 个股实时行情 ====================

def get_stock_realtime(stock_code):
    """
    获取个股实时行情（AKShare 东方财富）
    注意：实时行情暂保留EM数据源，新浪无独立实时行情接口
    """
    try:
        df = ak.stock_zh_a_spot_em()
        if df is None or df.empty:
            logger.warning(f"[{stock_code}] 实时行情获取为空")
            return {}
        row = df[df['代码'] == stock_code]
        if row.empty:
            logger.warning(f"[{stock_code}] 未在行情列表中找到")
            return {}
        d = row.iloc[0]
        result = {
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
        logger.debug(f"[{stock_code}] 实时行情: price={result.get('price')}, change_pct={result.get('change_pct')}%")
        return result
    except Exception as e:
        logger.error(f"[{stock_code}] 实时行情获取失败: {type(e).__name__}: {e}")
        return {}


# ==================== 6. 市场概况（指数+情绪+成交量） ====================

def get_index_realtime():
    """
    获取主要指数实时数据（腾讯接口）

    返回:
        dict: {
            '上证指数': {'code': 'sh000001', 'price': 3927.60, 'change_pct': -0.53, 'amount': 5165},
            '中证全指': {...},
            ...
        }
    """
    import requests

    indices = {
        'sh000001': '上证指数',
        'sh000985': '中证全指',
        'sz399001': '深证成指',
        'sz399006': '创业板指',
        'sh000300': '沪深300',
        'sh000905': '中证500',
        'sh000852': '中证1000',
    }

    result = {}
    try:
        codes_str = ','.join(indices.keys())
        url = f'https://qt.gtimg.cn/q={codes_str}'
        resp = requests.get(url, timeout=10)
        resp.encoding = 'gbk'

        for line in resp.text.strip().split('\n'):
            if '=' not in line or '""' in line:
                continue
            parts = line.split('=')[1].strip('"').split('~')
            if len(parts) < 40:
                continue
            code = parts[2]
            price = _safe_float(parts[3])
            change_pct = _safe_float(parts[32])
            amount = _safe_float(parts[37])  # 成交额（万元）

            for idx_code, idx_name in indices.items():
                if code == idx_code[-6:]:
                    result[idx_name] = {
                        'code': idx_code,
                        'price': price,
                        'change_pct': change_pct,
                        'amount': round(amount / 10000, 0),  # 转为亿元
                    }
                    break

        logger.info(f"获取主要指数: {len(result)} 个")
        return result

    except Exception as e:
        logger.error(f"获取指数数据失败: {type(e).__name__}: {e}")
        return {}


def get_market_sentiment():
    """
    获取市场情绪（上涨/下跌/涨停/跌停）

    返回:
        dict: {'up': 941, 'down': 4172, 'limit_up': 22, 'limit_down': 16, 'flat': 69, 'activity_rate': 13.48}
    """
    try:
        df = ak.stock_market_activity_legu()
        if df is None or df.empty:
            logger.warning("市场情绪数据为空")
            return {}

        data = {}
        for _, row in df.iterrows():
            item = str(row.get('item', ''))
            value = row.get('value', 0)
            if item == '上涨':
                data['up'] = int(value) if value else 0
            elif item == '下跌':
                data['down'] = int(value) if value else 0
            elif item == '涨停':
                data['limit_up'] = int(value) if value else 0
            elif item == '跌停':
                data['limit_down'] = int(value) if value else 0
            elif item == '平盘':
                data['flat'] = int(value) if value else 0
            elif item == '活跃度':
                raw = str(value).replace('%', '')
                data['activity_rate'] = float(raw) if raw else 0.0

        logger.info(f"市场情绪: 上涨{data.get('up',0)}, 下跌{data.get('down',0)}, 涨停{data.get('limit_up',0)}, 跌停{data.get('limit_down',0)}, 活跃度{data.get('activity_rate', 0)}%")
        return data

    except Exception as e:
        logger.error(f"获取市场情绪失败: {type(e).__name__}: {e}")
        return {}


def get_market_volume_compare():
    """
    获取大盘成交量对比（今日vs昨日，计算放量/缩量）

    返回:
        dict: {'today_amount': 11867, 'yesterday_amount': 10789, 'change_pct': 10.0, 'is_fangliang': True}
    """
    import requests

    try:
        # 从腾讯接口获取中证全指实时成交额
        url = 'https://qt.gtimg.cn/q=sh000985'
        resp = requests.get(url, timeout=10)
        resp.encoding = 'gbk'
        parts = resp.text.split('=')[1].strip('"').split('~')
        today_amount = _safe_float(parts[37]) / 10000  # 万元转亿元

        # 获取历史数据（昨日成交额）
        url2 = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=sh000985,day,,,5,qfq'
        resp2 = requests.get(url2, timeout=10)
        data2 = resp2.json()
        day_data = data2['data']['sh000985'].get('day', [])

        # 用成交量比例估算昨日成交额
        if len(day_data) >= 2:
            today_vol = _safe_float(day_data[-1][5])
            yesterday_vol = _safe_float(day_data[-2][5])
            if today_vol > 0:
                yesterday_amount = today_amount * (yesterday_vol / today_vol)
            else:
                yesterday_amount = today_amount
        else:
            yesterday_amount = today_amount

        today_yi = round(today_amount, 0)
        yesterday_yi = round(yesterday_amount, 0)

        if yesterday_yi > 0:
            change_pct = round((today_yi - yesterday_yi) / yesterday_yi * 100, 1)
        else:
            change_pct = 0

        result = {
            'today_amount': today_yi,
            'yesterday_amount': yesterday_yi,
            'change_pct': change_pct,
            'is_fangliang': change_pct > 0,
        }

        logger.info(f"成交量对比: 今日{today_yi}亿 vs 昨日{yesterday_yi}亿 ({'放量' if result['is_fangliang'] else '缩量'}{abs(change_pct)}%)")
        return result

    except Exception as e:
        logger.error(f"获取成交量对比失败: {type(e).__name__}: {e}")
        return {}
