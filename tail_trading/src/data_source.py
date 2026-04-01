"""
统一数据源路由器
对所有在线A股数据获取进行统一包装，支持新浪数据源

数据源：
- 板块排名：新浪 (ak.stock_sector_spot)
- 板块成分股：新浪 (ak.stock_sector_detail)
- 个股日K：新浪 (ak.stock_zh_a_daily)
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

    time.sleep(random.uniform(0.3, 1.0))
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
            logger.warning(f"[新浪] {sina_code} 日K返回空数据")
            return pd.DataFrame()

        # 新浪返回字段: date, open, high, low, close, volume, amount, outstanding_share, turnover
        # 标准化列名（akshare返回的已经是英文小写）
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        elif 'Date' in df.columns:
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
        logger.error(f"[新浪] {sina_code} 日K获取失败: {type(e).__name__}: {e}")
        return pd.DataFrame()


def get_stock_daily_kline(stock_code, market='sh', days=60):
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

    time.sleep(random.uniform(1, 3))  # 防ban
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

    time.sleep(random.uniform(1, 3))  # 防ban
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


# ==================== 测试入口 ====================

if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

    logger.info("=" * 50)
    logger.info("测试新浪数据源路由器")
    logger.info(f"SECTOR_RANKING_SOURCE={SECTOR_RANKING_SOURCE}")
    logger.info(f"SECTOR_STOCKS_SOURCE={SECTOR_STOCKS_SOURCE}")
    logger.info(f"STOCK_KLINE_SOURCE={STOCK_KLINE_SOURCE}")
    logger.info("=" * 50)

    logger.info("--- 1. 概念板块排名 ---")
    sectors = get_sector_ranking(sector_type=2, limit=5)
    logger.info(f"获取到 {len(sectors)} 个概念板块")
    for s in sectors[:3]:
        logger.info(f"  [{s['code']}] {s['name']}: {s['change_percent']}%")

    time.sleep(2)

    logger.info("--- 2. 行业板块排名 ---")
    industry = get_sector_ranking(sector_type=1, limit=5)
    logger.info(f"获取到 {len(industry)} 个行业板块")
    for s in industry[:3]:
        logger.info(f"  [{s['code']}] {s['name']}: {s['change_percent']}%")

    time.sleep(2)

    # 测试板块成分股（用第一个概念板块）
    if sectors:
        test_sector = sectors[0]
        logger.info(f"--- 3. 板块成分股: {test_sector['name']} (label={test_sector['code']}) ---")
        stocks = get_sector_stocks(test_sector['code'], limit=5)
        logger.info(f"获取到 {len(stocks)} 只成分股")
        for st in stocks[:3]:
            logger.info(f"  {st['code']} {st['name']}: {st['change_pct']}% 换手={st['turnover']}%")

    time.sleep(2)

    logger.info("--- 4. 个股日K ---")
    df = get_stock_daily_kline('002409', market='sz', days=30)
    logger.info(f"获取到 {len(df)} 条日K数据")
    if not df.empty:
        logger.info(f"  最新3条:\n{df.tail(3).to_string()}")
