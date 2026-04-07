#!/usr/bin/env python3
"""
Job: 更新全市场最新日K数据
直接调用新浪财经原生API获取全A股当日行情，写入 daily_kline 表

数据源：http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData
定时：每个交易日 14:48 和 19:00

写入字段（比ak.stock_zh_a_spot多）：
  - OHLCV（开高低收量额）
  - change_pct: 涨跌幅(%)
  - turnover: 换手率(%)
  - pe_ratio: 市盈率
  - pb_ratio: 市净率
  - mktcap: 总市值(万元)
  - nmc: 流通市值(万元)
  - outstanding_share: 流通股本(股), 由 nmc/close 反推
  - volume_ratio: 量比 = 当日成交量 / 前5日平均成交量
"""

import sys
import os
import time
import logging
import sqlite3
import random
import requests
from datetime import datetime

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.paths import DB_PATH
from akshare.utils import demjson

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

SINA_URL = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
SINA_PAYLOAD = {
    "page": "1",
    "num": "100",
    "sort": "symbol",
    "asc": "1",
    "node": "hs_a",
    "symbol": "",
    "_s_r_a": "page",
}


def _get_page_count():
    params = SINA_PAYLOAD.copy()
    params["page"] = "1"
    params["num"] = "1"
    try:
        r = requests.get(SINA_URL, params=params, timeout=15)
        data = demjson.decode(r.text)
        if isinstance(data, list) and len(data) > 0:
            return int(data[0].get('total_page', 69))
    except Exception:
        pass
    return 69


def fetch_all_market():
    logger.info("开始获取全市场行情（新浪原生API）...")
    start = time.time()

    page_count = _get_page_count()
    logger.info(f"总页数: {page_count}")

    all_data = []
    for page in range(1, page_count + 1):
        params = SINA_PAYLOAD.copy()
        params["page"] = str(page)

        for attempt in range(3):
            try:
                r = requests.get(SINA_URL, params=params, timeout=15)
                data = demjson.decode(r.text)
                if isinstance(data, list):
                    all_data.extend(data)
                break
            except Exception as e:
                logger.warning(f"第{page}页 第{attempt+1}次失败: {e}")
                time.sleep(5)

        if page < page_count:
            time.sleep(0.5 + random.random() * 0.5)

    elapsed = time.time() - start
    logger.info(f"获取完成: {len(all_data)} 只, 耗时 {elapsed:.1f}秒")
    return all_data


def _load_avg_volume(conn, trade_date, lookback_days=5):
    """
    预加载所有股票的前N个交易日平均成交量

    通过交易日历表获取精确的交易日范围
    返回: dict[code] = avg_volume
    """
    # 从交易日历获取前N个交易日
    trade_days = conn.execute("""
        SELECT trade_date FROM trade_calendar
        WHERE trade_status = 1 AND trade_date < ?
        ORDER BY trade_date DESC
        LIMIT ?
    """, (trade_date, lookback_days)).fetchall()

    if not trade_days:
        logger.warning(f"交易日历无数据，无法计算量比")
        return {}

    day_list = [d['trade_date'] for d in trade_days]
    start_str = day_list[-1]  # 最早的那天

    rows = conn.execute("""
        SELECT code, AVG(volume) as avg_vol
        FROM daily_kline
        WHERE date >= ? AND date < ?
        GROUP BY code
        HAVING COUNT(*) >= 2
    """, (start_str, trade_date)).fetchall()

    avg_map = {}
    for r in rows:
        if r['avg_vol'] and r['avg_vol'] > 0:
            avg_map[r['code']] = float(r['avg_vol'])

    logger.info(f"加载前{len(day_list)}个交易日均量({start_str}~{day_list[0]}): {len(avg_map)} 只")
    return avg_map


def save_to_db(data_list, trade_date):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 预加载前5日均量（用于计算量比）
    avg_vol_map = _load_avg_volume(conn, trade_date)

    success = 0
    skip = 0
    error = 0

    for item in data_list:
        code = str(item.get('symbol', '')).strip()
        # 去掉市场前缀（sh/sz/bj），保持与已有数据格式一致
        for prefix in ('sh', 'sz', 'bj'):
            if code.startswith(prefix):
                code = code[len(prefix):]
                break
        name = str(item.get('name', '')).strip()

        if not code:
            skip += 1
            continue

        close_price = _safe_float(item.get('trade'))
        if close_price is None or close_price <= 0:
            skip += 1
            continue

        # 反推流通股本
        nmc_val = _safe_float(item.get('nmc'))
        outstanding_share = nmc_val * 10000 / close_price if nmc_val and close_price > 0 else None

        # 计算量比
        volume = _safe_int(item.get('volume'))
        avg_vol = avg_vol_map.get(code)
        volume_ratio = volume / avg_vol if avg_vol and volume is not None else None

        try:
            cursor.execute("""
                INSERT OR REPLACE INTO daily_kline
                (code, name, date, open, high, low, close, volume, amount,
                 change_pct, turnover, pe_ratio, pb_ratio, mktcap, nmc,
                 outstanding_share, volume_ratio)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                code, name, trade_date,
                _safe_float(item.get('open')),
                _safe_float(item.get('high')),
                _safe_float(item.get('low')),
                close_price,
                volume,
                _safe_float(item.get('amount')),
                _safe_float(item.get('changepercent')),
                _safe_float(item.get('turnoverratio')),
                _safe_float(item.get('per')),
                _safe_float(item.get('pb')),
                _safe_float(item.get('mktcap')),
                nmc_val,
                outstanding_share,
                volume_ratio,
            ))
            success += 1
        except Exception as e:
            logger.debug(f"[{code}] 写入失败: {e}")
            error += 1

    conn.commit()
    conn.close()
    logger.info(f"写入完成: 成功{success}, 跳过{skip}, 失败{error}")
    return success


def _safe_float(val):
    if val is None or (isinstance(val, str) and val.strip() in ('', '-', 'nan', 'None')):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val):
    if val is None or (isinstance(val, str) and val.strip() in ('', '-', 'nan', 'None')):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def run():
    today = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"=== 更新全市场日K — {today} ===")

    data = fetch_all_market()
    if not data:
        logger.warning("获取数据为空，跳过")
        return

    count = save_to_db(data, today)
    logger.info(f"=== 完成: {count} 只更新到 {today} ===")


if __name__ == '__main__':
    run()
