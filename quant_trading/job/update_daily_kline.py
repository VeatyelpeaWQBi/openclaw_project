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
import random
import requests
import json
from datetime import datetime

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.storage import (
    get_trading_day_offset_from,
    get_watchlist_index_codes,
    get_tracked_indices, batch_upsert_daily_kline,
    batch_upsert_index_daily_kline,
    get_recent_trade_dates, get_avg_volume_by_code,
)
from job.calc_scores import preload_data, run_scores_without_index, run_rs
from strategies.trend_trading.score._base import get_all_stock_codes

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
        data = json.loads(r.text)
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
    failed_pages = []
    for page in range(1, page_count + 1):
        params = SINA_PAYLOAD.copy()
        params["page"] = str(page)
        page_ok = False

        for attempt in range(3):
            try:
                r = requests.get(SINA_URL, params=params, timeout=15)
                data = json.loads(r.text)
                if isinstance(data, list) and len(data) > 0:
                    all_data.extend(data)
                    page_ok = True
                    # 数据量不足一页，已到末尾
                    if len(data) < int(SINA_PAYLOAD["num"]):
                        logger.info(f"第{page}页返回{len(data)}条，已达末尾")
                        elapsed = time.time() - start
                        logger.info(f"获取完成: {len(all_data)} 只, 耗时 {elapsed:.1f}秒")
                        return all_data
                else:
                    page_ok = True  # 空列表视为末尾
                break
            except Exception as e:
                logger.warning(f"第{page}页 第{attempt+1}次失败: {e}")
                time.sleep(5)

        if not page_ok:
            failed_pages.append(page)
            logger.error(f"第{page}页 3次重试全部失败，跳过")

        if page < page_count:
            time.sleep(0.5 + random.random() * 0.5)

    if failed_pages:
        logger.error(f"以下页码获取失败（数据可能缺失）: {failed_pages}")

    elapsed = time.time() - start
    logger.info(f"获取完成: {len(all_data)} 只, 耗时 {elapsed:.1f}秒")
    return all_data


def _load_avg_volume(trade_date, lookback_days=5):
    """
    预加载所有股票的前N个交易日平均成交量
    返回: dict[code] = avg_volume
    """
    trade_days = get_recent_trade_dates(trade_date, lookback_days)
    if not trade_days:
        logger.warning(f"交易日历无数据，无法计算量比")
        return {}

    start_str = trade_days[-1]  # 最早的那天
    avg_map = get_avg_volume_by_code(start_str, trade_date)
    logger.info(f"加载前{len(trade_days)}个交易日均量({start_str}~{trade_days[0]}): {len(avg_map)} 只")
    return avg_map


def save_to_db(data_list, trade_date):
    # 预加载前5日均量（用于计算量比）
    avg_vol_map = _load_avg_volume(trade_date)

    rows = []
    skip = 0

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

        rows.append((
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

    success, error = batch_upsert_daily_kline(rows)
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


def fetch_and_save_index_daily_kline(trade_date):
    """
    批量获取各大指数当天日K，写入 index_daily_kline 表

    数据源：
    - ak.stock_zh_index_spot_sina（新浪批量指数接口，一次获取全部）
    - 腾讯API单独获取000985（新浪不提供中证全指）
    过滤：只保留 index_info 表中跟踪的指数
    """
    import akshare as ak
    import requests

    logger.info("开始获取指数日K（新浪批量接口）...")
    start = time.time()

    # 1. 从 storage 获取跟踪的指数代码
    tracked_map = get_tracked_indices()
    if not tracked_map:
        logger.info("index_info 无跟踪指数，跳过")
        return 0

    # 2. 批量获取全部指数实时行情
    try:
        df = ak.stock_zh_index_spot_sina()
    except Exception as e:
        logger.error(f"获取指数行情失败: {e}")
        return 0

    if df is None or df.empty:
        logger.warning("指数行情为空")
        return 0

    # 3. 过滤出跟踪的指数，构建批量数据
    rows = []
    for _, row in df.iterrows():
        code_raw = str(row.get('代码', '')).strip()
        # 去掉 sh/sz 前缀
        code = code_raw
        for prefix in ('sh', 'sz', 'bj'):
            if code.startswith(prefix):
                code = code[len(prefix):]
                break

        if code not in tracked_map:
            continue

        rows.append((
            code, tracked_map[code], trade_date,
            _safe_float(row.get('今开')),
            _safe_float(row.get('最高')),
            _safe_float(row.get('最低')),
            _safe_float(row.get('最新价')),
            _safe_float(row.get('成交量')),
            _safe_float(row.get('成交额')),
            _safe_float(row.get('涨跌额')),
            _safe_float(row.get('涨跌幅')),
        ))

    success = batch_upsert_index_daily_kline(rows)
    elapsed = time.time() - start
    logger.info(f"指数日K更新完成: {success}个指数, 耗时{elapsed:.1f}秒")

    # ========== 4. 单独获取000985（中证全指）- 新浪不提供 ==========
    # 腾讯API格式: https://qt.gtimg.cn/q=sh000985
    # 返回字段: 名称(1), 最新价(3), 昨收(4), 今开(5), 成交量(6), 成交额(37万元)
    try:
        resp = requests.get("https://qt.gtimg.cn/q=sh000985", timeout=10)
        resp.encoding = 'gbk'

        if 'v_sh000985' in resp.text and 'pv_none_match' not in resp.text:
            parts = resp.text.split('=')[1].strip('"').split('~')

            # 解析字段
            name = parts[1]  # 中证全指
            latest_price = _safe_float(parts[3])  # 最新价
            prev_close = _safe_float(parts[4])  # 昨收
            today_open = _safe_float(parts[5])  # 今开
            volume = _safe_float(parts[6])  # 成交量(手)
            amount = _safe_float(parts[37]) / 10000  # 成交额(万元转亿元)
            change = latest_price - prev_close if latest_price and prev_close else 0
            change_pct = (change / prev_close * 100) if prev_close else 0

            # 写入数据库
            # 注意：腾讯实时行情无最高/最低，用最新价近似
            row_985 = (
                '000985', '中证全指', trade_date,
                today_open, latest_price, latest_price, latest_price,  # open, high, low, close
                volume, amount, change, change_pct
            )
            batch_upsert_index_daily_kline([row_985])
            logger.info(f"000985(中证全指)单独获取成功: 收盘{latest_price:.2f}, 成交额{amount:.2f}亿")
            success += 1
        else:
            logger.warning("000985腾讯API返回无效数据")

    except Exception as e:
        logger.warning(f"000985获取失败: {e}")

    return success

def _update_klines(today):
    """
    步骤1：更新个股+指数日K数据

    返回:
        dict: {'kline_count': int, 'index_count': int}
    """
    # 更新个股日K
    data = fetch_all_market()
    kline_count = save_to_db(data, today) if data else 0

    # 更新指数日K
    index_count = fetch_and_save_index_daily_kline(today)

    return {'kline_count': kline_count or 0, 'index_count': index_count or 0}


def _update_scores(today):
    """
    步骤2：统一计算 VCP + ADX + RS 评分

    返回:
        dict: {'vcp_count': int, 'adx_count': int, 'rs_count': int, 'score_error': bool}
    """
    vcp_count = 0
    adx_count = 0
    rs_count = 0
    score_error = False

    logger.info("=== 启动评分流水线 ===")
    try:
        index_codes = get_watchlist_index_codes()
        if not index_codes:
            index_codes = ['000510']  # 中证A500，作为默认基准

        codes = get_all_stock_codes()
        if codes:
            days = 1  # 只计算当天
            max_lookback = 250 + days
            # 用第一个指数做预加载（VCP/ADX 只需要 stock_data 和 all_dates）
            stock_data, index_closes, all_dates = preload_data(
                codes, index_codes[0], today, max_lookback
            )

            if all_dates:
                # VCP + ADX 只跑一次
                vcp_count, adx_count = run_scores_without_index(stock_data, all_dates, days)

                # RS 按 watchlist 中每个指数循环
                for index_code in index_codes:
                    rs_count += run_rs(index_code, stock_data, all_dates, days)
    except Exception as e:
        logger.error(f"评分流水线执行失败: {e}", exc_info=True)
        score_error = True

    return {
        'vcp_count': vcp_count or 0,
        'adx_count': adx_count or 0,
        'rs_count': rs_count or 0,
        'score_error': score_error,
    }


def run():
    today = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"=== 更新全市场日K — {today} ===")
    start_time = time.time()

    # 步骤1：更新日K数据
    kline_result = _update_klines(today)

    # 步骤2：计算评分
    score_result = _update_scores(today)

    logger.info(f"=== 完成: 个股+指数+评分更新到 {today} ===")

    elapsed = time.time() - start_time
    elapsed_str = f"{int(elapsed//60)}分{int(elapsed%60)}秒"
    logger.info(f"⏱️ 总耗时: {elapsed_str}")

    # 返回统计结果供shell脚本通知用
    return {
        'date': today,
        **kline_result,
        **score_result,
        'elapsed': elapsed_str,
    }


if __name__ == '__main__':
    result = run()
    # 输出统计JSON到stdout最后一行，供shell脚本解析
    print(f"RESULT_JSON:{json.dumps(result, ensure_ascii=False)}")
