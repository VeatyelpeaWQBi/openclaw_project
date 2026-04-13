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
    get_trading_day_offset,
    get_trade_days_range, get_rs_score_last_date,
    get_index_daily_closes, get_stocks_daily_closes,
    batch_upsert_rs_score,
    get_watchlist_index_codes, get_watchlist_stocks_by_index,
    get_tracked_indices, batch_upsert_daily_kline,
    batch_upsert_index_daily_kline,
    get_daily_kline_max_date, get_index_daily_kline_max_date,
    get_recent_trade_dates, get_avg_volume_by_code,
    get_trading_day_offset_from, get_trading_day_offset_from_end,
    is_trade_day,
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

    数据源：ak.stock_zh_index_spot_sina（新浪批量指数接口，一次获取全部）
    过滤：只保留 index_info 表中跟踪的指数
    """
    import akshare as ak

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
    return success


def calc_rs_batch(calc_dates, stock_codes, stock_closes, index_closes, all_trade_days, benchmark_code, lookback=250):
    """
    对指定日期列表批量计算RS Score并写入数据库

    这是RS计算的核心逻辑，被 calc_rs_score_incremental 和 refresh_recent_rs_score 共用。

    参数:
        calc_dates: 待计算日期列表
        stock_codes: 成分股代码列表
        stock_closes: dict[code] = dict[date] = close
        index_closes: dict[date] = close
        all_trade_days: 完整交易日列表（用于回溯lookback天）
        benchmark_code: 基准指数代码
        lookback: 回看天数

    返回:
        int: 写入条数
    """
    date_to_idx = {d: i for i, d in enumerate(all_trade_days)}
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    total_rows = 0

    for calc_date in calc_dates:
        date_idx = date_to_idx.get(calc_date)
        if date_idx is None or date_idx < lookback:
            continue

        past_date = all_trade_days[date_idx - lookback]

        index_today = index_closes.get(calc_date)
        index_past = index_closes.get(past_date)
        if index_today is None or index_past is None or index_past == 0:
            continue

        benchmark_return = (index_today - index_past) / index_past
        benchmark_ratio = 1 + benchmark_return
        if benchmark_ratio == 0:
            continue

        valid_ratios = []
        for code in stock_codes:
            closes = stock_closes.get(code, {})
            s_today = closes.get(calc_date)
            s_past = closes.get(past_date)
            if s_today is None or s_past is None or s_past == 0:
                continue
            stock_return = (s_today - s_past) / s_past
            rs_ratio = (1 + stock_return) / benchmark_ratio
            valid_ratios.append((code, rs_ratio, stock_return))

        if not valid_ratios:
            continue

        valid_ratios.sort(key=lambda x: x[1])
        total_valid = len(valid_ratios)

        batch = []
        for rank_idx, (code, rs_ratio, stock_return) in enumerate(valid_ratios):
            rs_rank = rank_idx + 1
            rs_score_val = round(rs_rank / total_valid * 100, 2)
            batch.append((
                code, benchmark_code, calc_date,
                round(rs_ratio, 6), rs_score_val, rs_rank,
                round(stock_return, 6), round(benchmark_return, 6),
                lookback, now_str,
            ))

        written = batch_upsert_rs_score(batch)
        total_rows += written

    return total_rows


def calc_rs_score_incremental(trade_date, lookback=250):
    """
    增量计算RS Score（自动回溯补全缺失日期）

    流程：
      1. 从 watchlist 获取唯一的 index_code 列表
      2. 对每个 index_code：
         a. 查 rs_score 最新日期
         b. 查 daily_kline / index_daily_kline 最新日期
         c. 自动从最新往前补全缺失的交易日
      3. 每个缺失日：加载250天数据 → 计算RS → 写入
    """
    logger.info("开始增量计算RS Score...")
    start = time.time()

    # 1. 获取候选池关联的指数代码
    index_codes = get_watchlist_index_codes()
    if not index_codes:
        logger.info("候选池无关联指数，跳过RS Score计算")
        return 0

    # 2. 获取今天（或最近交易日）
    today = get_trading_day_offset(0)
    if not today:
        today = trade_date

    total_rows = 0

    for index_code in index_codes:
        # 获取该指数关联的成分股
        stock_codes = get_watchlist_stocks_by_index(index_code)
        if not stock_codes:
            continue

        # 查 rs_score 最新日期
        rs_last = get_rs_score_last_date(index_code)

        # 查可计算的最新日期
        dk_last = get_daily_kline_max_date(stock_codes[0])
        idx_last = get_index_daily_kline_max_date(index_code)

        if not dk_last or not idx_last:
            logger.warning(f"[{index_code}] 日K数据不足，跳过")
            continue

        calc_end = min(dk_last, idx_last)

        # 确定需要计算的日期范围
        # 原则：历史缺失补全（已有不动），当天强制更新
        if rs_last:
            # 从 rs_last 后一天开始补全历史缺口
            calc_start = get_trading_day_offset_from(rs_last, 1)
            if not calc_start:
                # 无缺口，仍需计算今天
                calc_dates = []
            else:
                calc_dates = get_trade_days_range(calc_start, calc_end)
        else:
            # 无历史RS Score，从最早可计算日开始
            calc_start = get_trading_day_offset_from_end(calc_end, -lookback)
            if not calc_start:
                continue
            calc_dates = get_trade_days_range(calc_start, calc_end)

        # 强制刷新最近2个交易日的RS Score（含calc_end本身）
        force_days = get_recent_trade_dates(calc_end, 1)
        force_days.insert(0, calc_end)
        for d in force_days:
            if d not in calc_dates:
                calc_dates.append(d)
                logger.info(f"[{index_code}] 追加强制更新日期: {d}")

        if not calc_dates:
            logger.info(f"[{index_code}] RS Score已是最新")
            continue

        logger.info(f"[{index_code}] 待补全: {calc_dates[0]} ~ {calc_dates[-1]} ({len(calc_dates)}天)")

        # 加载历史数据（最早需要 lookback 天前的数据）
        data_start = get_trading_day_offset_from(calc_dates[0], -lookback)
        if not data_start:
            continue

        stock_closes = get_stocks_daily_closes(stock_codes, data_start, calc_end)
        index_closes = get_index_daily_closes(index_code, data_start, calc_end)

        # 获取完整交易日历（用于回溯 lookback 天）
        all_days = get_trade_days_range(data_start, calc_end)
        date_to_idx = {d: i for i, d in enumerate(all_days)}

        # 逐日计算RS Score
        rows = calc_rs_batch(calc_dates, stock_codes, stock_closes, index_closes, all_days, index_code, lookback)
        total_rows += rows

        logger.info(f"[{index_code}] {len(stock_codes)}只成分股, {len(calc_dates)}天补全完成, 写入{rows}条")

    elapsed = time.time() - start
    logger.info(f"RS Score增量计算完成: {total_rows}条, 耗时{elapsed:.1f}秒")
    return total_rows

def run():
    today = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"=== 更新全市场日K — {today} ===")

    # 1. 更新个股日K
    data = fetch_all_market()
    if data:
        save_to_db(data, today)

    # 2. 更新指数日K
    fetch_and_save_index_daily_kline(today)

    # 3. 统一计算 VCP + ADX 评分（不依赖基准指数，只跑一次）
    logger.info("=== 启动评分流水线 ===")
    try:
        index_codes = get_watchlist_index_codes()
        if not index_codes:
            index_codes = ['000510'] # 中证A500，作为默认基准

        codes = get_all_stock_codes()
        if codes:
            days = 2  # 只计算最近2天，包含当天（增量模式）
            max_lookback = 250 + days
            # 用第一个指数做预加载（VCP/ADX 只需要 stock_data 和 all_dates）
            stock_data, index_closes, all_dates = preload_data(
                codes, index_codes[0], today, max_lookback
            )

            if all_dates:
                # VCP + ADX 只跑一次
                run_scores_without_index(stock_data, all_dates, days)

                # RS 按 watchlist 中每个指数循环
                for index_code in index_codes:
                    run_rs(index_code, stock_data, all_dates, days)
    except Exception as e:
        logger.error(f"评分流水线执行失败: {e}", exc_info=True)

    logger.info(f"=== 完成: 个股+指数+评分更新到 {today} ===")


if __name__ == '__main__':
    run()
