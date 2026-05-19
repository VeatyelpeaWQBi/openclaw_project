#!/usr/bin/env python3
"""
Job: 更新全市场最新日K数据
直接调用新浪财经原生API获取全A股当日行情，写入 daily_kline 表

数据源：
  - 个股：http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData
  - 指数：ak.stock_zh_index_spot_sina + 腾讯API(000985)
  - ETF：ak.fund_etf_category_sina（新浪ETF实时行情）
定时：每个交易日 14:48 和 19:00

写入字段：
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
import asyncio
from datetime import datetime
import pandas as pd

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# 项目根目录（用于引用公共 indicators 包）
_project_base = os.path.dirname(_project_root)
if _project_base not in sys.path:
    sys.path.insert(0, _project_base)

from core.storage import (
    get_trading_day_offset_from,
    get_watchlist_index_codes,
    get_tracked_indices, batch_upsert_daily_kline,
    batch_upsert_index_daily_kline,
    batch_upsert_etf_daily_kline,
    get_recent_trade_dates, get_avg_volume_by_code,
    get_db_connection, save_technical_indicators,
)
from indicators.utils import (
    calculate_ma, calculate_ma_slope,
    calculate_supertrend, calculate_atr,
    calculate_macd, calculate_macd_slope,
    calculate_rsi, calculate_volume_ratio,
    identify_candle_patterns,
)
from job.calc_scores import preload_data, run_scores_without_index, run_rs
from strategies.trend_trading.score._base import get_all_codes

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

# 除权检测允许的微小差异（浮点数精度）
ADJUSTMENT_TOLERANCE = 0.01


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


def _get_latest_trading_day_le_today(today):
    """获取小于等于today的最新交易日"""
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT trade_date FROM trade_calendar
            WHERE trade_status = 1 AND trade_date <= ?
            ORDER BY trade_date DESC LIMIT 1
        """, (today,)).fetchone()
        return row['trade_date'] if row else None
    finally:
        conn.close()


def _load_previous_day_close(trade_date):
    """获取上一个交易日所有股票的收盘价"""
    prev_date = get_trading_day_offset_from(trade_date, -1)
    if not prev_date:
        return {}

    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT code, close FROM daily_kline WHERE date = ?",
            (prev_date,)
        ).fetchall()
        return {row['code']: row['close'] for row in rows if row['close']}
    finally:
        conn.close()


def _is_market_closed():
    """判断当前是否已收盘（15:00后 或 中午休盘时间 11:30~12:59）"""
    now = datetime.now()
    current_time = now.time()
    current_hour = current_time.hour
    current_minute = current_time.minute

    # 周末视为收盘
    if now.weekday() >= 5:
        return True

    # 15:00后 或 11:30~12:59 休盘时间视为可执行复权刷新
    if current_hour >= 15:
        return True
    if current_hour == 11 and current_minute >= 30:
        return True
    if current_hour == 12:
        return True

    return False


async def _async_start_adjustment_job(adjustment_queue):
    """异步启动独立JOB执行复权刷新（步骤2+3）"""
    import tempfile, json
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(adjustment_queue, f)
        queue_file = f.name

    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "job.refresh_adjustment_stocks",
        "--queue-file", queue_file,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL
    )
    logger.info(f"已异步启动复权刷新JOB (pid={proc.pid}), 队列文件: {queue_file}")


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


def save_to_db(data_list, trade_date, prev_close_map):
    # 预加载前5日均量（用于计算量比）
    avg_vol_map = _load_avg_volume(trade_date)

    rows = []
    skip = 0
    adjustment_queue = []

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

        # 除权检测：对比API settlement与DB昨收
        api_settlement = _safe_float(item.get('settlement'))
        if api_settlement and prev_close_map.get(code):
            db_yest_close = prev_close_map[code]
            if db_yest_close and db_yest_close > 0:
                # 直接检查是否不一致（考虑浮点数精度）
                if abs(api_settlement - db_yest_close) > ADJUSTMENT_TOLERANCE:
                    adjustment_queue.append({
                        'code': code,
                        'name': name,
                        'diff_pct': abs(api_settlement - db_yest_close) / db_yest_close * 100
                    })

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
    return success, adjustment_queue


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


def fetch_and_save_etf_daily_kline(trade_date):
    """
    批量获取全市场ETF当天日K，写入 etf_daily_kline 表

    数据源：ak.fund_etf_category_sina（新浪ETF实时行情）
    """
    import akshare as ak

    logger.info("开始获取ETF日K（新浪批量接口）...")
    start = time.time()

    try:
        df = ak.fund_etf_category_sina(symbol='ETF基金')
    except Exception as e:
        logger.error(f"获取ETF行情失败: {e}")
        return 0

    if df is None or df.empty:
        logger.warning("ETF行情为空")
        return 0

    rows = []
    for _, row in df.iterrows():
        code_raw = str(row.get('代码', '')).strip()
        # 去掉 sh/sz 前缀
        code = code_raw
        for prefix in ('sh', 'sz', 'bj'):
            if code.startswith(prefix):
                code = code[len(prefix):]
                break

        name = str(row.get('名称', '')).strip()

        rows.append((
            code, name, trade_date,
            _safe_float(row.get('今开')),
            _safe_float(row.get('最高')),
            _safe_float(row.get('最低')),
            _safe_float(row.get('最新价')),
            _safe_int(row.get('成交量')),
            _safe_float(row.get('成交额')),
            _safe_float(row.get('涨跌幅')),
        ))

    success = batch_upsert_etf_daily_kline(rows)
    elapsed = time.time() - start
    logger.info(f"ETF日K更新完成: {success}个ETF, 耗时{elapsed:.1f}秒")
    return success


def _update_klines(today):
    """
    步骤1：更新个股+指数+ETF日K数据

    返回:
        dict: {'kline_count': int, 'index_count': int, 'etf_count': int, 'adjustment_queue': list}
    """
    # 获取昨收数据用于除权检测
    prev_close_map = _load_previous_day_close(today)

    # 更新个股日K
    data = fetch_all_market()
    kline_count, adjustment_queue = save_to_db(data, today, prev_close_map) if data else (0, [])

    # 更新指数日K
    index_count = fetch_and_save_index_daily_kline(today)

    # 更新ETF日K
    etf_count = fetch_and_save_etf_daily_kline(today)

    return {'kline_count': kline_count or 0, 'index_count': index_count or 0, 'etf_count': etf_count or 0, 'adjustment_queue': adjustment_queue}


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

        codes = get_all_codes(include_etf=True)
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


def _should_force_ti_update():
    """判断当前是否非交易时间（中午休盘、下午收盘后、节假日）"""
    now = datetime.now()
    current_time = now.time()
    current_hour = current_time.hour
    current_minute = current_time.minute

    # 周末
    if now.weekday() >= 5:
        return True

    # 15:00后收盘
    if current_hour >= 15:
        return True

    # 11:30~12:59 午休
    if current_hour == 11 and current_minute >= 30:
        return True
    if current_hour == 12:
        return True

    # 检查是否为交易日
    try:
        conn = get_db_connection()
        today = now.strftime('%Y-%m-%d')
        row = conn.execute(
            "SELECT trade_status FROM trade_calendar WHERE trade_date = ?",
            (today,)
        ).fetchone()
        conn.close()
        if row and row['trade_status'] == 0:
            return True
    except Exception:
        pass

    return False


def get_stale_indicator_codes(calc_date: str) -> list:
    """
    获取需要更新技术指标的股票代码（增量模式）
    - 返回当日未更新过的股票代码列表
    """
    try:
        conn = get_db_connection()
        try:
            # 获取所有有日K数据的股票代码
            cursor = conn.execute("""
                SELECT DISTINCT code FROM daily_kline
                WHERE date >= ?
            """, (calc_date,))
            all_codes = [row[0] for row in cursor.fetchall()]

            # 获取当日已更新的股票代码
            cursor = conn.execute("""
                SELECT DISTINCT code FROM technical_indicators
                WHERE calc_date = ?
            """, (calc_date,))
            updated_codes = set(row[0] for row in cursor.fetchall())

            # 返回未更新的代码
            stale_codes = [code for code in all_codes if code not in updated_codes]
            logger.debug(f"需要更新技术指标的股票: {len(stale_codes)}/{len(all_codes)}")
            return stale_codes
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"获取待更新股票列表失败: {e}")
        return []


def batch_update_technical_indicators(force: bool = False) -> dict:
    """
    批量更新全市场技术指标（增量模式）

    流程：
    1. 获取最新交易日
    2. 如果force=True，全量计算
    3. 否则，只计算当日未更新过的股票
    4. 批量写入technical_indicators表

    返回: {'total': 5102, 'calculated': 123, 'skipped': 4979, 'elapsed': 12.5}
    """
    from core.storage import get_daily_data_from_sqlite

    start_time = time.time()

    # 1. 获取最新交易日
    try:
        conn = get_db_connection()
        try:
            row = conn.execute("""
                SELECT trade_date FROM trade_calendar
                WHERE trade_status = 1 AND trade_date <= ?
                ORDER BY trade_date DESC LIMIT 1
            """, (datetime.now().strftime('%Y-%m-%d'),)).fetchone()
            if not row:
                logger.warning("无交易日数据，跳过技术指标更新")
                return {'total': 0, 'calculated': 0, 'skipped': 0, 'elapsed': 0}
            calc_date = row[0]
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"获取交易日失败: {e}")
        return {'total': 0, 'calculated': 0, 'skipped': 0, 'elapsed': 0}

    logger.info(f"开始批量更新技术指标: {calc_date}")

    # 2. 获取需要更新的股票代码
    if force:
        # 全量：获取所有有日K数据的股票
        conn = get_db_connection()
        try:
            cursor = conn.execute("SELECT DISTINCT code FROM daily_kline")
            codes = [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()
        logger.info(f"全量模式: 共 {len(codes)} 只股票")
    else:
        # 增量：只计算未更新的股票
        codes = get_stale_indicator_codes(calc_date)
        if not codes:
            logger.info(f"技术指标已是最新 ({calc_date})，跳过")
            return {'total': 0, 'calculated': 0, 'skipped': 0, 'elapsed': 0}

    total = len(codes)
    calculated = 0
    skipped = 0

    # 3. 逐股计算并保存
    for idx, code in enumerate(codes, 1):
        try:
            # 读取日K数据（MA250需要至少250天，SuperTrend 90天ATR需要至少100天）
            df = get_daily_data_from_sqlite(code, days=None)
            if df.empty or len(df) < 250:
                skipped += 1
                continue

            # 计算各项指标
            indicators = {'calc_date': calc_date}

            # MA
            ma_values = calculate_ma(df, [5, 10, 20, 60, 120, 250])
            indicators.update(ma_values)
            ma_slopes = calculate_ma_slope(df, [5, 10, 20])
            indicators.update(ma_slopes)

            # SuperTrend
            st_df = calculate_supertrend(df, 90, 3.0)
            atr_val = calculate_atr(df, 90)
            if not st_df.empty:
                indicators['st_direction'] = 1 if st_df['supertrend'].iloc[-1] else -1
                indicators['st_upper_band'] = float(st_df['upper_band'].iloc[-1])
                indicators['st_lower_band'] = float(st_df['lower_band'].iloc[-1])
                indicators['st_atr'] = float(atr_val.iloc[-1]) if not atr_val.empty else None

            # MACD
            macd_data = calculate_macd(df, 12, 26, 9)
            indicators['macd_dif'] = macd_data.get('dif')
            indicators['macd_dea'] = macd_data.get('dea')
            indicators['macd_histogram'] = macd_data.get('histogram')

            slope_data = calculate_macd_slope(df)
            indicators['macd_histogram_slope'] = slope_data.get('macd_histogram_slope', 0)
            indicators['macd_dif_slope'] = slope_data.get('macd_dif_slope', 0)
            indicators['macd_dea_slope'] = slope_data.get('macd_dea_slope', 0)
            indicators['macd_slope_summary'] = slope_data.get('macd_slope_summary', '→震荡')

            # RSI
            indicators['rsi_14'] = calculate_rsi(df, 14)

            # 量比
            indicators['volume_ratio_5'] = calculate_volume_ratio(df, 5)
            indicators['volume_ratio_20'] = calculate_volume_ratio(df, 20)

            # OBV能量潮
            obv_values = [0]
            close_vals = df['close'].values
            volume_vals = df['volume'].values
            for i in range(1, len(close_vals)):
                prev_obv = obv_values[-1]
                if close_vals[i] > close_vals[i-1]:
                    obv_values.append(prev_obv + volume_vals[i])
                elif close_vals[i] < close_vals[i-1]:
                    obv_values.append(prev_obv - volume_vals[i])
                else:
                    obv_values.append(prev_obv)

            indicators['obv'] = int(obv_values[-1])

            # OBV的30日均线
            if len(obv_values) >= 30:
                indicators['ma_obv'] = round(pd.Series(obv_values).rolling(30).mean().iloc[-1], 2)
            else:
                indicators['ma_obv'] = None

            # K线形态
            candle_data = identify_candle_patterns(df) or {}
            indicators['is_long_upper_shadow'] = candle_data.get('is_long_upper_shadow', 0)
            indicators['is_long_lower_shadow'] = candle_data.get('is_long_lower_shadow', 0)
            indicators['is_bullish_candle'] = candle_data.get('is_bullish_candle', 0)
            indicators['is_bearish_candle'] = candle_data.get('is_bearish_candle', 0)

            # 保存
            save_technical_indicators(code, indicators)
            calculated += 1

            # 进度日志
            if idx % 100 == 0:
                elapsed = time.time() - start_time
                logger.info(f"进度: {idx}/{total} ({calculated}计算), 耗时: {elapsed:.1f}s")

        except Exception as e:
            skipped += 1
            logger.debug(f"[{code}] 计算失败: {e}")

    elapsed = time.time() - start_time
    logger.info(f"技术指标更新完成: 总计{total}, 已计算{calculated}, 跳过{skipped}, 耗时{elapsed:.1f}s")

    return {
        'total': total,
        'calculated': calculated,
        'skipped': skipped,
        'elapsed': elapsed
    }


def run():
    # 获取当前日期
    current_date = datetime.now().strftime('%Y-%m-%d')

    # 如果今天是非交易日，使用小于等于今天的最新交易日作为上下文日期
    latest_trade = _get_latest_trading_day_le_today(current_date)
    if latest_trade and latest_trade != current_date:
        context_date = latest_trade
        logger.info(f"今天 {current_date} 为非交易日，使用上下文日期 {context_date}")
    else:
        context_date = current_date

    logger.info(f"=== 更新全市场日K — {context_date} ===")
    start_time = time.time()

    # 步骤1：更新日K数据
    kline_result = _update_klines(context_date)
    adjustment_queue = kline_result.get('adjustment_queue', [])

    # 步骤4：计算评分
    score_result = _update_scores(context_date)

    # 步骤5：批量更新全市场技术指标（增量模式，非交易时间强制全量）
    force_ti = _should_force_ti_update()
    ti_result = batch_update_technical_indicators(force=force_ti)

    # 步骤2+3：异步启动独立JOB执行通知和复权刷新（移到步骤5之后）
    if adjustment_queue and _is_market_closed():
        asyncio.run(_async_start_adjustment_job(adjustment_queue))

    logger.info(f"=== 完成: 个股+指数+评分+技术指标更新到 {context_date} ===")

    elapsed = time.time() - start_time
    elapsed_str = f"{int(elapsed//60)}分{int(elapsed%60)}秒"
    logger.info(f"⏱️ 总耗时: {elapsed_str}")

    # 返回统计结果供shell脚本通知用（不返回adjustment_queue减少数据量）
    return {
        'date': current_date,
        'context_date': context_date,
        'kline_count': kline_result.get('kline_count', 0),
        'index_count': kline_result.get('index_count', 0),
        'etf_count': kline_result.get('etf_count', 0),
        'adjustment_count': len(adjustment_queue),
        **score_result,
        'ti_total': ti_result.get('total', 0),
        'ti_calculated': ti_result.get('calculated', 0),
        'ti_skipped': ti_result.get('skipped', 0),
        'elapsed': elapsed_str,
    }


if __name__ == '__main__':
    result = run()
    # 输出统计JSON到stdout最后一行，供shell脚本解析
    print(f"RESULT_JSON:{json.dumps(result, ensure_ascii=False)}")
