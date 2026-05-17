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
from datetime import datetime
import pandas as pd

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.storage import (
    get_trading_day_offset_from,
    get_watchlist_index_codes,
    get_tracked_indices, batch_upsert_daily_kline,
    batch_upsert_index_daily_kline,
    batch_upsert_etf_daily_kline,
    get_recent_trade_dates, get_avg_volume_by_code,
    get_db_connection,
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

# 除权除息检测配置
QFQ_START_DATE = '2014-01-01'  # 复权刷新起始日期
QFQ_MIN_INTERVAL = 3.0  # 最小刷新间隔（秒）
QFQ_MAX_INTERVAL = 6.0  # 最大刷新间隔（秒）

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


def _notify_adjustment_queue(queue):
    """输出待刷新股票列表"""
    if not queue:
        return

    logger.info("=" * 60)
    logger.info(f"🔔 除权除息检测完成: {len(queue)}只股票待复权刷新")
    logger.info("=" * 60)

    for i, item in enumerate(queue[:20], 1):
        code, name, diff_pct = item['code'], item['name'], item['diff_pct']
        logger.info(f"  {i:2d}. {code} {name}: 差异{diff_pct:.1f}%")

    if len(queue) > 20:
        logger.info(f"  ... 还有{len(queue)-20}只股票")

    logger.info("=" * 60)


def _code_to_market(code):
    """根据代码判断市场"""
    if code.startswith(('6', '9')):
        return 'sh'
    return 'sz'


def _refresh_single_stock_qfq(code, name):
    """刷新单只股票2014年至今的前复权日K"""
    market = _code_to_market(code)

    try:
        from core.data_access import _sina_daily_kline

        df = _sina_daily_kline(
            code, market=market,
            start_date=QFQ_START_DATE.replace('-', ''),
            end_date=datetime.now().strftime('%Y%m%d')
        )

        if df.empty:
            return False

        # 删除旧数据
        conn = get_db_connection()
        conn.execute("DELETE FROM daily_kline WHERE code = ?", (code,))
        conn.commit()
        conn.close()

        # 写入前复权数据
        rows = []
        for _, row in df.iterrows():
            date_str = row['date'].strftime('%Y-%m-%d')
            rows.append((
                code, name, date_str,
                float(row['open']) if pd.notna(row['open']) else None,
                float(row['high']) if pd.notna(row['high']) else None,
                float(row['low']) if pd.notna(row['low']) else None,
                float(row['close']) if pd.notna(row['close']) else None,
                int(row['volume']) if pd.notna(row['volume']) else None,
                float(row['amount']) if pd.notna(row['amount']) else None,
                None, None, None, None, None, None, None, None
            ))

        success, _ = batch_upsert_daily_kline(rows)
        return success > 0

    except Exception as e:
        logger.error(f"刷新 {code} 失败: {e}")
        return False


def _adjust_dividend_stocks(queue):
    """批量刷新待复权股票的前复权日K（盘后执行）"""
    if not queue or not _is_market_closed():
        return 0

    logger.info(f"开始刷新 {len(queue)} 只股票的前复权日K...")

    success_count = 0
    for i, item in enumerate(queue, 1):
        code, name = item['code'], item['name']
        if _refresh_single_stock_qfq(code, name):
            success_count += 1
        else:
            logger.warning(f"  [{i}/{len(queue)}] {code} {name} 刷新失败")

        # 间隔3~6秒防封
        time.sleep(QFQ_MIN_INTERVAL + random.random() * (QFQ_MAX_INTERVAL - QFQ_MIN_INTERVAL))

        if i % 10 == 0:
            logger.info(f"  进度: {i}/{len(queue)}, 成功{success_count}")

    logger.info(f"复权刷新完成: {success_count}/{len(queue)} 只")
    return success_count


def _recalc_scores_for_codes(codes):
    """
    重新计算指定股票的全量历史技术指标

    除权除息后，复权价格变化会影响所有历史指标，需要全量重新计算
    """
    if not codes:
        return

    logger.info(f"开始全量刷新 {len(codes)} 只股票的历史技术指标...")
    logger.info(f"指标: RS + ADX + VCP")

    from core.storage import get_daily_data_from_sqlite, save_adx_score, save_vcp_score
    from strategies.trend_trading.score.adx_core import _extract_adx_records, DEFAULT_PERIOD
    from strategies.trend_trading.score.vcp_core import _calc_vcp_for_stock_df

    total_rs = 0
    total_adx = 0
    total_vcp = 0
    failed_codes = []

    # 获取基准指数列表
    index_codes = get_watchlist_index_codes()
    if not index_codes:
        index_codes = ['000510']

    start_time = time.time()

    for idx, code in enumerate(codes, 1):
        try:
            logger.info(f"[{idx}/{len(codes)}] 处理 {code}...")

            # 1. 获取股票历史日K数据（使用足够大的天数获取全部）
            df = get_daily_data_from_sqlite(code, days=10000)
            if df.empty or len(df) < 250:
                logger.warning(f"  {code} 数据不足，跳过")
                failed_codes.append(code)
                continue

            # 2. ADX 全量刷新
            try:
                conn = get_db_connection()
                conn.execute("DELETE FROM adx_score WHERE code = ?", (code,))
                conn.commit()
                conn.close()

                adx_records = _extract_adx_records(code, df, DEFAULT_PERIOD)
                if adx_records:
                    save_adx_score(adx_records)
                    total_adx += len(adx_records)
                    logger.info(f"  ADX: {len(adx_records)} 条")
            except Exception as e:
                logger.warning(f"  {code} ADX计算失败: {e}")

            # 3. VCP 全量刷新
            try:
                conn = get_db_connection()
                conn.execute("DELETE FROM vcp_score WHERE code = ?", (code,))
                conn.commit()
                conn.close()

                vcp_records = _calc_vcp_for_stock_df(code, df)
                if vcp_records:
                    save_vcp_score(vcp_records)
                    total_vcp += len(vcp_records)
                    logger.info(f"  VCP: {len(vcp_records)} 条")
            except Exception as e:
                logger.warning(f"  {code} VCP计算失败: {e}")

            # 4. RS 全量刷新（需要对每个基准指数计算）
            rs_count_for_code = 0
            for index_code in index_codes:
                try:
                    # 删除该股票在这个基准指数下的RS数据
                    conn = get_db_connection()
                    conn.execute("DELETE FROM rs_score WHERE code = ? AND benchmark_code = ?",
                                (code, index_code))
                    conn.commit()
                    conn.close()

                    # 计算RS
                    from strategies.trend_trading.score.rs_core import (
                        calc_rs_for_date, get_all_trade_dates, get_index_members,
                        load_index_closes, load_stock_closes
                    )

                    trade_days = get_all_trade_dates()
                    stock_codes = get_index_members(index_code)
                    if code not in stock_codes:
                        continue

                    lookback = 250
                    start_date = trade_days[0]
                    end_date = trade_days[-1]

                    index_closes = load_index_closes(index_code, start_date, end_date)
                    stock_closes = load_stock_closes([code], start_date, end_date)

                    if not index_closes or code not in stock_closes:
                        continue

                    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    # 遍历所有可计算的日期
                    for calc_idx in range(lookback, len(trade_days)):
                        calc_date = trade_days[calc_idx]
                        past_date = trade_days[calc_idx - lookback]

                        result = calc_rs_for_date(
                            stock_closes, index_closes, [code], calc_date, past_date
                        )

                        if result:
                            from core.storage import batch_upsert_rs_score
                            batch_upsert_rs_score([
                                (code, index_code, calc_date,
                                 round(r['rs_ratio'], 6), r['rs_score'], r['rs_rank'],
                                 round(r['stock_return'], 6), round(r['benchmark_return'], 6),
                                 lookback, now_str)
                                for r in result if r[0] == code
                            ])
                            rs_count_for_code += 1

                except Exception as e:
                    logger.warning(f"  {code} RS({index_code})计算失败: {e}")

            if rs_count_for_code > 0:
                total_rs += rs_count_for_code
                logger.info(f"  RS: {rs_count_for_code} 条")

            # 输出进度
            if idx % 5 == 0 or idx == len(codes):
                elapsed = time.time() - start_time
                speed = idx / elapsed if elapsed > 0 else 0
                logger.info(f"  进度: {idx}/{len(codes)}, 速度: {speed:.1f}只/秒")

            # 间隔防止CPU占用过高
            time.sleep(0.1)

        except Exception as e:
            logger.error(f"  {code} 处理失败: {e}")
            failed_codes.append(code)

    elapsed = time.time() - start_time
    elapsed_str = f"{int(elapsed//60)}分{int(elapsed%60)}秒"

    logger.info(f"技术指标全量刷新完成:")
    logger.info(f"  RS: {total_rs} 条")
    logger.info(f"  ADX: {total_adx} 条")
    logger.info(f"  VCP: {total_vcp} 条")
    logger.info(f"  失败: {len(failed_codes)} 只")
    logger.info(f"  耗时: {elapsed_str}")

    if failed_codes:
        logger.warning(f"失败的股票代码: {', '.join(failed_codes[:10])}")


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

    # 步骤2：通知待刷新队列
    _notify_adjustment_queue(adjustment_queue)

    # 步骤3：如果收盘后（或中午休盘时间），执行复权刷新
    if adjustment_queue and _is_market_closed():
        _adjust_dividend_stocks(adjustment_queue)
        _recalc_scores_for_codes([item['code'] for item in adjustment_queue])

    # 步骤4：计算评分
    score_result = _update_scores(context_date)

    logger.info(f"=== 完成: 个股+指数+评分更新到 {context_date} ===")

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
        'elapsed': elapsed_str,
    }


if __name__ == '__main__':
    result = run()
    # 输出统计JSON到stdout最后一行，供shell脚本解析
    print(f"RESULT_JSON:{json.dumps(result, ensure_ascii=False)}")
