"""
RS (相对强度) 核心计算模块

算法：
  RS_比值 = (1 + 个股250日涨跌幅) / (1 + 基准指数250日涨跌幅)
  RS_得分 = (排名序号 / 有效股票总数) * 100

支持模式：
  - 全量刷新：calc_rs_scores_full(index_code)
  - 近日增量：calc_rs_scores_recent(index_code, end_date, days)

用法：
  from strategies.trend_trading.score.rs_core import (
      calc_rs_scores_full, calc_rs_scores_recent
  )
"""

import logging
from datetime import datetime

from core.storage import (
    get_db_connection,
    batch_upsert_rs_score,
)
from strategies.trend_trading.score._base import (
    get_all_trade_dates,
    get_recent_trade_dates,
    get_index_members,
    load_index_closes,
    load_stock_closes,
)

logger = logging.getLogger(__name__)

DEFAULT_LOOKBACK = 250  # 回看交易日数


def calc_rs_for_date(stock_closes, index_closes, stock_codes,
                     today, past_date):
    """
    计算单个交易日的所有成分股RS评分

    返回:
        list[tuple]: [(code, rs_ratio, rs_score, rs_rank, stock_return, benchmark_return), ...]
        或 None（数据不足时）
    """
    index_today = index_closes.get(today)
    index_past = index_closes.get(past_date)
    if index_today is None or index_past is None or index_past == 0:
        return None

    benchmark_return = (index_today - index_past) / index_past
    benchmark_ratio = 1 + benchmark_return
    if benchmark_ratio == 0:
        return None

    valid_ratios = []
    for code in stock_codes:
        closes = stock_closes.get(code, {})
        stock_today = closes.get(today)
        stock_past = closes.get(past_date)
        if stock_today is None or stock_past is None or stock_past == 0:
            continue
        stock_return = (stock_today - stock_past) / stock_past
        rs_ratio = (1 + stock_return) / benchmark_ratio
        valid_ratios.append((code, rs_ratio, stock_return))

    if not valid_ratios:
        return None

    valid_ratios.sort(key=lambda x: x[1])
    total_valid = len(valid_ratios)

    results = []
    for rank_idx, (code, rs_ratio, stock_return) in enumerate(valid_ratios):
        rs_rank = rank_idx + 1
        rs_score = round(rs_rank / total_valid * 100, 2)
        results.append((code, rs_ratio, rs_score, rs_rank, stock_return, benchmark_return))

    return results


def _calc_rs_core(trade_days, stock_codes, stock_closes, index_closes,
                  calc_dates, lookback, index_code):
    """
    核心计算逻辑（全量/增量共用）

    参数:
        trade_days: 全量交易日列表（用于 past_date 查找）
        stock_codes: 成分股列表
        stock_closes: 个股收盘价
        index_closes: 指数收盘价
        calc_dates: 需要计算的日期列表
        lookback: 回看天数
        index_code: 指数代码

    返回:
        int: 写入条数
    """
    if not calc_dates:
        return 0

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    total_rows = 0

    # 预构建日期→索引映射，将查找从O(N)降为O(1)
    date_to_idx = {d: i for i, d in enumerate(trade_days)}

    for calc_date in calc_dates:
        # 从 trade_days 中找到 calc_date 前 lookback 个交易日
        idx = date_to_idx.get(calc_date, -1)
        if idx < lookback:
            continue

        past_date = trade_days[idx - lookback]

        results = calc_rs_for_date(stock_closes, index_closes, stock_codes,
                                   calc_date, past_date)
        if results is None:
            continue

        batch = [
            (code, index_code, calc_date,
             round(rs_ratio, 6), rs_score, rs_rank,
             round(stock_return, 6), round(benchmark_return, 6),
             lookback, now_str)
            for code, rs_ratio, rs_score, rs_rank, stock_return, benchmark_return in results
        ]

        batch_upsert_rs_score(batch)
        total_rows += len(batch)

    return total_rows


def calc_rs_scores_full(index_code, lookback=DEFAULT_LOOKBACK):
    """
    全量刷新：计算指定指数成分股的全部历史RS Score

    参数:
        index_code: 指数代码
        lookback: 回看天数

    返回:
        int: 写入总条数
    """
    logger.info(f"[RS] 全量刷新: {index_code} (lookback={lookback})")

    trade_days = get_all_trade_dates()
    if len(trade_days) < lookback + 1:
        logger.error(f"交易日不足{lookback + 1}天，无法计算")
        return 0

    stock_codes = get_index_members(index_code)
    if not stock_codes:
        logger.error(f"未找到 {index_code} 的成分股")
        return 0
    logger.info(f"成分股: {len(stock_codes)}只, 交易日: {len(trade_days)}天")

    start_date = trade_days[0]
    end_date = trade_days[-1]
    index_closes = load_index_closes(index_code, start_date, end_date)
    stock_closes = load_stock_closes(stock_codes, start_date, end_date)
    logger.info(f"数据加载完成: 指数{len(index_closes)}天, 个股{len(stock_closes)}只")

    # 清除旧数据
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM rs_score WHERE benchmark_code = ?", (index_code,))
        conn.commit()
        logger.info("旧RS Score数据已清除")
    finally:
        conn.close()

    # 需要计算的日期：从 trade_days[lookback] 开始
    calc_dates = trade_days[lookback:]
    count = _calc_rs_core(trade_days, stock_codes, stock_closes, index_closes,
                          calc_dates, lookback, index_code)

    logger.info(f"[RS] 全量完成: {count} 条")
    return count


def calc_rs_scores_recent(index_code, end_date, days=30,
                          lookback=DEFAULT_LOOKBACK):
    """
    近日增量刷新：计算最近N个交易日的RS Score

    自动从交易日历获取预热数据（end_date前 lookback 个交易日），
    然后计算 [end_date前days天, end_date] 范围内的RS评分。

    参数:
        index_code: 指数代码
        end_date: 结束日期 'YYYY-MM-DD'（通常是最新交易日）
        days: 要刷新的交易日数（默认30）
        lookback: 回看天数

    返回:
        int: 写入条数
    """
    logger.info(f"[RS] 近日刷新: {index_code}, 最近{days}天到 {end_date}")

    stock_codes = get_index_members(index_code)
    if not stock_codes:
        logger.error(f"未找到 {index_code} 的成分股")
        return 0

    # 1. 获取完整日期范围：预热(lookback) + 计算(days)
    all_dates = get_recent_trade_dates(end_date, lookback + days)
    if len(all_dates) < lookback + 1:
        logger.error(f"交易日不足: 需要{lookback + days}天，实际{len(all_dates)}天")
        return 0

    # 2. 分离预热期和计算期
    # all_dates 前 lookback 天仅用于预热（提供 past_date），后 days 天才是计算目标
    calc_dates = all_dates[lookback:]
    data_start = all_dates[0]

    # 3. 加载数据
    index_closes = load_index_closes(index_code, data_start, end_date)
    stock_closes = load_stock_closes(stock_codes, data_start, end_date)
    logger.info(f"数据加载: 指数{len(index_closes)}天, 个股{len(stock_closes)}只, "
                f"预热{lookback}天, 计算{len(calc_dates)}天")

    # 5. 删除旧数据
    conn = get_db_connection()
    try:
        for d in calc_dates:
            conn.execute("DELETE FROM rs_score WHERE benchmark_code = ? AND calc_date = ?",
                         (index_code, d))
        conn.commit()
    finally:
        conn.close()

    # 6. 计算（使用 all_dates 作为 trade_days 以支持 past_date 查找）
    count = _calc_rs_core(all_dates, stock_codes, stock_closes, index_closes,
                          calc_dates, lookback, index_code)

    logger.info(f"[RS] 近日完成: {count} 条")
    return count


def calc_rs_scores_from_data(index_code, stock_closes, index_closes,
                             all_dates, days, lookback=DEFAULT_LOOKBACK):
    """
    从预加载数据计算RS评分（由 calc_scores.py 统一调度调用）

    参数:
        index_code: 基准指数代码
        stock_closes: {code: {date: close}} 预加载的个股收盘价
        index_closes: {date: close} 预加载的指数收盘价
        all_dates: list[str] 指数日期列表（升序），作为全局时间轴
        days: 计算天数
        lookback: 回看天数

    返回:
        int: 写入条数
    """
    if len(all_dates) < lookback + days:
        logger.error(f"[RS] 日期不足: 需要{lookback + days}天，实际{len(all_dates)}天")
        return 0

    calc_dates = all_dates[-days:]
    stock_codes = list(stock_closes.keys())

    logger.info(f"[RS] 从预加载数据计算: {index_code}, {len(calc_dates)}天, "
                f"{len(stock_codes)}只成分股")

    # 删除旧数据
    conn = get_db_connection()
    try:
        for d in calc_dates:
            conn.execute("DELETE FROM rs_score WHERE benchmark_code = ? AND calc_date = ?",
                         (index_code, d))
        conn.commit()
    finally:
        conn.close()

    count = _calc_rs_core(all_dates, stock_codes, stock_closes, index_closes,
                          calc_dates, lookback, index_code)

    logger.info(f"[RS] 完成: {count} 条")
    return count


def calc_single_rs(stock_code, benchmark_code, calc_date, lookback=DEFAULT_LOOKBACK):
    """
    计算单只股票在某日的RS评分

    返回:
        dict or None: {rs_ratio, rs_score, stock_return, benchmark_return}
    """
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT trade_date FROM trade_calendar
            WHERE trade_status = 1 AND trade_date < ?
            ORDER BY trade_date DESC LIMIT 1 OFFSET ?
        """, (calc_date, lookback)).fetchone()
        if not row:
            return None
        past_date = row['trade_date']
    finally:
        conn.close()

    index_closes = load_index_closes(benchmark_code, past_date, calc_date)
    stock_closes_map = load_stock_closes([stock_code], past_date, calc_date)

    index_today = index_closes.get(calc_date)
    index_past = index_closes.get(past_date)
    closes = stock_closes_map.get(stock_code, {})
    stock_today = closes.get(calc_date)
    stock_past = closes.get(past_date)

    if any(v is None for v in [index_today, index_past, stock_today, stock_past]):
        return None
    if index_past == 0 or stock_past == 0:
        return None

    assert index_today is not None and index_past is not None
    assert stock_today is not None and stock_past is not None

    benchmark_return = (index_today - index_past) / index_past
    benchmark_ratio = 1 + benchmark_return
    if benchmark_ratio == 0:
        return None

    stock_return = (stock_today - stock_past) / stock_past
    rs_ratio = (1 + stock_return) / benchmark_ratio

    return {
        'rs_ratio': round(rs_ratio, 6),
        'stock_return': round(stock_return, 6),
        'benchmark_return': round(benchmark_return, 6),
        'lookback': lookback,
    }
