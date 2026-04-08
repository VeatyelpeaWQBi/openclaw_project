#!/usr/bin/env python3
"""
Job: 计算RS Score
通过指定指数代码，计算该指数成分股的历史RS Score

纯本地SQLite运算，不调用任何外部API

用法：
  python3 job/calc_rs_score.py 000510

算法：
  RS_比值 = (1 + 个股250日涨跌幅) / (1 + 基准指数250日涨跌幅)
  RS_得分 = (排名序号 / 有效股票总数) * 100
"""

import sys
import os
import sqlite3
import logging
from datetime import datetime

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.paths import DB_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

LOOKBACK = 250  # 回看交易日数


def load_data(index_code):
    """
    预加载所有必要数据到内存

    返回:
        trade_days: list[str] 交易日列表（升序）
        stock_codes: list[str] 成分股代码列表
        stock_closes: dict[code] = dict[date] = close  个股收盘价
        index_closes: dict[date] = close  基准指数收盘价
    """
    conn = sqlite3.connect(DB_PATH)

    # 1. 交易日列表
    rows = conn.execute("""
        SELECT trade_date FROM trade_calendar
        WHERE trade_status = 1 AND trade_date >= '2014-01-01'
        ORDER BY trade_date ASC
    """).fetchall()
    trade_days = [r[0] for r in rows]
    logger.info(f"交易日: {len(trade_days)}天 ({trade_days[0]} ~ {trade_days[-1]})")

    # 2. 成分股列表
    rows = conn.execute("""
        SELECT DISTINCT stock_code FROM index_members
        WHERE index_code = ?
    """, (index_code,)).fetchall()
    stock_codes = [r[0] for r in rows]
    logger.info(f"成分股: {len(stock_codes)}只")

    # 3. 成分股日K数据（只加载需要的列）
    placeholders = ','.join(['?'] * len(stock_codes))
    rows = conn.execute(f"""
        SELECT code, date, close FROM daily_kline
        WHERE code IN ({placeholders})
          AND date >= '2014-01-01'
          AND volume > 0
        ORDER BY code, date
    """, stock_codes).fetchall()

    stock_closes = {}
    for code, date, close in rows:
        if code not in stock_closes:
            stock_closes[code] = {}
        stock_closes[code][date] = close
    logger.info(f"个股日K: {len(rows)}条 ({len(stock_closes)}只股票)")

    # 4. 基准指数日K数据
    rows = conn.execute("""
        SELECT date, close FROM index_daily_kline
        WHERE index_code = ?
          AND date >= '2014-01-01'
        ORDER BY date
    """, (index_code,)).fetchall()

    index_closes = {date: close for date, close in rows}
    logger.info(f"指数日K: {len(index_closes)}条 ({index_code})")

    conn.close()

    return trade_days, stock_codes, stock_closes, index_closes


def calc_rs_scores(index_code):
    """
    计算全部历史RS Score

    流程：
      1. 预加载数据
      2. 遍历交易日（从最新往前，到最早+LOOKBACK天）
      3. 每天计算所有有效成分股的RS比值
      4. 排名计算RS得分
      5. 批量写入DB
    """
    trade_days, stock_codes, stock_closes, index_closes = load_data(index_code)

    if len(trade_days) < LOOKBACK + 1:
        logger.error(f"交易日不足{LOOKBACK + 1}天，无法计算")
        return 0

    # 循环范围：从最新一天到 trade_days[LOOKBACK]（第251天起才有完整数据）
    total_days = len(trade_days)
    valid_start = LOOKBACK  # 最早可计算的日期索引

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 清除旧数据
    cursor.execute("DELETE FROM rs_score WHERE benchmark_code = ?", (index_code,))
    conn.commit()
    logger.info(f"已清除旧RS Score数据")

    total_rows = 0

    # 从最新一天往前遍历
    for i in range(total_days - 1, valid_start - 1, -1):
        today = trade_days[i]
        past_date = trade_days[i - LOOKBACK]

        # 基准指数涨跌幅
        index_today = index_closes.get(today)
        index_past = index_closes.get(past_date)
        if index_today is None or index_past is None or index_past == 0:
            continue

        benchmark_return = (index_today - index_past) / index_past
        benchmark_ratio = 1 + benchmark_return
        if benchmark_ratio == 0:
            continue

        # 计算每只有效成分股的RS比值
        valid_ratios = []  # [(code, rs_ratio, stock_return), ...]

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
            continue

        # 按rs_ratio升序排序（最弱在前，最强在后）
        valid_ratios.sort(key=lambda x: x[1])
        total_valid = len(valid_ratios)

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        batch = []

        for rank_idx, (code, rs_ratio, stock_return) in enumerate(valid_ratios):
            rs_rank = rank_idx + 1                         # 整数排名：1=最弱, N=最强
            rs_score = round(rs_rank / total_valid * 100, 2)  # 百分比得分：最弱≈0, 最强≈100

            batch.append((
                code, index_code, today,
                round(rs_ratio, 6), rs_score, rs_rank,
                round(stock_return, 6), round(benchmark_return, 6),
                LOOKBACK, now_str,
            ))

        cursor.executemany("""
            INSERT OR REPLACE INTO rs_score
            (code, benchmark_code, calc_date, rs_ratio, rs_score, rs_rank,
             stock_return, benchmark_return, lookback_days, write_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch)

        total_rows += len(batch)

        # 每100天提交一次
        if (total_days - i) % 100 == 0:
            conn.commit()
            progress = total_days - i
            logger.info(f"  进度: {progress}/{total_days - valid_start}天, 已写入{total_rows}条")

    conn.commit()
    conn.close()

    logger.info(f"计算完成: {total_rows}条RS Score")
    return total_rows


def run(index_code):
    """主入口"""
    logger.info(f"=== 计算 {index_code} 成分股RS Score ===")
    logger.info(f"回看天数: {LOOKBACK}")

    count = calc_rs_scores(index_code)

    return count


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python3 calc_rs_score.py <指数代码>")
        print("示例: python3 calc_rs_score.py 000510")
        sys.exit(1)

    code = sys.argv[1]
    run(code)
