#!/usr/bin/env python3
"""
Job: 批量计算历史 VCP 波动收缩评分

对数据库中所有有日K数据的股票，从第104根K线开始逐日顺推计算VCP评分，
写入 vcp_score 表。

用法：
  python job/calc_vcp_score.py

算法：
  每只股票取全部日K，从 index=103（第104天）开始，
  每次取最近104天（90+14）的窗口数据，调用 analyze_vcp 计算评分。
"""

import sys
import os
import sqlite3
import logging
import time
from datetime import datetime

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.paths import DB_PATH
from core.storage import save_vcp_score
from strategies.trend_trading.vcp_zigzag import analyze_vcp
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

WINDOW = 104  # 90天分析窗口 + 14天ATR计算


def get_all_stock_codes():
    """从 daily_kline 获取所有有数据的股票代码"""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT DISTINCT code FROM daily_kline
        WHERE volume > 0
        ORDER BY code
    """).fetchall()
    conn.close()
    return [r[0] for r in rows]


def load_kline(code):
    """加载单只股票全部日K数据，返回 DataFrame"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        """SELECT date, open, high, low, close, volume
           FROM daily_kline
           WHERE code = ? AND volume > 0
           ORDER BY date""",
        conn, params=(code,)
    )
    conn.close()
    return df


def calc_vcp_for_stock(code):
    """
    对单只股票计算全部历史VCP评分

    返回:
        list[dict]: 评分记录列表
    """
    df = load_kline(code)
    n = len(df)
    if n < WINDOW:
        return []

    records = []
    for i in range(WINDOW - 1, n):
        df_window = df.iloc[i - WINDOW + 1 : i + 1]
        try:
            result = analyze_vcp(df_window)
        except Exception:
            continue

        d = result.get('details', {})
        records.append({
            'code': code,
            'calc_date': str(df_window['date'].iloc[-1])[:10],
            'score': result['score'],
            'score_compression': d.get('compression', {}).get('得分'),
            'score_contraction': d.get('contraction_quality', {}).get('得分'),
            'score_credibility': d.get('shape_credibility', {}).get('得分'),
            'score_swing_count': d.get('swing_count', {}).get('得分'),
            'score_volume': d.get('volume_dryup', {}).get('得分'),
            'score_triangle_type': d.get('triangle_bonus', {}).get('得分'),
            'data_start': str(df_window['date'].iloc[0])[:10],
            'data_end': str(df_window['date'].iloc[-1])[:10],
        })

    return records


def run():
    """主入口：批量计算全部股票的历史VCP评分"""
    logger.info(f"=== 批量计算历史VCP评分 ===")
    logger.info(f"窗口大小: {WINDOW}天")

    codes = get_all_stock_codes()
    logger.info(f"共 {len(codes)} 只股票")

    total_records = 0
    start_time = time.time()

    for idx, code in enumerate(codes):
        records = calc_vcp_for_stock(code)
        if records:
            save_vcp_score(records)
            total_records += len(records)

        # 每100只股票输出进度
        if (idx + 1) % 100 == 0 or idx == len(codes) - 1:
            elapsed = time.time() - start_time
            speed = (idx + 1) / elapsed if elapsed > 0 else 0
            logger.info(f"  进度: {idx + 1}/{len(codes)} 只, "
                        f"已写入 {total_records} 条, "
                        f"速度: {speed:.1f}只/秒")

    elapsed = time.time() - start_time
    logger.info(f"完成! 共 {len(codes)} 只股票, {total_records} 条记录, 耗时 {elapsed:.0f}秒")
    return total_records


if __name__ == '__main__':
    run()
