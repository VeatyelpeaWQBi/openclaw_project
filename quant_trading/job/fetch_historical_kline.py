#!/usr/bin/env python3
"""
Job: 获取A股全市场历史日K数据（2014年起，前复权）

功能：
  - 从新浪数据源获取所有个股从2014年到当前日期的前复权日K数据
  - 支持断点续传，使用 stock_info 表的 daily_kline_done 字段记录进度
  - 每只股票间隔3~5秒防止被BAN
  - 支持指定股票代码列表进行测试

用法：
  # 测试模式：获取指定股票
  python job/fetch_historical_kline.py --codes 600000,000001

  # 全量模式：获取所有股票（断点续传）
  python job/fetch_historical_kline.py

  # 全量模式：强制重新获取所有股票
  python job/fetch_historical_kline.py --force
"""

import sys
import os
import time
import random
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.paths import DB_PATH
from core.storage import get_db_connection, batch_upsert_daily_kline
from core.data_access import _sina_daily_kline

# 日志配置（移到main块内以避免路径问题）
logger = logging.getLogger(__name__)

# 配置参数
START_DATE = '2014-01-01'  # 起始日期
MIN_INTERVAL = 3.0  # 最小间隔秒数
MAX_INTERVAL = 5.0  # 最大间隔秒数


def get_all_stock_codes():
    """从 stock_info 表获取所有股票代码"""
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT code, name, daily_kline_done FROM stock_info ORDER BY code").fetchall()
        return [(r['code'], r['name'], r['daily_kline_done']) for r in rows]
    finally:
        conn.close()


def get_pending_stock_codes():
    """获取尚未完成历史日K下载的股票代码（断点续传用）"""
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT code, name FROM stock_info
            WHERE daily_kline_done = 0 OR daily_kline_done IS NULL
            ORDER BY code
        """).fetchall()
        return [(r['code'], r['name']) for r in rows]
    finally:
        conn.close()


def mark_stock_done(code):
    """标记股票的历史日K已下载完成"""
    conn = get_db_connection()
    try:
        conn.execute("""
            UPDATE stock_info SET daily_kline_done = 1, last_update_at = ?
            WHERE code = ?
        """, (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code))
        conn.commit()
    finally:
        conn.close()


def reset_all_done_status():
    """重置所有股票的下载状态（用于强制重新下载）"""
    conn = get_db_connection()
    try:
        cursor = conn.execute("UPDATE stock_info SET daily_kline_done = 0")
        conn.commit()
        return cursor.rowcount
    finally:
        conn.close()


def delete_stock_kline(code):
    """删除指定股票的所有日K数据"""
    conn = get_db_connection()
    try:
        cursor = conn.execute("DELETE FROM daily_kline WHERE code = ?", (code,))
        conn.commit()
        return cursor.rowcount
    finally:
        conn.close()


def code_to_market(code):
    """根据代码判断市场前缀"""
    if code.startswith('6'):
        return 'sh'
    elif code.startswith(('0', '3')):
        return 'sz'
    return 'sh'


def fetch_and_save_stock(code, name, start_date, end_date, force=False):
    """
    获取并保存单只股票的日K数据

    参数:
        code: 股票代码
        name: 股票名称
        start_date: 起始日期 'YYYYMMDD'
        end_date: 结束日期 'YYYYMMDD'
        force: 是否强制重新获取

    返回:
        tuple: (成功条数, 错误信息)
    """
    market = code_to_market(code)

    try:
        logger.debug(f"获取日K: {market}{code} {name} ({start_date}~{end_date})")
        df = _sina_daily_kline(code, market=market, start_date=start_date, end_date=end_date)

        if df.empty:
            return 0, "数据为空"

        # 删除旧数据（如果是强制模式）
        if force:
            deleted = delete_stock_kline(code)
            if deleted > 0:
                logger.debug(f"删除 {code} 旧数据 {deleted} 条")

        # 构建批量写入数据
        rows = []
        for _, row in df.iterrows():
            date_val = row.get('date', '')
            if hasattr(date_val, 'strftime'):
                date_str = date_val.strftime('%Y-%m-%d')
            else:
                date_str = str(date_val)[:10]

            rows.append((
                code, name, date_str,
                _safe_float(row.get('open')),
                _safe_float(row.get('high')),
                _safe_float(row.get('low')),
                _safe_float(row.get('close')),
                _safe_int(row.get('volume')),
                _safe_float(row.get('amount')),
                None,  # change_pct
                _safe_float(row.get('turnover')),
                None,  # pe_ratio
                None,  # pb_ratio
                None,  # mktcap
                None,  # nmc
                None,  # outstanding_share
                None,  # volume_ratio
            ))

        success, error = batch_upsert_daily_kline(rows)
        return success, None

    except Exception as e:
        logger.warning(f"[{code}] {name}: 获取失败 - {e}")
        return 0, str(e)


def _safe_float(val):
    """安全转换为float"""
    if val is None or (isinstance(val, str) and val.strip() in ('', '-', 'nan', 'None')):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val):
    """安全转换为int"""
    if val is None or (isinstance(val, str) and val.strip() in ('', '-', 'nan', 'None')):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def run_test(codes):
    """测试模式：获取指定股票列表"""
    logger.info(f"=== 测试模式：获取 {len(codes)} 只股票 ===")

    # 获取股票名称
    conn = get_db_connection()
    try:
        placeholders = ','.join(['?'] * len(codes))
        stock_list = conn.execute(
            f"SELECT code, name FROM stock_info WHERE code IN ({placeholders})",
            codes
        ).fetchall()
    finally:
        conn.close()

    if not stock_list:
        logger.error("未找到指定的股票代码")
        return

    today = datetime.now().strftime('%Y%m%d')
    start_sina = START_DATE.replace('-', '')
    end_sina = today

    total_success = 0
    total_error = 0

    for idx, (code, name) in enumerate(stock_list):
        success, error = fetch_and_save_stock(code, name, start_sina, end_sina, force=True)

        total_success += success
        if success > 0:
            mark_stock_done(code)  # 测试模式也标记完成

        if error:
            total_error += 1

        logger.info(f"[{idx+1}/{len(stock_list)}] {code} {name}: {success} 条" +
                   (f" - 错误: {error}" if error else ""))

        # 间隔
        if idx < len(stock_list) - 1:
            sleep_time = random.uniform(MIN_INTERVAL, MAX_INTERVAL)
            time.sleep(sleep_time)

    logger.info(f"=== 测试完成: 成功{total_success}条, 失败{total_error}只 ===")


def run_full(force=False):
    """全量模式：获取所有股票（支持断点续传）"""
    logger.info(f"=== 全量模式获取历史日K数据 ===")
    logger.info(f"起始日期: {START_DATE}")
    logger.info(f"强制重新获取: {'是' if force else '否（断点续传）'}")

    if force:
        # 强制模式：重置所有股票的下载状态
        count = reset_all_done_status()
        logger.info(f"已重置 {count} 只股票的下载状态")

    # 获取待处理的股票（断点续传）
    stock_list = get_pending_stock_codes()
    logger.info(f"待处理股票数: {len(stock_list)}")

    today = datetime.now().strftime('%Y%m%d')
    start_sina = START_DATE.replace('-', '')
    end_sina = today

    # 统计
    total_success = 0
    total_error = 0
    start_time = time.time()

    for idx, (code, name) in enumerate(stock_list):
        success, error = fetch_and_save_stock(code, name, start_sina, end_sina, force=force)

        if success > 0:
            total_success += success
            # 标记为已完成
            mark_stock_done(code)
            logger.info(f"[{idx+1}/{len(stock_list)}] {code} {name}: {success} 条")
        else:
            total_error += 1
            logger.warning(f"[{idx+1}/{len(stock_list)}] {code} {name}: 失败 - {error}")

        # 输出进度
        if (idx + 1) % 10 == 0 or (idx + 1) == len(stock_list):
            elapsed = time.time() - start_time
            speed = (idx + 1) / elapsed if elapsed > 0 else 0
            logger.info(f"进度: {idx+1}/{len(stock_list)} (成功{total_success}条，失败{total_error}只)，"
                       f"速度: {speed:.1f}只/秒，耗时: {elapsed:.0f}秒")

        # 间隔（防止被BAN）
        if idx < len(stock_list) - 1:
            sleep_time = random.uniform(MIN_INTERVAL, MAX_INTERVAL)
            time.sleep(sleep_time)

    elapsed = time.time() - start_time
    elapsed_str = f"{int(elapsed//3600)}时{int((elapsed%3600)//60)}分{int(elapsed%60)}秒"

    logger.info(f"=== 完成: 成功{total_success}条，失败{total_error}只 ===")
    logger.info(f"总耗时: {elapsed_str}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='获取A股全市场历史日K数据')
    parser.add_argument('--codes', type=str, help='指定股票代码列表，逗号分隔（测试模式）')
    parser.add_argument('--force', action='store_true', help='强制重新获取所有股票')

    args = parser.parse_args()

    # 确保日志目录存在
    os.makedirs('logs', exist_ok=True)

    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler('logs/fetch_historical_kline.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    if args.codes:
        # 测试模式
        codes = [c.strip() for c in args.codes.split(',')]
        run_test(codes)
    else:
        # 全量模式
        run_full(force=args.force)
