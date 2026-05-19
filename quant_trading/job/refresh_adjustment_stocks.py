#!/usr/bin/env python3
"""
Job: 复权刷新 — 独立后台JOB

接收待复权股票队列，执行：
1. 前复权日K刷新（逐只从新浪API获取）
2. 全量重新计算历史技术指标（RS + ADX + VCP + 技术指标）

完成后通过QQ发送通知。

启动方式（由 update_daily_kline.py 异步调用）：
    python3 -m job.refresh_adjustment_stocks --queue-file /tmp/adjustment_queue.json
"""

import sys
import os
import time
import logging
import random
import json
import argparse
import subprocess
from datetime import datetime
import pandas as pd

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

_project_base = os.path.dirname(_project_root)
if _project_base not in sys.path:
    sys.path.insert(0, _project_base)

from core.storage import (
    get_db_connection, batch_upsert_daily_kline,
    get_watchlist_index_codes,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# QQ通知目标（与 cron_script/*.sh 保持一致）
QQ_TARGET = "25FECE7190A47B86BD5D34E7CCFD8D88"

# 除权除息检测配置
QFQ_START_DATE = '2014-01-01'
QFQ_MIN_INTERVAL = 3.0
QFQ_MAX_INTERVAL = 6.0
ADJUSTMENT_TOLERANCE = 0.01


def _is_market_closed():
    """判断当前是否已收盘（15:00后 或 中午休盘时间 11:30~12:59）"""
    now = datetime.now()
    current_time = now.time()
    current_hour = current_time.hour
    current_minute = current_time.minute

    if now.weekday() >= 5:
        return True
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
    if code.startswith('92'):
        return 'bj'
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

        conn = get_db_connection()
        conn.execute("DELETE FROM daily_kline WHERE code = ?", (code,))
        conn.commit()
        conn.close()

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
    logger.info(f"指标: RS + ADX + VCP + 技术指标(MA/SuperTrend/MACD/RSI/OBV/量比/K线形态)")

    from core.storage import get_daily_data_from_sqlite, save_adx_score, save_vcp_score
    from strategies.trend_trading.score.adx_core import _extract_adx_records, DEFAULT_PERIOD
    from strategies.trend_trading.score.vcp_core import _calc_vcp_for_stock_df
    from strategies.trend_trading.score.indicators_core import _extract_indicator_records as _extract_ti_records
    from strategies.trend_trading.score.indicators_core import _save_indicators_batch

    total_rs = 0
    total_adx = 0
    total_vcp = 0
    total_indicators = 0
    failed_codes = []

    index_codes = get_watchlist_index_codes()
    if not index_codes:
        index_codes = ['000510']

    start_time = time.time()

    for idx, code in enumerate(codes, 1):
        try:
            logger.info(f"[{idx}/{len(codes)}] 处理 {code}...")

            df = get_daily_data_from_sqlite(code, days=10000)
            if df.empty or len(df) < 250:
                logger.warning(f"  {code} 数据不足，跳过")
                failed_codes.append(code)
                continue

            # ADX
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

            # VCP
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

            # RS
            rs_count_for_code = 0
            for index_code in index_codes:
                try:
                    conn = get_db_connection()
                    conn.execute("DELETE FROM rs_score WHERE code = ? AND benchmark_code = ?",
                                (code, index_code))
                    conn.commit()
                    conn.close()

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
                                 round(r[1], 6), r[2], r[3],
                                 round(r[4], 6), round(r[5], 6),
                                 lookback, now_str)
                                for r in result if r[0] == code
                            ])
                            rs_count_for_code += 1

                except Exception as e:
                    logger.warning(f"  {code} RS({index_code})计算失败: {e}")

            if rs_count_for_code > 0:
                total_rs += rs_count_for_code
                logger.info(f"  RS: {rs_count_for_code} 条")

            # 技术指标
            try:
                conn = get_db_connection()
                conn.execute("DELETE FROM technical_indicators WHERE code = ?", (code,))
                conn.commit()
                conn.close()

                ti_records = _extract_ti_records(code, df)
                if ti_records:
                    _save_indicators_batch(ti_records)
                    total_indicators += len(ti_records)
                    logger.info(f"  技术指标: {len(ti_records)} 条")
            except Exception as e:
                logger.warning(f"  {code} 技术指标计算失败: {e}")

            if idx % 5 == 0 or idx == len(codes):
                elapsed = time.time() - start_time
                speed = idx / elapsed if elapsed > 0 else 0
                logger.info(f"  进度: {idx}/{len(codes)}, 速度: {speed:.1f}只/秒")

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
    logger.info(f"  技术指标: {total_indicators} 条")
    logger.info(f"  失败: {len(failed_codes)} 只")
    logger.info(f"  耗时: {elapsed_str}")

    if failed_codes:
        logger.warning(f"失败的股票代码: {', '.join(failed_codes[:10])}")

    return {
        'rs_count': total_rs,
        'adx_count': total_adx,
        'vcp_count': total_vcp,
        'indicators_count': total_indicators,
        'failed_count': len(failed_codes),
        'elapsed': elapsed_str,
    }


def run_adjustment(queue):
    """执行复权刷新流水线：通知 -> 复权刷新 -> 重新计算指标"""
    start_time = time.time()

    # 步骤2：通知待刷新队列
    _notify_adjustment_queue(queue)

    # 步骤3：复权刷新 + 重新计算
    success_count = 0
    score_result = {}
    if _is_market_closed():
        success_count = _adjust_dividend_stocks(queue)
        codes = [item['code'] for item in queue]
        score_result = _recalc_scores_for_codes(codes)
    else:
        logger.info("市场未收盘，跳过复权刷新")

    elapsed = time.time() - start_time
    elapsed_str = f"{int(elapsed//60)}分{int(elapsed%60)}秒"

    return {
        'total': len(queue),
        'success_count': success_count,
        'elapsed': elapsed_str,
        **score_result,
    }


def _send_qq_notification(message):
    """通过 openclaw CLI 发送QQ通知"""
    try:
        subprocess.run(
            [
                "openclaw", "message", "send",
                "--channel", "qqbot",
                "--target", QQ_TARGET,
                "--message", message,
            ],
            capture_output=True,
            check=False,
        )
        logger.info("已发送QQ通知")
    except Exception as e:
        logger.error(f"QQ通知发送失败: {e}")


def main():
    parser = argparse.ArgumentParser(description="复权刷新独立JOB")
    parser.add_argument("--queue-file", required=True, help="待复权股票队列JSON文件路径")
    args = parser.parse_args()

    # 读取队列
    with open(args.queue_file, 'r', encoding='utf-8') as f:
        queue = json.load(f)

    if not queue:
        logger.info("复权队列为空，无需执行")
        return

    date_str = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"=== 复权刷新JOB启动 — {date_str} ===")

    result = run_adjustment(queue)

    logger.info(f"=== 复权刷新JOB完成 — {date_str} ===")

    # 构建QQ通知消息
    total = result.get('total', 0)
    success = result.get('success_count', 0)
    elapsed = result.get('elapsed', '?')
    failed = result.get('failed_count', 0)

    lines = [
        f"📊 复权刷新完成 — {date_str}",
        f"",
        f"共处理 {total} 只股票",
        f"✅ 复权刷新成功: {success} 只",
    ]
    if failed > 0:
        lines.append(f"⚠️ 指标重算失败: {failed} 只")
    lines.append(f"⏱️ 耗时: {elapsed}")

    notify_msg = "\n".join(lines)
    _send_qq_notification(notify_msg)

    # 输出 RESULT_JSON 供调试
    print(f"RESULT_JSON:{json.dumps(result, ensure_ascii=False)}")


if __name__ == '__main__':
    main()
