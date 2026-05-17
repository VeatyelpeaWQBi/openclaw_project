#!/usr/bin/env python3
"""
Job: 全量刷新历史RS、ADX、VCP、技术指标数据

功能：
  - 全量计算RS、ADX、VCP三个技术指标评分的历史数据
  - 全量计算技术指标（MA、SuperTrend、MACD、RSI、OBV、量比、K线形态）的历史数据
  - 支持分别计算或批量计算各指标
  - RS：相对强度评分，基于与基准指数的相对表现
  - ADX：平均趋向指数，衡量趋势强度
  - VCP：波动率收缩形态，识别平台整理后的突破机会
  - 技术指标：MA、SuperTrend、MACD、RSI、OBV、量比、K线形态

用法：
  # 计算所有指标（默认）
  python job/fetch_historical_indicators.py

  # 只计算RS指标
  python job/fetch_historical_indicators.py --indicators rs

  # 只计算ADX指标
  python job/fetch_historical_indicators.py --indicators adx

  # 只计算VCP指标
  python job/fetch_historical_indicators.py --indicators vcp

  # 只计算技术指标
  python job/fetch_historical_indicators.py --indicators indicators

  # 计算RS和ADX
  python job/fetch_historical_indicators.py --indicators rs,adx

  # 计算所有指标
  python job/fetch_historical_indicators.py --indicators rs,adx,vcp,indicators

  # 指定基准指数（仅RS使用）
  python job/fetch_historical_indicators.py --benchmark 000510

  # 指定ADX周期
  python job/fetch_historical_indicators.py --adx-period 14
"""

import sys
import os
import time
import logging
import argparse
from pathlib import Path

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from strategies.trend_trading.score.rs_core import calc_rs_scores_full
from strategies.trend_trading.score.adx_core import calc_adx_batch
from strategies.trend_trading.score.vcp_core import calc_vcp_batch
from strategies.trend_trading.score.indicators_core import calc_indicators_batch

# 日志配置（移到main块内以避免路径问题）
logger = logging.getLogger(__name__)

# 配置参数
DEFAULT_BENCHMARK = '000985'  # 默认基准指数（中证全指）
DEFAULT_ADX_PERIOD = 14       # 默认ADX周期


def run_full(indicators, benchmark=None, adx_period=None):
    """
    全量模式：计算指定指标的历史数据

    参数:
        indicators: list[str] 要计算的指标列表 ['rs', 'adx', 'vcp', 'indicators']
        benchmark: str 基准指数代码（仅RS使用）
        adx_period: int ADX计算周期

    返回:
        dict: 各指标的写入条数统计
    """
    if adx_period is None:
        adx_period = DEFAULT_ADX_PERIOD
    if benchmark is None:
        benchmark = DEFAULT_BENCHMARK

    results = {}
    total_start_time = time.time()

    for indicator in indicators:
        logger.info(f"\n{'='*60}")
        logger.info(f"开始计算: {indicator.upper()}")
        logger.info(f"{'='*60}")
        start_time = time.time()

        try:
            if indicator == 'rs':
                count = calc_rs_scores_full(benchmark)
                results['rs'] = count
            elif indicator == 'adx':
                count = calc_adx_batch(adx_period)
                results['adx'] = count
            elif indicator == 'vcp':
                count = calc_vcp_batch()
                results['vcp'] = count
            elif indicator == 'indicators':
                count = calc_indicators_batch()
                results['indicators'] = count
            else:
                logger.warning(f"未知指标: {indicator}，跳过")
                results[indicator] = 0
                continue

            elapsed = time.time() - start_time
            logger.info(f"{indicator.upper()} 完成: {count} 条，耗时 {elapsed:.0f}秒")

        except Exception as e:
            logger.error(f"{indicator.upper()} 计算失败: {e}")
            results[indicator] = 0

    total_elapsed = time.time() - total_start_time
    total_str = f"{int(total_elapsed//3600)}时{int((total_elapsed%3600)//60)}分{int(total_elapsed%60)}秒"

    logger.info(f"\n{'='*60}")
    logger.info(f"=== 全部完成 ===")
    logger.info(f"总耗时: {total_str}")
    for indicator, count in results.items():
        logger.info(f"  {indicator.upper()}: {count} 条")
    logger.info(f"{'='*60}")

    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='全量刷新历史RS、ADX、VCP、技术指标数据')
    parser.add_argument('--indicators', type=str, default='rs,adx,vcp,indicators',
                        help='要计算的指标，逗号分隔（rs,adx,vcp,indicators）')
    parser.add_argument('--benchmark', type=str, default=DEFAULT_BENCHMARK,
                        help=f'基准指数代码（仅RS使用，默认{DEFAULT_BENCHMARK}）')
    parser.add_argument('--adx-period', type=int, default=DEFAULT_ADX_PERIOD,
                        help=f'ADX计算周期（默认{DEFAULT_ADX_PERIOD}）')

    args = parser.parse_args()

    # 确保日志目录存在
    os.makedirs('logs', exist_ok=True)

    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler('logs/fetch_historical_indicators.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    # 解析指标列表
    indicators = [i.strip().lower() for i in args.indicators.split(',') if i.strip()]
    valid_indicators = {'rs', 'adx', 'vcp', 'indicators'}
    indicators = [i for i in indicators if i in valid_indicators]

    if not indicators:
        logger.error("没有有效的指标，请指定 rs、adx、vcp 或 indicators")
        sys.exit(1)

    logger.info(f"=== 开始全量刷新历史指标数据 ===")
    logger.info(f"计算指标: {', '.join(indicators).upper()}")
    logger.info(f"基准指数: {args.benchmark}")
    logger.info(f"ADX周期: {args.adx_period}")
    logger.info(f"{'='*60}")

    run_full(indicators, args.benchmark, args.adx_period)