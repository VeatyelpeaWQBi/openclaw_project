"""
T+1回测引擎
基于日K/分钟线数据的尾盘买入次日卖出回测
"""

import pandas as pd
import numpy as np
import os
import sys
import json
from datetime import datetime, timedelta

# 回测引擎独立模块
sys.path.append(os.path.dirname(__file__))
from data_loader import MinuteDataLoader
from sell_strategy import SellStrategyEngine
from paths import DATA_DIR as _DEFAULT_DATA_DIR, MINUTE_DATA_DIR as _DEFAULT_MINUTE_DIR


class BacktestEngine:
    """T+1回测引擎"""

    def __init__(self, config=None):
        config = config or {}
        data_dir = config.get('data_dir', _DEFAULT_DATA_DIR)
        minute_dir = config.get('minute_data_dir', _DEFAULT_MINUTE_DIR)
        self.loader = MinuteDataLoader(data_dir, minute_dir)
        self.sell_engine = SellStrategyEngine(config.get('sell_strategy', {}))
        self.initial_capital = config.get('initial_capital', 100000)
        self.position_size = config.get('position_size', 0.2)  # 每次用20%资金
        self.results = []
        self.daily_returns = []

    def run_backtest(self, buy_signals, use_minute_data=False):
        """
        运行回测

        参数:
            buy_signals: list of dict
                {
                    'date': 'YYYY-MM-DD' 或 'YYYYMMDD',
                    'code': '000001',
                    'name': '平安银行',
                    'buy_price': 10.5,  # 尾盘买入价（可选，默认用日K收盘价）
                    'sector': '银行'     # 板块（可选）
                }
            use_minute_data: 是否使用分钟线数据（更精确的卖出价）

        返回:
            list[dict]: 每笔交易的结果
        """
        if not buy_signals:
            print("⚠️ 没有买入信号，跳过回测")
            return []

        # 获取所有交易日
        all_dates = self.loader.get_trade_dates()
        if not all_dates:
            print("⚠️ 未找到交易日数据")
            return []

        # 按日期排序买入信号
        signals_by_date = {}
        for sig in buy_signals:
            date_str = self.loader.str_to_datefmt(sig['date'])
            signals_by_date.setdefault(date_str, []).append(sig)

        self.results = []
        self.daily_returns = []
        capital = self.initial_capital
        peak_capital = capital
        max_drawdown = 0

        for date_str in sorted(signals_by_date.keys()):
            signals = signals_by_date[date_str]
            next_date = self.loader.get_next_trade_date(date_str, all_dates)

            if next_date is None:
                print(f"  {date_str}: 无下一个交易日，跳过")
                continue

            for sig in signals:
                code = sig['code']
                name = sig.get('name', '')

                # 获取买入日日K数据
                buy_day_data = self.loader.get_daily_data(code, name)
                if buy_day_data.empty:
                    continue

                # 确定买入价格
                buy_price = sig.get('buy_price', None)
                if buy_price is None:
                    # 用买入日收盘价
                    buy_day_row = buy_day_data[
                        buy_day_data['date'].dt.strftime('%Y-%m-%d') == date_str
                    ]
                    if buy_day_row.empty:
                        continue
                    buy_price = float(buy_day_row.iloc[0]['close'])

                # === 卖出逻辑 ===
                sell_price = None
                sell_reason = None
                sell_time = None

                if use_minute_data:
                    # 使用分钟线数据
                    buy_date_raw = self.loader.date_to_str(date_str)
                    next_date_raw = self.loader.date_to_str(next_date)

                    minute_data = self.loader.get_minute_data(code, name, next_date_raw)
                    if minute_data is not None:
                        sell_result = self.sell_engine.find_sell_point(
                            buy_price, minute_data, buy_date_raw, next_date_raw
                        )
                        if sell_result:
                            sell_price = sell_result['sell_price']
                            sell_reason = sell_result['reason']
                            sell_time = sell_result['sell_time']

                if sell_price is None:
                    # Fallback: 用次日开盘价（日K的open）模拟09:45卖出
                    next_day_data = buy_day_data[
                        buy_day_data['date'].dt.strftime('%Y-%m-%d') == next_date
                    ]
                    if next_day_data.empty:
                        # 尝试用收盘价
                        next_day_data = buy_day_data[
                            buy_day_data['date'].dt.strftime('%Y-%m-%d') <= next_date
                        ].tail(1)
                        if next_day_data.empty:
                            continue

                    next_row = next_day_data.iloc[0]
                    # 模拟09:45价格：用 (open + close) / 2 近似
                    sell_price = (float(next_row['open']) + float(next_row['close'])) / 2
                    sell_reason = 'approx_open_close_avg'
                    sell_time = '09:45'

                    # 检查止损止盈
                    day_high = float(next_row['high'])
                    day_low = float(next_row['low'])
                    pnl_with_high = (day_high - buy_price) / buy_price
                    pnl_with_low = (day_low - buy_price) / buy_price

                    if pnl_with_low <= self.sell_engine.stop_loss:
                        sell_price = buy_price * (1 + self.sell_engine.stop_loss)
                        sell_reason = 'stop_loss (approx)'
                    elif pnl_with_high >= self.sell_engine.take_profit:
                        sell_price = buy_price * (1 + self.sell_engine.take_profit)
                        sell_reason = 'take_profit (approx)'

                # 计算收益
                return_pct = (sell_price - buy_price) / buy_price
                trade_capital = capital * self.position_size
                trade_pnl = trade_capital * return_pct
                capital += trade_pnl

                # 跟踪最大回撤
                if capital > peak_capital:
                    peak_capital = capital
                dd = (peak_capital - capital) / peak_capital
                if dd > max_drawdown:
                    max_drawdown = dd

                trade = {
                    'code': code,
                    'name': name,
                    'sector': sig.get('sector', ''),
                    'buy_date': date_str,
                    'sell_date': next_date,
                    'buy_price': round(buy_price, 2),
                    'sell_price': round(sell_price, 2),
                    'return_pct': round(return_pct * 100, 2),
                    'pnl_amount': round(trade_pnl, 2),
                    'capital_after': round(capital, 2),
                    'sell_reason': sell_reason,
                    'sell_time': sell_time,
                }
                self.results.append(trade)

                self.daily_returns.append({
                    'date': next_date,
                    'return': return_pct,
                    'capital': capital
                })

        print(f"\n📊 回测完成: {len(self.results)} 笔交易")
        return self.results

    def run_single_trade(self, code, name, buy_date, buy_price=None, use_minute_data=False):
        """
        运行单笔交易回测

        参数:
            code: 股票代码
            name: 股票名称
            buy_date: 买入日期
            buy_price: 买入价格（可选，默认用日K收盘价）
            use_minute_data: 是否使用分钟线

        返回:
            dict: 交易结果
        """
        signals = [{
            'date': buy_date,
            'code': code,
            'name': name,
            'buy_price': buy_price
        }]
        results = self.run_backtest(signals, use_minute_data)
        return results[0] if results else None

    def get_results_df(self):
        """获取结果DataFrame"""
        return pd.DataFrame(self.results) if self.results else pd.DataFrame()

    def get_daily_returns_df(self):
        """获取每日收益DataFrame"""
        return pd.DataFrame(self.daily_returns) if self.daily_returns else pd.DataFrame()


def run_backtest_from_signals(signals_file, config=None):
    """
    从信号文件运行回测

    参数:
        signals_file: 信号CSV文件路径
        config: 回测配置

    返回:
        BacktestEngine: 回测引擎实例
    """
    if not os.path.exists(signals_file):
        print(f"⚠️ 信号文件不存在: {signals_file}")
        return None

    df = pd.read_csv(signals_file)
    if df.empty:
        print("⚠️ 信号文件为空")
        return None

    # 转换为买入信号格式
    buy_signals = []
    for _, row in df.iterrows():
        sig = {
            'date': str(row.get('date', row.get('buy_date', ''))),
            'code': str(row.get('code', '')),
            'name': str(row.get('name', '')),
            'buy_price': row.get('buy_price', row.get('price', None)),
            'sector': row.get('sector', row.get('所属板块', '')),
        }
        if sig['code'] and sig['date']:
            # 转换 buy_price
            if sig['buy_price'] is not None:
                try:
                    sig['buy_price'] = float(sig['buy_price'])
                except (ValueError, TypeError):
                    sig['buy_price'] = None
            buy_signals.append(sig)

    engine = BacktestEngine(config)
    engine.run_backtest(buy_signals, use_minute_data=config.get('use_minute_data', False))
    return engine


if __name__ == '__main__':
    # 测试：手动构造几笔交易
    print("=== T+1回测引擎测试 ===\n")

    # 列出已有数据的股票
    loader = MinuteDataLoader(_DEFAULT_DATA_DIR)
    stocks = loader.get_stock_list()
    print(f"已有日K数据的股票: {len(stocks)} 只")
    for s in stocks[:5]:
        print(f"  {s['code']} {s['name']}")

    dates = loader.get_trade_dates()
    print(f"\n可用交易日: {len(dates)} 天")
    if dates:
        print(f"  最早: {dates[0]}, 最晚: {dates[-1]}")

    # 用最近的交易日做测试
    if len(dates) >= 2 and stocks:
        test_code = stocks[0]['code']
        test_name = stocks[0]['name']
        test_date = dates[-2]  # 倒数第二个交易日

        print(f"\n测试单笔交易: {test_name}({test_code}) 买入日: {test_date}")

        engine = BacktestEngine()
        result = engine.run_single_trade(test_code, test_name, test_date)
        if result:
            print(f"  买入价: {result['buy_price']}")
            print(f"  卖出价: {result['sell_price']}")
            print(f"  收益率: {result['return_pct']}%")
            print(f"  卖出原因: {result['sell_reason']}")
        else:
            print("  交易未能执行")
