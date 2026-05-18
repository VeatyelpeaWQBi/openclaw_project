"""
回测引擎核心

执行回测逻辑，协调各模块工作
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from .config import BacktestConfig
from .data_loader import DataLoader
from .signal_detector import SignalDetector, calculate_macd
from .cross_ranker import CrossRanker
from .position_manager import PositionManager
from .report_generator import BacktestResult

logger = logging.getLogger(__name__)


class BacktestEngine:
    """回测引擎"""

    def __init__(self, config: Optional[BacktestConfig] = None):
        self.config = config or BacktestConfig()
        self.data_loader = DataLoader()
        self.signal_detector = SignalDetector(self.config)
        self.cross_ranker = CrossRanker()
        self.position_manager = PositionManager(self.config.INITIAL_CAPITAL, self.config)

    def run(self) -> BacktestResult:
        """运行回测"""
        logger.info(f"=== 开始回测 ===")
        logger.info(f"时间范围: {self.config.START_DATE} ~ {self.config.END_DATE}")
        logger.info(f"初始资金: {self.config.INITIAL_CAPITAL}")
        logger.info(f"信号窗口: {self.config.SIGNAL_WINDOW}天")

        # 1. 加载交易日历
        trading_dates = self.data_loader.load_trading_dates(
            self.config.START_DATE, self.config.END_DATE
        )
        logger.info(f"交易日数量: {len(trading_dates)}")

        # 2. 加载股票列表
        stock_codes = self.data_loader.load_all_stock_codes()
        logger.info(f"股票数量: {len(stock_codes)}")

        # 3. 获取股票名称
        stock_names = self._load_stock_names(stock_codes)

        # 4. 预加载需要处理的数据范围（增加预热天数）
        extra_days = max(
            self.config.MACD_SLOW + self.config.MACD_SIGNAL,
            self.config.EMA_SLOW,
            100
        )
        from core.storage import get_trading_day_offset_from
        data_start = get_trading_day_offset_from(self.config.START_DATE, -extra_days)
        if not data_start:
            data_start = self.config.START_DATE

        logger.info(f"数据范围: {data_start} ~ {self.config.END_DATE}")

        # 5. 逐日处理
        for idx, date in enumerate(trading_dates, 1):
            self._process_day(date, stock_codes, stock_names)

            if idx % 50 == 0 or idx == len(trading_dates):
                logger.info(f"进度: {idx}/{len(trading_dates)} ({idx/len(trading_dates)*100:.1f}%)")

        # 6. 生成结果
        result = BacktestResult(
            self.position_manager.initial_capital,
            self.position_manager.get_trades(),
            self.position_manager.get_daily_portfolio_value(),
            trading_dates
        )

        logger.info("=== 回测完成 ===")
        logger.info(f"总收益率: {result.total_return:.2f}%")
        logger.info(f"年化收益率: {result.annualized_return:.2f}%")
        logger.info(f"最大回撤: {result.max_drawdown:.2f}%")
        logger.info(f"交易次数: 买入{result.buy_count}次, 卖出{result.sell_count}次")

        return result

    def _load_stock_names(self, codes: List[str]) -> Dict[str, str]:
        """加载股票名称"""
        names = {}
        for code in codes:
            info = self.data_loader.load_stock_info(code)
            if info:
                names[code] = info.get('name', '')
        return names

    def _process_day(self, date: str, stock_codes: List[str],
                      stock_names: Dict[str, str]):
        """处理单个交易日"""
        logger.debug(f"处理日期: {date}")

        # 1. 获取当日所有持仓股票的收盘价
        positions = self.position_manager.get_positions()
        current_prices: Dict[str, float] = {}

        # 2. 检查卖出信号
        to_sell = []
        for position in positions:
            code = position.code

            # 加载日K数据
            df = self.data_loader.load_daily_kline(code, position.entry_date, date)
            if df.empty or len(df) < 10:
                continue

            # 获取当前价格
            current_row = df[df['date'] == date]
            if current_row.empty:
                continue
            current_price = float(current_row['close'].iloc[0])
            current_prices[code] = current_price

            # 检查卖出信号
            if self.signal_detector.detect_sell_signal(df, code, position.entry_date):
                to_sell.append(code)

        # 执行卖出
        for code in to_sell:
            price = current_prices.get(code)
            if price:
                self.position_manager.execute_sell(code, price, date)
                logger.debug(f"[{date}] 卖出 {code} @ {price:.2f}")

        # 3. 更新投资组合价值
        # 需要重新加载所有持仓的价格
        positions = self.position_manager.get_positions()
        all_prices = {}

        # 加载所有需要查询的价格
        if positions:
            conn = self.data_loader.get_connection()
            try:
                code_list = [p.code for p in positions]
                placeholders = ','.join(['?'] * len(code_list))
                rows = conn.execute(f"""
                    SELECT code, close FROM daily_kline
                    WHERE code IN ({placeholders}) AND date = ?
                """, code_list + [date]).fetchall()

                all_prices = {r['code']: r['close'] for r in rows if r['close']}
            finally:
                conn.close()

        self.position_manager.update_portfolio_value(all_prices, date)

        # 4. 扫描买入信号
        available_cash = self.position_manager.get_available_cash()
        if available_cash < self.config.MIN_POSITION_PCT * self.config.INITIAL_CAPITAL:
            return

        buy_signals = []

        # 批量检查买入信号（分批处理避免内存过大）
        BATCH_SIZE = 100
        for i in range(0, len(stock_codes), BATCH_SIZE):
            batch_codes = stock_codes[i:i+BATCH_SIZE]

            # 批量加载日K数据
            conn = self.data_loader.get_connection()
            try:
                placeholders = ','.join(['?'] * len(batch_codes))
                rows = conn.execute(f"""
                    SELECT * FROM daily_kline
                    WHERE code IN ({placeholders}) AND date >= ? AND date <= ? AND volume > 0
                    ORDER BY code, date
                """, batch_codes + [data_start, self.config.END_DATE]).fetchall()
            finally:
                conn.close()

            # 按股票分组
            stock_data = {}
            for r in rows:
                code = r['code']
                if code not in stock_data:
                    stock_data[code] = []
                stock_data[code].append(r)

            # 检查每只股票的买入信号
            for code, kline_rows in stock_data.items():
                # 跳过已持仓
                if code in positions or code in to_sell:
                    continue

                # 构建DataFrame
                df = pd.DataFrame(kline_rows)

                # 计算MACD
                df = calculate_macd(df, self.config.MACD_FAST, self.config.MACD_SLOW, self.config.MACD_SIGNAL)

                # 检测买入信号
                signal = self.signal_detector.detect_buy_signals(df, code)
                if signal:
                    # 获取当前价格
                    current_row = df[df['date'] == date]
                    if current_row.empty:
                        continue
                    current_price = float(current_row['close'].iloc[0])
                    current_prices[code] = current_price

                    # 计算金叉级别
                    macd_rank = self.cross_ranker.calculate_rank(df, date)

                    # 计算OBV斜率
                    obv_slope = self.signal_detector.get_obv_slope(df)

                    # 计算综合评分
                    score = self.cross_ranker.calculate_score(macd_rank, obv_slope)

                    buy_signals.append({
                        'code': code,
                        'date': date,
                        'price': current_price,
                        'strength': score,
                        'macd_rank': macd_rank,
                        'obv_slope': obv_slope,
                    })

        if not buy_signals:
            return

        # 5. 分配仓位
        allocations = self.position_manager.allocate_position(buy_signals, current_prices, stock_names)

        # 6. 执行买入
        for alloc in allocations:
            code = alloc['code']
            price = alloc['price']
            quantity = alloc['quantity']
            amount = alloc['amount']

            self.position_manager.execute_buy(
                code, alloc['name'], price, quantity, amount, date,
                alloc['signal_strength'], alloc['macd_rank'], alloc['obv_slope']
            )
            logger.debug(f"[{date}] 买入 {code} {alloc['name']} @ {price:.2f} x{quantity}")