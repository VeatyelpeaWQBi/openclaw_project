"""
仓位管理模块

管理仓位、资金分配和交易执行
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

from .config import BacktestConfig

logger = logging.getLogger(__name__)


class Position:
    """持仓信息"""

    def __init__(self, code: str, name: str, entry_date: str,
                 entry_price: float, quantity: int, amount: float,
                 entry_dif: float = None):
        self.code = code
        self.name = name
        self.entry_date = entry_date
        self.entry_price = entry_price
        self.quantity = quantity
        self.amount = amount
        self.entry_dif = entry_dif

    @property
    def market_value(self) -> float:
        """当前市值（需要外部传入当前价格）"""
        return 0.0


class Trade:
    """交易记录"""

    def __init__(self, code: str, name: str, trade_type: str,
                 trade_date: str, price: float, quantity: int, amount: float,
                 signal_strength: float = 0.0, macd_rank: float = 0.0,
                 obv_slope: float = 0.0):
        self.code = code
        self.name = name
        self.trade_type = trade_type  # 'buy' or 'sell'
        self.trade_date = trade_date
        self.price = price
        self.quantity = quantity
        self.amount = amount
        self.signal_strength = signal_strength
        self.macd_rank = macd_rank
        self.obv_slope = obv_slope


class PositionManager:
    """仓位管理器"""

    def __init__(self, initial_capital: float, config: BacktestConfig):
        self.initial_capital = initial_capital
        self.config = config
        self.cash = initial_capital
        self.positions: Dict[str, Position] = {}  # code -> Position
        self.trades: List[Trade] = []
        self.daily_portfolio_value: Dict[str, float] = {}  # date -> total_value

    def get_available_cash(self) -> float:
        """获取可用资金"""
        return self.cash

    def get_position(self, code: str) -> Optional[Position]:
        """获取持仓"""
        return self.positions.get(code)

    def get_positions(self) -> List[Position]:
        """获取所有持仓"""
        return list(self.positions.values())

    def get_positions_count(self) -> int:
        """获取持仓数量"""
        return len(self.positions)

    def allocate_position(self, signals: List[dict], current_prices: Dict[str, float],
                          stock_names: Dict[str, str]) -> List[dict]:
        """
        分配仓位

        参数:
            signals: 信号列表 [{'code', 'date', 'strength', ...}]
            current_prices: 当前价格 {code: price}
            stock_names: 股票名称 {code: name}

        返回:
            list[dict]: 分配结果 [{'code', 'amount', 'quantity', 'price', ...}]
        """
        available_cash = self.get_available_cash()
        if available_cash <= 0:
            return []

        # 按信号强度排序
        sorted_signals = sorted(signals, key=lambda x: x.get('strength', 0), reverse=True)

        allocations = []
        max_daily_open = self.config.MAX_DAILY_OPEN

        for signal in sorted_signals[:max_daily_open]:
            code = signal['code']
            price = current_prices.get(code)

            if price is None or price <= 0:
                continue

            # 计算仓位比例（基于信号强度，带波动）
            strength = signal.get('strength', 0.5)
            base_pct = self.config.MIN_POSITION_PCT + (self.config.MAX_POSITION_PCT - self.config.MIN_POSITION_PCT) * strength

            # 加入波动（±3%）
            import random
            fuzzy = random.uniform(-self.config.POSITION_FUZZY, self.config.POSITION_FUZZY)
            position_pct = max(self.config.MIN_POSITION_PCT - self.config.POSITION_FUZZY,
                              min(self.config.MAX_POSITION_PCT + self.config.POSITION_FUZZY,
                                  base_pct + fuzzy))

            # 计算金额和数量
            amount = available_cash * position_pct
            quantity = int(amount / price / 100) * 100  # 整手交易

            if quantity < 100:
                continue  # 至少1手

            actual_amount = quantity * price

            if actual_amount > available_cash:
                continue  # 资金不足

            allocations.append({
                'code': code,
                'name': stock_names.get(code, ''),
                'amount': actual_amount,
                'quantity': quantity,
                'price': price,
                'signal_strength': signal.get('strength', 0.0),
                'macd_rank': signal.get('macd_rank', 0.0),
                'obv_slope': signal.get('obv_slope', 0.0),
            })

        return allocations

    def execute_buy(self, code: str, name: str, price: float, quantity: int,
                     amount: float, date: str, signal_strength: float = 0.0,
                     macd_rank: float = 0.0, obv_slope: float = 0.0) -> bool:
        """执行买入"""
        if amount > self.cash:
            logger.warning(f"[{date}] {code} 资金不足: 需要{amount:.2f}, 可用{self.cash:.2f}")
            return False

        if code in self.positions:
            logger.warning(f"[{date}] {code} 已持仓，跳过买入")
            return False

        # 扣除资金
        self.cash -= amount

        # 创建持仓
        self.positions[code] = Position(code, name, date, price, quantity, amount)

        # 记录交易
        self.trades.append(Trade(
            code, name, 'buy', date, price, quantity, amount,
            signal_strength, macd_rank, obv_slope
        ))

        return True

    def execute_sell(self, code: str, price: float, date: str) -> Optional[float]:
        """执行卖出"""
        position = self.positions.get(code)
        if not position:
            return None

        # 计算卖出金额
        amount = position.quantity * price

        # 加回资金
        self.cash += amount

        # 移除持仓
        del self.positions[code]

        # 记录交易
        self.trades.append(Trade(
            code, position.name, 'sell', date, price, position.quantity, amount
        ))

        return amount

    def update_portfolio_value(self, prices: Dict[str, float], date: str):
        """
        更新投资组合总价值

        参数:
            prices: 当前价格 {code: price}
            date: 日期
        """
        # 持仓市值
        position_value = 0.0
        for code, position in self.positions.items():
            price = prices.get(code)
            if price and price > 0:
                position_value += position.quantity * price

        # 总价值
        total_value = self.cash + position_value
        self.daily_portfolio_value[date] = total_value

    def get_daily_portfolio_value(self) -> Dict[str, float]:
        """获取每日投资组合价值"""
        return self.daily_portfolio_value.copy()

    def get_trades(self) -> List[Trade]:
        """获取所有交易记录"""
        return self.trades.copy()

    def get_total_return(self) -> float:
        """获取总收益率"""
        if not self.daily_portfolio_value:
            return 0.0

        final_value = list(self.daily_portfolio_value.values())[-1]
        return (final_value - self.initial_capital) / self.initial_capital * 100

    def get_annualized_return(self) -> float:
        """获取年化收益率"""
        if len(self.daily_portfolio_value) < 2:
            return 0.0

        dates = sorted(self.daily_portfolio_value.keys())
        start_date = dates[0]
        end_date = dates[-1]

        # 计算年数
        from datetime import datetime
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        years = (end_dt - start_dt).days / 365.25

        if years <= 0:
            return 0.0

        total_return = self.get_total_return() / 100
        annualized = (1 + total_return) ** (1 / years) - 1
        return annualized * 100

    def get_max_drawdown(self) -> float:
        """获取最大回撤"""
        if len(self.daily_portfolio_value) < 2:
            return 0.0

        values = list(self.daily_portfolio_value.values())
        peak = values[0]
        max_drawdown = 0.0

        for value in values:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak * 100
            if drawdown > max_drawdown:
                max_drawdown = drawdown

        return max_drawdown