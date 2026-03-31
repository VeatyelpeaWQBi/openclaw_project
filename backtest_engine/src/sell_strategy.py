"""
T+1卖出策略引擎
固定时间卖出（09:45）— 纯回测用，计算买入价与卖出价的损益
"""


class SellStrategy:
    """T+1卖出策略 — 固定09:45卖出"""

    def __init__(self, config=None):
        config = config or {}
        self.sell_time = config.get('sell_time', '09:45')

    def get_sell_price(self, bar):
        """
        获取卖出价格（09:45 K线收盘价）

        参数:
            bar: 包含 'time' 和 'close' 的K线数据

        返回:
            float: 卖出价格，如果不是卖出时间则返回None
        """
        if bar.get('time', '').startswith(self.sell_time):
            return bar['close']
        return None

    def calculate_pnl(self, buy_price, sell_price):
        """
        计算损益

        参数:
            buy_price: 买入价格
            sell_price: 卖出价格

        返回:
            dict: {return_pct, pnl_amount}
        """
        return_pct = (sell_price - buy_price) / buy_price
        return {
            'buy_price': buy_price,
            'sell_price': sell_price,
            'return_pct': return_pct,
            'pnl_amount': sell_price - buy_price,
        }
