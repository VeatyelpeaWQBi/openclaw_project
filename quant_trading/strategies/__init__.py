"""
策略包
包含策略注册表和所有可用策略
"""


def get_strategy(name='trend_trading'):
    """
    获取策略实例

    参数:
        name: 策略名称

    返回:
        BaseStrategy 实例
    """
    from strategies.trend_trading.strategy import TrendTradingStrategy

    STRATEGY_MAP = {
        'trend_trading': TrendTradingStrategy,
    }

    cls = STRATEGY_MAP.get(name)
    if cls is None:
        raise ValueError(f"未知策略: {name}，可用: {list(STRATEGY_MAP.keys())}")
    return cls()
