"""
策略包
包含策略注册表和所有可用策略
"""
from strategies.nomad_t1.strategy import NomadT1Strategy
from strategies.trend_trading.strategy import TrendTradingStrategy

# 策略注册表：名称 -> 策略类
STRATEGY_MAP = {
    'nomad_t1': NomadT1Strategy,
    'trend_trading': TrendTradingStrategy,
}


def get_strategy(name='nomad_t1'):
    """
    获取策略实例

    参数:
        name: 策略名称

    返回:
        BaseStrategy 实例
    """
    cls = STRATEGY_MAP.get(name)
    if cls is None:
        raise ValueError(f"未知策略: {name}，可用: {list(STRATEGY_MAP.keys())}")
    return cls()
