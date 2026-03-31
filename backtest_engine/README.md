# 📊 回测引擎 (Backtest Engine)

A股T+1尾盘交易回测系统，支持日K和分钟线数据回测。

## 📁 项目结构

```
backtest_engine/
├── src/
│   ├── backtest.py          # 回测引擎核心
│   ├── sell_strategy.py     # 卖出策略（09:45 + 止损止盈）
│   ├── report.py            # 回测报告生成
│   ├── data_loader.py       # 数据加载器（支持SQLite + CSV）
│   └── __init__.py
├── data/                    # 回测数据（与tail_trading共享）
├── BACKTEST_README.md       # 回测系统详细说明
├── SELL_TIMING_RESEARCH.md  # 卖出时机研究
└── requirements.txt
```

## 🚀 使用方式

```python
from src.backtest import BacktestEngine

# 单笔交易回测（路径自动从配置文件加载）
engine = BacktestEngine({
    'initial_capital': 100000,
    'sell_strategy': {
        'sell_time': '09:45',
        'stop_loss': -0.03,
        'take_profit': 0.05,
    }
})

result = engine.run_single_trade('002409', '雅克科技', '2026-03-27')
print(result)
```

## 📊 功能

- 支持日K和分钟线数据回测
- 固定时间卖出（09:45）
- 止损 -3% / 止盈 +5%
- 生成HTML报告和图表

## ⚠️ 状态

本项目仍在开发中，尚未完成。
