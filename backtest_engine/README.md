# 📊 backtest_engine — T+1回测引擎

A股T+1尾盘交易回测系统，支持日K和分钟线数据回测，用于验证选股策略的历史表现。

## 🚀 快速开始

### 安装依赖

```bash
pip install -r requirements.txt
```

依赖：`pandas`, `numpy`, `matplotlib`

### 使用方式

```python
from src.backtest import BacktestEngine

# 配置回测参数
engine = BacktestEngine({
    'initial_capital': 100000,     # 初始资金
    'position_size': 0.2,          # 每次用20%资金
    'sell_strategy': {
        'sell_time': '09:45',      # 固定卖出时间
        'stop_loss': -0.03,        # 止损 -3%
        'take_profit': 0.05,       # 止盈 +5%
    }
})

# 单笔交易回测
result = engine.run_single_trade('002409', '雅克科技', '2026-03-27')
print(result)

# 批量回测（从信号文件）
from src.backtest import run_backtest_from_signals
engine = run_backtest_from_signals('signals_2026-04-02.csv')
```

### 下载数据

```bash
python3 download_daily_kline.py
```

## 📁 项目结构

```
backtest_engine/
├── src/
│   ├── backtest.py          # 回测引擎核心（BacktestEngine）
│   ├── sell_strategy.py     # 卖出策略（固定09:45 + 止损止盈）
│   ├── report.py            # 回测报告生成
│   ├── data_loader.py       # 数据加载器（SQLite + CSV）
│   └── paths.py             # 路径配置加载
├── download_daily_kline.py  # 日K数据下载脚本
├── config/                  # 配置目录
├── BACKTEST_README.md       # 回测系统详细文档
├── SELL_TIMING_RESEARCH.md  # 卖出时机研究报告
├── README.md
└── requirements.txt
```

## 📊 功能

| 功能 | 说明 |
|------|------|
| 日K回测 | 基于日K数据的T+1回测 |
| 分钟线回测 | 支持分钟线数据精确卖出点 |
| 止损止盈 | 固定止损-3% / 止盈+5% |
| 固定时间卖出 | 模拟09:45卖出 |
| 资金管理 | 按仓位比例分配资金 |
| 回测报告 | 收益率、最大回撤、交易明细 |

## ⚙️ 环境要求

- Python 3.10+
- 与 tail_trading 共享 SQLite 数据库
- 日K数据需先通过 `download_daily_kline.py` 下载

## ⚠️ 状态

本项目仍在开发中，部分功能尚未完善。

## ⚠️ 免责声明

本项目仅供学习研究使用，不构成任何投资建议。股市有风险，投资需谨慎。
