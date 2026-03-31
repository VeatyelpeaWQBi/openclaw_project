# T+1 尾盘交易回测系统

## 概述

基于日K/分钟线数据的A股T+1尾盘交易回测系统，支持固定时间卖出、止损止盈策略。

## 系统架构

```
tail_trading/
├── src/
│   ├── main.py              # 原有选股系统（SuperTrend信号）
│   ├── sell_strategy.py     # 卖出策略引擎 (新增)
│   ├── backtest.py          # 回测引擎 (新增)
│   ├── data_loader.py       # 数据加载器 (新增)
│   ├── report.py            # 回测报告生成 (新增)
│   ├── supertrend.py        # SuperTrend指标
│   ├── stock_data.py        # 股票数据获取
│   ├── stock_filter.py      # 股票筛选
│   ├── sector_data.py       # 板块数据
│   └── data_storage.py      # 数据存储
├── data/kline/              # 日K数据（按月归档）
├── minute_data/             # 分钟线数据（可选）
├── reports/                 # 回测报告输出
├── requirements.txt
└── BACKTEST_README.md
```

## 卖出策略

### 当前实现（v1.0）
- **固定时间卖出**：次日 09:45 以该5分钟K线收盘价卖出
- **止损**：-3%（买入价下方3%触发止损）
- **止盈**：+5%（买入价上方5%触发止盈）

### 优先级
1. 止损 > 止盈 > 固定时间卖出

### 未来扩展方向
- 移动止盈（跟踪止损）
- 分批卖出
- 尾盘二次买入策略
- 基于量能的动态卖出

## 使用方式

### 1. 安装依赖

```bash
pip3 install --break-system-packages -i https://pypi.tuna.tsinghua.edu.cn/simple \
  --trusted-host pypi.tuna.tsinghua.edu.cn backtrader pandas matplotlib
```

### 2. 单笔交易回测

```python
from backtest import BacktestEngine

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

### 3. 批量回测（从信号文件）

```python
from backtest import run_backtest_from_signals

engine = run_backtest_from_signals(
    '/path/to/signals/signals_2026-03-28.csv',  # 替换为实际信号文件路径
    config={'initial_capital': 100000}
)

# 生成报告
from report import BacktestReport
report = BacktestReport(engine.get_results_df())
print(report.to_text())
report.to_html()
report.generate_charts()
```

### 4. 手动构造买入信号回测

```python
from backtest import BacktestEngine
from report import BacktestReport

signals = [
    {'date': '2026-03-20', 'code': '002409', 'name': '雅克科技', 'sector': '半导体'},
    {'date': '2026-03-21', 'code': '000807', 'name': '云铝股份', 'sector': '有色金属'},
]

engine = BacktestEngine()
results = engine.run_backtest(signals)

report = BacktestReport(results)
print(report.to_text())
report.generate_charts()
report.to_html()
```

### 5. 使用分钟线数据（更精确）

如果有分钟线数据，可以启用更精确的卖出价格计算：

```python
engine = BacktestEngine()  # 路径自动从配置文件加载
results = engine.run_backtest(signals, use_minute_data=True)
```

## 回测指标

| 指标 | 说明 |
|------|------|
| 胜率 | 盈利交易占比 |
| 平均收益 | 每笔交易平均收益率 |
| 累计收益 | 所有交易的复合累计收益 |
| 年化收益 | 按时间跨度折算的年化收益率 |
| 最大回撤 | 资金曲线最大回撤幅度 |
| 夏普比率 | 风险调整后收益（无风险利率3%） |
| 盈亏比 | 平均盈利 / 平均亏损 |
| 最大连续盈利/亏损 | 连续盈利/亏损的最大次数 |

## 数据格式

### 日K数据（已有）
路径: `data/kline/YYYY-MM/code_name.csv`
列: `date, open, close, high, low, volume, amount, amplitude, change_pct, change_amt, 所属板块`

### 分钟线数据（可选）
路径: `minute_data/YYYY-MM/code_name/code_name_YYYYMMDD.csv`
列: `时间, 开盘, 收盘, 最高, 最低, 成交量, 成交额`

## 注意事项

- 回测系统独立于选股系统，可以单独运行
- 没有分钟线数据时，系统自动用日K的 (open+close)/2 近似 09:45 价格
- 止损止盈在日K回测中用当日 high/low 判断是否触发
- 所有收益率计算基于扣除交易费用前的毛收益
