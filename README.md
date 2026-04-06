# 🦐 OpenClaw 项目合集

基于 OpenClaw 的 AI 辅助项目合集，涵盖量化交易、回测引擎、AI写作等方向。

## 📁 项目总览

| 项目 | 说明 | 状态 | 目录 |
|------|------|------|------|
| [tail_trading](tail_trading/) | 📈 A股量化交易系统（尾盘T+1 + 海龟交易法） | ✅ 运行中 | [README](tail_trading/README.md) |
| [backtest_engine](backtest_engine/) | 📊 T+1回测引擎 | 🔄 开发中 | [README](backtest_engine/README.md) |
| [feng-shang-ren](feng-shang-ren/) | 📚 风上忍小说AI写作引擎 | ⏸️ 暂停 | [README](feng-shang-ren/README.md) |

## 📈 tail_trading — A股量化交易系统

核心交易系统，包含两个策略和完整的执行引擎。

### 子模块

| 模块 | 说明 | 状态 |
|------|------|------|
| **Nomad T1** | 尾盘T+1选股策略，每日14:50筛选热门板块个股 | ✅ 稳定 |
| **Turtle** | 海龟交易法趋势跟踪策略，多账户管理 | ✅ 已完成核心逻辑 |
| **Executor** | 通用交易执行器，人工/机器账户统一接口 | ✅ 已完成 |
| **Core** | 数据访问、指标计算、存储层 | ✅ 稳定 |

### 主要功能

- **市场概况**：主要指数、分化系数分析、市场情绪、成交量对比
- **板块筛选**：热门板块TOP10，识别进攻型板块
- **个股筛选**：涨幅/换手率/量比/SuperTrend多头条件
- **ETF备选**：板块无个股时，筛选对应ETF
- **海龟交易法**：多账户持仓管理、信号检测（止损/退出/加仓/开仓）、ATR仓位计算
- **交易执行器**：TradeExecutor（底层执行）+ RobotExecutor（业务调度）
- **自动推送**：工作日14:50运行，QQ通知

### 快速开始

```bash
cd tail_trading
pip install -r requirements.txt
python3 main.py
```

详见 [tail_trading/README.md](tail_trading/README.md)

---

## 📊 backtest_engine — T+1回测引擎

基于日K/分钟线数据的A股T+1尾盘交易回测系统。

### 功能

- 支持日K和分钟线数据回测
- 固定时间卖出（09:45）+ 止损止盈
- 生成回测报告

```bash
cd backtest_engine
pip install -r requirements.txt
```

详见 [backtest_engine/README.md](backtest_engine/README.md)

---

## 📚 feng-shang-ren — 风上忍小说AI写作引擎

基于 RAG 的中文网络小说AI写作系统，模拟特定作者写作风格。

**当前状态**：暂停开发。已完成语料清洗、风格分析、RAG索引等基础工作。

详见 [feng-shang-ren/README.md](feng-shang-ren/README.md)

---

## 📁 其他目录

| 目录 | 说明 |
|------|------|
| `DATA/` | 共享数据目录 |
| `prod/` | 生产环境部署目录（不提交Git） |
| `scripts/` | 通用脚本 |
| `reference_project/` | 参考项目源码（不提交Git） |

## ⚠️ 免责声明

量化交易项目仅供学习研究使用，不构成任何投资建议。股市有风险，投资需谨慎。
