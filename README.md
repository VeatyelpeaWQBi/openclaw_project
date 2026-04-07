# 🦐 OpenClaw 项目合集

基于 OpenClaw 的 AI 辅助项目合集，涵盖量化交易、回测引擎、AI写作等方向。

## 📁 项目总览

| 项目 | 说明 | 状态 | 目录 |
|------|------|------|------|
| [quant_trading](quant_trading/) | 📈 A股量化交易系统（尾盘T+1 + 趋势交易） | ✅ 运行中 | [README](quant_trading/README.md) |
| [backtest_engine](backtest_engine/) | 📊 T+1回测引擎 | 🔄 开发中 | [README](backtest_engine/README.md) |
| [feng-shang-ren](feng-shang-ren/) | 📚 风上忍小说AI写作引擎 | ⏸️ 暂停 | [README](feng-shang-ren/README.md) |

## 📈 quant_trading — A股量化交易系统

核心交易系统，包含两个策略和分层架构的执行引擎。

### 架构

```
Strategies（策略层）     → 趋势交易特有计算（ATR/止损/S1/S2）
Executor（调度层）       → 信号排序⇾调用策略执行器
Infra（基础设施层）      → 纯通用执行（T+1/CRUD/费率/流水）
Core（核心层）           → 数据获取/存储/指标
```

### 子模块

| 模块 | 说明 | 状态 |
|------|------|------|
| **Nomad T1** | 尾盘T+1选股策略，每日14:50筛选热门板块个股 | ✅ 稳定 |
| **Trend Trading** | 趋势交易策略，目前已实现海龟交易法的核心策略和部分股票魔术师的趋势分析策略，多账户管理，ATR仓位控制 | ✅ 已完成核心逻辑 |
| **Infra** | 基础设施层（TradeExecutor/PositionManager/AccountManager） | ✅ 已完成 |
| **Core** | 数据访问、指标计算、存储层 | ✅ 稳定 |
| **Job** | 日K数据更新（新浪原生API，全字段写入） | ✅ 稳定 |

### 主要功能

- **板块筛选**：热门板块TOP10，识别进攻型板块
- **个股筛选**：涨幅/换手率/量比/SuperTrend多头条件
- **趋势交易**：多账户持仓管理、信号检测（止损/退出/加仓/开仓）、ATR仓位计算
- **分层执行**：Infra层纯通用 + 策略层turtle特有逻辑，手工/机器账户统一接口
- **日K更新**：新浪原生API获取涨跌幅/换手率/PE/PB/市值/流通股本/量比
- **自动推送**：工作日14:50运行，QQ通知

### 快速开始

```bash
cd quant_trading
pip install -r requirements.txt
python3 main.py
```

详见 [quant_trading/README.md](quant_trading/README.md)

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
