# 🦐 OpenClaw 项目合集

基于 OpenClaw 的 AI 辅助项目合集，涵盖量化交易、盯盘助手、回测引擎、AI写作等方向。

## 📁 项目总览

| 项目 | 说明 | 状态 | 目录 |
|------|------|------|------|
| [quant_trading](quant_trading/) | 📈 A股量化交易系统（尾盘T+1 + 趋势交易） | ✅ 运行中 | [README](quant_trading/README.md) |
| [WatchMonitor](WatchMonitor/) | 📊 盯盘助手（持仓风险 + 候选抄底 + 大盘分析） | ✅ 运行中 | [README](WatchMonitor/README.md) |
| [backtest_engine](backtest_engine/) | 📊 T+1回测引擎 | 🔄 开发中 | [README](backtest_engine/README.md) |
| [feng-shang-ren](feng-shang-ren/) | 📚 风上忍小说AI写作引擎 | ⏸️ 暂停 | [README](feng-shang-ren/README.md) |

## 📈 quant_trading — A股量化交易系统

核心交易系统，包含两个策略（Nomad T1 尾盘选股 + 趋势交易法趋势跟踪）和一套分层架构的交易执行引擎。

> **<font color="red">⚠️ 严肃提醒</font>**：该项目由于回测结果不理想，策略研发难度大已经搁置，如完全参考该系统的策略进行交易可能产生<font color="red">**持续亏损**</font>，该项目仅作为学习参考用，本项目开发者不对任何人的<font color="red">**买卖行为和投资盈亏负任何责任**</font>

### 核心能力

- **板块筛选**：热门板块TOP10，识别进攻型板块
- **个股筛选**：涨幅/换手率/量比/SuperTrend多头条件
- **趋势交易**：多账户持仓管理、信号检测（止损/退出/加仓/开仓）、ATR仓位控制
- **评分体系**：RS相对强度 + VCP波动收缩 + ADX趋势强度三维评分
- **日K更新**：新浪原生API获取全字段，增量同步到本地SQLite
- **自动推送**：工作日14:50运行，QQ通知

### 四层架构

```
Strategies（策略层）    → 趋势交易特有计算（ATR/止损/S1/S2 + 评分模块）
Executor（调度层）      → 信号排序⇾调用策略执行器
Infra（基础设施层）     → 纯通用执行（T+1/CRUD/费率/流水）
Core（核心层）          → 数据获取/存储/指标
```

详见 [quant_trading/README.md](quant_trading/README.md)

---

## 📊 WatchMonitor — 盯盘助手

盘中实时监控系统，负责持仓池风险检测、候选池抄底信号、大盘分析报告生成。

### 核心能力

- **大盘分析**：主要指数/市场分化/恐贪指数/热门板块/ADX市场情绪
- **持仓风险**：MA破位/SuperTrend翻空/顶背离/扫雷检测
- **候选抄底**：跌幅评分/抄底机会判断/技术概览
- **报告推送**：4个独立报告文件（大盘/持仓风险/扫雷/候选）

### 🔥 技术指标模块化架构（2026-04新重构）

每个技术指标完全独立封装：
- 指标类对应单一股票，初始化时完成所有计算
- 统一输出接口：信号/报告/评分（黑盒）
- YAML配置控制启用/权重/参数/顺序
- Manager动态实例化、循环调用、分析后释放

```
┌─────────────────────────────────────────────────────┐
│  指标模块（独立封装）                                 │
│  SuperTrend / MACD / RSI / MA / ADX / Volume / K线  │
│  ↓ 内部计算 → 信号 → 报告 → 评分                     │
├─────────────────────────────────────────────────────┤
│  IndicatorManager（循环调用）                        │
│  根据YAML配置实例化 → 获取结果 → 权重汇总             │
├─────────────────────────────────────────────────────┤
│  report.py（简化）                                   │
│  manager.analyze_stock() → 直接获取报告内容           │
└─────────────────────────────────────────────────────┘
```

详见 [WatchMonitor/README.md](WatchMonitor/README.md)

---

## 📊 backtest_engine — T+1回测引擎

基于日K/分钟线数据的A股T+1尾盘交易回测系统。

- 支持日K和分钟线数据回测
- 固定时间卖出（09:45）+ 止损止盈
- 回测报告生成

详见 [backtest_engine/README.md](backtest_engine/README.md)

---

## 📚 feng-shang-ren — 风上忍小说AI写作引擎

基于 RAG 的中文网络小说AI写作系统，模拟特定作者写作风格。

**当前状态**：暂停开发。已完成语料清洗、风格分析、RAG索引、Agent调研等基础工作。

详见 [feng-shang-ren/README.md](feng-shang-ren/README.md)

---

## ⚠️ 免责声明

量化交易项目仅供学习研究使用，不构成任何投资建议。股市有风险，投资需谨慎。
