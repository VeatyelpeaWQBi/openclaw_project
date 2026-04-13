# 🦐 OpenClaw 项目合集

基于 OpenClaw 的 AI 辅助项目合集，涵盖量化交易、回测引擎、AI写作等方向。

## 📁 项目总览

| 项目 | 说明 | 状态 | 目录 |
|------|------|------|------|
| [quant_trading](quant_trading/) | 📈 A股量化交易系统（尾盘T+1 + 趋势交易） | ✅ 运行中 | [README](quant_trading/README.md) |
| [backtest_engine](backtest_engine/) | 📊 T+1回测引擎 | 🔄 开发中 | [README](backtest_engine/README.md) |
| [feng-shang-ren](feng-shang-ren/) | 📚 风上忍小说AI写作引擎 | ⏸️ 暂停 | [README](feng-shang-ren/README.md) |
| [cron_script](cron_script/) | ⏰ 定时任务脚本 | ✅ 运行中 | — |
| [macro_indicators](macro_indicators/) | 📋 宏观经济指标研究 | 📝 调研阶段 | — |

## 📈 quant_trading — A股量化交易系统

核心交易系统，包含两个策略（Nomad T1 尾盘选股 + 趋势交易法趋势跟踪）和一套分层架构的交易执行引擎。

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

## ⏰ cron_script — 定时任务脚本

纯脚本运行的定时任务，不经过大模型，直接执行+QQ通知。

| 脚本 | 调度 | 功能 |
|------|------|------|
| `run_tail_trading.sh` | 工作日 14:50 | 尾盘T+1信号分析 → QQ通知 |
| `check_douyin_live.sh` | 每天 9:00-21:00 整点 | 抖音直播检测 → 开播时QQ通知 |

系统crontab配置：

```
5 9-21 * * *   openclaw_project/cron_script/check_douyin_live.sh
50 14 * * 1-5  openclaw_project/cron_script/run_tail_trading.sh
```

---

## 📁 其他目录

| 目录 | 说明 |
|------|------|
| `DATA/` | 共享数据目录（含SQLite数据库 `stock_data.db`） |
| `shares/` | 报告输出目录（含信号文件 + 日报） |
| `scripts/` | 通用脚本（`scan_before_push.sh` 提交前敏感信息扫描） |
| `reference_project/` | 参考项目源码（free-code, openclaw） |
| `mempalace-venv/` | 记忆宫殿 Python 虚拟环境 |
| `prod/` | 生产环境部署目录（不提交Git） |

## ⚠️ 免责声明

量化交易项目仅供学习研究使用，不构成任何投资建议。股市有风险，投资需谨慎。
