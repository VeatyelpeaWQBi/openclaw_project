# 🦐 OpenClaw 项目合集

基于 OpenClaw 的多个 AI 辅助项目。

## 📁 项目列表

| 项目 | 说明 | 状态 |
|------|------|------|
| [tail_trading](tail_trading/) | 📈 尾盘T+1量化交易系统 | ✅ 稳定运行 |
| [feng-shang-ren](feng-shang-ren/) | 📚 风上忍小说AI写作引擎 | ⏸️ 暂停 |
| backtest_engine | 📊 回测引擎 | 🔄 开发中 |

---

## 📈 尾盘T+1量化交易系统

A股尾盘T+1量化选股系统，每日14:50自动运行，通过QQ推送报告。

### 功能特性

- **市场概况**：主要指数、分化系数分析、市场情绪、成交量对比
- **板块筛选**：热门板块TOP10，识别进攻型板块
- **个股筛选**：涨幅/换手率/量比/SuperTrend多头条件
- **ETF备选**：板块无个股时，筛选对应ETF
- **自动推送**：工作日14:50运行，QQ通知

### 快速开始

```bash
# 安装依赖
pip3 install -r tail_trading/requirements.txt

# 手动运行
python3 tail_trading/src/main.py
```

详见 [tail_trading/README.md](tail_trading/README.md)

---

## 📚 风上忍小说AI写作引擎

基于 RAG 的中文网络小说AI写作系统，模拟特定作者写作风格。

**当前状态**：暂停开发，已完成语料清洗、风格分析、RAG索引等基础工作。

---

## 📊 回测引擎

股票回测框架，开发中。

---

## ⚠️ 免责声明

量化交易项目仅供学习研究使用，不构成任何投资建议。股市有风险，投资需谨慎。
