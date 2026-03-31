# 🦐 OpenClaw 项目合集

基于 OpenClaw 的多个 AI 辅助项目，包含量化交易系统和小说AI写作引擎。

## 📁 项目结构

```
openclaw_project/
├── tail_trading/              # 📈 尾盘T+1量化交易系统
├── backtest_engine/           # 📊 回测引擎（开发中）
├── feng-shang-ren/            # 📚 风上忍小说AI写作引擎
└── README.md
```

---

## 📈 项目一：尾盘T+1量化交易系统

A股尾盘T+1量化交易系统，包含选股、回测、恐贪指数等完整功能。

### 功能概览

| 功能 | 说明 | 状态 |
|------|------|------|
| 尾盘选股 | 每日14:45自动运行，SuperTrend + 进攻型板块筛选 | ✅ 已完成 |
| 回测引擎 | 日K/分钟线回测，止损止盈策略 | ✅ 已完成 |
| 恐贪指数 | 8因子自建情绪指标 | 🔄 开发中 |
| 板块配置 | YAML管理进攻型/防御型板块关键词 | ✅ 已完成 |

### 目录结构

```
tail_trading/
├── src/
│   ├── main.py                # 主程序（每日14:45选股）
│   ├── supertrend.py          # SuperTrend指标计算
│   ├── stock_filter.py        # 选股过滤逻辑
│   ├── sector_data.py         # 板块数据获取（东方财富API）
│   ├── stock_data.py          # 个股数据获取
│   ├── backtest.py            # 回测引擎
│   ├── sell_strategy.py       # 卖出策略（09:45 + 止损止盈）
│   ├── report.py              # 回测报告生成
│   ├── data_loader.py         # 数据加载器
│   ├── data_storage.py        # 数据存储
│   └── fear_greed_index.py    # 恐贪指数（8因子）
├── config/
│   ├── sectors.py             # 进攻型/防御型板块判断
│   └── sectors_config.yaml    # 板块关键词配置
├── data/                      # 日K数据（按月归档）
├── daily_data/                # 个股日K明细（1469只）
├── minute_data/               # 分钟线数据
├── index_history/             # 指数K线（沪深300/中证1000）
├── fix_data_sources.py        # 数据补充工具
├── download_daily.py          # 日K数据下载
└── requirements.txt           # Python依赖
```

### 使用方式

```bash
# 安装依赖
pip3 install -r tail_trading/requirements.txt

# 手动运行选股
python3 tail_trading/src/main.py

# 运行回测
python3 -c "
from tail_trading.src.backtest import BacktestEngine
engine = BacktestEngine({'initial_capital': 100000})
result = engine.run_single_trade('002409', '雅克科技', '2026-03-27')
print(result)
"

# 计算恐贪指数
python3 -c "
from tail_trading.src.fear_greed_index import FearGreedIndex
fg = FearGreedIndex('tail_trading/daily_data', 'tail_trading/index_history')
result = fg.calculate()
print(result)
"
```

---

## 📚 项目二：风上忍小说AI写作引擎

基于 RAG（检索增强生成）的中文网络小说AI写作系统，模拟"风上忍"的写作风格。

### 功能概览

| 功能 | 说明 | 状态 |
|------|------|------|
| 语料清洗 | 5本小说UTF-8清洗，去除广告/乱码 | ✅ 已完成 |
| 风格分析 | 深度分析写作习惯、常用句式、场景偏好 | ✅ 已完成 |
| RAG 索引 | sentence-transformers 向量索引，FAISS检索 | ✅ 已完成 |
| 写作Skill | OpenClaw AgentSkill，支持风格化续写 | ✅ 已完成 |
| 7-Agent管线 | 多Agent协作写作（参考InkOS架构） | ⏸️ 暂停待开发 |

### 目录结构

```
feng-shang-ren/
├── cleaned_v3/                # 清洗后的语料
│   ├── 异体_clean.txt
│   ├── 时空之头号玩家_clean.txt
│   ├── 末日贩卖1_clean.txt
│   ├── 末日贩卖2_clean.txt
│   └── 第七脑域_clean.txt
├── deep-analysis/             # 深度风格分析
│   ├── 综合风格画像.md
│   ├── 异体-analysis.md
│   └── ...
├── rag/                       # RAG系统
│   ├── build_index_v3.py      # 索引构建脚本
│   ├── query_v3.py            # 查询脚本
│   ├── faiss_index.bin        # FAISS向量索引
│   ├── w2v_model.bin          # Word2Vec模型
│   └── chunks_*.json          # 分块文本
├── skill/                     # OpenClaw AgentSkill
│   ├── SKILL.md               # 技能定义
│   ├── style-guide.md         # 风格指南
│   └── reference-chunks/      # 参考语料片段
├── 异体_utf8.txt              # 原始语料
├── style-analysis-report.md   # 风格分析报告
└── agent-research-report.md   # 多Agent架构调研
```

### 核心技术

- **语料处理**：5本小说共约200万字，清洗后约180万字
- **向量模型**：sentence-transformers / paraphrase-multilingual-MiniLM-L12-v2
- **索引**：FAISS HNSW索引，支持快速相似检索
- **风格特征**：第一人称视角、吐槽式叙事、快节奏爽文、都市异能题材

---

## 📊 数据说明

| 数据类型 | 覆盖范围 | 来源 |
|---------|---------|------|
| 日K数据 | 1469只股票，2020-2026 | baostock + 东方财富 |
| 分钟线数据 | 1466只股票，2020-2026 | baostock |
| 指数K线 | 沪深300 + 中证1000 | 东方财富 |
| 换手率 | 部分补充自东方财富 | 东方财富API |
| 量比 | 全部计算 | 本地计算（成交量/5日均量） |
| 小说语料 | 5本风上忍小说，约180万字 | 人工收集 |

## 📝 相关文档

### 尾盘交易系统
- [回测系统说明](tail_trading/BACKTEST_README.md)
- [选股可行性调研](tail_trading/BACKTEST_STOCK_SELECTION_RESEARCH.md)
- [恐贪指数调研](tail_trading/FEAR_GREED_INDEX_RESEARCH.md)
- [卖出时机研究](tail_trading/SELL_TIMING_RESEARCH.md)
- [代码Review报告](tail_trading/REVIEW_REPORT.md)

### 风上忍小说AI
- [风格分析报告](feng-shang-ren/style-analysis-report.md)
- [Agent架构调研](feng-shang-ren/agent-research-report.md)
- [写作Skill](feng-shang-ren/skill/SKILL.md)

## ⚠️ 免责声明

- 量化交易项目仅供学习研究使用，不构成任何投资建议。股市有风险，投资需谨慎。
- 小说AI项目仅供技术研究，生成内容不代表原作者观点。
