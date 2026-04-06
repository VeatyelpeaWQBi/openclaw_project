# 📚 feng-shang-ren — 风上忍小说AI写作引擎

基于 RAG 的中文网络小说AI写作系统，模拟特定作者（风上忍）的写作风格进行小说创作。

## ⏸️ 当前状态：暂停开发

已完成基础工作，等待主人安排后续开发。

### 已完成

| 模块 | 说明 |
|------|------|
| 语料清洗 | 5部小说原文清洗，去除广告/乱码/重复段落 |
| 风格分析 | 词频统计、句式分析、写作特征提取 |
| RAG索引 | 基于 sentence-transformers 的向量检索 |
| 写作Skill | 基础写作能力（OpenClaw Skill） |
| Agent调研 | 7-Agent写作管线架构调研报告 |

### 待开发

| 模块 | 说明 |
|------|------|
| 7-Agent管线 | 多Agent协作写作（参考InkOS架构） |
| 实时生成 | 边写边检索的动态创作流程 |
| 质量评估 | AI自动评估写作质量并迭代优化 |

## 📁 项目结构

```
feng-shang-ren/
├── *.txt                        # 原始小说语料（5部作品）
├── clean_text_v3.py             # 语料清洗脚本
├── cleaning_result_v3.json      # 清洗结果
├── style-analysis-report.md     # 风格分析报告
├── analysis-draft.md            # 分析草稿
├── agent-research-report.md     # Agent架构调研报告
├── deep-analysis/               # 深度分析结果
├── rag/                         # RAG索引与检索
├── skill/                       # OpenClaw写作Skill
├── cleaned_v3/                  # 清洗后语料
├── venv/                        # Python虚拟环境
└── progress.json                # 开发进度记录
```

## 📖 语料来源

| 作品 | 说明 |
|------|------|
| 异体 | 玄幻小说 |
| 时空之头号玩家 | 科幻小说 |
| 末日咆哮1 | 末日题材 |
| 末日咆哮2 | 末日题材 |
| 第七脑域 | 科幻小说 |

## ⚙️ 技术栈

- Python 3.10+
- sentence-transformers（向量嵌入）
- RAG（检索增强生成）
- OpenClaw Agent框架
