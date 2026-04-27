# 📊 WatchMonitor — 盯盘助手

盘中实时监控系统，负责持仓池风险检测、候选池抄底信号、大盘分析报告生成。

## 🚀 快速开始

```bash
cd WatchMonitor
python strategy.py
```

运行后会生成4个报告文件：
- `report_{date}_part1.md` — 大盘分析（主要指数/市场分化/恐贪指数/热门板块）
- `report_{date}_part2_upper.md` — 持仓池风险信号（技术概要+综合判断）
- `report_{date}_part2_mine.md` — 持仓池扫雷风险（可选，仅在有扫雷项时生成）
- `report_{date}_part3.md` — 候选池抄底信号（技术概览+抄底机会判断）

## 📁 项目结构

```
WatchMonitor/
├── strategy.py                      # 主程序入口
├── signal_detector.py               # 信号检测引擎
├── report.py                        # 报告生成模块
├── pool_manager.py                  # 持仓池/候选池管理CLI
├── filters.py                       # ETF候选筛选
├── fetch_fear_index.py              # 恐贪指数获取
│
├── config/                          # 配置
│   ├── sectors.py                   #   进攻型板块配置
│   └── indicators.yaml              #   🔥 技术指标配置（启用/权重/参数）
│
├── core/                            # 核心模块
│   ├── data_access.py               #   数据访问（板块/ETF/指数）
│   ├── storage.py                   #   SQLite存储
│   ├── adx_analyzer.py              #   ADX趋势分析模块
│   ├── indicator_funcs.py           #   技术指标计算函数库
│   └── indicators/                  #   🔥 技术指标模块化系统
│       ├── base.py                  #     指标基类（单一职责封装）
│       ├── manager.py               #     指标管理器（循环调用/权重汇总）
│       ├── supertrend_indicator.py  #     SuperTrend指标
│       ├── macd_indicator.py        #     MACD指标
│       ├── rsi_indicator.py         #     RSI指标
│       ├── ma_indicator.py          #     MA均线指标
│       ├── adx_indicator.py         #     ADX趋势强度指标
│       ├── volume_indicator.py      #     量比指标
│       └── candle_indicator.py      #     K线形态指标
│
└── job/                             # 定时任务
    └── update_candidate_pool.py     #   候选池自动更新
```

## 🏗️ 技术指标模块化架构

### 设计原则

**每个技术指标完全独立封装**：
- 初始化时传入日K DataFrame，内部完成所有计算
- 提供统一输出接口：信号、报告、评分
- 对象内部完全黑盒，外部只获取结果，不做二次加工

### 使用方式

```python
from core.indicators import IndicatorManager

# 创建管理器
manager = IndicatorManager()

# 一站式分析（计算+信号+报告+评分全部内部完成）
context = {
    'current_price': 11.0,
    'is_position': True,
}
result = manager.analyze_stock('000001', df, context)

# 获取结果（黑盒输出）
signals = result['signals']          # 检测到的信号列表
report_lines = result['report_lines'] # 报告内容（可直接输出）
total_score = result['total_score']  # 加权综合评分
score_reasons = result['score_reasons'] # 评分原因
```

### 配置文件 `config/indicators.yaml`

```yaml
indicators:
  - name: supertrend
    enabled: true        # 是否启用
    weight: 2.0          # 评分权重
    params:
      atr_period: 10
      multiplier: 3.0
    report_order: 1      # 报告展示顺序

  - name: ma
    enabled: true
    weight: 1.0
    params:
      periods: [5, 10, 20, 60, 120, 250]
    report_order: 4

  # 可随时禁用某个指标
  - name: volume
    enabled: false       # 禁用量比指标
```

### 指标权重评分

| 指标 | 默认权重 | 评分规则 |
|-----|---------|---------|
| SuperTrend | 2.0 | 多头+2 / 空头-2 |
| MACD | 1.0 | 柱向上+1 / 向下-1 |
| RSI | 1.0 | 抄底：超卖+2 / 超买-2 |
| MA均线 | 1.0 | 价>MA5+1 / 价<MA5-1 |
| ADX | 2.0 | 多头+2 / 空头-2 |
| 量比 | 0.5 | 放量异动+0.5 |
| K线形态 | 0.5 | 长下影线+0.5 |

综合评分通过加权汇总，report.py直接获取结果。

## 📊 报告结构

### 部分1：大盘分析

- 主要指数（上证/深证/创业板/沪深300）
- 市场分化系数（鳄鱼张嘴/题材先行等）
- 市场情绪（涨跌停/涨跌比）
- 恐贪指数（8维度因子）
- ADX市场情绪分布
- 热门板块TOP10 + 进攻型板块

### 部分2：持仓池风险

- 持仓基本信息（成本/现价/盈亏）
- 风险信号（MA破位/SuperTrend翻空/顶背离等）
- **技术概要**（通过IndicatorManager黑盒生成）
- 综合判断（评分+趋势评价）

### 部分3：候选池抄底

- 候选基本信息（关注价/跌幅）
- **技术概览**（通过IndicatorManager黑盒生成）
- 抄底机会判断（绝佳/较好/可关注/观望/不宜）

## 🔧 持仓池/候选池管理

```bash
# 添加持仓
python pool_manager.py position add --code 002594 --name "比亚迪" --entry-price 250

# 列出持仓
python pool_manager.py position list

# 添加候选
python pool_manager.py candidate add --code 300750 --name "宁德时代" --watch-price 210

# 列出候选
python pool_manager.py candidate list

# 批量导入（CSV）
python pool_manager.py position import --file positions.csv
python pool_manager.py candidate import --file candidates.csv
```

## ⚙️ 依赖

- Python 3.10+
- pandas, numpy
- akshare（指数数据）
- adata（扫雷接口）
- yaml（配置解析）

## 📝 更新日志

### 2026-04-24 技术指标模块化重构

**重大架构升级**：
- 每个技术指标完全独立封装为类
- 指标实例对应单一股票，初始化时完成所有计算
- 提供统一接口：信号/报告/评分（黑盒输出）
- YAML配置控制启用/权重/参数/顺序
- IndicatorManager动态实例化、循环调用、分析后释放
- report.py简化为只获取结果，不做二次加工

**新增文件**：
- `core/indicators/base.py` — 指标基类
- `core/indicators/manager.py` — 指标管理器
- `core/indicators/*.py` — 7个独立指标模块
- `config/indicators.yaml` — 配置文件

**重构文件**：
- `report.py` — 技术概要简化为`manager.analyze_stock()`调用
- `signal_detector.py` — 添加模块化检测辅助函数
- `core/indicator_funcs.py` — 原indicators.py重命名

## ⚠️ 免责声明

本项目仅供学习研究使用，不构成任何投资建议。股市有风险，投资需谨慎。