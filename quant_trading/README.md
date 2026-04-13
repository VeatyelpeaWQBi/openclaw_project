# 📈 quant_trading — A股量化交易系统

包含两个策略（Nomad T1 尾盘选股 + 趋势交易法趋势跟踪）和一套分层架构的交易执行引擎。

## 🚀 快速开始

### 安装依赖

```bash
pip install -r requirements.txt
```

依赖：`pandas`, `numpy`, `matplotlib`, `akshare`, `sentence-transformers`

### 运行

```bash
# 直接运行主程序（默认 nomad_t1 策略）
python3 main.py

# 运行趋势交易策略
python3 main.py trend_trading
```

### 配置

路径配置在 `config/paths.json`（不提交Git，首次需自行创建）：

```json
{
  "project_root": "/path/to/quant_trading",
  "data_dir": "/path/to/DATA",
  "db_path": "/path/to/DATA/stock_data.db",
  "reports_dir": "/path/to/reports"
}
```

## 📁 项目结构

```
quant_trading/
├── main.py                          # 主程序入口
├── requirements.txt                 # Python依赖
│
├── config/                          # 配置
│   ├── paths.json                   # 路径配置（不提交）
│   ├── sectors.py                   # 板块配置
│   └── sectors_config.yaml          # 板块详细配置
│
├── core/                            # 核心模块（数据/指标/存储）
│   ├── data_access.py               #   数据访问（API调用封装）
│   ├── storage.py                   #   SQLite存储（含ADX/RS/VCP读写）
│   ├── indicators.py                #   技术指标计算（SuperTrend等）
│   ├── log_setup.py                 #   日志配置
│   └── paths.py                     #   路径加载
│
├── infra/                           # 🔹 基础设施层（纯通用，不绑定策略）
│   ├── account_manager.py           #   账户管理（资金/入出金/绑定）
│   ├── position_manager.py          #   持仓管理（CRUD + 流水 + 费率）
│   ├── trade_executor.py            #   通用交易执行器（T+1校验/指令分发）
│   └── models.py                    #   数据模型（TradeCommand/TradeResult/StrategyContext）
│
├── executor/                        # 机器人执行器（薄代理）
│   └── robot_executor.py            #   代理 TrendTradingExecutor
│
├── strategies/                      # 策略层
│   ├── base.py                      #   策略基类
│   ├── nomad_t1/                    #   Nomad T1 尾盘T+1策略
│   │   ├── strategy.py
│   │   ├── filters.py
│   │   └── report.py
│   └── trend_trading/               #   趋势交易策略
│       ├── strategy.py              #     策略主逻辑（遍历账户，调度信号+执行）
│       ├── trend_trading_executor.py      #     趋势交易执行层
│       ├── trend_trading_position_manager.py  # 趋势交易持仓层（ATR/S1/S2）
│       ├── signal_checker.py        #     信号检测（止损/退出/加仓/开仓）
│       ├── candidate_pool.py        #     候选池管理
│       ├── breakout.py              #     唐奇安通道突破检测
│       ├── atr.py                   #     ATR计算与仓位管理
│       ├── filters.py               #     趋势过滤（均线多头）
│       ├── report.py                #     报告生成
│       ├── message_parser.py        #     QQ消息解析
│       └── score/                   #     ⭐ 评分模块
│           ├── _base.py             #       共享工具（日期获取、批量预加载）
│           ├── rs_core.py           #       RS相对强度评分
│           ├── vcp_core.py          #       VCP波动收缩评分
│           └── adx_core.py          #       ADX趋势强度评分
│
├── db_init/                         # 数据库初始化SQL
│   └── init_all.sql                 #   全部表定义
│
├── job/                             # 定时任务
│   ├── update_daily_kline.py        #   全市场日K增量更新（新浪原生API）
│   ├── calc_scores.py               #   ⭐ 统一评分入口（RS+VCP+ADX一键刷新）
│   ├── calc_rs_score.py             #   RS相对强度评分（支持 --full/--days N）
│   ├── calc_vcp_score.py            #   VCP波动收缩评分（支持 --full/--days N）
│   ├── calc_adx_score.py            #   ADX趋势强度评分（支持 --full/--days N）
│   ├── fetch_index_info.py          #   获取全A股指数元数据（中证指数官网）
│   ├── fetch_index_members.py       #   获取指数成分股列表
│   ├── fetch_index_daily_kline.py   #   获取指数日K数据
│   └── calc_index_median_volume.py  #   计算指数成分股成交量/额中位数
│
├── test_case/                       # 测试用例
├── scripts/                         # 辅助脚本
├── docs/                            # 文档
└── reports/                         # 本地报告缓存
```

## 🏗️ 架构说明

### 四层架构

```
┌─────────────────────────────────────────────────┐
│  Strategies（策略层）                              │
│  负责：策略特有计算 + 评分模块                       │
│  ┌─────────────────────────────────────────┐     │
│  │ score/                                   │     │
│  │  RS(相对强度) · VCP(波动收缩) · ADX(趋势)  │     │
│  └─────────────────────────────────────────┘     │
├─────────────────────────────────────────────────┤
│  Executor（调度层）                               │
│  RobotExecutor（薄代理）                          │
│  负责：信号队列排序、调用策略执行器                   │
├─────────────────────────────────────────────────┤
│  Infra（基础设施层）                               │
│  TradeExecutor / PositionManager / AccountManager│
│  负责：纯通用执行（T+1校验、CRUD、费率、流水）       │
├─────────────────────────────────────────────────┤
│  Core（核心层）                                   │
│  data_access / storage / indicators              │
│  负责：数据获取、存储、指标计算                      │
└─────────────────────────────────────────────────┘
```

### 手工账户 vs 机器账户

| 账户类型 | simulator值 | 执行方式 |
|---------|------------|---------|
| 机器模拟 | 0 | 信号 → TrendTradingExecutor 自动执行 |
| 手工账户 | 1 | 信号 → QQ通知 → 虾虾子调用 TradeExecutor 执行 |

### 趋势交易信号优先级

1. **止损**：收盘价 ≤ 入场价 - 2×ATR → 立即退出
2. **退出**：收盘价 < N日唐奇安通道下轨 → 趋势结束
3. **减仓**：盈利达1N → 减1单位（仅执行一次）
4. **加仓**：收盘价 ≥ 上次加仓价 + 0.5×ATR → 加1单位（最多4单位）
5. **开仓**：N日突破 + 均线多头 → 入场

## ⭐ 评分体系

### 三维评分

| 评分 | 算法 | 维度 | 输出表 |
|------|------|------|--------|
| RS（相对强度） | 个股250日涨幅 vs 基准指数涨幅，百分位排名 | 动量 | `rs_score` |
| VCP（波动收缩） | 收缩率+突破+成交量，多周期 | 形态 | `vcp_score` |
| ADX（趋势强度） | Wilder's 平滑方向指标，0-100映射 | 趋势 | `adx_score` |

### 统一入口

```bash
# 全量刷新（计算全部股票历史）
python3 -m job.calc_scores --full

# 增量刷新（最近N天，含预热数据）
python3 -m job.calc_scores --days 30

# 单独刷新某项
python3 -m job.calc_rs_score 000510 --days 30
python3 -m job.calc_adx_score --days 30
python3 -m job.calc_vcp_score --days 30
```

### ADX 评分算法

- 严格遵循 Wilder's 平滑（RMA）
- 首个ADX周期使用SMA初始化，后续递推
- 预热期27日（14日TR/DI + 13日ADX/DX）为NaN
- 评分公式：`score = int(ADX / 80 * 100)`，映射到 0-100

## ⏰ JOB 定时任务

| JOB 文件 | 功能 | 数据源 | 输出表 |
|-----------|------|--------|--------|
| `update_daily_kline.py` | 全市场日K增量更新 | 新浪财经原生API | `daily_kline` |
| `calc_scores.py` | 统一评分（RS+VCP+ADX） | 本地DB | `rs_score` / `vcp_score` / `adx_score` |
| `calc_rs_score.py` | RS相对强度评分 | 本地DB | `rs_score` |
| `calc_vcp_score.py` | VCP波动收缩评分 | 本地DB | `vcp_score` |
| `calc_adx_score.py` | ADX趋势强度评分 | 本地DB | `adx_score` |
| `fetch_index_info.py` | 获取全A股指数元数据 | 中证指数官网 | `index_info` |
| `fetch_index_members.py` | 获取指定指数成分股列表 | 中证指数官网 | `index_members` |
| `fetch_index_daily_kline.py` | 获取指定指数日K数据 | 中证指数官网 | `index_daily_kline` |
| `calc_index_median_volume.py` | 计算指数成分股中位成交量 | 本地DB | `index_info` |

### Cron 配置

| 任务 | 调度时间 | 说明 |
|------|----------|------|
| `update_daily_kline` | 工作日 14:48 / 19:00 | 盘中+盘后各更新一次 |
| `calc_scores` | 日K更新后自动触发 | 统一评分流水线 |

## 📊 日K数据更新

通过新浪财经原生API获取全市场当日行情，写入 `daily_kline` 表。

| 字段 | 来源 |
|------|------|
| OHLCV | 新浪API直接获取 |
| 涨跌幅 change_pct | 新浪API |
| 换手率 turnover | 新浪API |
| 市盈率/市净率 | 新浪API |
| 总市值/流通市值 | 新浪API |
| 流通股本 | 由 流通市值÷收盘价 反推 |
| 量比 | 由 当日成交量÷前5交易日均量 计算 |

`update_daily_kline.py` 更新完成后自动调用 `calc_scores.py` 刷新当日评分。

## ⚙️ 环境要求

- Python 3.10+
- SQLite 3
- Python依赖：`pandas`, `numpy`, `matplotlib`, `akshare`, `sentence-transformers`

### 数据库初始化

```bash
sqlite3 data/stock_data.db < db_init/init_all.sql
```

### 表结构概览

| 表名 | 说明 |
|------|------|
| daily_kline | 日K线数据（含换手率/PE/PB/市值/涨跌幅） |
| minute_kline | 分钟K线数据 |
| index_daily_kline | 指数日K线数据 |
| rs_score | RS相对强度评分历史 |
| vcp_score | VCP波动收缩评分历史 |
| adx_score | ADX趋势强度评分历史 |
| index_info | 指数元数据 |
| index_members | 指数成分股列表 |
| trade_calendar | 交易日历 |
| account | 账户（多账户） |
| account_flow | 资金流水 |
| positions | 持仓（策略特有字段带 turtle_ 前缀） |
| position_flow | 持仓流水 |
| watchlist | 候选池 |
| stock_info | 股票基础信息 |
| fear_greed_history | 恐贪指数历史（8维度因子） |

## ⚠️ 免责声明

本项目仅供学习研究使用，不构成任何投资建议。股市有风险，投资需谨慎。
