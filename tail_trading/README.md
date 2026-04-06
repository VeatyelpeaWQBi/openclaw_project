# 📈 tail_trading — A股量化交易系统

包含两个策略（Nomad T1 尾盘选股 + 海龟交易法趋势跟踪）和一套通用交易执行引擎。

## 🚀 快速开始

### 安装依赖

```bash
pip install -r requirements.txt
```

依赖：`pandas`, `numpy`, `matplotlib`

### 运行

```bash
# 直接运行主程序
python3 main.py
```

### 配置

路径配置在 `config/paths.json`（不提交Git，首次需自行创建）：

```json
{
  "project_root": "/path/to/tail_trading",
  "data_dir": "/path/to/tail_trading/data",
  "db_path": "/path/to/tail_trading/data/stock_data.db",
  "reports_dir": "/path/to/reports"
}
```

## 📁 项目结构

```
tail_trading/
├── main.py                          # 主程序入口
├── requirements.txt                 # Python依赖
│
├── config/                          # 配置
│   ├── paths.json                   # 路径配置（不提交）
│   ├── sectors.py                   # 板块配置
│   └── sectors_config.yaml          # 板块详细配置
│
├── core/                            # 核心层（数据/指标/存储）
│   ├── data_access.py               # 数据访问（API调用封装）
│   ├── storage.py                   # SQLite存储 + 海龟交易表初始化
│   ├── indicators.py                # 技术指标计算
│   ├── log_setup.py                 # 日志配置
│   └── paths.py                     # 路径加载
│
├── executor/                        # 🔹 交易执行器（新）
│   ├── models.py                    #   TradeCommand / TradeResult 数据模型
│   ├── trade_executor.py            #   通用交易执行器（底层，不绑定账户类型）
│   └── robot_executor.py            #   海龟机器人执行器（业务调度层）
│
├── strategies/                      # 策略层
│   ├── base.py                      #   策略基类
│   ├── nomad_t1/                    #   Nomad T1 尾盘T+1策略
│   │   ├── strategy.py              #     策略主逻辑
│   │   ├── filters.py               #     选股过滤条件
│   │   └── report.py                #     报告生成
│   └── turtle/                      #   海龟交易法策略
│       ├── strategy.py              #     策略主逻辑（遍历账户，调度信号+执行）
│       ├── signal_checker.py        #     信号检测（止损/退出/加仓/开仓）
│       ├── position_manager.py      #     持仓管理（CRUD + 流水）
│       ├── account_manager.py       #     账户管理（资金/入出金/绑定）
│       ├── candidate_pool.py        #     候选池管理
│       ├── breakout.py              #     唐奇安通道突破检测
│       ├── atr.py                   #     ATR计算与仓位管理
│       ├── filters.py               #     趋势过滤（均线多头）
│       ├── report.py                #     海龟报告生成
│       └── message_parser.py        #     QQ消息解析
│
├── job/                             # 定时任务
│   └── update_daily_kline.py        #   日K数据更新
│
├── test_case/                       # 测试用例
├── data/                            # SQLite数据库（不提交）
├── reports/                         # 报告输出（不提交）
├── logs/                            # 运行日志（不提交）
└── output/                          # 其他输出（不提交）
```

## 🏗️ 架构说明

### 三层架构

```
┌─────────────────────────────────────────────┐
│  Strategies（策略层）                         │
│  Nomad T1 / Turtle                           │
│  负责：什么时候做、做什么                       │
├─────────────────────────────────────────────┤
│  Executor（执行层）                           │
│  RobotExecutor / TradeExecutor               │
│  负责：怎么做、能不能做                         │
├─────────────────────────────────────────────┤
│  Core（基础层）                               │
│  data_access / storage / indicators          │
│  负责：数据获取、存储、指标计算                  │
└─────────────────────────────────────────────┘
```

### TradeExecutor（通用交易执行器）

独立模块，不绑定账户类型。人工账户和机器账户均可调用。

```python
from executor.trade_executor import TradeExecutor
from executor.models import TradeCommand, TradeAction

executor = TradeExecutor()
cmd = TradeCommand(action=TradeAction.CLOSE, code='000001', price=12.50, reason='止损')
result = executor.execute(account_id, cmd)
```

支持操作：`OPEN(开仓)` / `ADD(加仓)` / `REDUCE(减仓)` / `CLOSE(平仓)`

内含前置校验：资金检查、持仓存在性、T+1锁定、单位数上限、冷却期

### RobotExecutor（机器人执行器）

调用 TradeExecutor 执行海龟交易法业务逻辑。

```python
from executor.robot_executor import RobotExecutor

robot = RobotExecutor()
# 批量执行信号队列
result = robot.execute_signals(account_id, action_queue)
# 单笔操作
robot.close_position(account_id, '000001', 12.50, reason='stop_loss')
robot.add_position(account_id, '000001', 13.00, atr=0.85)
```

### 海龟交易法信号优先级

1. **止损**：收盘价 ≤ 入场价 - 2×ATR → 立即退出
2. **退出**：收盘价 < N日唐奇安通道下轨 → 趋势结束
3. **减仓**：盈利达1N → 减1单位（仅执行一次）
4. **加仓**：收盘价 ≥ 上次加仓价 + 0.5×ATR → 加1单位（最多4单位）
5. **开仓**：N日突破 + 均线多头 → 入场

### 多账户体系

| 账户类型 | simulator值 | 说明 |
|---------|------------|------|
| 机器模拟 | 0 | 信号自动执行，通过 RobotExecutor |
| 手工账户 | 1 | 仅生成信号，人工手动操作 |

支持多账户并行运行，每账户独立资金、持仓、冷却状态。

## ⚙️ 环境要求

- Python 3.10+
- SQLite 3
- 妙想金融API Key（环境变量 `MX_APIKEY`）

## ⚠️ 免责声明

本项目仅供学习研究使用，不构成任何投资建议。股市有风险，投资需谨慎。
