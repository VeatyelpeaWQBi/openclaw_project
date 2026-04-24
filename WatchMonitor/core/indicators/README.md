# 📈 Indicators — 技术指标模块化系统

## 设计原则

### 单一职责封装

每个技术指标类完全独立，遵循以下原则：

1. **初始化时完成所有计算**：传入日K DataFrame，内部自动计算
2. **统一输出接口**：信号、报告、评分三个接口
3. **黑盒设计**：外部只获取结果，不做二次加工
4. **生命周期管理**：分析完毕后由Manager负责释放

### 生命周期

```
┌─────────────────────────────────────────────────────┐
│  1. IndicatorManager.analyze_stock(code, df, ctx)   │
│     ↓                                               │
│  2. 动态实例化各指标对象                              │
│     cls(df, config, context)                        │
│     ↓                                               │
│  3. __init__内部完成计算                             │
│     _calculate() → _detect_signals()                │
│     _generate_report() → _calculate_score()         │
│     ↓                                               │
│  4. 外部获取结果                                     │
│     get_signals() / get_report_lines() / get_score()│
│     ↓                                               │
│  5. cleanup()释放资源                               │
└─────────────────────────────────────────────────────┘
```

## 模块列表

| 模块 | 类名 | 功能 | 权重 |
|-----|------|------|------|
| `supertrend_indicator.py` | SuperTrendIndicator | 趋势方向判断 | 2.0 |
| `macd_indicator.py` | MACDIndicator | 趋势强度判断 | 1.0 |
| `rsi_indicator.py` | RSIIndicator | 超买超卖判断 | 1.0 |
| `ma_indicator.py` | MAIndicator | 多周期均线支撑压力 | 1.0 |
| `adx_indicator.py` | ADXIndicator | 趋势强度判断 | 2.0 |
| `volume_indicator.py` | VolumeIndicator | 放量缩量判断 | 0.5 |
| `candle_indicator.py` | CandleIndicator | K线形态识别 | 0.5 |

## 使用示例

```python
from core.indicators import IndicatorManager

manager = IndicatorManager()

# 一站式分析
context = {
    'current_price': 11.0,
    'is_position': True,
    'position_type': '趋势',
}
result = manager.analyze_stock('000001', df, context)

# 直接获取报告内容（无需二次加工）
for line in result['report_lines']:
    print(line)

# 输出示例：
#     - SuTd: 多头⬆ （多→空切换点10.77，距-2.2%）
#     - RSI: 43.8(中性)
#     - MACD: 柱→ DIF→ DEA→ →无方向
#     - 均线位置: MA5⬇ MA10⬇ MA20⬇ MA60⬆
#     - 均线趋势: MA5→ MA10→
#     - ⚪ ADX: 5
```

## 配置文件

`config/indicators.yaml`：

```yaml
indicators:
  - name: supertrend
    enabled: true        # 是否启用
    weight: 2.0          # 评分权重
    params:              # 计算参数
      atr_period: 10
      multiplier: 3.0
    report_order: 1      # 报告展示顺序

  - name: volume
    enabled: false       # 可随时禁用某个指标
```

## 扩展新指标

1. 创建新文件 `xxx_indicator.py`
2. 继承 `BaseIndicator` 基类
3. 实现四个内部方法：
   - `_calculate()` — 计算指标值
   - `_detect_signals()` — 检测信号
   - `_generate_report()` — 生成报告内容
   - `_calculate_score()` — 计算评分
4. 在 `manager.py` 的 `_indicator_registry` 中注册
5. 在 `indicators.yaml` 中添加配置

## 基类接口

```python
class BaseIndicator(ABC):
    name: str = ""           # 指标名称
    display_name: str = ""   # 报告展示名称

    def __init__(self, df, config, context):
        # 初始化时完成所有计算
        self._calculate()
        self._detect_signals()
        self._generate_report()
        self._calculate_score()

    # 内部方法（子类实现）
    def _calculate(self) -> None: pass
    def _detect_signals(self) -> None: pass
    def _generate_report(self) -> None: pass
    def _calculate_score(self) -> None: pass

    # 外部接口（统一API）
    def get_signals(self) -> List[Dict]: pass
    def get_report_lines(self) -> List[str]: pass
    def get_score(self) -> Tuple[float, List[str]]: pass
    def cleanup(self) -> None: pass
```