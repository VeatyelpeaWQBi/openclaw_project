# tail_trading 重构任务

## 背景
东方财富(EM)数据源在服务器上被完全封禁，需要切换到同花顺(THS)数据源。
同时项目架构需要优化，将数据获取、策略逻辑、入口编排分离。

## 可用的 THS API（已验证通过）

### 板块相关
- `ak.stock_board_concept_name_ths()` → 返回 `['name', 'code']`，375个概念板块
- `ak.stock_board_industry_name_ths()` → 返回 `['name', 'code']`，行业板块
- `ak.stock_board_concept_index_ths(symbol='板块名')` → 返回 `['日期','开盘价','最高价','最低价','收盘价','成交量','成交额']`，历史指数数据
- `ak.stock_board_industry_index_ths(symbol='板块名')` → 同上，行业板块指数

### 板块涨跌幅计算方法
取 `stock_board_concept_index_ths` 返回的最后2行数据：
```python
today_close = df.iloc[-1]['收盘价']
yesterday_close = df.iloc[-2]['收盘价']
change_pct = (today_close - yesterday_close) / yesterday_close * 100
```

### 成分股问题
THS 没有可用的成分股 API：
- `ak.stock_board_concept_cons_ths` 不存在
- `adata.stock.info.concept_constituent_ths()` 返回空 DataFrame

**解决方案**: 成分股获取仍走 EM (`ak.stock_board_concept_cons_em`)，如果 EM 也失败则跳过该板块。

### 个股日K线
- `ak.stock_zh_a_hist(symbol, period="daily", start_date, end_date, adjust="qfq")` — EM日K，可能在服务器上也被封
- 备用: `ak.stock_zh_a_hist_tx()` 腾讯数据源
- 备用: `adata.stock.market.sina_market.get_market()` 新浪数据源

## 重构要求

### 1. data_source.py 重构

#### 1.1 数据源配置化
在文件顶部定义数据源优先级配置：
```python
# 数据源配置
SECTOR_RANKING_SOURCE = 'ths'  # 可选: 'em', 'ths'
SECTOR_STOCKS_SOURCE = 'em'    # 成分股获取（THS无可用接口，暂用EM）
STOCK_KLINE_SOURCE = 'auto'    # 可选: 'em', 'tx', 'sina', 'auto'（自动降级）
```

#### 1.2 THS 板块排名方法（新增）
```python
def _ths_sector_ranking_concept():
    """THS 概念板块排名（含涨跌幅）"""
    # 1. 获取板块列表: ak.stock_board_concept_name_ths()
    # 2. 逐板块获取指数数据计算涨跌幅: ak.stock_board_concept_index_ths()
    # 3. 返回标准化 DataFrame: ['code', 'name', 'change_percent', 'volume', 'amount']
    # 注意: 需要 sleep 避免请求过快，预计2-3分钟完成

def _ths_sector_ranking_industry():
    """THS 行业板块排名（含涨跌幅）"""
    # 同上，用 industry 接口
```

#### 1.3 THS 成分股方法（新增，带降级）
```python
def _ths_sector_stocks(sector_name):
    """THS 板块成分股（尝试THS，失败降级到EM）"""
    # 由于THS无成分股接口，直接走EM降级
    # 或者未来如果有可用THS接口可以加回来
```

#### 1.4 路由函数改造
`get_sector_ranking()` 根据 `SECTOR_RANKING_SOURCE` 配置选择数据源：
```python
def get_sector_ranking(sector_type=2, limit=20):
    if SECTOR_RANKING_SOURCE == 'ths':
        # 调用 THS 方法
    else:
        # 调用 EM 方法（保留原有逻辑）
```

`get_sector_stocks()` 根据 `SECTOR_STOCKS_SOURCE` 配置选择数据源。

`get_stock_daily_kline()` 保持多数据源自动降级不变。

#### 1.5 保留所有原有 EM 方法
不要删除任何 EM 相关函数，只在上面添加新的 THS 方法。

### 2. sector_data.py 和 stock_data.py 改造
这两个文件是薄封装层，调用者不应关心数据源。

sector_data.py 应暴露：
- `get_sector_ranking(sector_type, limit)` — 获取板块排名
- `get_sector_stocks(sector_code, sector_type, limit)` — 获取板块成分股

stock_data.py 应暴露：
- `get_stock_daily_kline(code, market, days)` — 获取日K数据
- `get_stock_daily_kline_range(code, market, start, end)` — 获取日K数据（指定日期范围）
- `get_stock_realtime(code)` — 获取实时行情

### 3. 策略引擎抽取（新建 strategy.py）

将 main.py 中的策略逻辑抽取到 `strategy.py`，main.py 只保留：
- 环境初始化
- 调用策略引擎
- 通知/报告输出

strategy.py 应包含：
- `class TailT1Strategy` 或 `def run_tail_t1_strategy()`
- 内部调用 sector_data、stock_data 获取数据
- 内部实现选股逻辑、SuperTrend 判断
- 返回筛选结果

### 4. main.py 简化

main.py 最终结构：
```python
def run():
    # 1. 环境初始化
    # 2. 调用策略引擎
    result = run_tail_t1_strategy()
    # 3. 生成报告
    # 4. 保存信号和报告
    # 5. 返回结果（供通知使用）
```

## 约束
- 不要删除任何现有代码中的 EM 方法，保留作为备用
- 所有 THS API 调用之间加 `time.sleep(random.uniform(1, 3))` 避免被封
- 代码风格保持与现有代码一致
- 路径引用使用 `paths.py` 中的常量
