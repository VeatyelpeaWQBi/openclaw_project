# 回测引擎选股逻辑可行性调研报告

> 调研日期：2026-03-31
> 调研人：虾虾子 🦐

---

## 1. 数据可用性分析

### 1.1 daily_data 目录

| 指标 | 数值 |
|------|------|
| 总股票数 | 1,469 只 |
| 数据时间跨度 | 2020-01-02 ~ 2026-03-30 |
| 每只股票平均行数 | ~1,500 行（约6年日K） |
| CSV 列 | 日期,开盘,最高,最低,收盘,成交量,成交额,换手率,市盈率,市净率,市销率,市现率 |

### 1.2 沪深300 + 中证1000 成分股覆盖

| 指标 | 数值 |
|------|------|
| 沪深300 成分股 | 300 只 |
| 中证1000 成分股 | 1,000 只 |
| 合并去重后 | 1,300 只 |
| 在 daily_data 中找到 | **1,300 只（100%覆盖）** ✅ |

### 1.3 数据质量评估

| 质量指标 | 数量 | 占比 |
|----------|------|------|
| 有非零 成交额/换手率 | 772 只 | 52.6% |
| 成交额/换手率为零 | 697 只 | 47.4% |
| **有成交量(成交量)数据** | **1,469 只** | **100%** ✅ |

**关键发现：**
- ✅ **OHLCV（开高低收量）数据完整**：所有1,300只成分股都有完整的开盘价、最高价、最低价、收盘价、成交量数据
- ⚠️ **成交额/换手率不完整**：约一半股票的成交额和换手率字段为零，这是数据源问题（可能是不同API下载的）
- ✅ **成交量可用于计算量比**：量比 = 当日成交量 / N日平均成交量，完全可行
- ⚠️ **换手率需补充**：如果筛选条件严格依赖换手率，需要从其他数据源补充

### 1.4 指数数据

| 文件 | 内容 | 说明 |
|------|------|------|
| `hs300_202*.csv` | 沪深300成分股列表（代码+名称） | 按年快照，无指数价格 |
| `zz1000_current.csv` | 中证1000成分股列表（代码+名称） | 1000只，无指数价格 |

**注意：** 指数文件只有成分股列表，**没有指数价格数据**。如果需要计算"相对强弱"（个股相对大盘的超额收益），需要额外获取沪深300/中证1000的指数价格序列。

### 1.5 板块归属数据

**daily_data CSV 中没有"所属板块"列。** 列仅为：日期,开盘,最高,最低,收盘,成交量,成交额,换手率,市盈率,市净率,市销率,现率。

板块数据的获取方式：
- 现有 `sector_data.py` 通过**东方财富API**实时获取板块排名和板块内个股
- 这意味着板块归属需要**运行时从API获取**，无法离线回测
- 需要预先构建 **股票→板块的映射表** 才能用于回测

---

## 2. "走势最好"指标推荐

### 2.1 候选指标对比

| 指标 | 计算复杂度 | 数据需求 | 适合T+1 | 推荐度 |
|------|-----------|----------|---------|--------|
| 当日涨跌幅 | 低 | 收盘价 | ⭐⭐⭐ | 基础排序 |
| 5日累计涨幅 | 低 | 收盘价 | ⭐⭐⭐⭐ | 推荐 |
| 10日/20日累计涨幅 | 低 | 收盘价 | ⭐⭐⭐ | 辅助 |
| 相对强弱（vs大盘） | 中 | 指数价格 | ⭐⭐⭐⭐ | 推荐但缺数据 |
| SuperTrend趋势强度 | 中 | OHLC | ⭐⭐⭐⭐ | 推荐 |
| ADX趋势强度 | 中 | OHLC | ⭐⭐⭐ | 辅助 |
| 综合评分（多因子） | 高 | 多指标 | ⭐⭐⭐⭐⭐ | 最佳但复杂 |

### 2.2 推荐方案：多因子复合评分

针对T+1场景，建议使用以下复合排序指标：

```
走势评分 = w1 × N日累计涨幅排名 + w2 × 量比排名 + w3 × SuperTrend趋势强度 + w4 × 相对位置排名
```

具体建议：
1. **5日累计涨幅**（权重40%）：反映近期强势程度
2. **量比**（权重20%）：当日成交量/5日均量，反映资金关注度
3. **SuperTrend状态**（权重20%）：日线多头=加分，空头=减分
4. **相对位置**（权重20%）：当前价在近N日高低价区间的位置（越高越好，说明在突破）

**简化版（如果不想太复杂）：**
- 仅用 **5日累计涨幅** 排序，简单有效
- 数据完全可得，计算量小

### 2.3 数据可行性

| 指标 | 数据来源 | 可行性 |
|------|----------|--------|
| N日涨幅 | daily_data 收盘价 | ✅ 完全可行 |
| 量比 | daily_data 成交量 | ✅ 完全可行 |
| SuperTrend | daily_data OHLC | ✅ 已有 `supertrend.py` |
| 相对强弱 | 需要指数价格 | ⚠️ 需额外获取 |
| 换手率 | daily_data 部分有 | ⚠️ 需补充数据源 |

---

## 3. 板块归属数据现状

### 3.1 现状

- ❌ daily_data CSV **无板块列**
- ✅ `sector_data.py` 可通过东方财富API获取板块排名和板块内个股
- ✅ `sectors.py` 定义了完整的进攻型/防御型板块关键词
- ❌ **无离线板块映射表**

### 3.2 回测中的板块筛选方案

**方案A：预构建板块映射表（推荐）**
```python
# 从东方财富API获取所有概念板块及其成分股
# 保存为 JSON/CSV：{stock_code: [sector_name1, sector_name2, ...]}
# 回测时直接查表
```

**方案B：回测时实时获取（不推荐）**
- 每个回测日调用API获取板块数据
- API调用量大（每天可能需要几十次），有被封IP风险
- 回测速度慢

**方案C：使用股票名称/代码推断板块（粗糙）**
- 基于关键词匹配股票名称
- 准确率低，不推荐

### 3.3 建议

构建一个离线板块映射文件：
- 从东方财富API批量获取所有概念板块的成分股
- 保存到 `config/stock_sector_map.json`
- 定期更新（如每周）
- 格式：`{"002409": ["半导体", "芯片", "AI芯片"], ...}`

---

## 4. 技术可行性评估

### 4.1 总体评估：**✅ 可行（有条件）**

### 4.2 计算量分析

| 步骤 | 涉及数据量 | 耗时估计 |
|------|-----------|----------|
| 加载全部日K数据 | 1,300只 × 1,500行 | ~5秒（pandas读CSV） |
| 计算N日涨幅排序 | 1,300只 | ~1秒 |
| 计算SuperTrend | 1,300只 × 60行 | ~3秒 |
| 板块筛选（查表） | ~130只（前10%） | <1秒 |
| 量化条件过滤 | ~130只 | <1秒 |
| **单日总计** | | **~10秒** |
| **回测1年（~244个交易日）** | | **~40分钟** |

**结论：** 计算量可接受。如果预计算排序指标，可以进一步加速到 ~20分钟/年。

### 4.3 现有代码复用性

| 组件 | 文件 | 复用性 |
|------|------|--------|
| SuperTrend计算 | `supertrend.py` | ✅ 直接复用 |
| 量比计算 | `stock_filter.py` | ✅ 直接复用 |
| 板块数据获取 | `sector_data.py` | ⚠️ 需改造为离线模式 |
| 量化条件过滤 | `stock_filter.py` 的 `filter_stocks()` | ✅ 直接复用 |
| 回测引擎 | `backtest.py` | ✅ 直接复用 |
| 数据加载器 | `data_loader.py` | ⚠️ 当前指向 `data/kline/`，需适配 `daily_data/` |

### 4.4 数据结构建议

```
回测流程：
1. 预处理阶段（一次性）
   - 加载 daily_data/ 所有CSV到内存（dict of DataFrames）
   - 加载成分股列表（HS300 + ZZ1000）
   - 加载板块映射表
   
2. 每日回测循环
   a. 对所有1,300只成分股计算"走势评分"
   b. 按评分排序，取前10%（~130只）
   c. 在130只中筛选有进攻型板块的
   d. 应用量化条件过滤（涨幅3-7%、换手率5-15%、量比>1.2、SuperTrend多头）
   e. 通过的股票作为买入信号
   f. 调用回测引擎执行T+1买卖
```

---

## 5. 具体实现方案

### 5.1 新增文件结构

```
tail_trading/
├── src/
│   ├── stock_screener.py      # 新增：选股筛选器
│   ├── ranking_engine.py      # 新增：走势评分排序引擎
│   └── sector_mapper.py       # 新增：板块映射工具
├── config/
│   └── stock_sector_map.json  # 新增：离线板块映射
└── src/backtest.py            # 修改：集成选股逻辑
```

### 5.2 核心代码框架

#### ranking_engine.py — 走势评分排序

```python
"""
走势评分排序引擎
对股票池中的所有股票计算综合走势评分，用于排序
"""

import pandas as pd
import numpy as np
from supertrend import is_supertrend_bullish

class RankingEngine:
    def __init__(self, daily_data_dict, stock_pool):
        """
        daily_data_dict: {code: DataFrame} 所有股票的日K数据
        stock_pool: list of str 成分股代码列表
        """
        self.data = daily_data_dict
        self.pool = stock_pool
    
    def calc_score(self, code, date_str):
        """
        计算单只股票在指定日期的走势评分
        
        返回: float 综合评分（越高越好）
        """
        df = self.data.get(code)
        if df is None or df.empty:
            return -999
        
        # 找到date_str对应行
        row_idx = df[df['date'].dt.strftime('%Y-%m-%d') == date_str].index
        if len(row_idx) == 0:
            return -999
        idx = row_idx[0]
        
        if idx < 20:  # 需要至少20天历史
            return -999
        
        # 1. 5日累计涨幅
        close_now = df.loc[idx, 'close']
        close_5d = df.loc[idx-5, 'close']
        ret_5d = (close_now - close_5d) / close_5d * 100
        
        # 2. 量比（当日量/5日均量）
        vol_now = df.loc[idx, 'volume']
        vol_avg = df.loc[idx-5:idx-1, 'volume'].mean()
        vol_ratio = vol_now / vol_avg if vol_avg > 0 else 0
        
        # 3. SuperTrend状态
        st_df = df.loc[idx-30:idx]  # 取30天数据算SuperTrend
        st_bullish = is_supertrend_bullish(st_df)
        st_score = 1.0 if st_bullish else -0.5
        
        # 4. 相对位置（20日内）
        high_20d = df.loc[idx-20:idx, 'high'].max()
        low_20d = df.loc[idx-20:idx, 'low'].max()
        position = (close_now - low_20d) / (high_20d - low_20d) if high_20d != low_20d else 0.5
        
        # 综合评分（权重可调）
        score = (ret_5d * 0.4 + 
                 vol_ratio * 10 * 0.2 + 
                 st_score * 20 * 0.2 + 
                 position * 100 * 0.2)
        
        return score
    
    def rank_all(self, date_str, top_pct=0.1):
        """对所有成分股评分并排序，返回前top_pct的股票"""
        scores = []
        for code in self.pool:
            score = self.calc_score(code, date_str)
            if score > -999:
                scores.append({'code': code, 'score': score})
        
        scores.sort(key=lambda x: x['score'], reverse=True)
        top_n = max(1, int(len(scores) * top_pct))
        return scores[:top_n]
```

#### stock_screener.py — 多层筛选器

```python
"""
多层选股筛选器
实现：走势排序 → 板块筛选 → 量化条件过滤
"""

class StockScreener:
    def __init__(self, daily_data_dict, sector_map, config=None):
        self.data = daily_data_dict
        self.sector_map = sector_map  # {code: [sector_names]}
        config = config or {}
        self.min_change = config.get('min_change', 3)
        self.max_change = config.get('max_change', 7)
        self.min_turnover = config.get('min_turnover', 5)
        self.max_turnover = config.get('max_turnover', 15)
        self.min_vol_ratio = config.get('min_vol_ratio', 1.2)
    
    def screen(self, ranked_stocks, date_str):
        """
        对排序后的股票进行多层筛选
        
        ranked_stocks: [{'code': str, 'score': float}] 排序后的股票
        date_str: 'YYYY-MM-DD'
        
        返回: list of dict 通过筛选的股票
        """
        candidates = []
        
        for stock in ranked_stocks:
            code = stock['code']
            
            # === 第二层：板块筛选 ===
            sectors = self.sector_map.get(code, [])
            has_attack_sector = any(
                is_attack_sector(s) for s in sectors
            )
            if not has_attack_sector:
                continue
            
            # === 第三层：量化条件过滤 ===
            df = self.data.get(code)
            if df is None:
                continue
            
            row = df[df['date'].dt.strftime('%Y-%m-%d') == date_str]
            if row.empty:
                continue
            row = row.iloc[0]
            
            # 涨幅
            prev_close = df[df['date'].dt.strftime('%Y-%m-%d') < date_str].tail(1)
            if prev_close.empty:
                continue
            change_pct = (row['close'] - prev_close.iloc[0]['close']) / prev_close.iloc[0]['close'] * 100
            if not (self.min_change <= change_pct <= self.max_change):
                continue
            
            # 换手率（如果有数据）
            turnover = row.get('turnover', 0)
            if turnover > 0 and not (self.min_turnover <= turnover <= self.max_turnover):
                continue
            
            # 量比
            idx = row.name
            if idx < 5:
                continue
            vol_ratio = row['volume'] / df.loc[idx-5:idx-1, 'volume'].mean()
            if vol_ratio < self.min_vol_ratio:
                continue
            
            # SuperTrend
            st_df = df.loc[max(0, idx-30):idx]
            if not is_supertrend_bullish(st_df):
                continue
            
            # 通过所有筛选
            candidates.append({
                'code': code,
                'name': code,  # 需要从文件名获取
                'score': stock['score'],
                'change_pct': round(change_pct, 2),
                'vol_ratio': round(vol_ratio, 2),
                'sectors': [s for s in sectors if is_attack_sector(s)],
            })
        
        return candidates
```

### 5.3 集成到回测引擎

在 `backtest.py` 的 `BacktestEngine` 中添加选股逻辑：

```python
def run_backtest_with_screener(self, start_date, end_date, screener_config=None):
    """
    运行带自动选股的回测
    
    流程：每日 → 排序 → 筛选 → 买入 → 次日卖出
    """
    # 1. 加载所有数据
    daily_data = self._load_all_daily_data()
    stock_pool = self._load_stock_pool()  # HS300 + ZZ1000
    sector_map = self._load_sector_map()
    
    ranking = RankingEngine(daily_data, stock_pool)
    screener = StockScreener(daily_data, sector_map, screener_config)
    
    all_dates = sorted(...)
    buy_signals = []
    
    for date_str in all_dates:
        # 排序 → 筛选
        ranked = ranking.rank_all(date_str, top_pct=0.1)
        candidates = screener.screen(ranked, date_str)
        
        for c in candidates:
            buy_signals.append({
                'date': date_str,
                'code': c['code'],
                'name': c['name'],
                'sector': ', '.join(c['sectors']),
            })
    
    # 调用现有回测引擎
    return self.run_backtest(buy_signals)
```

---

## 6. 风险和限制

### 6.1 数据风险

| 风险 | 严重程度 | 说明 | 应对方案 |
|------|----------|------|----------|
| 成交额/换手率缺失 | ⚠️ 中 | 47%的股票无此数据 | 用成交量代替量比；换手率条件设为可选 |
| 无指数价格数据 | ⚠️ 中 | 无法计算相对强弱 | 先用绝对涨幅排序，后续再补充指数数据 |
| 成分股变动 | ⚠️ 低 | HS300/ZZ1000成分股会定期调整 | 用当前成分股列表做回测，结果会有幸存者偏差 |
| 上市日期差异 | ⚠️ 低 | 部分股票数据不足20天 | 排除数据不足的股票 |

### 6.2 逻辑风险

| 风险 | 严重程度 | 说明 |
|------|----------|------|
| 板块数据回测偏差 | 🔴 高 | 回测时用的板块映射是"当前"的，历史上板块归属可能不同 |
| 幸存者偏差 | 🔴 高 | HS300/ZZ1000成分股是当前的，历史上可能包含已退市股票 |
| 过拟合风险 | ⚠️ 中 | 多因子排序+多层筛选，参数多，容易过拟合 |
| 换手率条件依赖 | ⚠️ 中 | 如果严格要求换手率5-15%，可选股票大幅减少 |

### 6.3 性能限制

| 限制 | 影响 |
|------|------|
| 每日计算1300只股票评分 | ~10秒/日，可接受 |
| SuperTrend逐行计算 | 已有实现，但对1300只股票会较慢 |
| 内存占用 | 1300只 × 1500行 ≈ 100MB，可接受 |

---

## 7. 结论和建议

### 7.1 可行性结论

**✅ 技术上可行，但需要补充以下工作：**

1. **必做：** 构建板块映射表（`stock_sector_map.json`）
2. **必做：** 适配数据加载器，使其支持 `daily_data/` 目录
3. **建议：** 将换手率筛选条件设为可选（因数据缺失）
4. **可选：** 获取指数价格数据以支持相对强弱排序
5. **可选：** 预计算排序指标以加速回测

### 7.2 推荐实现路径

**Phase 1（最小可用版，预计2-3天）：**
- 用5日累计涨幅作为排序指标
- 板块映射表用东方财富API一次性生成
- 换手率条件设为可选
- 集成到现有回测引擎

**Phase 2（增强版，预计2-3天）：**
- 实现多因子复合评分排序
- 补充指数价格数据，加入相对强弱
- 预计算指标，优化回测速度

**Phase 3（完善版，预计1-2天）：**
- 处理成分股历史变动（减少幸存者偏差）
- 参数优化和过拟合检测
- 完善回测报告

### 7.3 替代方案

如果实现复杂度太高，简化方案：
1. **仅用当日涨幅排序** → 最简单，无需额外计算
2. **跳过板块筛选** → 直接用量化条件过滤，省去板块数据问题
3. **缩小股票池** → 仅用沪深300（300只），减少计算量

---

## 附录：关键数据汇总

| 项目 | 数值 |
|------|------|
| daily_data 总股票数 | 1,469 只 |
| HS300 + ZZ1000 合并 | 1,300 只 |
| 数据覆盖率 | 100% |
| 有成交额数据 | 770/1,300 (59%) |
| 数据时间范围 | 2020-01 ~ 2026-03 |
| 平均每只股票数据量 | ~1,500 个交易日 |
| 预计单日回测耗时 | ~10 秒 |
| 预计1年回测耗时 | ~40 分钟 |
| 现有代码复用率 | ~70% |
