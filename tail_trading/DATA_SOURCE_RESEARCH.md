# A股板块数据源调研报告

> 调研日期：2026-04-01 | akshare 1.18.49 | adata (latest)

## 一、板块排名接口测试结果

### 1.1 可用接口汇总

| # | 接口 | 数据源 | 板块数 | 关键字段 | 耗时 | 状态 |
|---|------|--------|--------|----------|------|------|
| 1 | `ak.stock_sector_spot('新浪行业')` | 新浪 | 49 | 板块、涨跌幅、涨跌额、总成交量、总成交额、公司家数、**个股-涨跌幅** | 0.27s | ✅ 可用 |
| 2 | `ak.stock_sector_spot('概念')` | 新浪 | 175 | 同上 | 0.25s | ✅ 可用 |
| 3 | `ak.stock_sector_spot('地域')` | 新浪 | 31 | 同上 | ~0.3s | ✅ 可用 |
| 4 | `ak.stock_sector_spot('行业')` | 新浪 | 84 | 同上 | ~0.3s | ✅ 可用 |
| 5 | `ak.stock_board_concept_name_ths()` | 同花顺 | 365 | name, code | 7.46s | ✅ 可用 |
| 6 | `ak.stock_board_industry_name_ths()` | 同花顺 | 90 | name, code | 0.23s | ✅ 可用 |
| 7 | `ak.stock_board_concept_summary_ths()` | 同花顺 | 10(摘要) | 日期、概念名称、驱动事件、龙头股、成分股数量 | 3.07s | ✅ 可用 |
| 8 | `ak.stock_board_industry_summary_ths()` | 同花顺 | 90 | 序号、板块、**涨跌幅**、总成交量、总成交额、净流入、上涨/下跌家数、领涨股 | 0.72s | ✅ 可用 |
| 9 | `ak.stock_szse_sector_summary()` | 深交所 | 20 | 项目名称、成交金额、成交股数、成交笔数 | 0.80s | ✅ 但无涨跌幅 |
| 10 | `adata.stock.info.all_concept_code_ths()` | 同花顺 | 391 | index_code, name, concept_code, source | 1.51s | ✅ 可用 |
| 11 | `adata.stock.info.get_concept_ths()` | 同花顺 | 按个股查 | stock_code, concept_code, name, reason | 0.19s | ✅ 可用 |
| 12 | `adata.stock.info.get_concept_east()` | 东方财富 | 按个股查 | stock_code, concept_code, name, reason | 0.11s | ✅ 可用 |
| 13 | `adata.stock.info.get_plate_east()` | 东方财富 | 按个股查 | stock_code, plate_code, plate_name, plate_type | 0.12s | ✅ 可用 |
| 14 | `adata.stock.info.market_rank_sina()` | 新浪 | 4080 | stock_code, short_name, exchange, list_date（**无涨跌幅**） | 40s | ⚠️ 可用但太慢且无涨跌幅 |
| 15 | `adata.stock.info.get_industry_sw()` | 申万 | 0 | 字段完整但数据为空 | 0.11s | ❌ 无数据 |

### 1.2 被封禁接口

| # | 接口 | 数据源 | 错误 | 状态 |
|---|------|--------|------|------|
| 1 | `ak.stock_board_concept_name_em()` | 东方财富 | RemoteDisconnected | 🚫 BANNED |
| 2 | `ak.stock_board_industry_name_em()` | 东方财富 | RemoteDisconnected | 🚫 BANNED |
| 3 | `ak.stock_board_concept_cons_em()` | 东方财富 | RemoteDisconnected | 🚫 BANNED |
| 4 | `ak.stock_board_industry_cons_em()` | 东方财富 | RemoteDisconnected | 🚫 BANNED |
| 5 | `adata.stock.info.concept_constituent_east()` | 东方财富 | RemoteDisconnected | 🚫 BANNED |
| 6 | `ak.stock_zh_a_hist()` | 东方财富 | RemoteDisconnected | 🚫 BANNED |
| 7 | `adata.stock.info.all_concept_code_east()` | 东方财富 | 缓存文件不存在 | ❌ 不可用 |

> **结论：所有东方财富(eastmoney)数据源均被封禁，不可用。**

### 1.3 接口不可用但非封禁

| # | 接口 | 原因 |
|---|------|------|
| 1 | `ak.stock_board_concept_cons_ths()` | akshare无此函数 |
| 2 | `ak.stock_board_industry_cons_ths()` | akshare无此函数 |
| 3 | `ak.stock_sector_sectors()` | akshare无此函数 |
| 4 | `ak.stock_sector_fund_flow_rank('新浪行业')` | sector_type参数不接受'新浪行业' |
| 5 | `ak.stock_zh_a_hist_tx()` | period参数格式问题（需int非string） |
| 6 | `ak.stock_concept_cons_futu()` | 执行报错 |
| 7 | `adata.stock.info.get_industry_sw()` | 返回空数据 |

---

## 二、板块成分股接口测试结果

| # | 接口 | 参数方式 | 返回结果 | 状态 |
|---|------|----------|----------|------|
| 1 | `adata.stock.info.concept_constituent_ths(index_code='886108')` | **index_code** | 478只成分股，字段: stock_code, short_name | ✅ **推荐** |
| 2 | `adata.stock.info.concept_constituent_ths(name='人工智能')` | name | 0行 | ❌ 无效 |
| 3 | `adata.stock.info.concept_constituent_ths(concept_code='886072')` | concept_code | 0行 | ❌ 无效 |
| 4 | `adata.stock.info.concept_constituent_east()` | concept_code | 被封禁 | 🚫 BANNED |
| 5 | `ak.stock_board_concept_cons_em()` | name | 被封禁 | 🚫 BANNED |
| 6 | `ak.stock_board_industry_cons_em()` | name | 被封禁 | 🚫 BANNED |

### 关键发现：adata THS成分股的正确调用方式

```python
import adata
# ✅ 正确：使用 index_code（来自 all_concept_code_ths 的 index_code 列）
df = adata.stock.info.concept_constituent_ths(index_code='886108')  # AI应用

# ❌ 错误：使用 name 或 concept_code 都返回空
df = adata.stock.info.concept_constituent_ths(name='AI应用')  # 返回0行
df = adata.stock.info.concept_constituent_ths(concept_code='309264')  # 返回0行
```

---

## 三、个股行情接口测试结果

| # | 接口 | 数据源 | 数据量 | 耗时 | 状态 |
|---|------|--------|--------|------|------|
| 1 | `ak.stock_zh_a_daily('sz000001')` | 新浪 | 8322行(自上市起), 9字段 | 0.47s | ✅ 可用 |
| 2 | `adata.stock.market.get_market('000001')` | adata | 0行 | 0.04s | ❌ 无数据 |
| 3 | `ak.stock_zh_a_hist()` | 东方财富 | - | - | 🚫 BANNED |
| 4 | `ak.stock_zh_a_hist_tx()` | 腾讯 | - | - | ⚠️ 参数需修正 |

### ak.stock_zh_a_daily 字段

```
date, open, high, low, close, volume, amount, outstanding_share, turnover
```

---

## 四、推荐方案

### 方案一：新浪板块排名 + adata THS成分股（交叉数据源） ⭐推荐

这是**最可行的方案**：

```python
import akshare as ak
import adata

# Step 1: 获取板块排名（带涨跌幅）
sectors = ak.stock_sector_spot('概念')  # 175个概念板块，含涨跌幅
# 返回字段：板块、涨跌幅、涨跌额、总成交量、总成交额、公司家数 等

# Step 2: 获取 THS 概念板块代码列表
ths_codes = adata.stock.info.all_concept_code_ths()  # 391个概念
# 返回：index_code, name, concept_code, source

# Step 3: 用 index_code 获取成分股
constituents = adata.stock.info.concept_constituent_ths(index_code='886108')
# 返回：stock_code, short_name

# Step 4: 获取个股行情
import time
for stock_code in constituents['stock_code']:
    df = ak.stock_zh_a_daily(f'sz{stock_code}' if stock_code.startswith('0') or stock_code.startswith('3') else f'sh{stock_code}')
    # 处理行情数据
    time.sleep(0.5)  # 控制频率
```

**注意**：此方案为交叉数据源，新浪板块名称和同花顺板块名称**不完全一致**，需要做名称匹配或放弃名称匹配，直接使用THS板块排名。

### 方案二：同花顺行业排名 + adata THS成分股（同源方案）

```python
import akshare as ak
import adata

# Step 1: 获取THS行业排名（含涨跌幅）
industry_rank = ak.stock_board_industry_summary_ths()  # 90个行业，含涨跌幅
# 返回：板块、涨跌幅、总成交量、总成交额、净流入、上涨/下跌家数、领涨股

# Step 2: 获取THS行业代码
industry_names = ak.stock_board_industry_name_ths()  # 90个行业，name + code

# Step 3: 合并排名和代码
# 同花顺行业板块，industry_names 的 code 可直接用于 adata

# Step 4: 获取成分股（需要找到正确的映射关系）
# adata THS成分股使用 index_code，需要确认行业板块的 index_code
```

### 方案三：纯新浪方案（板块排名 + 仅领涨股）

```python
import akshare as ak

# 新浪板块包含领涨股信息
sectors = ak.stock_sector_spot('概念')
# 每个板块包含：股票代码、个股-涨跌幅、个股-当前价、个股-涨跌额、股票名称
# 但只有领涨的一只股票，不是完整成分股列表

# 获取完整成分股需另行解决
```

---

## 五、需要避免的数据源

| 数据源 | 原因 | 替代方案 |
|--------|------|----------|
| **东方财富 (eastmoney/EM)** | 全面封禁，所有接口 Connection refused | 使用新浪/同花顺 |
| `ak.stock_zh_a_hist()` | 依赖东方财富，被封禁 | 使用 `ak.stock_zh_a_daily()` (新浪) |
| `ak.stock_zh_a_hist_tx()` | 参数格式有问题 | 使用 `ak.stock_zh_a_daily()` (新浪) |
| `adata.stock.market.get_market()` | 返回空数据 | 使用 `ak.stock_zh_a_daily()` |
| `adata.stock.info.get_industry_sw()` | 返回空数据 | 使用 akshare 同花顺行业 |
| `adata.stock.info.market_rank_sina()` | 40秒超慢，无涨跌幅 | 使用 `ak.stock_sector_spot()` |

---

## 六、最终推荐架构

```
板块排名层：
├── 概念板块 → ak.stock_sector_spot('概念') [新浪] → 涨跌幅、成交量、成交额
├── 行业板块 → ak.stock_board_industry_summary_ths() [同花顺] → 涨跌幅、净流入、领涨股
└── 行业代码 → ak.stock_board_industry_name_ths() → 板块代码映射

成分股层：
├── THS概念成分股 → adata.stock.info.concept_constituent_ths(index_code=xxx)
└── THS行业成分股 → 待确认（adata暂无industry_constituent_ths接口）

行情层：
└── 个股日K → ak.stock_zh_a_daily('sz/sh + code') [新浪] → OHLCV完整数据
```

### 待解决问题

1. **THS行业成分股**：adata没有 `industry_constituent_ths` 接口，需要寻找其他方式获取行业板块成分股
2. **新浪板块与THS板块的名称映射**：如果使用交叉数据源，板块名称可能不一致
3. **成分股获取效率**：每个板块需单独请求成分股，391个板块 × 1次请求 = 较慢
4. **新浪行情限频**：批量获取个股行情需要控制频率（建议0.5s间隔）

---

*调研完成于 2026-04-01 22:24*
