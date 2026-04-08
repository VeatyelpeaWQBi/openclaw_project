# A股恐贪指数（Fear & Greed Index）自建方案调研报告

> 📅 调研日期：2026-03-31  
> 📌 目标：为A股尾盘交易系统自建恐贪指数（0-100，50为中性）  
> 📌 替代方案：funddb.cn（有反爬保护，无法直接获取）

---

## 1. CNN恐贪指数（美股经典方案）详解

### 1.1 概述

CNN的Fear & Greed Index是全球最知名的市场情绪指标，由CNN Business发布，综合7个因子判断美股市场情绪，范围0-100：
- **0-25**: Extreme Fear（极度恐慌）
- **25-45**: Fear（恐慌）
- **45-55**: Neutral（中性）
- **55-75**: Greed（贪婪）
- **75-100**: Extreme Greed（极度贪婪）

### 1.2 七个因子详解

#### 因子1：Stock Price Strength（股价强度）
| 项目 | 说明 |
|------|------|
| **含义** | 纽交所创新高的股票数量 vs 创新低的股票数量 |
| **计算** | 将创新高股票数映射为0-100的分值，越多新高越贪婪 |
| **逻辑** | 牛市中大量股票创新高，熊市中大量股票创新低 |
| **权重** | 1/7 ≈ 14.3% |

#### 因子2：Stock Price Breadth（股价广度）
| 项目 | 说明 |
|------|------|
| **含义** | 上涨成交量 vs 下跌成交量的比值（McClellan Volume Summation Index） |
| **计算** | 基于上涨股成交量和下跌股成交量的差值 |
| **逻辑** | 真正的牛市应该有广泛的成交量支撑 |
| **权重** | 1/7 ≈ 14.3% |

#### 因子3：Put and Call Options（看跌/看涨期权比）
| 项目 | 说明 |
|------|------|
| **含义** | CBOE的Put/Call Ratio |
| **计算** | 看跌期权成交量 / 看涨期权成交量 |
| **逻辑** | 高Put/Call Ratio → 恐慌（投资者买保护）；低比值 → 贪婪 |
| **权重** | 1/7 ≈ 14.3% |

#### 因子4：Junk Bond Demand（垃圾债券需求）
| 项目 | 说明 |
|------|------|
| **含义** | 垃圾债券（高收益债）收益率 vs 投资级债券收益率的差距 |
| **计算** | 投资级债券收益率 - 高收益债收益率的差值 |
| **逻辑** | 投资者追逐高收益（垃圾债） → 贪婪；逃离垃圾债 → 恐慌 |
| **权重** | 1/7 ≈ 14.3% |

#### 因子5：Market Volatility（市场波动率）
| 项目 | 说明 |
|------|------|
| **含义** | VIX指数（CBOE波动率指数） |
| **计算** | 将VIX值映射到0-100范围（VIX越高越恐慌） |
| **逻辑** | VIX被称为"恐慌指数"，高VIX意味着投资者大量买入期权保护 |
| **权重** | 1/7 ≈ 14.3% |

#### 因子6：Safe Haven Demand（避险需求）
| 项目 | 说明 |
|------|------|
| **含义** | 股票收益率 vs 国债收益率的差距（股债收益差） |
| **计算** | S&P 500收益率 - 10年期国债收益率的20日差值 |
| **逻辑** | 投资者更愿意持有国债避险 → 恐慌；愿意持有股票 → 贪婪 |
| **权重** | 1/7 ≈ 14.3% |

#### 因子7：Stock Price Momentum（股价动量）
| 项目 | 说明 |
|------|------|
| **含义** | S&P 500 vs 125日均线的偏离度 |
| **计算** | (S&P500当前价 - 125日均线) / 125日均线 × 100% |
| **逻辑** | 远高于均线 → 贪婪（可能过度扩张）；远低于均线 → 恐慌 |
| **权重** | 1/7 ≈ 14.3% |

### 1.3 综合计算方法

每个因子被标准化到0-100，然后取等权平均：
```
Fear_Greed_Index = (因子1 + 因子2 + 因子3 + 因子4 + 因子5 + 因子6 + 因子7) / 7
```

**标准化方法**：使用百分位数排名（Percentile Rank），对比过去N个交易日（通常1年=252个交易日）的历史数据，将当前值映射为百分位，再乘以100。

### 1.4 关键特点
- **等权设计**：7个因子权重相同（1/7）
- **百分位标准化**：基于历史分布，自动适应不同市场环境
- **多维度验证**：涵盖量价、期权、债券、波动率等多个维度
- **实时更新**：交易日内实时计算

---

## 2. 韭圈儿恐贪指数（A股）详解

### 2.1 概述

韭圈儿（funddb.cn）的恐贪指数是为A股市场设计的本土化情绪指标，同样采用0-100的评分体系。

### 2.2 六个维度详解

#### 维度1：指数波动（Market Volatility）
| 项目 | 说明 |
|------|------|
| **含义** | A股主要指数的短期波动率 |
| **数据源** | 沪深300、中证500等主要指数 |
| **逻辑** | 波动率升高 → 恐慌；波动率降低 → 贪婪/稳定 |
| **对标** | 类似CNN的VIX因子 |

#### 维度2：北上资金（Northbound Capital Flow）
| 项目 | 说明 |
|------|------|
| **含义** | 通过沪股通/深股通流入A股的外资净额 |
| **数据源** | 港交所互联互通数据 |
| **逻辑** | 外资大幅流入 → 贪婪；大幅流出 → 恐慌 |
| **特点** | 北上资金被视为"聪明钱"的代表 |

#### 维度3：股价强度（Price Strength）
| 项目 | 说明 |
|------|------|
| **含义** | A股中高于均线的股票占比，或涨跌家数比 |
| **数据源** | 全A个股日K数据 |
| **逻辑** | 更多股票站上均线 → 贪婪；更多跌破均线 → 恐慌 |
| **对标** | 类似CNN的Stock Price Strength |

#### 维度4：升贴水率（Futures Basis）
| 项目 | 说明 |
|------|------|
| **含义** | 股指期货（IF/IH/IC）的升贴水幅度 |
| **数据源** | 沪深300期货、上证50期货等 |
| **逻辑** | 贴水扩大 → 恐慌（期货投资者悲观）；升水 → 贪婪 |
| **特点** | 期货市场往往领先现货反映情绪 |

#### 维度5：避险天堂（Safe Haven）
| 项目 | 说明 |
|------|------|
| **含义** | 股票收益率 vs 债券收益率/国债收益率的比较 |
| **数据源** | A股指数收益率 vs 10年期国债收益率 |
| **逻辑** | 债券收益率走低（资金避险） → 恐慌；股票更受青睐 → 贪婪 |
| **对标** | 类似CNN的Safe Haven Demand |

#### 维度6：杠杆水平（Margin Leverage）
| 项目 | 说明 |
|------|------|
| **含义** | 融资买入额/融资余额的变化 |
| **数据源** | 两融数据（沪深交易所每日公布） |
| **逻辑** | 融资余额增加（加杠杆） → 贪婪；去杠杆 → 恐慌 |
| **特点** | 融资余额是A股特有的散户情绪晴雨表 |

### 2.3 与CNN指数的主要差异
| 对比项 | CNN（美股） | 韭圈儿（A股） |
|--------|------------|--------------|
| 期权数据 | Put/Call Ratio | 无（A股期权市场不成熟） |
| 波动率 | VIX | 指数波动率计算 |
| 外资 | 无 | 北上资金 |
| 期货 | 无 | 股指期货升贴水 |
| 杠杆 | 无 | 融资融券数据 |
| 债券 | 垃圾债利差 | 股债利差 |
| 量价 | 量价广度 | 股价强度 |

---

## 3. 其他情绪指标方案汇总

### 3.1 技术面指标

#### （1）相对强弱指数 RSI（Relative Strength Index）
```
RS = 过去N日上涨日平均涨幅 / 过去N日下跌日平均跌幅
RSI = 100 - 100/(1+RS)
```
- 通常取N=14
- RSI > 70 → 超买/贪婪；RSI < 30 → 超卖/恐慌
- 可以指数级RSI（EMA版本）替代简单RSI

#### （2）涨跌家数比（Advance-Decline Ratio）
```
ADR = 上涨家数 / 下跌家数
情绪分 = (ADR - ADR_min) / (ADR_max - ADR_min) × 100
```
- 反映市场整体广度
- ADR > 1 → 多头占优；ADR < 1 → 空头占优

#### （3）均线偏离度（MA Deviation）
```
偏离度 = (当前价 - N日均线) / N日均线 × 100
```
- 常用MA20、MA60、MA120、MA250
- 正偏离越大 → 贪婪；负偏离越大 → 恐慌

#### （4）腾落线（Advance-Decline Line）
```
ADLine = 前一日ADLine + (上涨家数 - 下跌家数)
```
- 累积广度指标，反映趋势
- ADLine创新高 → 确认牛市

### 3.2 资金面指标

#### （5）融资买入占比
```
融资买入占比 = 当日融资买入额 / 当日总成交额 × 100%
```
- 占比升高 → 杠杆资金入场 → 贪婪
- 占比降低 → 杠杆资金退潮 → 恐慌
- 典型值：8%-15%之间波动

#### （6）换手率指标
```
市场换手率 = 全A当日总成交额 / 全A流通市值
高换手率股票占比 = 换手率 > X% 的股票数量 / 总股票数量
```
- 高换手 → 市场活跃/投机情绪浓厚 → 偏贪婪
- 低换手 → 市场冷淡 → 偏恐慌

#### （7）量比指标
```
量比 = 当日成交量 / 过去5日平均成交量
市场平均量比 = 全A个股量比的均值/中位数
```
- 量比放大 → 情绪升温
- 量比萎缩 → 情绪冷却

### 3.3 基本面指标

#### （8）股债利差（Equity Risk Premium / Stock-Bond Yield Spread）
```
股债利差 = 1/PE(沪深300) - 10年期国债收益率
```
- 股债利差越高 → 股票相对债券越便宜 → 恐慌/价值洼地
- 股债利差越低 → 股票越贵 → 贪婪
- 参考标准：沪深300股债利差 > 6% → 非常有吸引力

#### （9）PE分位数
```
当前PE在历史PE分布中的百分位
```
- 高PE分位数 → 估值偏高 → 贪婪
- 低PE分位数 → 估值偏低 → 恐慌

#### （10）创新高/新低股票数
```
创新高股票占比 = 过去N日创新高股票数 / 总股票数
创新低股票占比 = 过去N日创新低股票数 / 总股票数
```
- 大量创新高 → 贪婪
- 大量创新低 → 恐慌
- N通常取20日（约一个月）或60日（约一个季度）

### 3.4 A股特有指标

#### （11）涨停/跌停家数
```
涨跌停比 = 涨停家数 / 跌停家数
```
- 涨停家数多 → 追涨情绪高 → 贪婪
- 跌停家数多 → 恐慌性抛售 → 恐慌

#### （12）次新股/题材股活跃度
- 次新股涨停数占比
- 连板股数量
- 反映投机情绪

---

## 4. 学术经典方法简介

### 4.1 Baker-Wurgler投资者情绪指数（2006）

**论文**：Baker, M., & Wurgler, J. (2006). "Investor Sentiment and the Cross-Section of Stock Returns."

这是学术界最经典的投资者情绪指数构建方法：

**步骤**：
1. **选择代理变量**：选择6个情绪代理变量
   - 封闭式基金折价率（CEFD）
   - NYSE成交量（Turnover）
   - IPO首日收益率（First-day returns）
   - IPO数量（Number of IPOs）
   - 股权融资占比（Equity share in new issues）
   - 股利溢价（Dividend premium）

2. **主成分分析（PCA）**：
   - 对6个代理变量进行标准化
   - 提取第一主成分作为综合情绪指数
   - 第一主成分捕获各代理变量的共同变化

3. **去除宏观经济影响**：
   - 将情绪指数对工业生产指数、耐用消费品增长率等回归
   - 取残差作为"纯粹的投资者情绪"

4. **验证**：
   - 情绪指数高时，小盘股、投机性股票后续收益低
   - 情绪指数低时，这些股票后续收益高

### 4.2 BW指数的变体和改进

#### BW-DMSW（2012改进版）
- 将6个代理变量分为"近期"和"远期"两组
- 对近期变量和远期变量分别提取主成分
- 取两个主成分的平均值

#### 中国市场的适配
学术界将BW方法本土化，常用的A股代理变量：
- 换手率（替代NYSE turnover）
- 消费者信心指数（CCI）
- 新增开户数
- 封闭式基金折价率
- IPO数量和首日收益率
- 融资余额变化

### 4.3 量化交易中的简化方法

实际量化交易中，学者和实践者常简化为：
- **多因子等权法**：选择若干情绪代理变量，标准化后等权平均
- **阈值法**：设定各指标的阈值，统计超过阈值的指标个数
- **滚动百分位法**：基于历史滚动窗口的百分位排名

---

## 5. 推荐的自建方案

### 5.1 设计原则

1. **纯自有数据**：仅使用已有日K数据，不依赖外部API
2. **实用导向**：因子简单明了，易于理解和维护
3. **A股特色**：融入A股市场特点（涨跌停、换手率等）
4. **等权公平**：借鉴CNN的等权设计，避免主观权重争议

### 5.2 推荐的6个因子

我们基于已有数据（沪深300/中证1000日K + 1469只个股日K），推荐以下6个因子：

---

#### 因子1：指数均线偏离度（MA Deviation）— 权重 1/6

| 项目 | 说明 |
|------|------|
| **数据源** | 沪深300收盘价 |
| **计算公式** | `dev = (close - MA60) / MA60 * 100` |
| **标准化** | 取过去250个交易日的百分位排名 × 100 |
| **情绪映射** | 偏离越大（正）→ 越贪婪；偏离越小（负）→ 越恐慌 |
| **逻辑** | 股价远高于中期均线 → 过热/贪婪；远低于均线 → 超卖/恐慌 |

```python
def factor_ma_deviation(close_prices, window=60, lookback=250):
    """均线偏离度因子"""
    ma = close_prices.rolling(window).mean()
    deviation = (close_prices - ma) / ma * 100
    # 百分位排名
    percentile = deviation.rolling(lookback).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1] * 100
    )
    return percentile
```

---

#### 因子2：市场量能比（Volume Ratio）— 权重 1/6

| 项目 | 说明 |
|------|------|
| **数据源** | 沪深300成交量 |
| **计算公式** | `vol_ratio = 当日成交量 / MA20成交量` |
| **标准化** | 取过去250个交易日的百分位排名 × 100 |
| **情绪映射** | 量比放大 → 情绪升温 → 偏贪婪；量比萎缩 → 情绪冷却 → 偏恐慌 |
| **逻辑** | 成交量反映市场参与度和资金热情 |

```python
def factor_volume_ratio(volumes, window=20, lookback=250):
    """市场量能比因子"""
    vol_ma = volumes.rolling(window).mean()
    ratio = volumes / vol_ma
    percentile = ratio.rolling(lookback).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1] * 100
    )
    return percentile
```

---

#### 因子3：涨跌家数比（Advance-Decline Ratio）— 权重 1/6

| 项目 | 说明 |
|------|------|
| **数据源** | 1469只个股日K数据 |
| **计算公式** | `adr = 上涨家数 / (上涨家数 + 下跌家数) * 100` |
| **说明** | 也叫涨跌比百分比（上涨占比） |
| **标准化** | 取过去250个交易日的百分位排名 × 100 |
| **情绪映射** | 上涨占比高 → 贪婪；上涨占比低 → 恐慌 |
| **逻辑** | 反映市场整体广度，类似CNN的Stock Price Breadth |

```python
def factor_advance_decline(all_stocks_df, date, lookback=250):
    """涨跌家数比因子（所有个股的当日涨跌统计）"""
    # all_stocks_df: 每只股票当日涨跌幅
    adv = (all_stocks_df['change_pct'] > 0).sum()
    dec = (all_stocks_df['change_pct'] < 0).sum()
    adr_pct = adv / (adv + dec) * 100
    return adr_pct
```

---

#### 因子4：市场振幅/波动率（Market Amplitude）— 权重 1/6

| 项目 | 说明 |
|------|------|
| **数据源** | 沪深300（最高-最低）/ 收盘价 |
| **计算公式** | `amplitude = (high - low) / close * 100`，取5日均值 |
| **标准化** | 取过去250个交易日的百分位排名 × 100 |
| **情绪映射** | **反向映射**：振幅越大 → 恐慌（波动剧烈）；振幅越小 → 贪婪（平静） |
| **逻辑** | 类似VIX的逻辑，高波动往往伴随恐慌 |

```python
def factor_amplitude(df, window=5, lookback=250):
    """市场振幅因子"""
    amp = (df['high'] - df['low']) / df['close'] * 100
    amp_ma = amp.rolling(window).mean()
    # 注意：振幅越大越恐慌，需要反向
    percentile = amp_ma.rolling(lookback).apply(
        lambda x: (1 - pd.Series(x).rank(pct=True).iloc[-1]) * 100
    )
    return percentile
```

---

#### 因子5：RSI相对强弱（Relative Strength Index）— 权重 1/6

| 项目 | 说明 |
|------|------|
| **数据源** | 沪深300收盘价 |
| **计算公式** | 标准RSI(14)：`RS = avg_gain14 / avg_loss14; RSI = 100 - 100/(1+RS)` |
| **标准化** | RSI本身就是0-100的指标，可直接使用或做百分位微调 |
| **情绪映射** | RSI > 70 → 贪婪；RSI < 30 → 恐慌 |
| **逻辑** | 经典动量指标，衡量近期涨幅强度 |

```python
def factor_rsi(prices, period=14):
    """RSI因子"""
    delta = prices.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi  # 直接返回0-100的RSI值
```

---

#### 因子6：高换手率股票占比（High Turnover Ratio）— 权重 1/6

| 项目 | 说明 |
|------|------|
| **数据源** | 1469只个股换手率数据 |
| **计算公式** | `high_turnover_pct = 换手率 > 阈值 的股票数 / 总股票数 × 100` |
| **阈值建议** | 换手率 > 5%（可根据历史分布调整，取75%分位） |
| **标准化** | 取过去250个交易日的百分位排名 × 100 |
| **情绪映射** | 高换手占比高 → 投机情绪高涨 → 贪婪；低 → 冷淡 → 恐慌 |
| **逻辑** | 高换手率反映散户投机情绪 |

```python
def factor_high_turnover(turnover_series, threshold=5.0, lookback=250):
    """高换手率股票占比因子"""
    high_turn_pct = (turnover_series > threshold).sum() / len(turnover_series) * 100
    return high_turn_pct
```

---

### 5.3 综合指数计算公式

#### 标准化方案：滚动百分位法

```python
def to_percentile_rank(series, lookback=250):
    """
    将原始值转换为基于历史滚动窗口的百分位排名（0-100）
    0 = 历史最低（最恐慌），100 = 历史最高（最贪婪）
    """
    return series.rolling(lookback, min_periods=60).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1] * 100
    )

def calculate_fear_greed_index(factor_scores):
    """
    计算综合恐贪指数
    factor_scores: DataFrame，每列是一个因子的0-100分值
    返回：0-100的恐贪指数
    """
    # 等权平均
    index = factor_scores.mean(axis=1)
    return index.clip(0, 100)
```

#### 情绪等级划分

```python
def get_sentiment_level(score):
    """根据分数返回情绪等级"""
    if score <= 15:
        return "极度恐慌 🔴🔴"
    elif score <= 30:
        return "恐慌 🔴"
    elif score <= 45:
        return "偏恐慌 🟠"
    elif score <= 55:
        return "中性 🟡"
    elif score <= 70:
        return "偏贪婪 🟢"
    elif score <= 85:
        return "贪婪 🟢🟢"
    else:
        return "极度贪婪 🟢🟢🟢"
```

#### 最终公式

```
Fear_Greed = (MA_Deviation_Score + Volume_Ratio_Score + AD_Ratio_Score 
              + Amplitude_Score + RSI_Score + High_Turnover_Score) / 6

其中每个因子的 Score 已经通过滚动百分位标准化到 0-100
```

### 5.4 历史回测方法

```python
def calculate_historical_fear_greed(all_kline_data, index_data, lookback=250):
    """
    计算历史恐贪指数
    
    参数：
    - all_kline_data: DataFrame, 所有个股日K数据
      列：date, code, open, high, low, close, volume, turnover_rate, volume_ratio
    - index_data: DataFrame, 沪深300日K数据
      列：date, open, high, low, close, volume
    - lookback: int, 滚动窗口天数，默认250（约一年）
    
    返回：
    - DataFrame: date, fear_greed_score, sentiment_level, factor1~factor6
    """
    results = []
    
    for date in index_data['date']:
        # 计算当日各因子原始值
        factors = {}
        
        # 因子1: 均线偏离度
        factors['ma_dev'] = calc_ma_deviation(index_data, date)
        
        # 因子2: 市场量能比
        factors['vol_ratio'] = calc_volume_ratio(index_data, date)
        
        # 因子3: 涨跌家数比
        factors['adr'] = calc_advance_decline(all_kline_data, date)
        
        # 因子4: 市场振幅
        factors['amplitude'] = calc_amplitude(index_data, date)
        
        # 因子5: RSI
        factors['rsi'] = calc_rsi(index_data, date)
        
        # 因子6: 高换手率占比
        factors['turnover'] = calc_high_turnover(all_kline_data, date)
        
        results.append({'date': date, **factors})
    
    df = pd.DataFrame(results)
    
    # 滚动百分位标准化
    for col in ['ma_dev', 'vol_ratio', 'adr', 'amplitude', 'rsi', 'turnover']:
        # 注意：amplitude需要反向（越大越恐慌）
        if col == 'amplitude':
            df[f'{col}_score'] = (1 - df[col].rolling(lookback, min_periods=60).rank(pct=True)) * 100
        else:
            df[f'{col}_score'] = df[col].rolling(lookback, min_periods=60).rank(pct=True) * 100
    
    # 综合指数
    score_cols = [f'{col}_score' for col in ['ma_dev', 'vol_ratio', 'adr', 'amplitude', 'rsi', 'turnover']]
    df['fear_greed'] = df[score_cols].mean(axis=1).clip(0, 100)
    
    # 情绪等级
    df['level'] = df['fear_greed'].apply(get_sentiment_level)
    
    return df
```

### 5.5 历史统计参考值

基于A股历史经验，恐贪指数的典型分布：

| 区间 | 事件特征 | 出现频率 |
|------|----------|----------|
| 0-15 | 股灾、熔断、黑天鹅事件 | ~5% |
| 15-30 | 大跌后的恐慌期 | ~10% |
| 30-45 | 震荡下跌期 | ~20% |
| 45-55 | 正常震荡 | ~30% |
| 55-70 | 小牛市、反弹期 | ~20% |
| 70-85 | 大牛市、狂热期 | ~10% |
| 85-100 | 极端牛市顶部 | ~5% |

### 5.6 交易信号参考

| 恐贪指数 | 信号 | 操作建议 |
|----------|------|----------|
| < 15 | 极度恐慌 | 关注抄底机会，分批建仓 |
| 15-30 | 恐慌 | 尾盘买入信号增强 |
| 30-45 | 偏恐慌 | 适当加仓 |
| 45-55 | 中性 | 按常规策略 |
| 55-70 | 偏贪婪 | 适当减仓 |
| 70-85 | 贪婪 | 减仓，尾盘买入信号减弱 |
| > 85 | 极度贪婪 | 高度警惕，考虑清仓 |

---

## 6. Python代码框架

### 6.1 主模块：fear_greed_index.py

```python
"""
A股恐贪指数（Fear & Greed Index）计算模块
基于已有数据：沪深300/中证1000日K + 1469只个股日K
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Tuple


class FearGreedIndex:
    """A股恐贪指数计算器"""
    
    def __init__(self, lookback: int = 250):
        """
        参数:
            lookback: 滚动窗口天数，默认250（约一年交易日）
        """
        self.lookback = lookback
        self.min_periods = 60  # 最少需要60个数据点
        
    def load_data(self, data_dir: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        加载数据
        
        参数:
            data_dir: 数据目录路径
        返回:
            (index_data, stocks_data)
        """
        data_path = Path(data_dir)
        
        # 加载沪深300指数数据
        index_file = data_path / "000300.SH.csv"  # 根据实际文件名调整
        index_data = pd.read_csv(index_file, parse_dates=['date'])
        index_data = index_data.sort_values('date').reset_index(drop=True)
        
        # 加载个股数据
        stocks_file = data_path / "all_stocks.csv"  # 根据实际文件名调整
        stocks_data = pd.read_csv(stocks_file, parse_dates=['date'])
        
        return index_data, stocks_data
    
    # ==================== 六个因子计算 ====================
    
    def factor_1_ma_deviation(self, index_data: pd.DataFrame) -> pd.Series:
        """
        因子1: 均线偏离度
        公式: (close - MA60) / MA60 * 100
        """
        close = index_data['close']
        ma60 = close.rolling(60, min_periods=30).mean()
        deviation = (close - ma60) / ma60 * 100
        return deviation
    
    def factor_2_volume_ratio(self, index_data: pd.DataFrame) -> pd.Series:
        """
        因子2: 市场量能比
        公式: 当日成交量 / MA20成交量
        """
        volume = index_data['volume']
        vol_ma20 = volume.rolling(20, min_periods=10).mean()
        ratio = volume / vol_ma20
        return ratio
    
    def factor_3_advance_decline(self, stocks_data: pd.DataFrame, 
                                  dates: pd.Series) -> pd.Series:
        """
        因子3: 涨跌家数比（上涨占比）
        公式: 上涨家数 / (上涨+下跌) * 100
        """
        adr_values = []
        for date in dates:
            day_data = stocks_data[stocks_data['date'] == date]
            if len(day_data) == 0:
                adr_values.append(np.nan)
                continue
            
            # 计算涨跌
            pct_change = day_data['close'] / day_data['pre_close'] - 1  # 或使用change_pct列
            adv = (pct_change > 0).sum()
            dec = (pct_change < 0).sum()
            total = adv + dec
            adr_pct = adv / total * 100 if total > 0 else 50
            adr_values.append(adr_pct)
        
        return pd.Series(adr_values, index=dates.index)
    
    def factor_4_amplitude(self, index_data: pd.DataFrame) -> pd.Series:
        """
        因子4: 市场振幅（5日均值）
        公式: (high - low) / close * 100
        """
        amp = (index_data['high'] - index_data['low']) / index_data['close'] * 100
        amp_ma5 = amp.rolling(5, min_periods=3).mean()
        return amp_ma5
    
    def factor_5_rsi(self, index_data: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        因子5: RSI相对强弱
        公式: 100 - 100/(1+RS), RS=avg_gain/avg_loss
        """
        close = index_data['close']
        delta = close.diff()
        
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)
        
        # 使用Wilder平滑（EMA方式）
        avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def factor_6_high_turnover(self, stocks_data: pd.DataFrame, 
                                dates: pd.Series,
                                threshold: float = 5.0) -> pd.Series:
        """
        因子6: 高换手率股票占比
        公式: 换手率>5%的股票数 / 总股票数 * 100
        """
        turnover_pcts = []
        for date in dates:
            day_data = stocks_data[stocks_data['date'] == date]
            if len(day_data) == 0:
                turnover_pcts.append(np.nan)
                continue
            
            high_turn = (day_data['turnover_rate'] > threshold).sum()
            pct = high_turn / len(day_data) * 100
            turnover_pcts.append(pct)
        
        return pd.Series(turnover_pcts, index=dates.index)
    
    # ==================== 标准化 ====================
    
    def to_percentile_score(self, series: pd.Series, 
                            reverse: bool = False) -> pd.Series:
        """
        将原始值转为滚动百分位分数（0-100）
        
        参数:
            series: 原始因子值序列
            reverse: 是否反向（值越大分数越低）
        """
        pct_rank = series.rolling(self.lookback, min_periods=self.min_periods).rank(pct=True)
        score = pct_rank * 100
        if reverse:
            score = 100 - score
        return score.clip(0, 100)
    
    # ==================== 综合计算 ====================
    
    def calculate(self, index_data: pd.DataFrame, 
                  stocks_data: pd.DataFrame) -> pd.DataFrame:
        """
        计算完整恐贪指数
        
        参数:
            index_data: 沪深300日K数据
            stocks_data: 全部A股日K数据
        返回:
            DataFrame包含: date, f1~f6原始值, f1_score~f6_score, 
                          fear_greed, sentiment_level
        """
        dates = index_data['date']
        result = pd.DataFrame({'date': dates})
        
        # 计算6个因子原始值
        result['f1_ma_dev'] = self.factor_1_ma_deviation(index_data)
        result['f2_vol_ratio'] = self.factor_2_volume_ratio(index_data)
        result['f3_adr'] = self.factor_3_advance_decline(stocks_data, dates)
        result['f4_amplitude'] = self.factor_4_amplitude(index_data)
        result['f5_rsi'] = self.factor_5_rsi(index_data)
        result['f6_turnover'] = self.factor_6_high_turnover(stocks_data, dates)
        
        # 标准化为0-100分数
        # 注意：amplitude是反向的（越大越恐慌）
        result['f1_score'] = self.to_percentile_score(result['f1_ma_dev'])
        result['f2_score'] = self.to_percentile_score(result['f2_vol_ratio'])
        result['f3_score'] = self.to_percentile_score(result['f3_adr'])
        result['f4_score'] = self.to_percentile_score(result['f4_amplitude'], reverse=True)
        result['f5_score'] = self.to_percentile_score(result['f5_rsi'])
        result['f6_score'] = self.to_percentile_score(result['f6_turnover'])
        
        # 综合指数（等权平均）
        score_cols = ['f1_score', 'f2_score', 'f3_score', 
                      'f4_score', 'f5_score', 'f6_score']
        result['fear_greed'] = result[score_cols].mean(axis=1).clip(0, 100)
        
        # 情绪等级
        result['sentiment_level'] = result['fear_greed'].apply(self.get_level)
        
        return result
    
    def get_level(self, score: float) -> str:
        """情绪等级判断"""
        if pd.isna(score):
            return "数据不足"
        if score <= 15:
            return "极度恐慌"
        elif score <= 30:
            return "恐慌"
        elif score <= 45:
            return "偏恐慌"
        elif score <= 55:
            return "中性"
        elif score <= 70:
            return "偏贪婪"
        elif score <= 85:
            return "贪婪"
        else:
            return "极度贪婪"
    
    def get_today_signal(self, result_df: pd.DataFrame) -> dict:
        """
        获取今日恐贪指数信号
        
        返回: {
            'date': '2026-03-31',
            'fear_greed': 45.2,
            'level': '偏恐慌',
            'factors': {...},
            'trading_signal': 'BUY/SELL/HOLD'
        }
        """
        latest = result_df.iloc[-1]
        
        # 各因子得分
        factors = {
            '均线偏离度': round(latest['f1_score'], 1),
            '市场量能比': round(latest['f2_score'], 1),
            '涨跌家数比': round(latest['f3_score'], 1),
            '市场振幅': round(latest['f4_score'], 1),
            'RSI强弱': round(latest['f5_score'], 1),
            '高换手占比': round(latest['f6_score'], 1),
        }
        
        score = latest['fear_greed']
        
        # 交易信号
        if score <= 30:
            signal = '强买入信号'
        elif score <= 45:
            signal = '买入信号'
        elif score <= 55:
            signal = '中性观望'
        elif score <= 70:
            signal = '卖出信号'
        else:
            signal = '强卖出信号'
        
        return {
            'date': str(latest['date'].date()) if hasattr(latest['date'], 'date') else str(latest['date']),
            'fear_greed': round(score, 1),
            'level': latest['sentiment_level'],
            'factors': factors,
            'trading_signal': signal
        }
    
    def save_history(self, result_df: pd.DataFrame, 
                     output_path: str):
        """保存历史恐贪指数数据"""
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(output, index=False)
        print(f"✅ 历史数据已保存到: {output}")
        print(f"   记录数: {len(result_df)}")
        print(f"   日期范围: {result_df['date'].min()} ~ {result_df['date'].max()}")


# ==================== 使用示例 ====================

def main():
    """主程序入口"""
    # 初始化
    fgi = FearGreedIndex(lookback=250)
    
    # 加载数据（根据实际路径调整）
    data_dir = "***REMOVED***/quant_trading/data"
    index_data, stocks_data = fgi.load_data(data_dir)
    
    # 计算恐贪指数
    result = fgi.calculate(index_data, stocks_data)
    
    # 保存历史数据
    fgi.save_history(result, 
                     "***REMOVED***/quant_trading/data/fear_greed_history.csv")
    
    # 获取今日信号
    signal = fgi.get_today_signal(result)
    print(f"\n📊 今日恐贪指数报告")
    print(f"日期: {signal['date']}")
    print(f"恐贪指数: {signal['fear_greed']} ({signal['level']})")
    print(f"交易信号: {signal['trading_signal']}")
    print(f"\n各因子得分:")
    for name, score in signal['factors'].items():
        print(f"  {name}: {score}")
    
    # 打印最近10天的数据
    print(f"\n📈 最近10天恐贪指数:")
    recent = result.tail(10)[['date', 'fear_greed', 'sentiment_level']]
    print(recent.to_string(index=False))


if __name__ == "__main__":
    main()
```

### 6.2 集成到尾盘交易系统

```python
"""
在现有的尾盘交易系统中集成恐贪指数
"""

# 在信号生成函数中添加恐贪指数过滤
def generate_trading_signal(stock_signal, fear_greed_score):
    """
    结合恐贪指数调整交易信号
    
    参数:
        stock_signal: 个股信号 ('BUY', 'SELL', 'HOLD')
        fear_greed_score: 恐贪指数 (0-100)
    """
    
    # 恐贪指数调节系数
    if fear_greed_score <= 20:
        # 极度恐慌 - 逆势买入增强
        multiplier = 1.3
    elif fear_greed_score <= 40:
        # 恐慌 - 买入增强
        multiplier = 1.15
    elif fear_greed_score <= 60:
        # 中性 - 无调节
        multiplier = 1.0
    elif fear_greed_score <= 80:
        # 贪婪 - 买入减弱
        multiplier = 0.85
    else:
        # 极度贪婪 - 强烈减仓
        multiplier = 0.7
    
    return {
        'original_signal': stock_signal,
        'fg_score': fear_greed_score,
        'multiplier': multiplier,
        'adjusted_confidence': stock_signal.get('confidence', 0.5) * multiplier
    }
```

---

## 7. 注意事项和改进建议

### 7.1 数据质量检查
- **缺失数据处理**：个股停牌时无数据，涨跌统计需排除停牌股
- **新股排除**：上市不满60天的股票建议排除（换手率和波动异常）
- **ST股票处理**：是否纳入取决于策略，涨跌停限制不同（5% vs 10%）

### 7.2 后续可扩展的因子（需外部数据）
| 因子 | 数据来源 | 价值 |
|------|----------|------|
| 北上资金净流入 | 港交所/东方财富 | 高，反映外资情绪 |
| 融资买入占比 | 沪深交易所 | 高，A股特有杠杆指标 |
| 股指期货升贴水 | 中金所 | 高，领先指标 |
| 期权Put/Call Ratio | 上交所 | 中，需50ETF期权数据 |
| 股债利差 | 国债收益率+指数PE | 高，长期择时价值 |

### 7.3 参数调优建议
- **lookback窗口**：250天（一年）是标准选择，也可尝试120天（半年）
- **RSI周期**：14天是经典参数，也可试用21天
- **换手率阈值**：5%是经验值，建议根据历史75%分位动态调整
- **均线周期**：60日是中期均线，也可同时用20日和120日

### 7.4 回测验证
- 计算过去几年的恐贪指数，与实际市场底部/顶部对比
- 验证极端值（<15和>85）是否对应重大转折点
- 统计恐贪指数在各区间时，后续N日的平均收益率

---

## 附录：参考资源

### 学术论文
1. Baker, M., & Wurgler, J. (2006). "Investor Sentiment and the Cross-Section of Stock Returns." *Journal of Finance*.
2. Baker, M., & Wurgler, J. (2007). "Investor Sentiment in the Stock Market." *Journal of Economic Perspectives*.
3. Huang, D., Jiang, F., Tu, J., & Zhou, G. (2015). "Investor Sentiment Aligned: A Powerful Predictor of Stock Returns." *Review of Financial Studies*.

### 实践参考
1. CNN Fear & Greed Index: https://edition.cnn.com/markets/fear-and-greed
2. 韭圈儿恐贪指数: https://funddb.cn/fgindex
3. AAII投资者情绪调查: https://www.aaii.com/sentimentsurvey
4. CBOE VIX指数: https://www.cboe.com/tradable_products/vix/

### Python库推荐
- `pandas`: 数据处理
- `numpy`: 数值计算
- `talib`: 技术指标计算（可选）
- `matplotlib/plotly`: 可视化

---

> 📝 报告完成。下一步：根据实际数据格式调整代码中的文件路径和列名，即可开始实现。
