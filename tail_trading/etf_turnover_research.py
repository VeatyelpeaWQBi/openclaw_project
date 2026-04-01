import akshare as ak
import pandas as pd

print('='*60)
print('测试1: ETF日K数据字段检查 (fund_etf_hist_sina)')
print('='*60)

# 测试 sh512010 (医药ETF)
try:
    df1 = ak.fund_etf_hist_sina(symbol='sh512010')
    print(f'sh512010 字段: {list(df1.columns)}')
    print(f'sh512010 最近3行:')
    print(df1.tail(3).to_string())
    print()
except Exception as e:
    print(f'sh512010 失败: {e}')

# 测试 sz159929 (医药ETF)
try:
    df2 = ak.fund_etf_hist_sina(symbol='sz159929')
    print(f'sz159929 字段: {list(df2.columns)}')
    print(f'sz159929 最近3行:')
    print(df2.tail(3).to_string())
except Exception as e:
    print(f'sz159929 失败: {e}')

print()
print('='*60)
print('测试2: 检查akshare是否有ETF换手率相关接口')
print('='*60)

# 尝试 fund_etf_spot_em (东方财富ETF实时行情)
try:
    df_spot = ak.fund_etf_spot_em()
    print(f'fund_etf_spot_em 字段: {list(df_spot.columns)}')
    # 检查是否有换手率相关字段
    turnover_cols = [c for c in df_spot.columns if '换手' in c or 'turnover' in c.lower()]
    print(f'换手率相关字段: {turnover_cols}')
    # 显示前2行
    print(f'前2行样本:')
    print(df_spot.head(2).to_string())
except Exception as e:
    print(f'fund_etf_spot_em 失败: {e}')

print()
print('='*60)
print('测试3: 检查stock_sector_detail是否有ETF相关板块')
print('='*60)

# 先获取板块列表
try:
    # 尝试获取概念板块
    df_concept = ak.stock_sector_spot(indicator='概念')
    print(f'概念板块 数量: {len(df_concept)}')
    print(f'概念板块 字段: {list(df_concept.columns)}')
    print(f'前5个板块:')
    print(df_concept.head(5).to_string())
    
    # 尝试获取某个板块的成分股
    # 选第一个板块看看成分股中是否有turnoverratio
    sector_name = df_concept.iloc[0]['板块名称'] if '板块名称' in df_concept.columns else df_concept.iloc[0][0]
    print(f'\n获取板块 [{sector_name}] 的成分股...')
    df_detail = ak.stock_sector_detail(sector=sector_name)
    print(f'成分股 字段: {list(df_detail.columns)}')
    turnover_cols2 = [c for c in df_detail.columns if 'turnover' in c.lower() or '换手' in c]
    print(f'换手率相关字段: {turnover_cols2}')
    print(f'前3行:')
    print(df_detail.head(3).to_string())
except Exception as e:
    print(f'stock_sector_spot/detail 失败: {e}')

print()
print('='*60)
print('测试4: 尝试东方财富ETF历史数据是否有换手率')
print('='*60)

try:
    df_hist = ak.fund_etf_hist_em(symbol='512010', period='daily', adjust='')
    print(f'fund_etf_hist_em 字段: {list(df_hist.columns)}')
    turnover_cols3 = [c for c in df_hist.columns if '换手' in c or 'turnover' in c.lower()]
    print(f'换手率相关字段: {turnover_cols3}')
    print(f'最近3行:')
    print(df_hist.tail(3).to_string())
except Exception as e:
    print(f'fund_etf_hist_em 失败: {e}')

print()
print('='*60)
print('总结')
print('='*60)
print('待分析上述结果...')
