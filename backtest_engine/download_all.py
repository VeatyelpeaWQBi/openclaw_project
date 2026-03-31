#!/usr/bin/env python3
"""下载沪深300+中证1000成分股的日K和5分钟线数据 (使用baostock)"""

import json
import os
import sys
import time
import traceback
import pandas as pd
import baostock as bs
import efinance as ef

BASE_DIR = "***REMOVED***/tail_trading"
DAILY_DIR = os.path.join(BASE_DIR, "daily_data")
MINUTE_DIR = os.path.join(BASE_DIR, "minute_data")
INDEX_DIR = os.path.join(BASE_DIR, "index_history")
PROGRESS_FILE = os.path.join(BASE_DIR, "download_progress.json")
RESULT_FILE = os.path.join(BASE_DIR, "download_result.json")

os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(INDEX_DIR, exist_ok=True)

def save_progress(step, detail="", stock_idx=0, total=0):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump({"step": step, "detail": detail, "stock_idx": stock_idx, "total": total, 
                    "time": time.strftime("%Y-%m-%d %H:%M:%S")}, f, ensure_ascii=False, indent=2)

def log(msg):
    print(msg, flush=True)

# ========== Step 1: 获取成分股列表 ==========
log("=" * 60)
log("Step 1: 获取沪深300历史成分股...")
save_progress("获取成分股列表", "沪深300")

lg = bs.login()
log(f"baostock login: {lg.error_code} {lg.error_msg}")

hs300_all = set()
for year in range(2020, 2027):
    date_str = f"{year}-01-01"
    rs = bs.query_hs300_stocks(date=date_str)
    data = []
    while (rs.error_code == '0') & rs.next():
        row = rs.get_row_data()
        code = row[1].replace('.', '')
        name = row[2]
        hs300_all.add((code, name))
        data.append([row[0], code, name])
    if data:
        df = pd.DataFrame(data, columns=['日期', '代码', '名称'])
        df.to_csv(os.path.join(INDEX_DIR, f"hs300_{date_str}.csv"), index=False, encoding='utf-8')
        log(f"  沪深300 {year}: {len(data)} 只")

bs.logout()
log(f"  沪深300历史总计(去重): {len(hs300_all)} 只")

# 中证1000
log("获取中证1000成分股...")
save_progress("获取成分股列表", "中证1000")
try:
    zz1000 = ef.stock.get_members('000852')
    zz1000_set = set()
    for _, row in zz1000.iterrows():
        code = str(row['股票代码']).zfill(6)
        zz1000_set.add((code, row['股票名称']))
    log(f"  中证1000: {len(zz1000_set)} 只")
    zz_df = pd.DataFrame(list(zz1000_set), columns=['代码', '名称'])
    zz_df.to_csv(os.path.join(INDEX_DIR, "zz1000_current.csv"), index=False, encoding='utf-8')
except Exception as e:
    log(f"  ⚠️ 中证1000获取失败: {e}")
    zz1000_set = set()

# 合并去重
all_stocks = sorted(list(hs300_all | zz1000_set), key=lambda x: x[0])
log(f"合并去重后总计: {len(all_stocks)} 只")

all_df = pd.DataFrame(all_stocks, columns=['代码', '名称'])
all_df.to_csv(os.path.join(BASE_DIR, "all_stocks_list.csv"), index=False, encoding='utf-8')

# ========== Step 2: 下载日K数据 ==========
log("\n" + "=" * 60)
log("Step 2: 下载日K数据 (baostock)...")

DAILY_FIELDS = 'date,open,high,low,close,volume,amount,turn,peTTM,pbMRQ,psTTM,pcfNcfTTM'
DAILY_HEADER = ['日期','开盘','最高','最低','收盘','成交量','成交额','换手率','市盈率','市净率','市销率','市现率']

existing_daily = set()
if os.path.exists(DAILY_DIR):
    for f in os.listdir(DAILY_DIR):
        if f.endswith('.csv'):
            code = f.split('_')[0]
            existing_daily.add(code)

daily_success = 0
daily_fail = 0
daily_skip = 0
failed_daily = []

lg = bs.login()

for idx, (code, name) in enumerate(all_stocks):
    if code in existing_daily:
        daily_skip += 1
        continue
    
    save_progress("下载日K", f"{code}_{name}", idx + 1, len(all_stocks))
    
    # baostock格式: sh.600519 或 sz.000001
    if code.startswith('6') or code.startswith('9'):
        bs_code = f"sh.{code}"
    else:
        bs_code = f"sz.{code}"
    
    success = False
    for attempt in range(3):
        try:
            rs = bs.query_history_k_data_plus(bs_code, DAILY_FIELDS,
                start_date='2020-01-01', end_date='2026-03-30', frequency='d', adjustflag='2')
            data = []
            while rs.next():
                data.append(rs.get_row_data())
            
            if rs.error_code == '0' and data:
                df = pd.DataFrame(data, columns=DAILY_HEADER)
                file_path = os.path.join(DAILY_DIR, f"{code}_{name}.csv")
                df.to_csv(file_path, index=False, encoding='utf-8')
                daily_success += 1
                success = True
            else:
                # 可能是新股或已退市
                daily_fail += 1
                failed_daily.append((code, name, rs.error_msg))
                success = True  # 不算网络错误
            break
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
            else:
                daily_fail += 1
                failed_daily.append((code, name, str(e)))
    
    if (idx + 1) % 50 == 0:
        log(f"  日K进度: {idx+1}/{len(all_stocks)} (成功:{daily_success} 跳过:{daily_skip} 失败:{daily_fail})")
    
    time.sleep(0.3)

bs.logout()
log(f"日K下载完成: 成功={daily_success} 跳过={daily_skip} 失败={daily_fail}")

# ========== Step 3: 下载5分钟线数据 ==========
log("\n" + "=" * 60)
log("Step 3: 下载5分钟线数据 (baostock)...")

MINUTE_FIELDS = 'date,time,open,high,low,close,volume,amount'
MINUTE_HEADER = ['日期','时间','开盘','最高','最低','收盘','成交量','成交额']

def has_minute_data(code):
    """检查股票是否已有分钟线数据"""
    if not os.path.exists(MINUTE_DIR):
        return False
    for ym_dir in os.listdir(MINUTE_DIR):
        ym_path = os.path.join(MINUTE_DIR, ym_dir)
        if not os.path.isdir(ym_path):
            continue
        for sd in os.listdir(ym_path):
            if sd.startswith(code + "_") and os.path.isdir(os.path.join(ym_path, sd)):
                files = os.listdir(os.path.join(ym_path, sd))
                if files:
                    return True
    return False

minute_success = 0
minute_fail = 0
minute_skip = 0

lg = bs.login()

for idx, (code, name) in enumerate(all_stocks):
    if has_minute_data(code):
        minute_skip += 1
        continue
    
    save_progress("下载5分钟线", f"{code}_{name}", idx + 1, len(all_stocks))
    
    if code.startswith('6') or code.startswith('9'):
        bs_code = f"sh.{code}"
    else:
        bs_code = f"sz.{code}"
    
    # 按月下载
    current = pd.Timestamp("2020-01-01")
    end_ts = pd.Timestamp("2026-03-30")
    stock_failed = False
    
    while current <= end_ts and not stock_failed:
        ym = current.strftime("%Y-%m")
        ms = current.strftime("%Y-%m-%d")
        me = min(current + pd.offsets.MonthEnd(0), end_ts).strftime("%Y-%m-%d")
        
        stock_dir = os.path.join(MINUTE_DIR, ym, f"{code}_{name}")
        
        for attempt in range(3):
            try:
                rs = bs.query_history_k_data_plus(bs_code, MINUTE_FIELDS,
                    start_date=ms, end_date=me, frequency='5', adjustflag='2')
                data = []
                while rs.next():
                    data.append(rs.get_row_data())
                
                if rs.error_code == '0' and data:
                    df = pd.DataFrame(data, columns=MINUTE_HEADER)
                    # 解析时间列 - 格式: 20250303093500000
                    df['时间解析'] = pd.to_datetime(df['时间'], format='%Y%m%d%H%M%S%f')
                    
                    # 筛选09:30~10:15和14:50~14:55
                    mask = (
                        ((df['时间解析'].dt.time >= pd.Timestamp('09:30:00').time()) & 
                         (df['时间解析'].dt.time <= pd.Timestamp('10:15:00').time())) |
                        ((df['时间解析'].dt.time >= pd.Timestamp('14:50:00').time()) & 
                         (df['时间解析'].dt.time <= pd.Timestamp('14:55:00').time()))
                    )
                    df = df[mask]
                    
                    if len(df) > 0:
                        os.makedirs(stock_dir, exist_ok=True)
                        for date, group in df.groupby(df['时间解析'].dt.date):
                            ds = date.strftime('%Y%m%d')
                            fp = os.path.join(stock_dir, f"{code}_{name}_{ds}.csv")
                            # 保存时用解析好的时间
                            out = group.copy()
                            out['时间'] = out['时间解析'].dt.strftime('%Y-%m-%d %H:%M:%S')
                            out.drop(columns=['时间解析'], inplace=True)
                            out.to_csv(fp, index=False, encoding='utf-8')
                break  # 无论成功与否，跳出重试循环
            except Exception as e:
                if attempt < 2:
                    time.sleep(2)
                else:
                    if minute_fail <= 10:
                        log(f"  ⚠️ 分钟线 {code} {ym}: {e}")
        
        current = current + pd.DateOffset(months=1)
        time.sleep(0.3)
    
    minute_success += 1
    
    if (idx + 1) % 20 == 0:
        log(f"  分钟线进度: {idx+1}/{len(all_stocks)} (成功:{minute_success} 跳过:{minute_skip})")

bs.logout()
log(f"5分钟线下载完成: 成功={minute_success} 跳过:{minute_skip}")

# ========== 保存结果 ==========
result = {
    "total_stocks": len(all_stocks),
    "hs300_unique": len(hs300_all),
    "zz1000_unique": len(zz1000_set),
    "daily_success": daily_success,
    "daily_skip": daily_skip,
    "daily_fail": daily_fail,
    "minute_success": minute_success,
    "minute_skip": minute_skip,
    "failed_daily": failed_daily[:50],  # 只保存前50个失败的
    "completed_at": time.strftime("%Y-%m-%d %H:%M:%S")
}

with open(RESULT_FILE, 'w') as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

save_progress("完成", "全部完成", len(all_stocks), len(all_stocks))

log("\n" + "=" * 60)
log("全部完成!")
log(json.dumps({k: v for k, v in result.items() if k != 'failed_daily'}, ensure_ascii=False, indent=2))
