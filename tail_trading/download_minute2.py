#!/usr/bin/env python3
"""下载5分钟线数据 - 使用年范围，更快速"""

import json
import os
import sys
import io
import time
import pandas as pd
import baostock as bs

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

BASE_DIR = "***REMOVED***/tail_trading"
MINUTE_DIR = os.path.join(BASE_DIR, "minute_data")
STOCK_LIST_FILE = os.path.join(BASE_DIR, "all_stocks_list.csv")
PROGRESS_FILE = os.path.join(BASE_DIR, "minute_progress2.json")

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def save_progress(idx, total, code, name, year, status="downloading"):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump({
            "step": "下载5分钟线",
            "current_index": idx,
            "total": total,
            "current_stock": f"{code}_{name}",
            "current_year": year,
            "status": status,
            "percentage": round(idx / total * 100, 2) if total > 0 else 0,
            "time": time.strftime("%Y-%m-%d %H:%M:%S")
        }, f, ensure_ascii=False, indent=2)

# Load stock list
all_df = pd.read_csv(STOCK_LIST_FILE)
all_stocks = list(zip(all_df['代码'].astype(str).str.zfill(6), all_df['名称']))

# Check which stocks already have sufficient minute data
existing_minute = set()
if os.path.exists(MINUTE_DIR):
    for month_dir in os.listdir(MINUTE_DIR):
        month_path = os.path.join(MINUTE_DIR, month_dir)
        if os.path.isdir(month_path) and '-' in month_dir:
            for stock_dir in os.listdir(month_path):
                code = stock_dir.split('_')[0]
                stock_path = os.path.join(month_path, stock_dir)
                if os.path.isdir(stock_path) and len(os.listdir(stock_path)) > 5:
                    existing_minute.add(code)

missing = [(c, n) for c, n in all_stocks if c not in existing_minute]
log(f"Total: {len(all_stocks)}, Have minute data: {len(existing_minute)}, Missing: {len(missing)}")

if not missing:
    log("All done!")
    sys.exit(0)

MINUTE_FIELDS = 'date,time,open,high,low,close,volume,amount'

# Year ranges
year_ranges = [
    ('2020', '2020-01-01', '2020-12-31'),
    ('2021', '2021-01-01', '2021-12-31'),
    ('2022', '2022-01-01', '2022-12-31'),
    ('2023', '2023-01-01', '2023-12-31'),
    ('2024', '2024-01-01', '2024-12-31'),
    ('2025', '2025-01-01', '2025-12-31'),
    ('2026', '2026-01-01', '2026-03-30'),
]

success = 0
fail = 0

lg = bs.login()
log(f"Login: {lg.error_code}")

for idx, (code, name) in enumerate(missing):
    save_progress(idx + 1, len(missing), code, name, "")
    
    bs_code = f"sh.{code}" if code.startswith(('6', '9')) else f"sz.{code}"
    
    for year, ys, ye in year_ranges:
        save_progress(idx + 1, len(missing), code, name, year)
        
        # Check if this year already has data
        year_done = True
        for month in range(1, 13):
            ym = f"{year}-{month:02d}"
            stock_dir = os.path.join(MINUTE_DIR, ym, f"{code}_{name}")
            if not os.path.exists(stock_dir) or not os.listdir(stock_dir):
                year_done = False
                break
        
        if year_done:
            continue
        
        for attempt in range(3):
            try:
                rs = bs.query_history_k_data_plus(bs_code, MINUTE_FIELDS,
                    start_date=ys, end_date=ye, frequency='5', adjustflag='2')
                data = []
                while rs.next():
                    data.append(rs.get_row_data())
                
                if rs.error_code == '0' and data:
                    df = pd.DataFrame(data, columns=['日期','时间','开盘','最高','最低','收盘','成交量','成交额'])
                    df['时间解析'] = pd.to_datetime(df['时间'], format='%Y%m%d%H%M%S%f')
                    
                    # Filter for 09:30-10:15 and 14:50-14:55
                    mask = (
                        ((df['时间解析'].dt.time >= pd.Timestamp('09:30:00').time()) & 
                         (df['时间解析'].dt.time <= pd.Timestamp('10:15:00').time())) |
                        ((df['时间解析'].dt.time >= pd.Timestamp('14:50:00').time()) & 
                         (df['时间解析'].dt.time <= pd.Timestamp('14:55:00').time()))
                    )
                    df = df[mask]
                    
                    if len(df) > 0:
                        for (ym_key, date), group in df.groupby([df['时间解析'].dt.strftime('%Y-%m'), df['时间解析'].dt.date]):
                            ds = date.strftime('%Y%m%d')
                            stock_dir = os.path.join(MINUTE_DIR, ym_key, f"{code}_{name}")
                            os.makedirs(stock_dir, exist_ok=True)
                            fp = os.path.join(stock_dir, f"{code}_{name}_{ds}.csv")
                            out = group.copy()
                            out['时间'] = out['时间解析'].dt.strftime('%Y-%m-%d %H:%M:%S')
                            out.drop(columns=['时间解析'], inplace=True)
                            out.to_csv(fp, index=False, encoding='utf-8')
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(3)
                else:
                    pass
        
        time.sleep(0.3)
    
    success += 1
    time.sleep(0.2)
    
    if (idx + 1) % 10 == 0:
        log(f"  Progress: {idx+1}/{len(missing)} (ok:{success})")
    
    # Re-login every 100 stocks
    if (idx + 1) % 100 == 0:
        bs.logout()
        time.sleep(5)
        lg = bs.login()

bs.logout()
log(f"Done! {success} stocks downloaded")
