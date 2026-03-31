#!/usr/bin/env python3
"""下载5分钟线数据 - 补齐剩余股票"""

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
DAILY_DIR = os.path.join(BASE_DIR, "daily_data")
STOCK_LIST_FILE = os.path.join(BASE_DIR, "all_stocks_list.csv")
PROGRESS_FILE = os.path.join(BASE_DIR, "minute_progress.json")

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def save_progress(idx, total, code, name, status="downloading"):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump({
            "step": "下载5分钟线",
            "current_index": idx,
            "total": total,
            "current_stock": f"{code}_{name}",
            "status": status,
            "percentage": round(idx / total * 100, 2) if total > 0 else 0,
            "time": time.strftime("%Y-%m-%d %H:%M:%S")
        }, f, ensure_ascii=False, indent=2)

# Load stock list
all_df = pd.read_csv(STOCK_LIST_FILE)
all_stocks = list(zip(all_df['代码'].astype(str).str.zfill(6), all_df['名称']))

# Check which stocks already have minute data
existing_minute = set()
if os.path.exists(MINUTE_DIR):
    for month_dir in os.listdir(MINUTE_DIR):
        month_path = os.path.join(MINUTE_DIR, month_dir)
        if os.path.isdir(month_path) and '-' in month_dir:
            for stock_dir in os.listdir(month_path):
                code = stock_dir.split('_')[0]
                # Check if has any files
                stock_path = os.path.join(month_path, stock_dir)
                if os.path.isdir(stock_path) and os.listdir(stock_path):
                    existing_minute.add(code)

missing = [(c, n) for c, n in all_stocks if c not in existing_minute]
log(f"Total: {len(all_stocks)}, Have minute data: {len(existing_minute)}, Missing: {len(missing)}")

if not missing:
    log("All stocks have minute data!")
    sys.exit(0)

MINUTE_FIELDS = 'date,time,open,high,low,close,volume,amount'

# Generate month ranges
months = pd.date_range('2020-01-01', '2026-03-30', freq='MS')
month_ranges = []
for m in months:
    start = m.strftime('%Y-%m-%d')
    end = (m + pd.offsets.MonthEnd(0)).strftime('%Y-%m-%d')
    ym = m.strftime('%Y-%m')
    month_ranges.append((ym, start, end))

log(f"Month ranges: {len(month_ranges)}")

success = 0
fail = 0

lg = bs.login()
log(f"Login: {lg.error_code}")

BATCH_SIZE = 20
BATCH_REST = 10

for batch_num, batch_start in enumerate(range(0, len(missing), BATCH_SIZE)):
    batch = missing[batch_start:batch_start + BATCH_SIZE]
    log(f"=== Batch {batch_num+1}/{(len(missing)-1)//BATCH_SIZE+1} ({len(batch)} stocks) ===")
    
    for idx, (code, name) in enumerate(batch):
        global_idx = batch_start + idx
        save_progress(global_idx + 1, len(missing), code, name)
        
        bs_code = f"sh.{code}" if code.startswith(('6', '9')) else f"sz.{code}"
        stock_failed = False
        
        for ym, ms, me in month_ranges:
            stock_dir = os.path.join(MINUTE_DIR, ym, f"{code}_{name}")
            
            # Skip if this month already has data
            if os.path.exists(stock_dir) and os.listdir(stock_dir):
                continue
            
            for attempt in range(3):
                try:
                    rs = bs.query_history_k_data_plus(bs_code, MINUTE_FIELDS,
                        start_date=ms, end_date=me, frequency='5', adjustflag='2')
                    data = []
                    while rs.next():
                        data.append(rs.get_row_data())
                    
                    if rs.error_code == '0' and data:
                        df = pd.DataFrame(data, columns=['日期','时间','开盘','最高','最低','收盘','成交量','成交额'])
                        # Parse time and filter for 09:30-10:15 and 14:50-14:55
                        df['时间解析'] = pd.to_datetime(df['时间'], format='%Y%m%d%H%M%S%f')
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
                                out = group.copy()
                                out['时间'] = out['时间解析'].dt.strftime('%Y-%m-%d %H:%M:%S')
                                out.drop(columns=['时间解析'], inplace=True)
                                out.to_csv(fp, index=False, encoding='utf-8')
                    break
                except Exception as e:
                    if attempt < 2:
                        time.sleep(3)
                    else:
                        pass  # Skip errors
            
            time.sleep(0.2)
        
        success += 1
        time.sleep(0.3)
    
    done = min(batch_start + BATCH_SIZE, len(missing))
    log(f"  Batch done. Progress: {done}/{len(missing)} (ok:{success})")
    
    if batch_start + BATCH_SIZE < len(missing):
        bs.logout()
        time.sleep(BATCH_REST)
        lg = bs.login()

bs.logout()
log(f"Done! Downloaded minute data for {success} stocks")
