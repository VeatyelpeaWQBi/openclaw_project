#!/usr/bin/env python3
"""重试失败的日K数据下载 (使用baostock，增加延迟和重试)"""

import json
import os
import sys
import io
import time
import pandas as pd
import baostock as bs

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

BASE_DIR = "***REMOVED***/tail_trading"
DAILY_DIR = os.path.join(BASE_DIR, "daily_data")
STOCK_LIST_FILE = os.path.join(BASE_DIR, "all_stocks_list.csv")
RESULT_FILE = os.path.join(BASE_DIR, "daily_retry_result.json")
PROGRESS_FILE = os.path.join(BASE_DIR, "daily_retry_progress.json")

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def save_progress(idx, total, code, name, status="downloading"):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump({
            "step": "重试日K",
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
log(f"Total stocks: {len(all_stocks)}")

# Check existing
existing = set()
if os.path.exists(DAILY_DIR):
    for f in os.listdir(DAILY_DIR):
        if f.endswith('.csv'):
            existing.add(f.split('_')[0])

missing = [(c, n) for c, n in all_stocks if c not in existing]
log(f"Existing: {len(existing)}, Missing: {len(missing)}")

if not missing:
    log("All stocks already downloaded!")
    sys.exit(0)

DAILY_FIELDS = 'date,open,high,low,close,volume,amount,turn,peTTM,pbMRQ,psTTM,pcfNcfTTM'
DAILY_HEADER = ['日期','开盘','最高','最低','收盘','成交量','成交额','换手率','市盈率','市净率','市销率','市现率']

success = 0
fail = 0
failed_list = []

lg = bs.login()
log(f"baostock login: {lg.error_code}")

# Process in batches with rest periods
BATCH_SIZE = 50
REST_TIME = 10  # seconds between batches

for batch_start in range(0, len(missing), BATCH_SIZE):
    batch = missing[batch_start:batch_start + BATCH_SIZE]
    log(f"Processing batch {batch_start//BATCH_SIZE + 1}/{(len(missing)-1)//BATCH_SIZE + 1} ({len(batch)} stocks)")
    
    for idx, (code, name) in enumerate(batch):
        global_idx = batch_start + idx
        save_progress(global_idx + 1, len(missing), code, name)
        
        if code.startswith('6') or code.startswith('9'):
            bs_code = f"sh.{code}"
        else:
            bs_code = f"sz.{code}"
        
        downloaded = False
        for attempt in range(5):  # More retries
            try:
                rs = bs.query_history_k_data_plus(bs_code, DAILY_FIELDS,
                    start_date='2020-01-01', end_date='2026-03-30', frequency='d', adjustflag='2')
                data = []
                while rs.next():
                    data.append(rs.get_row_data())
                
                if rs.error_code == '0' and data:
                    df = pd.DataFrame(data, columns=DAILY_HEADER)
                    df.to_csv(os.path.join(DAILY_DIR, f"{code}_{name}.csv"), index=False, encoding='utf-8')
                    success += 1
                    downloaded = True
                else:
                    fail += 1
                    failed_list.append((code, name, rs.error_msg))
                    downloaded = True
                break
            except Exception as e:
                if attempt < 4:
                    time.sleep(3 * (attempt + 1))  # Exponential backoff
                else:
                    fail += 1
                    failed_list.append((code, name, str(e)))
        
        time.sleep(0.5)  # Longer delay between requests
        
        if (global_idx + 1) % 20 == 0:
            log(f"  Progress: {global_idx+1}/{len(missing)} (success:{success} fail:{fail})")
    
    # Rest between batches
    if batch_start + BATCH_SIZE < len(missing):
        log(f"  Resting {REST_TIME}s between batches...")
        time.sleep(REST_TIME)

bs.logout()

# Merge results
total_success = len(existing) + success
result = {
    "total_stocks": len(all_stocks),
    "previously_done": len(existing),
    "retry_success": success,
    "retry_fail": fail,
    "total_success": total_success,
    "failed_stocks": failed_list[:100],
    "completed_at": time.strftime("%Y-%m-%d %H:%M:%S")
}

with open(RESULT_FILE, 'w') as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

save_progress(len(missing), len(missing), "", "", "completed")
log(f"Done! Total success: {total_success}/{len(all_stocks)}")
