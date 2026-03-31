#!/usr/bin/env python3
"""重试日K下载 - 保守策略，每50只休息一次"""

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
RESULT_FILE = os.path.join(BASE_DIR, "daily_result2.json")
PROGRESS_FILE = os.path.join(BASE_DIR, "daily_progress2.json")

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

# Check existing
existing = set()
if os.path.exists(DAILY_DIR):
    for f in os.listdir(DAILY_DIR):
        if f.endswith('.csv'):
            existing.add(f.split('_')[0])

missing = [(c, n) for c, n in all_stocks if c not in existing]
log(f"Total: {len(all_stocks)}, Already have: {len(existing)}, Need to download: {len(missing)}")

if not missing:
    log("All done!")
    with open(RESULT_FILE, 'w') as f:
        json.dump({"status": "all_done", "total": len(all_stocks)}, f)
    sys.exit(0)

DAILY_FIELDS = 'date,open,high,low,close,volume,amount,turn,peTTM,pbMRQ,psTTM,pcfNcfTTM'
DAILY_HEADER = ['日期','开盘','最高','最低','收盘','成交量','成交额','换手率','市盈率','市净率','市销率','市现率']

success = 0
fail = 0
failed_list = []

lg = bs.login()
log(f"Login: {lg.error_code}")

BATCH_SIZE = 50
BATCH_REST = 15

for batch_num, batch_start in enumerate(range(0, len(missing), BATCH_SIZE)):
    batch = missing[batch_start:batch_start + BATCH_SIZE]
    log(f"=== Batch {batch_num+1}/{(len(missing)-1)//BATCH_SIZE+1} ({len(batch)} stocks) ===")
    
    for idx, (code, name) in enumerate(batch):
        global_idx = batch_start + idx
        save_progress(global_idx + 1, len(missing), code, name)
        
        bs_code = f"sh.{code}" if code.startswith(('6', '9')) else f"sz.{code}"
        
        for attempt in range(3):
            try:
                rs = bs.query_history_k_data_plus(bs_code, DAILY_FIELDS,
                    start_date='2020-01-01', end_date='2026-03-30', frequency='d', adjustflag='2')
                data = []
                while rs.next():
                    data.append(rs.get_row_data())
                
                if rs.error_code == '0' and data:
                    pd.DataFrame(data, columns=DAILY_HEADER).to_csv(
                        os.path.join(DAILY_DIR, f"{code}_{name}.csv"), index=False, encoding='utf-8')
                    success += 1
                else:
                    fail += 1
                    failed_list.append((code, name, rs.error_msg))
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(5)
                else:
                    fail += 1
                    failed_list.append((code, name, str(e)))
        
        time.sleep(0.3)
    
    done = min(batch_start + BATCH_SIZE, len(missing))
    log(f"  Batch done. Progress: {done}/{len(missing)} (ok:{success} fail:{fail})")
    
    if batch_start + BATCH_SIZE < len(missing):
        log(f"  Resting {BATCH_REST}s...")
        # Re-login to keep connection fresh
        bs.logout()
        time.sleep(BATCH_REST)
        lg = bs.login()
        log(f"  Re-login: {lg.error_code}")

bs.logout()

result = {
    "total": len(all_stocks),
    "existing": len(existing),
    "new_success": success,
    "new_fail": fail,
    "final_success": len(existing) + success,
    "failed": failed_list[:50],
    "done_at": time.strftime("%Y-%m-%d %H:%M:%S")
}

with open(RESULT_FILE, 'w') as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

save_progress(len(missing), len(missing), "", "", "completed")
log(f"DONE! Final: {len(existing)+success}/{len(all_stocks)} stocks")
