#!/usr/bin/env python3
"""最终修复 - 下载剩余的日K数据"""

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

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# Load stock list
all_df = pd.read_csv(STOCK_LIST_FILE)
all_stocks = list(zip(all_df['代码'].astype(str).str.zfill(6), all_df['名称']))

# Check existing - extract clean 6-digit codes
existing = set()
if os.path.exists(DAILY_DIR):
    for f in os.listdir(DAILY_DIR):
        if f.endswith('.csv'):
            # Extract code - handle both "600519_xxx.csv" and "sh600519_xxx.csv"
            code_part = f.split('_')[0]
            # Remove sh/sz prefix if present
            if code_part.startswith('sh'):
                code_part = code_part[2:]
            elif code_part.startswith('sz'):
                code_part = code_part[2:]
            existing.add(code_part)

missing = [(c, n) for c, n in all_stocks if c not in existing]
log(f"Total: {len(all_stocks)}, Have: {len(existing)}, Missing: {len(missing)}")

# Also check which existing files have bad names and rename them
renamed = 0
for f in os.listdir(DAILY_DIR):
    if f.endswith('.csv'):
        code_part = f.split('_')[0]
        if code_part.startswith('sh') or code_part.startswith('sz'):
            clean_code = code_part[2:]
            rest = f[len(code_part):]
            new_name = clean_code + rest
            os.rename(os.path.join(DAILY_DIR, f), os.path.join(DAILY_DIR, new_name))
            renamed += 1
if renamed:
    log(f"Renamed {renamed} files with bad prefixes")

# Recalculate missing after rename
existing = set()
for f in os.listdir(DAILY_DIR):
    if f.endswith('.csv'):
        existing.add(f.split('_')[0])

missing = [(c, n) for c, n in all_stocks if c not in existing]
log(f"After cleanup - Have: {len(existing)}, Missing: {len(missing)}")

if not missing:
    log("All done!")
    sys.exit(0)

DAILY_FIELDS = 'date,open,high,low,close,volume,amount,turn,peTTM,pbMRQ,psTTM,pcfNcfTTM'
DAILY_HEADER = ['日期','开盘','最高','最低','收盘','成交量','成交额','换手率','市盈率','市净率','市销率','市现率']

success = 0
fail = 0
failed_list = []

lg = bs.login()
log(f"Login: {lg.error_code}")

for idx, (code, name) in enumerate(missing):
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
                time.sleep(3)
            else:
                fail += 1
                failed_list.append((code, name, str(e)))
    
    time.sleep(0.3)
    if (idx + 1) % 50 == 0:
        log(f"  Progress: {idx+1}/{len(missing)} (ok:{success} fail:{fail})")

bs.logout()

# Final count
final_count = len([f for f in os.listdir(DAILY_DIR) if f.endswith('.csv')])
log(f"Done! Files: {final_count}/{len(all_stocks)}, New success: {success}, New fail: {fail}")
if failed_list:
    log(f"Still failed: {len(failed_list)}")
    for c, n, e in failed_list[:10]:
        log(f"  {c} {n}: {e}")
