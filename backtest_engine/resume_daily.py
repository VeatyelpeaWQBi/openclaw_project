import akshare as ak
import pandas as pd
import os
import time
import json
import sys
from datetime import datetime

BASE_DIR = "***REMOVED***/tail_trading"
DAILY_DIR = os.path.join(BASE_DIR, "daily_data")
MINUTE_DIR = os.path.join(BASE_DIR, "minute_data")
INDEX_DIR = os.path.join(BASE_DIR, "index_history")
PROGRESS_FILE = os.path.join(BASE_DIR, "download_progress.json")
RESULT_FILE = os.path.join(BASE_DIR, "download_result.json")

os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(MINUTE_DIR, exist_ok=True)

# Load stock list from index history snapshots
def load_all_stocks():
    stocks = {}
    # HS300 snapshots
    for f in sorted(os.listdir(INDEX_DIR)):
        if f.startswith("hs300_") and f.endswith(".csv"):
            df = pd.read_csv(os.path.join(INDEX_DIR, f))
            for _, row in df.iterrows():
                raw_code = str(row.get('代码', ''))
                # Strip sh/sz prefix
                code = raw_code.replace('sh', '').replace('sz', '').zfill(6)
                name = str(row.get('名称', ''))
                if code and name and name != 'nan' and code.isdigit():
                    stocks[code] = name
    # ZZ1000 current
    zz1000_file = os.path.join(INDEX_DIR, "zz1000_current.csv")
    if os.path.exists(zz1000_file):
        df = pd.read_csv(zz1000_file)
        for _, row in df.iterrows():
            raw_code = str(row.get('代码', ''))
            code = raw_code.replace('sh', '').replace('sz', '').zfill(6)
            name = str(row.get('名称', ''))
            if code and name and name != 'nan' and code.isdigit():
                stocks[code] = name
    return stocks

# Get existing daily files
existing_daily = set()
for f in os.listdir(DAILY_DIR):
    if f.endswith('.csv'):
        existing_daily.add(f.replace('.csv', ''))

print(f"已有日K文件: {len(existing_daily)}")

# Load stock list
all_stocks = load_all_stocks()
stock_list = sorted(all_stocks.items())
print(f"总成分股数: {len(stock_list)}")

# Filter out already downloaded
to_download = []
for code, name in stock_list:
    key = f"{code}_{name}"
    if key not in existing_daily:
        to_download.append((code, name))

print(f"需要下载: {len(to_download)}")

# Load progress
start_index = 0
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, 'r') as f:
        prog = json.load(f)
        start_index = prog.get('current_index', 0)
        print(f"从进度文件恢复: index={start_index}")

# Download daily data
success = 0
failed = 0
skipped = len(stock_list) - len(to_download)
errors = []

for i, (code, name) in enumerate(to_download):
    idx = start_index + i
    key = f"{code}_{name}"
    
    # Update progress
    progress = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "current_index": idx,
        "total": len(stock_list),
        "current_stock": key,
        "status": "downloading",
        "percentage": round(idx / len(stock_list) * 100, 2)
    }
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)
    
    try:
        df = ak.stock_zh_a_hist(symbol=code, period="daily",
                                start_date="20200101", end_date="20260331", adjust="qfq")
        if df is not None and len(df) > 0:
            df.to_csv(os.path.join(DAILY_DIR, f"{key}.csv"), index=False, encoding='utf-8')
            success += 1
            if (i + 1) % 50 == 0:
                print(f"  [{i+1}/{len(to_download)}] ✓ {code} {name} (累计成功:{success})")
        else:
            failed += 1
            errors.append(f"{code}_{name}: empty data")
            print(f"  [{i+1}/{len(to_download)}] ✗ {code} {name} empty")
    except Exception as e:
        failed += 1
        err_msg = str(e)
        errors.append(f"{code}_{name}: {err_msg}")
        print(f"  [{i+1}/{len(to_download)}] ✗ {code} {name}: {err_msg[:80]}")
        time.sleep(5)  # Error backoff
    
    time.sleep(1)  # Rate limit

# Final result
result = {
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "total_stocks": len(stock_list),
    "already_done": skipped,
    "downloaded_now": success,
    "failed": failed,
    "errors": errors[:50],
    "status": "daily_complete" if failed == 0 else "daily_partial"
}
with open(RESULT_FILE, 'w') as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

# Update progress as complete
progress["status"] = "daily_complete"
progress["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
with open(PROGRESS_FILE, 'w') as f:
    json.dump(progress, f, ensure_ascii=False, indent=2)

print(f"\n=== 完成 ===")
print(f"总数: {len(stock_list)}")
print(f"已有: {skipped}")
print(f"本次下载: {success}")
print(f"失败: {failed}")
print(f"总完成: {skipped + success}/{len(stock_list)}")
