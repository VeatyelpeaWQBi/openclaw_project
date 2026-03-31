import requests
import pandas as pd
import os
import time
import json
from datetime import datetime

BASE_DIR = "***REMOVED***/tail_trading"
DAILY_DIR = os.path.join(BASE_DIR, "daily_data")
INDEX_DIR = os.path.join(BASE_DIR, "index_history")
PROGRESS_FILE = os.path.join(BASE_DIR, "download_progress.json")
RESULT_FILE = os.path.join(BASE_DIR, "download_result.json")

os.makedirs(DAILY_DIR, exist_ok=True)

def get_sina_code(code):
    """Convert 6-digit code to sina format"""
    code = str(code).zfill(6)
    if code.startswith('6') or code.startswith('9'):
        return f"sh{code}"
    else:
        return f"sz{code}"

def download_daily_sina(code, name):
    """Download daily K data from Sina finance API"""
    sina_code = get_sina_code(code)
    url = f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={sina_code}&scale=240&ma=no&datalen=2000"
    
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    
    if not data:
        return 0, "empty"
    
    df = pd.DataFrame(data)
    # Rename available columns
    df = df.rename(columns={'day': '日期', 'open': '开盘', 'high': '最高', 'low': '最低', 'close': '收盘', 'volume': '成交量'})
    
    # Add placeholder columns to match existing file format
    df['成交额'] = 0
    df['换手率'] = 0
    df['市盈率'] = 0
    df['市净率'] = 0
    df['市销率'] = 0
    df['市现率'] = 0
    
    # Filter date range 2020-01-01 to 2026-03-31
    df['日期'] = pd.to_datetime(df['日期'])
    df = df[(df['日期'] >= '2020-01-01') & (df['日期'] <= '2026-03-31')]
    
    if len(df) == 0:
        return 0, "no data in range"
    
    # Convert numeric columns
    for col in ['开盘', '最高', '最低', '收盘', '成交量']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Reorder columns to match existing format
    cols = ['日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额', '换手率', '市盈率', '市净率', '市销率', '市现率']
    df = df[cols]
    
    key = f"{code}_{name}"
    df.to_csv(os.path.join(DAILY_DIR, f"{key}.csv"), index=False, encoding='utf-8')
    return len(df), "ok"

def load_all_stocks():
    stocks = {}
    for f in sorted(os.listdir(INDEX_DIR)):
        if f.startswith("hs300_") and f.endswith(".csv"):
            df = pd.read_csv(os.path.join(INDEX_DIR, f))
            for _, row in df.iterrows():
                raw_code = str(row.get('代码', ''))
                code = raw_code.replace('sh', '').replace('sz', '').zfill(6)
                name = str(row.get('名称', ''))
                if code and name and name != 'nan' and code.isdigit():
                    stocks[code] = name
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

# Load stocks
all_stocks = load_all_stocks()
stock_list = sorted(all_stocks.items())
print(f"总成分股数: {len(stock_list)}", flush=True)

# Existing daily files
existing_daily = set()
for f in os.listdir(DAILY_DIR):
    if f.endswith('.csv'):
        existing_daily.add(f.replace('.csv', ''))
print(f"已有日K文件: {len(existing_daily)}", flush=True)

# Filter
to_download = [(c, n) for c, n in stock_list if f"{c}_{n}" not in existing_daily]
print(f"需要下载: {len(to_download)}", flush=True)

# Download
success = 0
failed = 0
skipped = len(stock_list) - len(to_download)
errors = []

for i, (code, name) in enumerate(to_download):
    key = f"{code}_{name}"
    
    # Update progress
    progress = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "current_index": skipped + i,
        "total": len(stock_list),
        "current_stock": key,
        "status": "downloading",
        "percentage": round((skipped + i) / len(stock_list) * 100, 2)
    }
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)
    
    try:
        rows, status = download_daily_sina(code, name)
        if status == "ok":
            success += 1
        else:
            failed += 1
            errors.append(f"{key}: {status}")
        
        if (i + 1) % 100 == 0:
            print(f"  [{i+1}/{len(to_download)}] ✓ {key} (成功:{success} 失败:{failed})", flush=True)
    except Exception as e:
        failed += 1
        err_msg = str(e)[:100]
        errors.append(f"{key}: {err_msg}")
        if "456" in err_msg or "Connection" in err_msg:
            print(f"  [{i+1}] 网络错误，等待10秒...", flush=True)
            time.sleep(10)
        else:
            time.sleep(2)
    
    time.sleep(0.5)  # Rate limit

# Save results
result = {
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "total_stocks": len(stock_list),
    "already_done": skipped,
    "downloaded_now": success,
    "failed": failed,
    "total_daily_files": len(existing_daily) + success,
    "errors": errors[:50],
    "status": "daily_complete" if failed == 0 else "daily_partial",
    "data_source": "sina_finance"
}
with open(RESULT_FILE, 'w') as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

progress["status"] = "daily_complete"
with open(PROGRESS_FILE, 'w') as f:
    json.dump(progress, f, ensure_ascii=False, indent=2)

print(f"\n=== 完成 ===", flush=True)
print(f"总数: {len(stock_list)}", flush=True)
print(f"已有: {skipped}", flush=True)
print(f"本次成功: {success}", flush=True)
print(f"失败: {failed}", flush=True)
print(f"日K总文件数: {skipped + success}", flush=True)
