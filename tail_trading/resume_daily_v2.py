import requests
import pandas as pd
import os
import time
import json
from datetime import datetime
import random

BASE_DIR = "***REMOVED***/tail_trading"
DAILY_DIR = os.path.join(BASE_DIR, "daily_data")
INDEX_DIR = os.path.join(BASE_DIR, "index_history")
PROGRESS_FILE = os.path.join(BASE_DIR, "download_progress.json")
RESULT_FILE = os.path.join(BASE_DIR, "download_result.json")

os.makedirs(DAILY_DIR, exist_ok=True)

def get_sina_code(code):
    code = str(code).zfill(6)
    if code.startswith('6') or code.startswith('9'):
        return f"sh{code}"
    else:
        return f"sz{code}"

def create_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer': 'https://finance.sina.com.cn/',
        'Connection': 'keep-alive',
    })
    return session

def download_daily_sina(session, code, name, retries=5):
    sina_code = get_sina_code(code)
    url = f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={sina_code}&scale=240&ma=no&datalen=2000"
    
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            
            if not data:
                return 0, "empty"
            
            df = pd.DataFrame(data)
            df = df.rename(columns={'day':'日期','open':'开盘','high':'最高','low':'最低','close':'收盘','volume':'成交量'})
            df['成交额'] = 0; df['换手率'] = 0; df['市盈率'] = 0
            df['市净率'] = 0; df['市销率'] = 0; df['市现率'] = 0
            df['日期'] = pd.to_datetime(df['日期'])
            df = df[(df['日期'] >= '2020-01-01') & (df['日期'] <= '2026-03-31')]
            
            if len(df) == 0:
                return 0, "no data in range"
            
            for c in ['开盘','最高','最低','收盘','成交量']:
                df[c] = pd.to_numeric(df[c], errors='coerce')
            
            cols = ['日期','开盘','最高','最低','收盘','成交量','成交额','换手率','市盈率','市净率','市销率','市现率']
            df = df[cols]
            
            key = f"{code}_{name}"
            df.to_csv(os.path.join(DAILY_DIR, f"{key}.csv"), index=False, encoding='utf-8')
            return len(df), "ok"
            
        except Exception as e:
            wait = min(30, (2 ** attempt) + random.uniform(0, 1))
            if attempt < retries - 1:
                time.sleep(wait)
            else:
                return 0, str(e)[:100]
    
    return 0, "max retries"

def load_all_stocks():
    stocks = {}
    for f in sorted(os.listdir(INDEX_DIR)):
        if f.startswith("hs300_") and f.endswith(".csv"):
            df = pd.read_csv(os.path.join(INDEX_DIR, f))
            for _, row in df.iterrows():
                raw_code = str(row.get('代码', ''))
                code = raw_code.replace('sh','').replace('sz','').zfill(6)
                name = str(row.get('名称', ''))
                if code and name and name != 'nan' and code.isdigit():
                    stocks[code] = name
    zz1000_file = os.path.join(INDEX_DIR, "zz1000_current.csv")
    if os.path.exists(zz1000_file):
        df = pd.read_csv(zz1000_file)
        for _, row in df.iterrows():
            raw_code = str(row.get('代码', ''))
            code = raw_code.replace('sh','').replace('sz','').zfill(6)
            name = str(row.get('名称', ''))
            if code and name and name != 'nan' and code.isdigit():
                stocks[code] = name
    return stocks

# Load
all_stocks = load_all_stocks()
stock_list = sorted(all_stocks.items())
print(f"总成分股数: {len(stock_list)}", flush=True)

existing_daily = set()
for f in os.listdir(DAILY_DIR):
    if f.endswith('.csv'):
        existing_daily.add(f.replace('.csv', ''))
print(f"已有日K文件: {len(existing_daily)}", flush=True)

to_download = [(c, n) for c, n in stock_list if f"{c}_{n}" not in existing_daily]
print(f"需要下载: {len(to_download)}", flush=True)

# Download
session = create_session()
success = 0
failed = 0
skipped = len(stock_list) - len(to_download)
errors = []
consecutive_errors = 0

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
    
    rows, status = download_daily_sina(session, code, name)
    
    if status == "ok":
        success += 1
        consecutive_errors = 0
    else:
        failed += 1
        consecutive_errors += 1
        errors.append(f"{key}: {status}")
        
        # If too many consecutive errors, recreate session and wait longer
        if consecutive_errors >= 5:
            print(f"  [{i+1}] 连续{consecutive_errors}个错误，重建连接等待30秒...", flush=True)
            time.sleep(30)
            session = create_session()
            consecutive_errors = 0
        else:
            time.sleep(5)
    
    if (i + 1) % 100 == 0:
        print(f"  [{i+1}/{len(to_download)}] 成功:{success} 失败:{failed} 总文件:{skipped+success}", flush=True)
    
    # Adaptive rate: 1-2 seconds between requests
    time.sleep(1.0 + random.uniform(0, 0.5))

# Save results
result = {
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "total_stocks": len(stock_list),
    "already_done": skipped,
    "downloaded_now": success,
    "failed": failed,
    "total_daily_files": skipped + success,
    "errors": errors[:100],
    "status": "daily_complete" if failed == 0 else "daily_partial",
    "data_source": "sina_finance"
}
with open(RESULT_FILE, 'w') as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

progress["status"] = "daily_complete" if failed == 0 else "daily_partial"
with open(PROGRESS_FILE, 'w') as f:
    json.dump(progress, f, ensure_ascii=False, indent=2)

print(f"\n=== 完成 ===", flush=True)
print(f"总数: {len(stock_list)}", flush=True)
print(f"已有: {skipped}", flush=True)
print(f"本次成功: {success}", flush=True)
print(f"失败: {failed}", flush=True)
print(f"日K总文件数: {skipped + success}", flush=True)
