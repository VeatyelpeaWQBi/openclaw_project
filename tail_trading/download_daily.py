#!/usr/bin/env python3
"""下载沪深300+中证1000成分股的日K数据 (使用baostock)"""

import json
import os
import sys
import io
import time
import pandas as pd
import baostock as bs
import efinance as ef

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

BASE_DIR = "***REMOVED***/tail_trading"
DAILY_DIR = os.path.join(BASE_DIR, "daily_data")
INDEX_DIR = os.path.join(BASE_DIR, "index_history")
PROGRESS_FILE = os.path.join(BASE_DIR, "daily_progress.json")
RESULT_FILE = os.path.join(BASE_DIR, "daily_result.json")
STOCK_LIST_FILE = os.path.join(BASE_DIR, "all_stocks_list.csv")

os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(INDEX_DIR, exist_ok=True)

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def save_progress(idx, total, code, name, status="downloading"):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump({
            "step": "下载日K",
            "current_index": idx,
            "total": total,
            "current_stock": f"{code}_{name}",
            "status": status,
            "percentage": round(idx / total * 100, 2) if total > 0 else 0,
            "time": time.strftime("%Y-%m-%d %H:%M:%S")
        }, f, ensure_ascii=False, indent=2)

# ========== 获取成分股列表 ==========
log("获取成分股列表...")

# 检查是否已有列表
if os.path.exists(STOCK_LIST_FILE):
    all_df = pd.read_csv(STOCK_LIST_FILE)
    all_stocks = list(zip(all_df['代码'].astype(str).str.zfill(6), all_df['名称']))
    log(f"从文件加载: {len(all_stocks)} 只")
else:
    lg = bs.login()
    log(f"baostock login: {lg.error_code}")

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
    log(f"  沪深300去重: {len(hs300_all)} 只")

    try:
        zz1000 = ef.stock.get_members('000852')
        zz1000_set = set()
        for _, row in zz1000.iterrows():
            code = str(row['股票代码']).zfill(6)
            zz1000_set.add((code, row['股票名称']))
        log(f"  中证1000: {len(zz1000_set)} 只")
    except Exception as e:
        log(f"  ⚠️ 中证1000失败: {e}")
        zz1000_set = set()

    all_stocks = sorted(list(hs300_all | zz1000_set), key=lambda x: x[0])
    all_df = pd.DataFrame(all_stocks, columns=['代码', '名称'])
    all_df.to_csv(STOCK_LIST_FILE, index=False, encoding='utf-8')

log(f"总计: {len(all_stocks)} 只")

# ========== 检查已完成的 ==========
existing = set()
if os.path.exists(DAILY_DIR):
    for f in os.listdir(DAILY_DIR):
        if f.endswith('.csv'):
            existing.add(f.split('_')[0])
log(f"已有日K: {len(existing)} 只")

# ========== 下载日K ==========
log("开始下载日K数据...")

DAILY_FIELDS = 'date,open,high,low,close,volume,amount,turn,peTTM,pbMRQ,psTTM,pcfNcfTTM'
DAILY_HEADER = ['日期','开盘','最高','最低','收盘','成交量','成交额','换手率','市盈率','市净率','市销率','市现率']

success = 0
skip = 0
fail = 0
failed_list = []

lg = bs.login()
log(f"baostock login: {lg.error_code}")

for idx, (code, name) in enumerate(all_stocks):
    if code in existing:
        skip += 1
        continue
    
    save_progress(idx + 1, len(all_stocks), code, name)
    
    # baostock格式
    if code.startswith('6') or code.startswith('9'):
        bs_code = f"sh.{code}"
    else:
        bs_code = f"sz.{code}"
    
    downloaded = False
    for attempt in range(3):
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
            if attempt < 2:
                time.sleep(2)
            else:
                fail += 1
                failed_list.append((code, name, str(e)))
    
    if (idx + 1) % 100 == 0:
        log(f"  进度: {idx+1}/{len(all_stocks)} (成功:{success} 跳过:{skip} 失败:{fail})")
    
    time.sleep(0.2)

bs.logout()

log(f"日K下载完成! 成功={success} 跳过={skip} 失败={fail}")

# ========== 保存结果 ==========
result = {
    "total_stocks": len(all_stocks),
    "success": success,
    "skip": skip,
    "fail": fail,
    "failed_stocks": [(c, n, e) for c, n, e in failed_list[:50]],
    "completed_at": time.strftime("%Y-%m-%d %H:%M:%S")
}

with open(RESULT_FILE, 'w') as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

save_progress(len(all_stocks), len(all_stocks), "", "", "completed")
log("完成!")
