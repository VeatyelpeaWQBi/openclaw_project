#!/usr/bin/env python3
"""
重试修复换手率 - 保守版
用Session复用连接，更长间隔，分批处理
"""

import os
import csv
import time
import json
import requests
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent
DAILY_DATA_DIR = BASE_DIR / "daily_data"
PROGRESS_FILE = BASE_DIR / "turnover_fix_progress.json"

KLINE_URL = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
KLINE_FIELDS = 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61'


def get_market(stock_code: str) -> int:
    prefix2 = stock_code[:2]
    prefix3 = stock_code[:3]
    if prefix2 in ('60', '68', '90'):
        return 1
    if prefix2 in ('00', '30', '20'):
        return 0
    if stock_code.startswith('688') or stock_code.startswith('689'):
        return 1
    if prefix3 in ('300', '301'):
        return 0
    if prefix2 in ('83', '87', '43'):
        return -1
    if stock_code[0] == '6':
        return 1
    return 0


def find_still_need_fix():
    csv_files = sorted([f for f in os.listdir(DAILY_DATA_DIR) if f.endswith('.csv')])
    need_fix = []
    for f in csv_files:
        filepath = DAILY_DATA_DIR / f
        with open(filepath, 'r', encoding='utf-8') as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)
        if not rows:
            continue
        has_nonzero = any(
            r.get('换手率', '').strip() and float(r.get('换手率', '0')) > 0
            for r in rows
        )
        if not has_nonzero:
            need_fix.append(f)
    return need_fix


def load_progress():
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {"done": [], "failed": []}


def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, ensure_ascii=False)


def parse_filename(filename: str):
    name = filename.replace('.csv', '')
    parts = name.split('_', 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (parts[0], '')


def fetch_kline(session, secid: str, retries: int = 3) -> list:
    params = {
        'secid': secid,
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': KLINE_FIELDS,
        'klt': '101',
        'fqt': '1',
        'beg': '20200101',
        'end': '20260401',
        'lmt': '5000',
    }
    for attempt in range(retries):
        try:
            resp = session.get(KLINE_URL, params=params, timeout=20)
            data = resp.json()
            if not data.get('data') or not data['data'].get('klines'):
                return []
            result = []
            for line in data['data']['klines']:
                parts = line.split(',')
                if len(parts) < 11:
                    continue
                result.append({
                    'date': parts[0],
                    'amount': parts[6],
                    'turnover': parts[10],
                })
            return result
        except Exception as e:
            if attempt < retries - 1:
                wait = (attempt + 1) * 10
                time.sleep(wait)
            else:
                return []
    return []


def main():
    print("="*60, flush=True)
    print("重试修复换手率 - 保守版", flush=True)
    print("="*60, flush=True)

    need_fix = find_still_need_fix()
    progress = load_progress()
    
    # 只处理还没做过的
    todo = [f for f in need_fix if f not in progress['done']]
    print(f"仍需修复: {len(need_fix)}, 待处理: {len(todo)}", flush=True)

    if not todo:
        print("全部完成!", flush=True)
        return

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'http://quote.eastmoney.com/',
        'Connection': 'keep-alive',
    })

    success = 0
    failed = 0
    consecutive_failures = 0

    for i, filename in enumerate(todo):
        stock_code, stock_name = parse_filename(filename)
        market = get_market(stock_code)

        if market < 0:
            progress['done'].append(filename)
            continue

        secid = f"{market}.{stock_code}"
        print(f"  [{i+1}/{len(todo)}] {stock_code} {stock_name}...", end="", flush=True)

        klines = fetch_kline(session, secid)

        if not klines:
            print(" 失败", flush=True)
            failed += 1
            consecutive_failures += 1
            progress['failed'].append(filename)
            
            # 连续失败超过5个，长休息
            if consecutive_failures >= 5:
                print(f"  --- 连续失败{consecutive_failures}个，休息30秒 ---", flush=True)
                time.sleep(30)
                consecutive_failures = 0
                # 重建session
                session.close()
                session = requests.Session()
                session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Referer': 'http://quote.eastmoney.com/',
                    'Connection': 'keep-alive',
                })
            else:
                time.sleep(3)
            continue

        consecutive_failures = 0

        filepath = DAILY_DATA_DIR / filename
        with open(filepath, 'r', encoding='utf-8') as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)
            fieldnames = reader.fieldnames

        kline_map = {k['date']: k for k in klines}
        updated = 0
        for row in rows:
            date = row['日期']
            if date in kline_map:
                k = kline_map[date]
                row['换手率'] = k['turnover']
                row['成交额'] = k['amount']
                updated += 1

        with open(filepath, 'w', encoding='utf-8', newline='') as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        print(f" 更新{updated}行", flush=True)
        success += 1
        progress['done'].append(filename)

        # 每10只保存进度并休息
        if (i + 1) % 10 == 0:
            save_progress(progress)
            print(f"  --- 进度已保存 ({i+1}/{len(todo)})，休息3秒 ---", flush=True)
            time.sleep(3)
        else:
            time.sleep(1.5)  # 每个请求间隔1.5秒

    save_progress(progress)
    print(f"\n{'='*60}", flush=True)
    print(f"完成! 成功: {success}, 失败: {failed}", flush=True)


if __name__ == '__main__':
    main()
