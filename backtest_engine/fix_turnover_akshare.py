#!/usr/bin/env python3
"""
用 akshare 修复换手率数据 - 不依赖被封的eastmoney push2his API
"""

import os
import csv
import time
import json
import sys
import akshare as ak
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent
DAILY_DATA_DIR = BASE_DIR / "daily_data"
PROGRESS_FILE = BASE_DIR / "turnover_fix_progress.json"


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


def get_market_prefix(stock_code: str) -> str:
    """akshare需要用 sh/sz 前缀"""
    prefix2 = stock_code[:2]
    prefix3 = stock_code[:3]
    if prefix2 in ('60', '68', '90') or stock_code.startswith('688') or stock_code.startswith('689'):
        return 'sh'
    if prefix2 in ('00', '30', '20') or prefix3 in ('300', '301'):
        return 'sz'
    return 'sz'  # default


def fetch_stock_data_akshare(stock_code: str) -> dict:
    """
    用akshare获取个股日K数据
    返回: {date: {turnover, amount}, ...}
    """
    try:
        # ak.stock_zh_a_hist 返回带换手率的DataFrame
        df = ak.stock_zh_a_hist(
            symbol=stock_code,
            period="daily",
            start_date="20200101",
            end_date="20260401",
            adjust="qfq"  # 前复权
        )
        if df is None or df.empty:
            return {}

        result = {}
        for _, row in df.iterrows():
            date_str = str(row['日期']).replace('-', '')
            result[date_str] = {
                'turnover': str(row.get('换手率', 0)),
                'amount': str(row.get('成交额', 0)),
            }
        return result
    except Exception as e:
        print(f"  akshare错误: {e}", flush=True)
        return {}


def main():
    print("="*60, flush=True)
    print("用 akshare 修复换手率", flush=True)
    print("="*60, flush=True)

    need_fix = find_still_need_fix()
    progress = load_progress()
    todo = [f for f in need_fix if f not in progress['done']]
    
    print(f"仍需修复: {len(need_fix)}, 待处理: {len(todo)}", flush=True)

    if not todo:
        print("全部完成!", flush=True)
        return

    success = 0
    failed = 0
    total_updated = 0

    for i, filename in enumerate(todo):
        stock_code, stock_name = parse_filename(filename)
        
        # 跳过北交所
        prefix2 = stock_code[:2]
        if prefix2 in ('83', '87', '43'):
            progress['done'].append(filename)
            continue

        print(f"  [{i+1}/{len(todo)}] {stock_code} {stock_name}...", end="", flush=True)

        kline_data = fetch_stock_data_akshare(stock_code)

        if not kline_data:
            print(" 无数据", flush=True)
            failed += 1
            progress['failed'].append(filename)
            time.sleep(1)
            continue

        filepath = DAILY_DATA_DIR / filename
        with open(filepath, 'r', encoding='utf-8') as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)
            fieldnames = reader.fieldnames

        updated = 0
        for row in rows:
            date = row['日期']
            if date in kline_data:
                k = kline_data[date]
                row['换手率'] = k['turnover']
                row['成交额'] = k['amount']
                updated += 1

        with open(filepath, 'w', encoding='utf-8', newline='') as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        print(f" 更新{updated}行", flush=True)
        success += 1
        total_updated += updated
        progress['done'].append(filename)

        if (i + 1) % 20 == 0:
            save_progress(progress)
            print(f"  --- 进度已保存 ({i+1}/{len(todo)}) ---", flush=True)

        time.sleep(0.5)  # akshare相对温和

    save_progress(progress)
    print(f"\n{'='*60}", flush=True)
    print(f"完成! 成功: {success}, 失败: {failed}, 总更新行数: {total_updated}", flush=True)


if __name__ == '__main__':
    main()
