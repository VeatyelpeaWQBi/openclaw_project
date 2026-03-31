#!/usr/bin/env python3
"""
用 baostock 修复换手率数据
baostock 不依赖 eastmoney push2his API，不会被封
"""

import os
import csv
import time
import json
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

import baostock as bs
import pandas as pd
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent
DAILY_DATA_DIR = BASE_DIR / "daily_data"
PROGRESS_FILE = BASE_DIR / "turnover_fix_bs_progress.json"


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


def get_bs_code(stock_code: str) -> str:
    """转换为baostock格式 sz.000001 / sh.600000"""
    prefix2 = stock_code[:2]
    prefix3 = stock_code[:3]
    if prefix2 in ('60', '68', '90') or stock_code.startswith('688') or stock_code.startswith('689'):
        return f"sh.{stock_code}"
    if prefix2 in ('00', '30', '20') or prefix3 in ('300', '301'):
        return f"sz.{stock_code}"
    return f"sz.{stock_code}"


def fetch_bs_data(bs_code: str, retries: int = 3) -> dict:
    """
    用baostock获取日K数据
    返回: {date: {turnover, amount}, ...}
    """
    for attempt in range(retries):
        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                'date,amount,turn',
                start_date='2020-01-01',
                end_date='2026-04-01',
                frequency='d',
                adjustflag='2'  # 前复权
            )
            
            if rs.error_code != '0':
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return {}
            
            data = {}
            while rs.next():
                row = rs.get_row_data()
                if len(row) >= 3:
                    date_str = row[0]  # baostock返回 '2026-03-20' 格式，和CSV一致
                    amount = row[1] if row[1] else '0'
                    turn = row[2] if row[2] else '0'
                    data[date_str] = {
                        'turnover': turn,
                        'amount': amount,
                    }
            
            # 如果返回空数据，可能是被限流，重试
            if not data and attempt < retries - 1:
                time.sleep(3)
                continue
            
            return data
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(3)
                continue
            return {}
    return {}


def main():
    print("="*60, flush=True)
    print("用 baostock 修复换手率", flush=True)
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print("="*60, flush=True)

    # 登录baostock
    lg = bs.login()
    print(f"baostock登录: {lg.error_code} {lg.error_msg}", flush=True)
    if lg.error_code != '0':
        print("登录失败，退出", flush=True)
        return

    need_fix = find_still_need_fix()
    progress = load_progress()
    todo = [f for f in need_fix if f not in progress['done']]
    
    print(f"仍需修复: {len(need_fix)}, 待处理: {len(todo)}", flush=True)

    if not todo:
        print("全部完成!", flush=True)
        bs.logout()
        return

    success = 0
    failed = 0
    total_updated = 0
    skip_bj = 0
    consecutive_failures = 0

    for i, filename in enumerate(todo):
        stock_code, stock_name = parse_filename(filename)
        
        # 跳过北交所
        prefix2 = stock_code[:2]
        if prefix2 in ('83', '87', '43'):
            skip_bj += 1
            progress['done'].append(filename)
            continue

        # 每100只重新登录避免会话超时
        if i > 0 and i % 100 == 0:
            bs.logout()
            time.sleep(1)
            lg = bs.login()
            print(f"  --- 重新登录: {lg.error_code} {lg.error_msg} ---", flush=True)

        bs_code = get_bs_code(stock_code)
        print(f"  [{i+1}/{len(todo)}] {stock_code} {stock_name} ({bs_code})...", end="", flush=True)

        kline_data = fetch_bs_data(bs_code)

        if not kline_data:
            print(" 无数据", flush=True)
            failed += 1
            consecutive_failures += 1
            progress['failed'].append(filename)
            
            # 连续失败超过5个，重新登录
            if consecutive_failures >= 5:
                print(f"  --- 连续失败{consecutive_failures}个，重新登录 ---", flush=True)
                bs.logout()
                time.sleep(2)
                lg = bs.login()
                print(f"  --- 重新登录: {lg.error_code} {lg.error_msg} ---", flush=True)
                consecutive_failures = 0
            
            time.sleep(1)
            continue

        consecutive_failures = 0

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

        # 每20只保存进度
        if (i + 1) % 20 == 0:
            save_progress(progress)
            print(f"  --- 进度已保存 ({i+1}/{len(todo)}, 成功:{success}, 失败:{failed}) ---", flush=True)

        time.sleep(0.8)  # baostock需要更长间隔避免限流

    save_progress(progress)
    bs.logout()
    
    print(f"\n{'='*60}", flush=True)
    print(f"完成!", flush=True)
    print(f"成功: {success}", flush=True)
    print(f"失败: {failed}", flush=True)
    print(f"跳过北交所: {skip_bj}", flush=True)
    print(f"总更新行数: {total_updated}", flush=True)
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)


if __name__ == '__main__':
    main()
