#!/usr/bin/env python3
"""
重试修复换手率 - 增强版
增加更长的sleep间隔和重试机制
"""

import os
import csv
import time
import requests
from pathlib import Path

BASE_DIR = Path(__file__).parent
DAILY_DATA_DIR = BASE_DIR / "daily_data"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'http://quote.eastmoney.com/'
}

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


def fetch_kline(secid: str, begin: str = "20200101", end: str = "20260401", retries: int = 3) -> list:
    params = {
        'secid': secid,
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': KLINE_FIELDS,
        'klt': '101',
        'fqt': '1',
        'beg': begin,
        'end': end,
        'lmt': '5000',
    }

    for attempt in range(retries):
        try:
            resp = requests.get(KLINE_URL, params=params, headers=HEADERS, timeout=20)
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
                    'open': parts[1],
                    'close': parts[2],
                    'high': parts[3],
                    'low': parts[4],
                    'volume': parts[5],
                    'amount': parts[6],
                    'turnover': parts[10],
                })
            return result
        except Exception as e:
            if attempt < retries - 1:
                wait = (attempt + 1) * 5  # 5s, 10s, 15s
                print(f"    重试 {attempt+1}/{retries} 等待{wait}s...", end="")
                time.sleep(wait)
            else:
                print(f"  最终失败: {e}")
                return []
    return []


def parse_filename(filename: str):
    name = filename.replace('.csv', '')
    parts = name.split('_', 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], ''


def find_still_need_fix():
    """找出仍然需要修复的文件"""
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


def main():
    print("="*60)
    print("重试修复换手率 - 增强版（长间隔+重试）")
    print("="*60)

    need_fix = find_still_need_fix()
    print(f"仍需修复: {len(need_fix)} 只")

    if not need_fix:
        print("全部完成!")
        return

    success = 0
    failed = 0
    failed_list = []

    for i, filename in enumerate(need_fix):
        stock_code, stock_name = parse_filename(filename)
        market = get_market(stock_code)

        if market < 0:
            print(f"  [{i+1}/{len(need_fix)}] 跳过北交所: {stock_code} {stock_name}")
            continue

        secid = f"{market}.{stock_code}"
        print(f"  [{i+1}/{len(need_fix)}] {stock_code} {stock_name}...", end="")

        klines = fetch_kline(secid)

        if not klines:
            print(" 无数据")
            failed += 1
            failed_list.append(stock_code)
            continue

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

        print(f" 更新{updated}行")
        success += 1

        # 每5只休息2秒，每20只休息5秒
        if (i + 1) % 20 == 0:
            print(f"  --- 已处理{i+1}只，休息5秒 ---")
            time.sleep(5)
        elif (i + 1) % 5 == 0:
            time.sleep(2)
        else:
            time.sleep(0.8)

    print(f"\n{'='*60}")
    print(f"完成! 成功: {success}, 失败: {failed}")
    if failed_list:
        print(f"失败列表: {', '.join(failed_list[:20])}{'...' if len(failed_list)>20 else ''}")


if __name__ == '__main__':
    main()
