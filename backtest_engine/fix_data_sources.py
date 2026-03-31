#!/usr/bin/env python3
"""
数据补充脚本 - 补充换手率、计算量比、下载指数数据

功能1: 用东方财富API补充daily_data中的换手率和成交额数据
功能2: 本地计算量比(5日)并写入CSV
功能3: 下载沪深300和中证1000指数历史K线
"""

import os
import csv
import time
import json
import requests
import re
from datetime import datetime
from pathlib import Path

# ============ 配置 ============
BASE_DIR = Path(__file__).parent
DAILY_DATA_DIR = BASE_DIR / "daily_data"
INDEX_HISTORY_DIR = BASE_DIR / "index_history"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'http://quote.eastmoney.com/'
}

# 东方财富K线API
KLINE_URL = "http://push2his.eastmoney.com/api/qt/stock/kline/get"

# 字段映射: f51=日期, f52=开盘, f53=收盘, f54=最高, f55=最低,
#           f56=成交量, f57=成交额, f58=振幅, f59=涨跌幅, f60=涨跌额, f61=换手率
KLINE_FIELDS = 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61'

# 指数代码配置
INDEX_CONFIG = {
    'hs300': {'code': '000300', 'market': 1, 'name': '沪深300'},
    'zz1000': {'code': '000852', 'market': 1, 'name': '中证1000'},
}


def get_market(stock_code: str) -> int:
    """
    根据股票代码判断市场
    返回: 0=深圳, 1=上海
    """
    code = stock_code.lstrip('0') or '0'
    prefix2 = stock_code[:2]
    prefix3 = stock_code[:3]

    # 上海: 60x, 68x, 900(B股)
    if prefix2 in ('60', '68', '90'):
        return 1
    # 深圳: 00x, 30x, 200(B股)
    if prefix2 in ('00', '30', '20'):
        return 0
    # 科创板 688xxx
    if stock_code.startswith('688') or stock_code.startswith('689'):
        return 1
    # 创业板 300xxx, 301xxx
    if prefix3 in ('300', '301'):
        return 0
    # 北交所 83x, 87x -> 暂不处理
    if prefix2 in ('83', '87', '43'):
        return -1  # 北交所
    # 默认根据首位判断
    if stock_code[0] == '6':
        return 1
    return 0


def fetch_kline(secid: str, begin: str = "20200101", end: str = "20260401") -> list:
    """
    从东方财富获取K线数据

    参数:
        secid: market.code 格式，如 "0.000001" 或 "1.000300"
        begin: 开始日期 YYYYMMDD
        end: 结束日期 YYYYMMDD

    返回:
        list of dict: [{date, open, close, high, low, volume, amount, turnover}, ...]
    """
    params = {
        'secid': secid,
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': KLINE_FIELDS,
        'klt': '101',  # 日K
        'fqt': '1',    # 前复权
        'beg': begin,
        'end': end,
        'lmt': '5000',
    }

    try:
        resp = requests.get(KLINE_URL, params=params, headers=HEADERS, timeout=15)
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
                'close': parts[2],  # 注意: API返回的是 开盘,收盘,最高,最低
                'high': parts[3],
                'low': parts[4],
                'volume': parts[5],
                'amount': parts[6],
                'turnover': parts[10],  # 换手率在第11个字段
            })
        return result
    except Exception as e:
        print(f"  获取 {secid} 失败: {e}")
        return []


def parse_filename(filename: str):
    """
    从文件名解析股票代码和名称
    格式: 000001_平安银行.csv -> code=000001, name=平安银行
    """
    name = filename.replace('.csv', '')
    parts = name.split('_', 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], ''


# ============ 功能1: 补充换手率和成交额 ============

def fix_turnover():
    """
    对换手率为0的股票，从东方财富API补充换手率和成交额数据
    """
    print("\n" + "="*60)
    print("功能1: 补充换手率和成交额数据")
    print("="*60)

    csv_files = sorted([f for f in os.listdir(DAILY_DATA_DIR) if f.endswith('.csv')])
    total = len(csv_files)
    need_fix = []
    already_ok = 0

    # 找出需要修复的文件
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
        else:
            already_ok += 1

    print(f"总股票数: {total}")
    print(f"已有换手率: {already_ok}")
    print(f"需要补充: {len(need_fix)}")

    if not need_fix:
        print("无需补充!")
        return

    success = 0
    failed = 0

    for i, filename in enumerate(need_fix):
        stock_code, stock_name = parse_filename(filename)
        market = get_market(stock_code)

        if market < 0:
            print(f"  [{i+1}/{len(need_fix)}] 跳过北交所: {stock_code} {stock_name}")
            continue

        secid = f"{market}.{stock_code}"
        print(f"  [{i+1}/{len(need_fix)}] 获取 {stock_code} {stock_name} (secid={secid})...", end="")

        klines = fetch_kline(secid)

        if not klines:
            print(" 无数据")
            failed += 1
            continue

        # 读取原CSV
        filepath = DAILY_DATA_DIR / filename
        with open(filepath, 'r', encoding='utf-8') as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)
            fieldnames = reader.fieldnames

        # 建立日期->kline数据的映射
        kline_map = {k['date']: k for k in klines}

        updated = 0
        for row in rows:
            date = row['日期']
            if date in kline_map:
                k = kline_map[date]
                row['换手率'] = k['turnover']
                row['成交额'] = k['amount']
                updated += 1

        # 写回CSV
        with open(filepath, 'w', encoding='utf-8', newline='') as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        print(f" 更新{updated}行")
        success += 1

        # 控制请求频率，避免被封
        if i % 10 == 9:
            time.sleep(1)
        else:
            time.sleep(0.2)

    print(f"\n完成! 成功: {success}, 失败: {failed}")


# ============ 功能2: 计算量比 ============

def calc_volume_ratio():
    """
    量比 = 当日成交量 / 前5日平均成交量
    将量比写入CSV（作为新列追加到末尾）
    """
    print("\n" + "="*60)
    print("功能2: 计算量比(5日)")
    print("="*60)

    csv_files = sorted([f for f in os.listdir(DAILY_DATA_DIR) if f.endswith('.csv')])
    N = 5  # 5日均量
    success = 0

    for i, filename in enumerate(csv_files):
        filepath = DAILY_DATA_DIR / filename

        with open(filepath, 'r', encoding='utf-8') as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)
            fieldnames = list(reader.fieldnames)

        if not rows:
            continue

        # 检查是否已有量比列
        if '量比' not in fieldnames:
            fieldnames.append('量比')

        # 计算量比
        for j, row in enumerate(rows):
            vol = float(row.get('成交量', 0) or 0)
            if j < N:
                # 前N天数据不足，无法计算
                row['量比'] = ''
            else:
                prev_vols = [float(rows[k].get('成交量', 0) or 0) for k in range(j-N, j)]
                avg_vol = sum(prev_vols) / N
                if avg_vol > 0:
                    row['量比'] = f"{vol / avg_vol:.4f}"
                else:
                    row['量比'] = ''

        # 写回
        with open(filepath, 'w', encoding='utf-8', newline='') as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        success += 1
        if (i + 1) % 200 == 0:
            print(f"  已处理 {i+1}/{len(csv_files)}")

    print(f"完成! 处理了 {success} 个文件")


# ============ 功能3: 下载指数历史K线 ============

def download_index_data():
    """
    下载沪深300和中证1000的2020-2026年日K数据
    保存到 index_history/ 目录
    """
    print("\n" + "="*60)
    print("功能3: 下载指数历史K线数据")
    print("="*60)

    INDEX_HISTORY_DIR.mkdir(exist_ok=True)

    for key, config in INDEX_CONFIG.items():
        code = config['code']
        market = config['market']
        name = config['name']
        secid = f"{market}.{code}"

        print(f"\n正在下载 {name} ({code}) 数据...")

        klines = fetch_kline(secid, begin="20200101", end="20260401")

        if not klines:
            print(f"  {name}: 无数据!")
            continue

        # 保存为CSV
        output_file = INDEX_HISTORY_DIR / f"{key}_kline.csv"
        with open(output_file, 'w', encoding='utf-8', newline='') as fh:
            writer = csv.writer(fh)
            writer.writerow(['日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额'])
            for k in klines:
                writer.writerow([
                    k['date'],
                    k['open'],
                    k['high'],
                    k['low'],
                    k['close'],
                    k['volume'],
                    k['amount'],
                ])

        print(f"  {name}: 保存 {len(klines)} 条数据 -> {output_file}")

        # 显示最新和最旧的数据
        print(f"  最早: {klines[0]['date']} 收盘={klines[0]['close']}")
        print(f"  最新: {klines[-1]['date']} 收盘={klines[-1]['close']}")


# ============ 主程序 ============

def main():
    print("=" * 60)
    print("  数据补充脚本 - 虾虾子出品")
    print(f"  运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 确认依赖
    try:
        import requests
    except ImportError:
        print("错误: 需要安装 requests 库")
        print("运行: pip install requests")
        return

    # 功能1: 补充换手率
    fix_turnover()

    # 功能2: 计算量比
    calc_volume_ratio()

    # 功能3: 下载指数数据
    download_index_data()

    print("\n" + "=" * 60)
    print("  全部完成!")
    print("=" * 60)


if __name__ == '__main__':
    main()
