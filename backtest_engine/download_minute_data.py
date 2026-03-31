#!/usr/bin/env python3
"""
下载沪深300+中证1000成分股的5分钟线数据（2020-01-01到2026-03-30）
包含早盘09:30~10:15和尾盘14:50~14:55
使用baostock作为数据源
"""

import json
import os
import sys
import time
import traceback
from datetime import datetime

import baostock as bs
import efinance as ef
import pandas as pd

DATA_DIR = "***REMOVED***/tail_trading/minute_data"
PROGRESS_FILE = "***REMOVED***/tail_trading/download_progress.json"
RESULT_FILE = "***REMOVED***/tail_trading/download_result.json"
STOCK_LIST_FILE = "***REMOVED***/tail_trading/stock_list.json"

START_DATE = "2020-01-01"
END_DATE = "2026-03-30"

# 需要筛选的时间段
MORNING_START = "09:30"
MORNING_END = "10:15"
AFTERNOON_START = "14:50"
AFTERNOON_END = "14:55"


def save_progress(current_idx, total, code, name, status="downloading"):
    """实时保存进度"""
    progress = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "current_index": current_idx,
        "total": total,
        "current_stock": f"{code}_{name}",
        "status": status,
        "percentage": round(current_idx / total * 100, 2) if total > 0 else 0
    }
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def get_stock_list():
    """获取完整的成分股列表"""
    if os.path.exists(STOCK_LIST_FILE):
        print("从缓存加载股票列表...")
        with open(STOCK_LIST_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            stocks = [tuple(s) for s in data['stocks']]
            print(f"缓存中有 {len(stocks)} 只股票")
            return stocks

    print("获取沪深300历史成分股...")
    lg = bs.login()
    print(f"baostock login: {lg.error_code} {lg.error_msg}")

    hs300_all = set()
    for year in range(2020, 2027):
        date_str = f"{year}-01-01"
        rs = bs.query_hs300_stocks(date=date_str)
        count = 0
        while (rs.error_code == '0') & rs.next():
            row = rs.get_row_data()
            hs300_all.add((row[1], row[2]))  # (code, name)
            count += 1
        print(f"  {date_str}: {count} 只沪深300成分股")

    bs.logout()
    print(f"沪深300累计（去重）: {len(hs300_all)} 只")

    print("获取中证1000当前成分股...")
    zz1000 = ef.stock.get_members('000852')
    zz1000_set = set()
    for _, row in zz1000.iterrows():
        code = str(row['股票代码']).zfill(6)
        name = row['股票名称']
        zz1000_set.add((code, name))
    print(f"中证1000: {len(zz1000_set)} 只")

    all_stocks = list(hs300_all | zz1000_set)
    all_stocks.sort(key=lambda x: x[0])
    print(f"合并去重后总计: {len(all_stocks)} 只股票")

    with open(STOCK_LIST_FILE, 'w', encoding='utf-8') as f:
        json.dump({"stocks": all_stocks, "count": len(all_stocks)},
                  f, ensure_ascii=False, indent=2)

    return all_stocks


def code_to_bs(code):
    """将6位代码转为baostock格式 sh.000001 / sz.000001"""
    if code.startswith('6') or code.startswith('9'):
        return f"sh.{code}"
    else:
        return f"sz.{code}"


def filter_time_window(df):
    """筛选早盘09:30~10:15和尾盘14:50~14:55的数据"""
    # baostock的time字段格式: 20240102093500000
    # 解析为datetime
    def parse_bs_time(t_str):
        # t_str like '20240102093500000'
        date_part = t_str[:8]
        hour = int(t_str[8:10])
        minute = int(t_str[10:12])
        return date_part, hour, minute

    filtered_rows = []
    for _, row in df.iterrows():
        t_str = str(row['time'])
        date_part, hour, minute = parse_bs_time(t_str)
        time_minutes = hour * 60 + minute

        # 早盘 09:30 (570) ~ 10:15 (615)
        morning_start = 9 * 60 + 30  # 570
        morning_end = 10 * 60 + 15   # 615
        # 尾盘 14:50 (890) ~ 14:55 (895)
        afternoon_start = 14 * 60 + 50  # 890
        afternoon_end = 14 * 60 + 55    # 895

        if (morning_start <= time_minutes <= morning_end) or \
           (afternoon_start <= time_minutes <= afternoon_end):
            # 添加日期和时间列
            row_dict = row.to_dict()
            row_dict['日期'] = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
            row_dict['时刻'] = f"{hour:02d}:{minute:02d}"
            filtered_rows.append(row_dict)

    return filtered_rows


def download_stock(code, name):
    """下载单只股票的5分钟线数据（早盘+尾盘），按年下载"""
    bs_code = code_to_bs(code)
    current_year = pd.Timestamp(START_DATE).year
    end_year = pd.Timestamp(END_DATE).year

    for year in range(current_year, end_year + 1):
        year_start = f"{year}-01-01"
        year_end = f"{year}-12-31"
        if year == 2020:
            year_start = START_DATE
        if year == end_year:
            year_end = END_DATE

        year_str = str(year)
        stock_dir = os.path.join(DATA_DIR, year_str, f"{code}_{name}")
        os.makedirs(stock_dir, exist_ok=True)

        # 检查该年是否已经有数据文件
        existing = [f for f in os.listdir(stock_dir) if f.endswith('.csv')] if os.path.exists(stock_dir) else []
        # 如果文件数接近一年的交易日数(约240天*2个时段的合并文件)，跳过
        # 不做跳过检查，因为需要确认数据完整

        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                'date,time,open,high,low,close,volume,amount,adjustflag',
                start_date=year_start,
                end_date=year_end,
                frequency='5',
                adjustflag='2'  # 前复权
            )

            if rs.error_code != '0':
                print(f"  ⚠️ {code} {year}: baostock error {rs.error_code} {rs.error_msg}")
                continue

            # 读取数据
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())

            if not rows:
                continue

            columns = ['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adjustflag']
            df = pd.DataFrame(rows, columns=columns)

            # 筛选时间段
            filtered = filter_time_window(df)

            if not filtered:
                continue

            # 按日期分组保存
            from collections import defaultdict
            date_groups = defaultdict(list)
            for row in filtered:
                date_groups[row['日期']].append(row)

            for date_str, data_rows in date_groups.items():
                date_compact = date_str.replace('-', '')
                file_path = os.path.join(stock_dir, f"{code}_{name}_{date_compact}.csv")
                out_df = pd.DataFrame(data_rows)
                out_df.to_csv(file_path, index=False, encoding='utf-8')

        except Exception as e:
            print(f"  ⚠️ {code} {year}: {e}")
            traceback.print_exc()

        time.sleep(0.15)  # 控制请求频率


def main():
    print("=" * 60)
    print("开始下载沪深300+中证1000成分股5分钟线数据")
    print(f"时间范围: {START_DATE} ~ {END_DATE}")
    print(f"数据目录: {DATA_DIR}")
    print(f"数据源: baostock (前复权)")
    print("=" * 60)

    # 登录baostock
    lg = bs.login()
    print(f"baostock login: {lg.error_code} {lg.error_msg}")

    try:
        all_stocks = get_stock_list()
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        traceback.print_exc()
        bs.logout()
        return

    total = len(all_stocks)

    # 断点续传：找到已完成的最后一只
    start_idx = 0
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                prev = json.load(f)
                if prev.get("status") == "completed":
                    print("上次已完成全部下载")
                    bs.logout()
                    return
                prev_stock = prev.get("current_stock", "")
                for i, (code, name) in enumerate(all_stocks):
                    if f"{code}_{name}" == prev_stock:
                        start_idx = i
                        break
                if start_idx > 0:
                    print(f"断点续传: 从第 {start_idx + 1} 只开始")
        except:
            pass

    failed_stocks = []
    success_count = 0

    for i in range(start_idx, total):
        code, name = all_stocks[i]
        print(f"[{i + 1}/{total}] {code} {name}", end="", flush=True)
        save_progress(i + 1, total, code, name, "downloading")

        try:
            download_stock(code, name)
            success_count += 1
            print(f" ✅")
        except Exception as e:
            failed_stocks.append({"code": code, "name": name, "error": str(e)})
            print(f" ❌ {e}")

        if (i + 1) % 20 == 0:
            pct = round((i + 1) / total * 100, 1)
            print(f"\n--- 进度: {i + 1}/{total} ({pct}%) | 成功: {success_count} | 失败: {len(failed_stocks)} ---\n")

    bs.logout()

    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_stocks": total,
        "success": success_count,
        "failed": len(failed_stocks),
        "failed_stocks": failed_stocks,
        "data_dir": DATA_DIR,
        "date_range": f"{START_DATE} ~ {END_DATE}",
        "data_source": "baostock"
    }

    with open(RESULT_FILE, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    save_progress(total, total, "", "", "completed")

    print("\n" + "=" * 60)
    print(f"下载完成! 成功: {success_count}, 失败: {len(failed_stocks)}")
    if failed_stocks:
        print("失败的股票:")
        for s in failed_stocks:
            print(f"  {s['code']} {s['name']}: {s['error']}")
    print(f"结果保存到: {RESULT_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()
