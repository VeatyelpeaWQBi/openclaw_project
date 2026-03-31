#!/usr/bin/env python3
"""
沪深300 + 中证1000成分股 5分钟线历史数据下载（下午时段）
时间窗口：14:50 ~ 14:55
复权方式：前复权
数据来源：baostock + efinance
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

import baostock as bs
import pandas as pd
import efinance as ef
import os
import json
import time
from datetime import datetime

DATA_DIR = "***REMOVED***/tail_trading/minute_data"
PROGRESS_FILE = "***REMOVED***/tail_trading/download_afternoon_progress.json"
RESULT_FILE = "***REMOVED***/tail_trading/download_afternoon_result.json"
LOG_FILE = "***REMOVED***/tail_trading/download_afternoon_data.log"

START_DATE = "2020-01-01"
END_DATE = "2026-03-30"


def log(msg):
    """写入日志文件并打印"""
    print(msg)
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(msg + "\n")
            f.flush()
    except:
        pass


def get_constituent_stocks():
    """获取沪深300 + 中证1000成分股列表"""
    log("=" * 60)
    log("获取成分股列表...")
    
    # 沪深300 from baostock
    lg = bs.login()
    log(f"baostock登录: {lg.error_code} {lg.error_msg}")
    
    rs = bs.query_hs300_stocks()
    hs300_data = []
    while rs.next():
        hs300_data.append(rs.get_row_data())
    hs300 = pd.DataFrame(hs300_data, columns=rs.fields)
    log(f"沪深300: {len(hs300)} 只")
    
    bs.logout()
    
    # 中证1000 from efinance
    zz1000 = ef.stock.get_members('000852')
    log(f"中证1000: {len(zz1000)} 只")
    
    # 合并去重
    hs300_codes = set(hs300['code'].tolist())
    zz1000_codes_raw = set(zz1000['股票代码'].tolist())
    
    zz1000_codes = set()
    for code in zz1000_codes_raw:
        if code.startswith('6'):
            zz1000_codes.add(f"sh.{code}")
        else:
            zz1000_codes.add(f"sz.{code}")
    
    all_codes = hs300_codes | zz1000_codes
    log(f"去重后总计: {len(all_codes)} 只")
    
    # 构建代码->名称映射
    code_name_map = {}
    for _, row in hs300.iterrows():
        code_name_map[row['code']] = row['code_name']
    
    for _, row in zz1000.iterrows():
        bs_code = f"sh.{row['股票代码']}" if row['股票代码'].startswith('6') else f"sz.{row['股票代码']}"
        if bs_code not in code_name_map:
            code_name_map[bs_code] = row['股票名称']
    
    return sorted(all_codes), code_name_map


def load_progress():
    """加载下载进度"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {"completed": [], "failed": [], "last_index": 0}


def save_progress(progress):
    """保存下载进度"""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def download_stock_data(code, name):
    """
    下载单只股票的5分钟线数据
    筛选14:50~14:55时段，按年月保存CSV
    如果文件已存在（早盘数据），则追加下午数据
    """
    start_year = int(START_DATE[:4])
    end_year = int(END_DATE[:4])
    
    total_days = 0
    
    for year in range(start_year, end_year + 1):
        y_start = f"{year}-01-01" if year > start_year else START_DATE
        y_end = f"{year}-12-31" if year < end_year else END_DATE
        
        current = pd.Timestamp(y_start)
        year_end = pd.Timestamp(y_end)
        
        while current <= year_end:
            month_start = current.strftime("%Y-%m-%d")
            month_end_val = (current + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")
            if month_end_val > y_end:
                month_end_val = y_end
            
            try:
                rs = bs.query_history_k_data_plus(
                    code,
                    "date,time,open,high,low,close,volume,amount,adjustflag",
                    start_date=month_start,
                    end_date=month_end_val,
                    frequency="5",
                    adjustflag="1"
                )
                
                if rs.error_code != '0':
                    current = current + pd.DateOffset(months=1)
                    continue
                
                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())
                
                if not data_list:
                    current = current + pd.DateOffset(months=1)
                    continue
                
                df = pd.DataFrame(data_list, columns=rs.fields)
                
                # 筛选14:50~14:55时段
                # baostock time格式: "20240102145000000"，前8位是日期，第8-12位是HHMM
                df['hour_min'] = df['time'].astype(str).str[8:12].astype(int)
                filtered = df[(df['hour_min'] >= 1450) & (df['hour_min'] <= 1455)].copy()
                
                if len(filtered) == 0:
                    current = current + pd.DateOffset(months=1)
                    continue
                
                # 按日保存
                filtered.loc[:, 'date_only'] = filtered['time'].astype(str).str[:8]
                
                year_month = current.strftime("%Y-%m")
                stock_dir = os.path.join(DATA_DIR, year_month, f"{code.replace('.', '_')}_{name}")
                os.makedirs(stock_dir, exist_ok=True)
                
                for date_str, group in filtered.groupby('date_only'):
                    file_path = os.path.join(stock_dir, f"{code.replace('.', '_')}_{name}_{date_str}.csv")
                    group_out = group[['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adjustflag']].copy()
                    
                    # 如果文件已存在（早盘数据），追加下午数据
                    if os.path.exists(file_path):
                        try:
                            existing = pd.read_csv(file_path)
                            combined = pd.concat([existing, group_out]).drop_duplicates(subset=['time'])
                            combined.to_csv(file_path, index=False, encoding='utf-8')
                        except Exception as e:
                            log(f"    ⚠️ 合并文件失败 {file_path}: {e}")
                            group_out.to_csv(file_path, index=False, encoding='utf-8')
                    else:
                        group_out.to_csv(file_path, index=False, encoding='utf-8')
                    
                    total_days += 1
                
            except Exception as e:
                log(f"  ⚠️ {code} {month_start}: {e}")
            
            current = current + pd.DateOffset(months=1)
            time.sleep(0.2)
    
    return total_days


def main():
    # 清空日志
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        f.write("")
    
    log("=" * 60)
    log("沪深300 + 中证1000 成分股 5分钟线数据下载（下午时段）")
    log(f"时间范围: {START_DATE} ~ {END_DATE}")
    log(f"时段筛选: 14:50 ~ 14:55")
    log(f"复权方式: 前复权")
    log(f"数据目录: {DATA_DIR}")
    log("=" * 60)
    
    # 获取成分股
    all_codes, code_name_map = get_constituent_stocks()
    
    # 加载进度
    progress = load_progress()
    completed = set(progress.get("completed", []))
    
    # 过滤已完成的
    remaining = [c for c in all_codes if c not in completed]
    log(f"\n总计: {len(all_codes)} 只, 已完成: {len(completed)} 只, 待下载: {len(remaining)} 只")
    
    # 登录（只登录一次）
    lg = bs.login()
    log(f"baostock登录: {lg.error_code} {lg.error_msg}")
    
    failed_list = []
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for i, code in enumerate(remaining):
        name = code_name_map.get(code, "")
        idx = len(completed) + i + 1
        log(f"\n[{idx}/{len(all_codes)}] {code} {name}")
        
        try:
            days = download_stock_data(code, name)
            log(f"  ✅ 完成: {days} 天数据")
            completed.add(code)
            progress["completed"] = list(completed)
            progress["last_index"] = idx
        except Exception as e:
            log(f"  ❌ 失败: {e}")
            failed_list.append({"code": code, "name": name, "error": str(e)})
            progress.setdefault("failed", []).append(code)
        
        # 每10只保存进度
        if idx % 10 == 0:
            save_progress(progress)
            log(f"  💾 进度已保存 ({idx}/{len(all_codes)})")
    
    bs.logout()
    
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    result = {
        "total": len(all_codes),
        "completed": len(completed),
        "failed_count": len(failed_list),
        "failed": failed_list,
        "start_time": start_time,
        "end_time": end_time
    }
    
    save_progress(progress)
    with open(RESULT_FILE, 'w') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    log("\n" + "=" * 60)
    log(f"下载完成!")
    log(f"成功: {len(completed)}/{len(all_codes)}")
    log(f"失败: {len(failed_list)}")
    log(f"结果: {RESULT_FILE}")
    log("=" * 60)


if __name__ == "__main__":
    main()
