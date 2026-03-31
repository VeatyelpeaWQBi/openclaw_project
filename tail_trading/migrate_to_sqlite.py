#!/usr/bin/env python3
"""CSV数据迁移到SQLite数据库"""
import sqlite3
import csv
import os
import sys
import json
import time
import glob
import re
from pathlib import Path
from datetime import datetime

# 路径配置
BASE_DIR = '***REMOVED***/tail_trading'
DB_PATH = os.path.join(BASE_DIR, 'data', 'stock_data.db')
DAILY_DIR = os.path.join(BASE_DIR, 'daily_data')
MINUTE_DIR = os.path.join(BASE_DIR, 'minute_data')
INDEX_DIR = os.path.join(BASE_DIR, 'index_history')
PROGRESS_FILE = os.path.join(BASE_DIR, 'data', 'migration_progress.json')

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)

def create_tables(conn):
    """创建所有表结构"""
    cursor = conn.cursor()
    
    cursor.executescript("""
    CREATE TABLE IF NOT EXISTS daily_kline (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT NOT NULL, name TEXT, date DATE NOT NULL,
        open REAL, high REAL, low REAL, close REAL,
        volume INTEGER, amount REAL, turnover REAL,
        pe_ratio REAL, pb_ratio REAL, ps_ratio REAL, pcf_ratio REAL,
        volume_ratio REAL,
        UNIQUE(code, date)
    );
    CREATE INDEX IF NOT EXISTS idx_daily_code_date ON daily_kline(code, date);
    CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_kline(date);

    CREATE TABLE IF NOT EXISTS minute_kline (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT NOT NULL, name TEXT, date DATE NOT NULL,
        datetime DATETIME NOT NULL,
        open REAL, high REAL, low REAL, close REAL,
        volume INTEGER, amount REAL,
        UNIQUE(code, datetime)
    );
    CREATE INDEX IF NOT EXISTS idx_minute_code_dt ON minute_kline(code, datetime);
    CREATE INDEX IF NOT EXISTS idx_minute_date ON minute_kline(date);
    CREATE INDEX IF NOT EXISTS idx_minute_code_date ON minute_kline(code, date);

    CREATE TABLE IF NOT EXISTS index_kline (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        index_code TEXT NOT NULL, index_name TEXT, date DATE NOT NULL,
        open REAL, high REAL, low REAL, close REAL,
        volume INTEGER, amount REAL,
        UNIQUE(index_code, date)
    );
    CREATE INDEX IF NOT EXISTS idx_index_code_date ON index_kline(index_code, date);

    CREATE TABLE IF NOT EXISTS index_members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        index_code TEXT NOT NULL, stock_code TEXT NOT NULL,
        stock_name TEXT, snapshot_date DATE,
        UNIQUE(index_code, stock_code, snapshot_date)
    );
    CREATE INDEX IF NOT EXISTS idx_members_index ON index_members(index_code);

    CREATE TABLE IF NOT EXISTS stock_sector (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stock_code TEXT NOT NULL, sector_name TEXT NOT NULL,
        sector_type INTEGER, updated_at DATETIME,
        UNIQUE(stock_code, sector_name)
    );
    CREATE INDEX IF NOT EXISTS idx_sector_code ON stock_sector(stock_code);

    CREATE TABLE IF NOT EXISTS fear_greed_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date DATE NOT NULL UNIQUE,
        score REAL, level TEXT,
        f_ma_deviation REAL, f_volume_ratio REAL, f_advance_decline REAL,
        f_limit_ratio REAL, f_amplitude REAL, f_rsi REAL,
        f_high_turnover REAL, f_offense_defense REAL
    );
    CREATE INDEX IF NOT EXISTS idx_fg_date ON fear_greed_history(date);
    """)
    conn.commit()
    print("✅ 表结构创建完成")

def safe_float(val):
    """安全转换为浮点数"""
    if val is None or val == '':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def safe_int(val):
    """安全转换为整数"""
    if val is None or val == '':
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None

def migrate_daily(conn):
    """迁移日K数据"""
    print("\n📊 开始迁移 daily_data...")
    progress = load_progress()
    done_files = set(progress.get('daily_done', []))
    
    csv_files = sorted(glob.glob(os.path.join(DAILY_DIR, '*.csv')))
    total = len(csv_files)
    print(f"  共 {total} 个CSV文件，已完成 {len(done_files)} 个")
    
    cursor = conn.cursor()
    count = 0
    rows_total = 0
    
    for i, filepath in enumerate(csv_files):
        filename = os.path.basename(filepath)
        if filename in done_files:
            continue
        
        # 从文件名提取 code 和 name
        basename = filename.replace('.csv', '')
        parts = basename.split('_', 1)
        if len(parts) != 2:
            print(f"  ⚠️ 跳过异常文件名: {filename}")
            continue
        code, name = parts[0], parts[1]
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)  # 跳过表头
                rows = []
                for row in reader:
                    if len(row) < 13:
                        continue
                    rows.append((
                        code, name, row[0],  # date
                        safe_float(row[1]), safe_float(row[2]), safe_float(row[3]), safe_float(row[4]),  # OHLC
                        safe_int(row[5]), safe_float(row[6]), safe_float(row[7]),  # vol, amt, turnover
                        safe_float(row[8]), safe_float(row[9]), safe_float(row[10]),  # pe, pb, ps
                        safe_float(row[11]), safe_float(row[12])  # pcf, volume_ratio
                    ))
                
                if rows:
                    cursor.executemany(
                        """INSERT OR REPLACE INTO daily_kline 
                           (code, name, date, open, high, low, close, volume, amount, turnover,
                            pe_ratio, pb_ratio, ps_ratio, pcf_ratio, volume_ratio)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        rows
                    )
                    rows_total += len(rows)
            
            done_files.add(filename)
            count += 1
            
            if count % 100 == 0:
                conn.commit()
                progress['daily_done'] = list(done_files)
                save_progress(progress)
                print(f"  进度: {count}/{total} 文件, {rows_total} 行已插入")
        
        except Exception as e:
            print(f"  ❌ 错误 {filename}: {e}")
    
    conn.commit()
    progress['daily_done'] = list(done_files)
    progress['daily_complete'] = True
    save_progress(progress)
    print(f"✅ daily_data 迁移完成: {count} 个文件, {rows_total} 行")
    return rows_total

def migrate_minute(conn):
    """迁移分钟线数据 - 支持断点续传"""
    print("\n📊 开始迁移 minute_data (2.1M文件，预计较长时间)...")
    progress = load_progress()
    done_files = set(progress.get('minute_done', []))
    last_file = progress.get('minute_last_file', '')
    
    # 收集所有csv文件
    csv_files = sorted(glob.glob(os.path.join(MINUTE_DIR, '**', '*.csv'), recursive=True))
    total = len(csv_files)
    print(f"  共 {total} 个CSV文件，已完成 {len(done_files)} 个")
    
    cursor = conn.cursor()
    count = 0
    rows_total = 0
    batch_rows = []
    start_time = time.time()
    
    for i, filepath in enumerate(csv_files):
        if filepath in done_files:
            continue
        
        # 从路径提取信息: minute_data/{YYYY-MM}/{code}_{name}/{code}_{name}_{YYYYMMDD}.csv
        filename = os.path.basename(filepath)
        basename = filename.replace('.csv', '')
        # 文件名格式: {code}_{name}_{YYYYMMDD}
        parts = basename.rsplit('_', 1)
        if len(parts) != 2:
            continue
        code_name, date_str = parts
        code_parts = code_name.split('_', 1)
        if len(code_parts) != 2:
            continue
        code, name = code_parts[0], code_parts[1]
        date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}" if len(date_str) == 8 else date_str
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)  # 跳过表头: 日期,时间,开盘,最高,最低,收盘,成交量,成交额
                for row in reader:
                    if len(row) < 8:
                        continue
                    dt = row[1]  # datetime
                    batch_rows.append((
                        code, name, date, dt,
                        safe_float(row[2]), safe_float(row[3]), safe_float(row[4]), safe_float(row[5]),
                        safe_int(row[6]), safe_float(row[7])
                    ))
            
            done_files.add(filepath)
            count += 1
            
            # 批量插入
            if len(batch_rows) >= 50000:
                cursor.executemany(
                    """INSERT OR IGNORE INTO minute_kline 
                       (code, name, date, datetime, open, high, low, close, volume, amount)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    batch_rows
                )
                rows_total += len(batch_rows)
                conn.commit()
                batch_rows = []
                
                elapsed = time.time() - start_time
                rate = count / elapsed if elapsed > 0 else 0
                remaining = (total - count) / rate if rate > 0 else 0
                print(f"  进度: {count}/{total} ({count/total*100:.1f}%), "
                      f"{rows_total} 行, {rate:.0f} 文件/秒, "
                      f"预计剩余 {remaining/60:.0f} 分钟")
                
                progress['minute_done'] = list(done_files)
                progress['minute_last_file'] = filepath
                progress['minute_rows'] = rows_total
                save_progress(progress)
        
        except Exception as e:
            print(f"  ❌ 错误 {filename}: {e}")
    
    # 插入剩余数据
    if batch_rows:
        cursor.executemany(
            """INSERT OR IGNORE INTO minute_kline 
               (code, name, date, datetime, open, high, low, close, volume, amount)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            batch_rows
        )
        rows_total += len(batch_rows)
    
    conn.commit()
    progress['minute_done'] = list(done_files)
    progress['minute_complete'] = True
    progress['minute_rows'] = rows_total
    save_progress(progress)
    elapsed = time.time() - start_time
    print(f"✅ minute_data 迁移完成: {count} 个文件, {rows_total} 行, 耗时 {elapsed/60:.1f} 分钟")
    return rows_total

def migrate_index(conn):
    """迁移指数数据"""
    print("\n📊 开始迁移指数数据...")
    cursor = conn.cursor()
    
    # 1. 沪深300 K线
    hs300_file = os.path.join(INDEX_DIR, 'hs300_kline.csv')
    if os.path.exists(hs300_file):
        rows = []
        with open(hs300_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # 跳过表头
            for row in reader:
                if len(row) < 7:
                    continue
                rows.append((
                    '000300', '沪深300', row[0],
                    safe_float(row[1]), safe_float(row[2]), safe_float(row[3]), safe_float(row[4]),
                    safe_int(row[5]), safe_float(row[6])
                ))
        if rows:
            cursor.executemany(
                """INSERT OR REPLACE INTO index_kline 
                   (index_code, index_name, date, open, high, low, close, volume, amount)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                rows
            )
            print(f"  ✅ 沪深300 K线: {len(rows)} 行")
    
    # 2. 中证1000 K线
    zz1000_file = os.path.join(INDEX_DIR, 'zz1000_kline.csv')
    if os.path.exists(zz1000_file):
        rows = []
        with open(zz1000_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if len(row) < 7:
                    continue
                rows.append((
                    '000852', '中证1000', row[0],
                    safe_float(row[1]), safe_float(row[2]), safe_float(row[3]), safe_float(row[4]),
                    safe_int(row[5]), safe_float(row[6])
                ))
        if rows:
            cursor.executemany(
                """INSERT OR REPLACE INTO index_kline 
                   (index_code, index_name, date, open, high, low, close, volume, amount)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                rows
            )
            print(f"  ✅ 中证1000 K线: {len(rows)} 行")
    
    # 3. 沪深300成分股快照
    hs300_pattern = os.path.join(INDEX_DIR, 'hs300_*.csv')
    member_files = [f for f in glob.glob(hs300_pattern) if 'kline' not in f]
    total_members = 0
    for mfile in sorted(member_files):
        filename = os.path.basename(mfile)
        # hs300_2020-01-01.csv -> snapshot_date = 2020-01-01
        snapshot_date = filename.replace('hs300_', '').replace('.csv', '')
        rows = []
        with open(mfile, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if len(row) < 3:
                    continue
                code = row[1].replace('sh', '').replace('sz', '')
                rows.append(('000300', code, row[2], snapshot_date))
        if rows:
            cursor.executemany(
                "INSERT OR REPLACE INTO index_members (index_code, stock_code, stock_name, snapshot_date) VALUES (?,?,?,?)",
                rows
            )
            total_members += len(rows)
    print(f"  ✅ 沪深300成分股快照: {len(member_files)} 个快照, {total_members} 条记录")
    
    # 4. 中证1000当前成分股
    zz1000_current = os.path.join(INDEX_DIR, 'zz1000_current.csv')
    if os.path.exists(zz1000_current):
        rows = []
        today = datetime.now().strftime('%Y-%m-%d')
        with open(zz1000_current, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if len(row) < 2:
                    continue
                rows.append(('000852', row[0], row[1], today))
        if rows:
            cursor.executemany(
                "INSERT OR REPLACE INTO index_members (index_code, stock_code, stock_name, snapshot_date) VALUES (?,?,?,?)",
                rows
            )
            print(f"  ✅ 中证1000当前成分股: {len(rows)} 行")
    
    conn.commit()
    print("✅ 指数数据迁移完成")

def verify(conn):
    """验证数据"""
    print("\n🔍 数据验证...")
    cursor = conn.cursor()
    
    # 各表行数统计
    tables = ['daily_kline', 'minute_kline', 'index_kline', 'index_members', 'stock_sector', 'fear_greed_history']
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count:,} 行")
    
    # 随机抽查 daily_kline
    print("\n📋 daily_kline 随机抽查:")
    cursor.execute("SELECT code, name, date, close FROM daily_kline ORDER BY RANDOM() LIMIT 3")
    for row in cursor.fetchall():
        print(f"  {row[0]} {row[1]} {row[2]} 收盘:{row[3]}")
    
    # 数据库文件大小
    if os.path.exists(DB_PATH):
        size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
        print(f"\n💾 数据库文件大小: {size_mb:.1f} MB")
    
    # 数据时间范围
    cursor.execute("SELECT MIN(date), MAX(date), COUNT(DISTINCT code) FROM daily_kline")
    row = cursor.fetchone()
    if row[0]:
        print(f"📅 日K数据范围: {row[0]} ~ {row[1]}, 覆盖 {row[2]} 只股票")

def main():
    # 确保目录存在
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    # 删除旧数据库（如果需要重新开始）
    if '--reset' in sys.argv:
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
            print("🗑️ 已删除旧数据库")
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
            print("🗑️ 已删除进度文件")
    
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
    conn.execute("PRAGMA temp_store=MEMORY")
    
    print(f"📂 数据库: {DB_PATH}")
    print(f"⏰ 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 创建表
    create_tables(conn)
    
    # 执行迁移
    step = sys.argv[1] if len(sys.argv) > 1 else 'all'
    
    if step in ('all', 'daily'):
        migrate_daily(conn)
    if step in ('all', 'index'):
        migrate_index(conn)
    if step in ('all', 'minute'):
        migrate_minute(conn)
    if step in ('all', 'verify'):
        verify(conn)
    
    conn.close()
    print(f"\n⏰ 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == '__main__':
    main()
