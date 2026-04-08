#!/usr/bin/env python3
"""
Job: 获取全A股指数元数据
通过中证指数官网API批量获取所有A股指数的概要信息

数据源：ak.index_csindex_all（中证指数有限公司，最全最准）
"""

import sys
import os
import time
import logging
import sqlite3
import csv

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.paths import DB_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# 输出目录
OUTPUT_DIR = os.path.join(_project_root, 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)


def fetch_index_list():
    """从中证指数官网获取全量指数列表"""
    import akshare as ak

    logger.info("获取中证指数全量列表（index_csindex_all）...")
    start = time.time()

    try:
        df = ak.index_csindex_all()
        elapsed = time.time() - start
        logger.info(f"获取完成: {len(df)} 个指数, 耗时 {elapsed:.1f}秒")
        return df
    except Exception as e:
        logger.error(f"获取指数列表失败: {e}")
        return None


def export_csv(df):
    """导出为CSV文件供详细研究"""
    csv_path = os.path.join(OUTPUT_DIR, 'index_csindex_all.csv')
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"CSV已导出: {csv_path} ({len(df)}行 {len(df.columns)}列)")
    return csv_path


def save_to_db(df):
    """写入 index_info 表"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 清空旧数据
    cursor.execute('DELETE FROM index_info')

    success = 0
    skipped = 0

    for _, row in df.iterrows():
        code = str(row.get('指数代码', '')).strip()
        name = str(row.get('指数全称', '')).strip()
        short_name = str(row.get('指数简称', '')).strip()

        if not code:
            skipped += 1
            continue

        # 推断类型
        idx_type = _infer_index_type(code)

        # 解析日期
        base_date = str(row.get('基日', '')).strip() if row.get('基日') else None
        publish_date = str(row.get('发布时间', '')).strip() if row.get('发布时间') else None

        # 样本数
        constituent_count = None
        sc = row.get('样本数量')
        if sc and str(sc) != 'nan':
            try:
                constituent_count = int(float(sc))
            except:
                pass

        cursor.execute("""
            INSERT INTO index_info
            (code, name, short_name, type, constituent_count, base_date, publish_date,
             daily_kline_done, created_at, last_update_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, datetime('now'), datetime('now'))
        """, (code, name, short_name, idx_type, constituent_count, base_date, publish_date))
        success += 1

    conn.commit()
    conn.close()

    logger.info(f"写入完成: {success}条, 跳过{skipped}条")
    return success


def _infer_index_type(code):
    """根据代码推断指数类型"""
    if code.startswith('000'):
        if code <= '000099':
            return 'exchange'      # 上交所官方
        else:
            return 'csindex'       # 中证系列
    elif code.startswith('399'):
        if code <= '399099':
            return 'exchange'      # 深交所官方
        else:
            return 'cnindex'       # 深证信息
    elif code.startswith('93') or code.startswith('99'):
        return 'csindex'           # 中证系列
    elif code.startswith('899'):
        return 'exchange'          # 北交所
    elif code.startswith('H') or code.startswith('L'):
        return 'csindex'           # 中证系列
    else:
        return 'custom'


def run():
    """主入口"""
    logger.info("=== 获取全A股指数元数据 ===")

    # Step 1: 获取
    df = fetch_index_list()
    if df is None or df.empty:
        logger.warning("获取数据为空，跳过")
        return

    # Step 2: 导出CSV
    csv_path = export_csv(df)

    # Step 3: 写入DB
    count = save_to_db(df)

    logger.info(f"=== 完成: {count} 个指数已同步 ===")


if __name__ == '__main__':
    run()
