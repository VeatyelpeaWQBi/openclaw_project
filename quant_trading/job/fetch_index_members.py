#!/usr/bin/env python3
"""
Job: 下载指数成分股并导入DB
通过中证指数官网API获取指定指数的成分股列表

用法：
  python3 job/fetch_index_members.py 000510        # 下载中证A500成分股
  python3 job/fetch_index_members.py 000300 000852  # 下载多个指数

数据源：ak.index_stock_cons_csindex（中证指数有限公司）
下载路径：DATA/attach/{code}cons.xls
导入表：index_members
"""

import sys
import os
import time
import logging
import sqlite3
import shutil
from datetime import datetime

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.paths import DB_PATH

# 下载目录
DATA_DIR = os.path.join(os.path.dirname(_project_root), 'DATA')
ATTACH_DIR = os.path.join(DATA_DIR, 'attach')
os.makedirs(ATTACH_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def download_xls(code):
    """
    下载指数成分股XLS文件

    参数:
        code: 指数代码

    返回:
        str: XLS文件路径，失败返回 None
    """
    import akshare as ak
    from io import BytesIO

    logger.info(f"[{code}] 下载成分股XLS...")

    try:
        # akshare内部下载XLS到内存，我们同时也保存到本地
        url = f"https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/{code}cons.xls"
        import requests
        r = requests.get(url, timeout=30)

        if r.status_code != 200:
            logger.error(f"[{code}] 下载失败: HTTP {r.status_code}")
            return None

        # 保存到本地
        xls_path = os.path.join(ATTACH_DIR, f'{code}cons.xls')
        with open(xls_path, 'wb') as f:
            f.write(r.content)

        logger.info(f"[{code}] XLS已保存: {xls_path} ({len(r.content)/1024:.1f}KB)")
        return xls_path

    except Exception as e:
        logger.error(f"[{code}] 下载异常: {e}")
        return None


def import_to_db(code, xls_path=None):
    """
    读取XLS并导入 index_members 表

    参数:
        code: 指数代码
        xls_path: XLS文件路径（None则调用akshare直接获取）

    返回:
        int: 导入的记录数
    """
    import akshare as ak

    logger.info(f"[{code}] 读取成分股数据...")

    try:
        if xls_path and os.path.exists(xls_path):
            # 从本地XLS读取
            import pandas as pd
            df = pd.read_excel(xls_path)
            df.columns = [
                "日期", "指数代码", "指数名称", "指数英文名称",
                "成分券代码", "成分券名称", "成分券英文名称",
                "交易所", "交易所英文名称",
            ]
            df["成分券代码"] = df["成分券代码"].astype(str).str.zfill(6)
        else:
            # 直接调用akshare
            df = ak.index_stock_cons_csindex(symbol=code)

        if df.empty:
            logger.warning(f"[{code}] 成分股数据为空")
            return 0

        # 获取快照日期
        snapshot_date = str(df.iloc[0].get('日期', datetime.now().strftime('%Y-%m-%d')))[:10]
        index_name = str(df.iloc[0].get('指数名称', ''))

        # 写入DB
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 先删除该指数该日期的旧数据
        cursor.execute(
            "DELETE FROM index_members WHERE index_code = ? AND snapshot_date = ?",
            (code, snapshot_date)
        )

        success = 0
        for _, row in df.iterrows():
            stock_code = str(row.get('成分券代码', '')).zfill(6)
            stock_name = str(row.get('成分券名称', '')).strip()

            if not stock_code or stock_code == 'nan':
                continue

            cursor.execute("""
                INSERT OR IGNORE INTO index_members
                (index_code, stock_code, stock_name, snapshot_date, created_at)
                VALUES (?, ?, ?, ?, datetime('now'))
            """, (code, stock_code, stock_name, snapshot_date))
            success += 1

        # 更新 index_info 的 constituent_count
        cursor.execute("""
            UPDATE index_info SET constituent_count = ?, last_update_at = datetime('now')
            WHERE code = ?
        """, (success, code))

        conn.commit()
        conn.close()

        logger.info(f"[{code}] {index_name}: {success}只成分股已导入 (快照日期: {snapshot_date})")
        return success

    except Exception as e:
        logger.error(f"[{code}] 导入异常: {e}")
        return 0


def run(code):
    """单个指数完整流程"""
    logger.info(f"=== 下载并导入 {code} 成分股 ===")

    # Step 1: 下载XLS
    xls_path = download_xls(code)
    if not xls_path:
        logger.error(f"[{code}] 下载失败，跳过")
        return 0

    # Step 2: 导入DB
    count = import_to_db(code, xls_path)

    return count


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python3 fetch_index_members.py <指数代码> [指数代码2] ...")
        print("示例: python3 fetch_index_members.py 000510")
        sys.exit(1)

    total = 0
    for code in sys.argv[1:]:
        count = run(code)
        total += count
        if len(sys.argv) > 2:
            time.sleep(1)  # 多个指数间隔1秒

    logger.info(f"=== 全部完成: 共导入 {total} 条 ===")
