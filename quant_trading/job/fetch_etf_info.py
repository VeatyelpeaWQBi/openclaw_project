#!/usr/bin/env python3
"""
Job: 获取ETF基金列表信息
通过新浪API批量获取所有ETF的概要信息

数据源：ak.fund_etf_category_sina（新浪）
"""

import sys
import os
import time
import logging
import sqlite3

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from core.paths import DB_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# 输出目录
OUTPUT_DIR = os.path.join(_project_root, 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)


def fetch_etf_list():
    """从新浪获取全量ETF列表"""
    import akshare as ak

    logger.info("获取ETF分类列表（fund_etf_category_sina）...")
    start = time.time()

    try:
        # 新浪接口返回ETF分类列表，包含代码和名称
        df = ak.fund_etf_category_sina(symbol="ETF基金")
        elapsed = time.time() - start
        logger.info(f"获取完成: {len(df)} 个ETF, 耗时 {elapsed:.1f}秒")
        return df
    except Exception as e:
        logger.error(f"获取ETF列表失败: {e}")
        return None


def export_csv(df):
    """导出为CSV文件供详细研究"""
    csv_path = os.path.join(OUTPUT_DIR, 'etf_list.csv')
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"CSV已导出: {csv_path} ({len(df)}行 {len(df.columns)}列)")
    return csv_path


def infer_etf_type(code, name):
    """根据代码和名称推断ETF类型"""
    code_str = str(code).strip()
    name_str = str(name).strip() if name else ''

    # 代码规则推断
    if code_str.startswith('51'):
        return '指数'  # 上交所指数ETF
    elif code_str.startswith('159'):
        return '指数'  # 深交所指数ETF
    elif code_str.startswith('56'):
        return '跨境'  # 跨境ETF
    elif code_str.startswith('58'):
        return '跨境'  # 跨境ETF
    elif code_str.startswith('50'):
        return '指数'  # 上交所50系列

    # 名称规则推断
    if '债券' in name_str or '债' in name_str:
        return '债券'
    elif '货币' in name_str or '现金' in name_str or '理财' in name_str:
        return '货币'
    elif '黄金' in name_str or '白银' in name_str or '商品' in name_str:
        return '商品'
    elif 'QDII' in name_str or '纳指' in name_str or '标普' in name_str or '恒生' in name_str:
        return '跨境'

    return '指数'


def infer_track_index(code, name):
    """根据代码和名称推断跟踪指数"""
    name_str = str(name).strip() if name else ''

    # 常见ETF与指数映射
    mapping = {
        '300ETF': '000300',
        '500ETF': '000905',
        '50ETF': '000016',
        '1000ETF': '000852',
        '沪深300': '000300',
        '中证500': '000905',
        '上证50': '000016',
        '中证1000': '000852',
        '创业板': '399006',
        '科创50': '000688',
        '深证100': '399330',
        '纳斯达克': 'NDAQ',
        '标普500': 'SPX',
        '恒生': 'HSI',
        '红利': '000016',  # 多种红利指数
        '医疗': '399989',
        '医药': '399989',
        '消费': '399932',
        '科技': '399971',
        '新能源': '399808',
        '芯片': '980001',
        '半导体': '980001',
        '证券': '399975',
        '银行': '399801',
        '地产': '399948',
        '军工': '399959',
        '酒': '399997',
        '白酒': '399997',
    }

    for keyword, idx_code in mapping.items():
        if keyword in name_str:
            return idx_code

    return None


def save_to_db(df):
    """写入 etf_info 表"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 清空旧数据
    cursor.execute('DELETE FROM etf_info')

    success = 0
    skipped = 0

    for _, row in df.iterrows():
        # 新浪接口字段名
        code = str(row.get('代码', row.get('code', ''))).strip()
        name = str(row.get('名称', row.get('name', ''))).strip()

        if not code:
            skipped += 1
            continue

        # 推断类型和跟踪指数
        etf_type = infer_etf_type(code, name)
        track_index = infer_track_index(code, name)

        cursor.execute("""
            INSERT INTO etf_info
            (code, name, etf_type, track_index, track_index_name,
             daily_kline_done, created_at, last_update_at)
            VALUES (?, ?, ?, ?, ?, 0, datetime('now'), datetime('now'))
        """, (code, name, etf_type, track_index, None))
        success += 1

    conn.commit()
    conn.close()

    logger.info(f"写入完成: {success}条, 跳过{skipped}条")
    return success


def run():
    """主入口"""
    logger.info("=== 获取ETF基金列表信息 ===")

    # Step 1: 获取
    df = fetch_etf_list()
    if df is None or df.empty:
        logger.warning("获取数据为空，跳过")
        return

    # Step 2: 导出CSV
    csv_path = export_csv(df)

    # Step 3: 写入DB
    count = save_to_db(df)

    logger.info(f"=== 完成: {count} 个ETF已同步 ===")


if __name__ == '__main__':
    run()