#!/usr/bin/env python3
"""
日K数据下载脚本
- 数据源：新浪（akshare）前复权
- 单线程，每分钟最多2个股票
- 支持断点续传
- 每次最多获取10个股票
- 同时获取行业和板块信息
"""

import sqlite3
import akshare as ak
import adata
import time
import os
import logging
from datetime import datetime

# 配置
DB_PATH = '/home/drizztbi/openclaw_project/DATA/stock_data.db'
BATCH_SIZE = 10  # 每次最多获取股票数
MAX_PER_MINUTE = 2  # 每分钟最多请求数
DELAY_BETWEEN_STOCKS = 30  # 每个股票之间的延迟（秒）
START_DATE = '20140101'  # 数据起始日期

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# 禁止代理干扰
for _k in ('HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy'):
    os.environ.pop(_k, None)


def get_pending_stocks(limit=10):
    """获取待下载的股票列表"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 获取去重后的未完成股票
    sql = """
    SELECT DISTINCT stock_code, stock_name 
    FROM index_members 
    WHERE daily_kline_done = 0 OR daily_kline_done IS NULL
    ORDER BY stock_code
    LIMIT ?
    """
    cursor.execute(sql, (limit,))
    stocks = cursor.fetchall()
    
    conn.close()
    return stocks


def download_stock_kline(stock_code, stock_name):
    """下载单个股票的日K数据+行业/板块信息"""
    try:
        # 确定市场前缀
        if stock_code.startswith('6'):
            symbol = f'sh{stock_code}'
        else:
            symbol = f'sz{stock_code}'
        
        logger.info(f'获取 {stock_code} {stock_name} 数据...')
        
        # 使用新浪接口获取完整历史数据（前复权，从2020年开始）
        df = ak.stock_zh_a_daily(symbol=symbol, start_date=START_DATE, adjust='qfq')
        
        # 获取板块信息（行业+概念）
        industry = ''
        concept = ''
        try:
            df_plate = adata.stock.info.get_plate_east(stock_code=stock_code)
            if not df_plate.empty:
                # 行业：所有plate_type='行业'的板块
                industries = df_plate[df_plate['plate_type'] == '行业']['plate_name'].tolist()
                industry = ','.join(industries) if industries else ''
                
                # 概念：所有plate_type='概念'的板块
                concepts = df_plate[df_plate['plate_type'] == '概念']['plate_name'].tolist()
                concept = ','.join(concepts[:5]) if concepts else ''  # 最多取5个概念
        except:
            pass
        
        if df is None or df.empty:
            logger.warning(f'{stock_code} 获取数据为空')
            return 0
        
        # 计算量比（当日成交量 / 过去5日平均成交量）
        df['volume_ratio'] = df['volume'] / df['volume'].rolling(window=5, min_periods=1).mean().shift(1)
        df['volume_ratio'] = df['volume_ratio'].fillna(0).round(2)
        
        # 保存到数据库
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        count = 0
        for _, row in df.iterrows():
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO daily_kline 
                    (code, name, date, open, high, low, close, volume, amount, turnover, volume_ratio)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    stock_code,
                    stock_name,
                    str(row['date']),
                    float(row['open']) if row['open'] else 0,
                    float(row['high']) if row['high'] else 0,
                    float(row['low']) if row['low'] else 0,
                    float(row['close']) if row['close'] else 0,
                    int(row['volume']) if row['volume'] else 0,
                    float(row['amount']) if row['amount'] else 0,
                    float(row['turnover']) if row['turnover'] else 0,
                    float(row['volume_ratio']) if row['volume_ratio'] else 0
                ))
                count += 1
            except Exception as e:
                logger.warning(f'{stock_code} 插入行数据失败: {e}')
        
        # 更新行业和概念信息
        cursor.execute('''
            UPDATE index_members 
            SET daily_kline_done = 1, industry = ?, concept = ?
            WHERE stock_code = ?
        ''', (industry, concept, stock_code))
        conn.commit()
        conn.close()
        
        logger.info(f'{stock_code} {stock_name}: {count}条日K, 行业={industry}, 概念={concept}')
        return count
        
    except Exception as e:
        logger.error(f'{stock_code} {stock_name} 获取失败: {type(e).__name__}: {e}')
        return 0


def main():
    start_time = time.time()
    
    logger.info('='*60)
    logger.info('日K数据下载脚本启动')
    logger.info(f'配置: 每次最多{BATCH_SIZE}只股票，每分钟最多{MAX_PER_MINUTE}个请求')
    logger.info('='*60)
    
    # 获取待下载股票
    stocks = get_pending_stocks(limit=BATCH_SIZE)
    
    if not stocks:
        logger.info('没有待下载的股票，脚本退出')
        return
    
    logger.info(f'本次将下载 {len(stocks)} 只股票')
    
    # 下载数据
    total_records = 0
    success_count = 0
    
    for i, (code, name) in enumerate(stocks):
        # 控制请求频率
        if i > 0 and i % MAX_PER_MINUTE == 0:
            logger.info(f'已处理 {i} 只股票，等待 {DELAY_BETWEEN_STOCKS} 秒...')
            time.sleep(DELAY_BETWEEN_STOCKS)
        elif i > 0:
            time.sleep(5)  # 股票间短延迟
        
        records = download_stock_kline(code, name)
        if records > 0:
            success_count += 1
            total_records += records
    
    # 统计结果
    elapsed = time.time() - start_time
    elapsed_str = f'{int(elapsed//60)}分{int(elapsed%60)}秒'
    
    logger.info('='*60)
    logger.info('下载完成统计')
    logger.info(f'  成功下载: {success_count}/{len(stocks)} 只股票')
    logger.info(f'  总记录数: {total_records} 条')
    logger.info(f'  运行时长: {elapsed_str}')
    logger.info('='*60)


if __name__ == '__main__':
    main()
