"""
个股数据获取模块（薄封装层）
实际数据获取逻辑在 data_source.py 中
"""

from data_source import get_stock_daily_kline, get_stock_daily_kline_range, get_stock_realtime

if __name__ == '__main__':
    print("测试个股数据获取...")
    df = get_stock_daily_kline('002409', market='sz', days=30)
    if not df.empty:
        print(f"获取到 {len(df)} 条日K数据")
        print(df.tail())
    else:
        print("未获取到数据")
