#!/usr/bin/env python3
"""
测试脚本 - 验证各模块功能
"""

import sys
sys.path.append('***REMOVED***/tail_trading/src')
sys.path.append('***REMOVED***/tail_trading/config')

def test_sector_data():
    """测试板块数据获取"""
    print("=" * 50)
    print("测试1: 板块数据获取")
    print("=" * 50)
    
    from sector_data import get_sector_ranking
    sectors = get_sector_ranking(sector_type=2, limit=10)
    
    if sectors:
        print(f"✅ 获取到 {len(sectors)} 个概念板块")
        print("前10热门板块:")
        for i, s in enumerate(sectors[:10], 1):
            print(f"  {i}. {s['name']}: {'+' if s['change_percent'] > 0 else ''}{s['change_percent']}%")
        return True
    else:
        print("❌ 获取板块数据失败")
        return False

def test_attack_sectors():
    """测试进攻型板块筛选"""
    print("\n" + "=" * 50)
    print("测试2: 进攻型板块筛选")
    print("=" * 50)
    
    from sector_data import get_sector_ranking
    from sectors import filter_attack_sectors, is_attack_sector
    
    sectors = get_sector_ranking(sector_type=2, limit=20)
    attack_sectors = filter_attack_sectors(sectors)
    
    print(f"热门板块总数: {len(sectors)}")
    print(f"进攻型板块数量: {len(attack_sectors)}")
    
    if attack_sectors:
        print("进攻型板块:")
        for s in attack_sectors[:5]:
            print(f"  • {s['name']}: {'+' if s['change_percent'] > 0 else ''}{s['change_percent']}%")
        return True
    else:
        print("今日无进攻型板块")
        return False

def test_supertrend():
    """测试SuperTrend计算"""
    print("\n" + "=" * 50)
    print("测试3: SuperTrend计算")
    print("=" * 50)
    
    from stock_data import get_stock_daily_kline
    from supertrend import calculate_supertrend, is_supertrend_bullish
    from stock_filter import get_weekly_kline
    
    # 测试东方财富
    df = get_stock_daily_kline('002409', market='sz', days=60)
    
    if df.empty:
        print("❌ 获取数据失败")
        return False
    
    print(f"✅ 获取到 {len(df)} 条日K数据")
    
    # 计算SuperTrend
    st = calculate_supertrend(df)
    current_trend = "多头" if st['supertrend'].iloc[-1] else "空头"
    print(f"当前SuperTrend趋势: {current_trend}")
    
    # 周线
    weekly_df = get_weekly_kline(df)
    if not weekly_df.empty:
        weekly_trend = is_supertrend_bullish(weekly_df)
        print(f"周线SuperTrend趋势: {'多头' if weekly_trend else '空头'}")
    
    return True

def test_full_flow():
    """测试完整流程"""
    print("\n" + "=" * 50)
    print("测试4: 完整流程")
    print("=" * 50)
    
    from main import run
    report = run()
    
    if report:
        print("\n生成的报告:")
        print(report)
        return True
    return False

if __name__ == '__main__':
    print("开始测试尾盘T+1信号系统...\n")
    
    results = []
    
    # 测试板块数据
    results.append(("板块数据", test_sector_data()))
    
    # 测试进攻型板块
    results.append(("进攻型板块", test_attack_sectors()))
    
    # 测试SuperTrend
    results.append(("SuperTrend", test_supertrend()))
    
    # 测试完整流程
    results.append(("完整流程", test_full_flow()))
    
    # 汇总结果
    print("\n" + "=" * 50)
    print("测试结果汇总")
    print("=" * 50)
    
    for name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{name}: {status}")
    
    all_passed = all(r[1] for r in results)
    if all_passed:
        print("\n🎉 所有测试通过！")
    else:
        print("\n⚠️ 部分测试失败，请检查")
