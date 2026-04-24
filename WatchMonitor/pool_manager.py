#!/usr/bin/env python3
"""
池子维护脚本 - 持仓池和候选池管理工具

用法:
    # 持仓池操作
    python3 pool_manager.py position add --code 002594 --name "比亚迪" --entry-price 250 --entry-date 2026-03-15 --stop-loss 235
    python3 pool_manager.py position list
    python3 pool_manager.py position remove --code 002594
    python3 pool_manager.py position import --file positions.csv
    python3 pool_manager.py position export --file positions.csv
    
    # 候选池操作
    python3 pool_manager.py candidate add --code 300750 --name "宁德时代" --watch-price 210 --watch-date 2026-04-15
    python3 pool_manager.py candidate list
    python3 pool_manager.py candidate remove --code 300750
    python3 pool_manager.py candidate import --file candidates.csv
    python3 pool_manager.py candidate export --file candidates.csv
    
    # 技术指标计算
    python3 pool_manager.py test-indicators              # 批量计算持仓池所有股票
    python3 pool_manager.py test-indicators --save       # 批量计算并保存
    python3 pool_manager.py test-indicators --code 002594  # 计算单只股票
    python3 pool_manager.py test-indicators --code 002594 --save  # 计算并保存单只股票
    
    # 其他操作
    python3 pool_manager.py generate-template              # 生成CSV模板文件
    python3 pool_manager.py init                           # 初始化数据库表

CSV文件格式:
    持仓池: code,name,entry_price,entry_date,shares,position_type,stop_loss,take_profit,notes
    候选池: code,name,watch_price,watch_date,target_price,watch_type,watch_reason,notes
"""

import argparse
import sys
import os
import logging
import json
from datetime import datetime

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.storage import (
    add_position, remove_position, update_position, get_all_positions, get_position_by_code,
    add_candidate, remove_candidate, update_candidate, get_all_candidates, get_candidate_by_code,
    get_daily_data_from_sqlite, init_all_tables, save_technical_indicators, get_technical_indicators
)
from core.indicator_funcs import calculate_all_indicators, calculate_ma, calculate_macd, calculate_rsi

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ==================== 持仓池操作 ====================

def cmd_position_add(args):
    """添加持仓"""
    if not args.code or not args.name or not args.entry_price or not args.entry_date:
        print("❌ 缺少必要参数: --code, --name, --entry-price, --entry-date")
        return False
    
    success = add_position(
        code=args.code,
        name=args.name,
        entry_price=float(args.entry_price),
        entry_date=args.entry_date,
        shares=int(args.shares) if args.shares else 0,
        position_type=args.position_type or '趋势',
        stop_loss=float(args.stop_loss) if args.stop_loss else None,
        take_profit=float(args.take_profit) if args.take_profit else None,
        notes=args.notes
    )
    
    if success:
        print(f"✅ 添加持仓成功: {args.code}({args.name})")
    else:
        print(f"❌ 添加持仓失败: {args.code} 可能已存在")
    
    return success


def cmd_position_remove(args):
    """删除持仓"""
    if not args.code:
        print("❌ 缺少必要参数: --code")
        return False
    
    success = remove_position(args.code)
    if success:
        print(f"✅ 删除持仓成功: {args.code}")
    else:
        print(f"❌ 删除持仓失败: {args.code}")
    
    return success


def cmd_position_update(args):
    """更新持仓"""
    if not args.code:
        print("❌ 缺少必要参数: --code")
        return False
    
    update_fields = {}
    if args.stop_loss:
        update_fields['stop_loss'] = float(args.stop_loss)
    if args.take_profit:
        update_fields['take_profit'] = float(args.take_profit)
    if args.shares:
        update_fields['shares'] = int(args.shares)
    if args.position_type:
        update_fields['position_type'] = args.position_type
    if args.notes:
        update_fields['notes'] = args.notes
    
    if not update_fields:
        print("❌ 缺少更新字段: --stop-loss, --take-profit, --shares, --position-type, --notes")
        return False
    
    success = update_position(args.code, **update_fields)
    if success:
        print(f"✅ 更新持仓成功: {args.code}")
        pos = get_position_by_code(args.code)
        if pos:
            print(f"   止损={pos.get('stop_loss')}, 止盈={pos.get('take_profit')}")
    else:
        print(f"❌ 更新持仓失败: {args.code}")
    
    return success


def cmd_position_list(args):
    """列出所有持仓"""
    positions = get_all_positions()
    
    if not positions:
        print("持仓池为空")
        return True
    
    print(f"\n{'='*80}")
    print(f"持仓池列表 ({len(positions)}只)")
    print(f"{'='*80}")
    
    for p in positions:
        print(f"\n股票代码: {p['code']}")
        print(f"股票名称: {p['name']}")
        print(f"买入价格: {p['entry_price']:.2f}")
        print(f"买入日期: {p['entry_date']}")
        print(f"持仓数量: {p['shares']}")
        print(f"持仓类型: {p['position_type']}")
        print(f"止损价:   {p['stop_loss'] if p['stop_loss'] else '未设置'}")
        print(f"止盈价:   {p['take_profit'] if p['take_profit'] else '未设置'}")
        print(f"备注:     {p['notes'] if p['notes'] else '无'}")
        print(f"添加时间: {p['created_at']}")
    
    return True


def cmd_position_detail(args):
    """查看持仓详情"""
    if not args.code:
        print("❌ 缺少必要参数: --code")
        return False
    
    pos = get_position_by_code(args.code)
    if not pos:
        print(f"❌ 未找到持仓: {args.code}")
        return False
    
    print(f"\n{'='*40}")
    print(f"持仓详情: {pos['code']}({pos['name']})")
    print(f"{'='*40}")
    print(json.dumps(pos, indent=2, ensure_ascii=False))
    
    return True


# ==================== 候选池操作 ====================

def cmd_candidate_add(args):
    """添加候选"""
    if not args.code or not args.name or not args.watch_price or not args.watch_date:
        print("❌ 缺少必要参数: --code, --name, --watch-price, --watch-date")
        return False
    
    success = add_candidate(
        code=args.code,
        name=args.name,
        watch_price=float(args.watch_price),
        watch_date=args.watch_date,
        target_price=float(args.target_price) if args.target_price else None,
        watch_type=args.watch_type or '趋势回调',
        watch_reason=args.watch_reason,
        notes=args.notes
    )
    
    if success:
        print(f"✅ 添加候选成功: {args.code}({args.name})")
    else:
        print(f"❌ 添加候选失败: {args.code} 可能已存在")
    
    return success


def cmd_candidate_remove(args):
    """删除候选"""
    if not args.code:
        print("❌ 缺少必要参数: --code")
        return False
    
    success = remove_candidate(args.code)
    if success:
        print(f"✅ 删除候选成功: {args.code}")
    else:
        print(f"❌ 删除候选失败: {args.code}")
    
    return success


def cmd_candidate_update(args):
    """更新候选"""
    if not args.code:
        print("❌ 缺少必要参数: --code")
        return False
    
    update_fields = {}
    if args.target_price:
        update_fields['target_price'] = float(args.target_price)
    if args.watch_type:
        update_fields['watch_type'] = args.watch_type
    if args.watch_reason:
        update_fields['watch_reason'] = args.watch_reason
    if args.notes:
        update_fields['notes'] = args.notes
    
    if not update_fields:
        print("❌ 缺少更新字段: --target-price, --watch-type, --watch-reason, --notes")
        return False
    
    success = update_candidate(args.code, **update_fields)
    if success:
        print(f"✅ 更新候选成功: {args.code}")
        cand = get_candidate_by_code(args.code)
        if cand:
            print(f"   目标价={cand.get('target_price')}, 类型={cand.get('watch_type')}")
    else:
        print(f"❌ 更新候选失败: {args.code}")
    
    return success


def cmd_candidate_list(args):
    """列出所有候选"""
    candidates = get_all_candidates()
    
    if not candidates:
        print("候选池为空")
        return True
    
    print(f"\n{'='*80}")
    print(f"候选池列表 ({len(candidates)}只)")
    print(f"{'='*80}")
    
    for c in candidates:
        print(f"\n股票代码: {c['code']}")
        print(f"股票名称: {c['name']}")
        print(f"关注价格: {c['watch_price']:.2f}")
        print(f"关注日期: {c['watch_date']}")
        print(f"目标价格: {c['target_price'] if c['target_price'] else '未设置'}")
        print(f"关注类型: {c['watch_type']}")
        print(f"关注原因: {c['watch_reason'] if c['watch_reason'] else '无'}")
        print(f"备注:     {c['notes'] if c['notes'] else '无'}")
        print(f"添加时间: {c['created_at']}")
    
    return True


def cmd_candidate_detail(args):
    """查看候选详情"""
    if not args.code:
        print("❌ 缺少必要参数: --code")
        return False
    
    cand = get_candidate_by_code(args.code)
    if not cand:
        print(f"❌ 未找到候选: {args.code}")
        return False
    
    print(f"\n{'='*40}")
    print(f"候选详情: {cand['code']}({cand['name']})")
    print(f"{'='*40}")
    print(json.dumps(cand, indent=2, ensure_ascii=False))
    
    return True


# ==================== CSV导入导出 ====================

import csv

def cmd_position_import(args):
    """从CSV文件导入持仓"""
    if not args.file:
        print("❌ 缺少必要参数: --file")
        return False
    
    filepath = args.file
    if not os.path.exists(filepath):
        print(f"❌ 文件不存在: {filepath}")
        return False
    
    print(f"\n{'='*60}")
    print(f"导入持仓池: {filepath}")
    print(f"{'='*60}")
    
    success_count = 0
    skip_count = 0
    error_count = 0
    
    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                code = row.get('code', '').strip()
                name = row.get('name', '').strip()
                entry_price = row.get('entry_price', '').strip()
                entry_date = row.get('entry_date', '').strip()
                
                if not code or not name or not entry_price or not entry_date:
                    print(f"  ⚠️ 跳过无效行: {row}")
                    skip_count += 1
                    continue
                
                success = add_position(
                    code=code,
                    name=name,
                    entry_price=float(entry_price),
                    entry_date=entry_date,
                    shares=int(row.get('shares', 0) or 0),
                    position_type=row.get('position_type', '趋势') or '趋势',
                    stop_loss=float(row.get('stop_loss')) if row.get('stop_loss') else None,
                    take_profit=float(row.get('take_profit')) if row.get('take_profit') else None,
                    notes=row.get('notes', '').strip() if row.get('notes') else None
                )
                
                if success:
                    print(f"  ✅ {code}({name})")
                    success_count += 1
                else:
                    print(f"  ⚠️ {code} 已存在，跳过")
                    skip_count += 1
    except Exception as e:
        print(f"❌ 导入失败: {e}")
        error_count += 1
    
    print(f"\n导入完成: 成功{success_count}只, 跳过{skip_count}只, 失败{error_count}只")
    return True


def cmd_position_export(args):
    """导出持仓到CSV文件"""
    if not args.file:
        print("❌ 缺少必要参数: --file")
        return False
    
    positions = get_all_positions()
    if not positions:
        print("持仓池为空，无需导出")
        return True
    
    filepath = args.file
    
    print(f"\n{'='*60}")
    print(f"导出持仓池: {filepath}")
    print(f"{'='*60}")
    
    try:
        with open(filepath, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['code', 'name', 'entry_price', 'entry_date', 'shares', 'position_type', 'stop_loss', 'take_profit', 'notes'])
            
            for p in positions:
                writer.writerow([
                    p['code'],
                    p['name'],
                    p['entry_price'],
                    p['entry_date'],
                    p['shares'],
                    p['position_type'],
                    p['stop_loss'] or '',
                    p['take_profit'] or '',
                    p['notes'] or ''
                ])
        
        print(f"✅ 导出成功: {len(positions)}只持仓")
    except Exception as e:
        print(f"❌ 导出失败: {e}")
        return False
    
    return True


def cmd_candidate_import(args):
    """从CSV文件导入候选"""
    if not args.file:
        print("❌ 缺少必要参数: --file")
        return False
    
    filepath = args.file
    if not os.path.exists(filepath):
        print(f"❌ 文件不存在: {filepath}")
        return False
    
    print(f"\n{'='*60}")
    print(f"导入候选池: {filepath}")
    print(f"{'='*60}")
    
    success_count = 0
    skip_count = 0
    error_count = 0
    
    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                code = row.get('code', '').strip()
                name = row.get('name', '').strip()
                watch_price = row.get('watch_price', '').strip()
                watch_date = row.get('watch_date', '').strip()
                
                if not code or not name or not watch_price or not watch_date:
                    print(f"  ⚠️ 跳过无效行: {row}")
                    skip_count += 1
                    continue
                
                success = add_candidate(
                    code=code,
                    name=name,
                    watch_price=float(watch_price),
                    watch_date=watch_date,
                    target_price=float(row.get('target_price')) if row.get('target_price') else None,
                    watch_type=row.get('watch_type', '趋势回调') or '趋势回调',
                    watch_reason=row.get('watch_reason', '').strip() if row.get('watch_reason') else None,
                    notes=row.get('notes', '').strip() if row.get('notes') else None
                )
                
                if success:
                    print(f"  ✅ {code}({name})")
                    success_count += 1
                else:
                    print(f"  ⚠️ {code} 已存在，跳过")
                    skip_count += 1
    except Exception as e:
        print(f"❌ 导入失败: {e}")
        error_count += 1
    
    print(f"\n导入完成: 成功{success_count}只, 跳过{skip_count}只, 失败{error_count}只")
    return True


def cmd_candidate_export(args):
    """导出候选到CSV文件"""
    if not args.file:
        print("❌ 缺少必要参数: --file")
        return False
    
    candidates = get_all_candidates()
    if not candidates:
        print("候选池为空，无需导出")
        return True
    
    filepath = args.file
    
    print(f"\n{'='*60}")
    print(f"导出候选池: {filepath}")
    print(f"{'='*60}")
    
    try:
        with open(filepath, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['code', 'name', 'watch_price', 'watch_date', 'target_price', 'watch_type', 'watch_reason', 'notes'])
            
            for c in candidates:
                writer.writerow([
                    c['code'],
                    c['name'],
                    c['watch_price'],
                    c['watch_date'],
                    c['target_price'] or '',
                    c['watch_type'],
                    c['watch_reason'] or '',
                    c['notes'] or ''
                ])
        
        print(f"✅ 导出成功: {len(candidates)}只候选")
    except Exception as e:
        print(f"❌ 导出失败: {e}")
        return False
    
    return True


def cmd_generate_template(args):
    """生成CSV模板文件"""
    template_dir = args.dir or '/mnt/hgfs/shares'
    
    print(f"\n{'='*60}")
    print(f"生成CSV模板文件")
    print(f"{'='*60}")
    
    # 持仓池模板
    position_template = os.path.join(template_dir, 'position_pool_template.csv')
    with open(position_template, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['code', 'name', 'entry_price', 'entry_date', 'shares', 'position_type', 'stop_loss', 'take_profit', 'notes'])
        writer.writerow(['002594', '比亚迪', '250.00', '2026-03-15', '200', '趋势', '235.00', '300.00', '趋势跟踪持仓'])
        writer.writerow(['600030', '中信证券', '22.50', '2026-04-10', '1000', '波段', '21.00', '26.00', '波段操作'])
    print(f"✅ 持仓池模板: {position_template}")
    
    # 候选池模板
    candidate_template = os.path.join(template_dir, 'candidate_pool_template.csv')
    with open(candidate_template, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['code', 'name', 'watch_price', 'watch_date', 'target_price', 'watch_type', 'watch_reason', 'notes'])
        writer.writerow(['300750', '宁德时代', '210.00', '2026-04-15', '195.00', '趋势回调', '趋势回调至关键支撑', '等60日线企稳'])
        writer.writerow(['300308', '中际旭创', '120.00', '2026-04-18', '105.00', '底部反转', '底部震荡等待反转', '关注底背离'])
    print(f"✅ 候选池模板: {candidate_template}")
    
    print(f"\n模板已生成到: {template_dir}")
    print("请编辑CSV文件后执行导入命令")
    return True


# ==================== 技术指标测试 ====================

def _calc_and_print_indicators(code, df, save=False):
    """计算并打印单只股票的技术指标"""
    if df.empty:
        print(f"  ❌ {code} 未找到日K数据")
        return None
    
    indicators = calculate_all_indicators(df)
    
    # 简洁输出
    close = df['close'].iloc[-1]
    st_dir_map = {1: '多头', -1: '空头'}
    slope_map = {-1: '下', 0: '平', 1: '上'}
    
    result = {
        'code': code,
        'close': close,
        'ma5': indicators.get('ma5'),
        'ma10': indicators.get('ma10'),
        'ma20': indicators.get('ma20'),
        'ma60': indicators.get('ma60'),
        'st_direction': indicators.get('st_direction'),
        'macd_dif': indicators.get('macd_dif'),
        'rsi': indicators.get('rsi_14'),
        'indicators': indicators
    }
    
    # 一行输出
    st_dir = st_dir_map.get(indicators.get('st_direction'), 'N/A')
    ma5_vs = '↑' if close > indicators.get('ma5', 0) else '↓'
    ma10_vs = '↑' if close > indicators.get('ma10', 0) else '↓'
    ma20_vs = '↑' if close > indicators.get('ma20', 0) else '↓'
    ma60_vs = '↑' if close > indicators.get('ma60', 0) else '↓'
    
    print(f"  {code}: 收盘{close:.2f} | MA5{ma5_vs} MA10{ma10_vs} MA20{ma20_vs} MA60{ma60_vs} | ST:{st_dir} | RSI:{indicators.get('rsi_14', 0):.1f}")
    
    if save:
        if save_technical_indicators(code, indicators):
            print(f"    ✅ 已保存到数据库")
        else:
            print(f"    ❌ 保存失败")
    
    return result


def cmd_test_indicators(args):
    """测试股票的技术指标计算（不传code则计算持仓池所有股票）"""
    
    # 如果没有传入code，则批量计算持仓池所有股票
    if not args.code:
        positions = get_all_positions()
        if not positions:
            print("❌ 持仓池为空，请先添加持仓")
            print("   使用: pool_manager.py position add --code xxx")
            return False
        
        print(f"\n{'='*60}")
        print(f"批量计算持仓池技术指标 ({len(positions)}只)")
        print(f"{'='*60}")
        
        results = []
        for pos in positions:
            code = pos['code']
            df = get_daily_data_from_sqlite(code, days=340)
            result = _calc_and_print_indicators(code, df, args.save)
            if result:
                result['name'] = pos['name']
                result['entry_price'] = pos['entry_price']
                results.append(result)
        
        print(f"\n{'='*60}")
        print(f"计算完成: {len(results)}/{len(positions)}只")
        if args.save:
            print(f"已保存到 technical_indicators 表")
        print(f"{'='*60}")
        
        return True
    
    # 传入了code，计算单只股票
    print(f"\n{'='*60}")
    print(f"测试股票技术指标: {args.code}")
    print(f"{'='*60}")
    
    df = get_daily_data_from_sqlite(args.code, days=340)
    
    if df.empty:
        print(f"❌ 未找到 {args.code} 的日K数据")
        print("   请先更新该股票的日K数据")
        return False
    
    print(f"✅ 获取到 {len(df)} 天K线数据")
    print(f"   日期范围: {df['date'].iloc[0]} ~ {df['date'].iloc[-1]}")
    
    indicators = calculate_all_indicators(df)
    
    # 详细输出
    print(f"\n{'='*40}")
    print("计算技术指标...")
    print(f"{'='*40}")
    
    print("\n【均线指标】")
    print(f"  MA5   = {indicators.get('ma5', 'N/A'):.2f}" if indicators.get('ma5') else "  MA5   = 数据不足")
    print(f"  MA10  = {indicators.get('ma10', 'N/A'):.2f}" if indicators.get('ma10') else "  MA10  = 数据不足")
    print(f"  MA20  = {indicators.get('ma20', 'N/A'):.2f}" if indicators.get('ma20') else "  MA20  = 数据不足")
    print(f"  MA60  = {indicators.get('ma60', 'N/A'):.2f}" if indicators.get('ma60') else "  MA60  = 数据不足")
    print(f"  MA120 = {indicators.get('ma120', 'N/A'):.2f}" if indicators.get('ma120') else "  MA120 = 数据不足")
    print(f"  MA250 = {indicators.get('ma250', 'N/A'):.2f}" if indicators.get('ma250') else "  MA250 = 数据不足")
    
    print("\n【均线斜率】")
    slope_map = {-1: '向下⬇', 0: '走平→', 1: '向上⬆', None: 'N/A'}
    print(f"  MA5斜率  = {slope_map.get(indicators.get('ma5_slope'), 'N/A')}")
    print(f"  MA10斜率 = {slope_map.get(indicators.get('ma10_slope'), 'N/A')}")
    print(f"  MA20斜率 = {slope_map.get(indicators.get('ma20_slope'), 'N/A')}")
    
    print("\n【SuperTrend指标】")
    st_dir_map = {1: '多头⬆', -1: '空头⬇', None: 'N/A'}
    print(f"  上轨   = {indicators.get('st_upper_band', 'N/A'):.2f}" if indicators.get('st_upper_band') else "  上轨   = 数据不足")
    print(f"  下轨   = {indicators.get('st_lower_band', 'N/A'):.2f}" if indicators.get('st_lower_band') else "  下轨   = 数据不足")
    print(f"  方向   = {st_dir_map.get(indicators.get('st_direction'), 'N/A')}")
    print(f"  ATR    = {indicators.get('st_atr', 'N/A'):.2f}" if indicators.get('st_atr') else "  ATR    = 数据不足")
    
    print("\n【MACD指标】")
    print(f"  DIF    = {indicators.get('macd_dif', 'N/A'):.4f}" if indicators.get('macd_dif') else "  DIF    = 数据不足")
    print(f"  DEA    = {indicators.get('macd_dea', 'N/A'):.4f}" if indicators.get('macd_dea') else "  DEA    = 数据不足")
    print(f"  柱状图 = {indicators.get('macd_histogram', 'N/A'):.4f}" if indicators.get('macd_histogram') else "  柱状图 = 数据不足")
    
    print("\n【RSI指标】")
    print(f"  RSI14  = {indicators.get('rsi_14', 'N/A'):.2f}" if indicators.get('rsi_14') else "  RSI14  = 数据不足")
    
    print("\n【量比指标】")
    print(f"  VR5    = {indicators.get('volume_ratio_5', 'N/A'):.2f}" if indicators.get('volume_ratio_5') else "  VR5    = 数据不足")
    print(f"  VR20   = {indicators.get('volume_ratio_20', 'N/A'):.2f}" if indicators.get('volume_ratio_20') else "  VR20   = 数据不足")
    
    print("\n【K线形态】")
    print(f"  阳线     = {'是' if indicators.get('is_bullish_candle') else '否'}")
    print(f"  阴线     = {'是' if indicators.get('is_bearish_candle') else '否'}")
    print(f"  长上影线 = {'是' if indicators.get('is_long_upper_shadow') else '否'}")
    print(f"  长下影线 = {'是' if indicators.get('is_long_lower_shadow') else '否'}")
    
    # 当前收盘价与均线关系
    close = df['close'].iloc[-1]
    print("\n【收盘价与均线关系】")
    for period in [5, 10, 20, 60, 120, 250]:
        ma_val = indicators.get(f'ma{period}')
        if ma_val:
            status = '上方⬆' if close > ma_val else '下方⬇'
            print(f"  收盘价 vs MA{period}: {close:.2f} {'>' if close > ma_val else '<'} {ma_val:.2f} ({status})")
    
    # 保存技术指标（可选）
    if args.save:
        print("\n【保存技术指标到数据库】")
        if save_technical_indicators(args.code, indicators):
            print(f"✅ 技术指标已保存: {args.code} {indicators['calc_date']}")
        else:
            print(f"❌ 技术指标保存失败")
    
    return True


# ==================== 初始化 ====================

def cmd_init(args):
    """初始化数据库表"""
    init_all_tables()
    print("✅ 所有数据库表初始化完成")
    return True


# ==================== 主命令解析 ====================

def main():
    parser = argparse.ArgumentParser(
        description='池子维护脚本 - 持仓池和候选池管理工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    subparsers = parser.add_subparsers(dest='pool_type', help='池子类型')
    
    # ========== 持仓池命令 ==========
    position_parser = subparsers.add_parser('position', help='持仓池操作')
    position_subparsers = position_parser.add_subparsers(dest='action', help='操作类型')
    
    # 添加持仓
    pos_add = position_subparsers.add_parser('add', help='添加持仓')
    pos_add.add_argument('--code', required=True, help='股票代码')
    pos_add.add_argument('--name', required=True, help='股票名称')
    pos_add.add_argument('--entry-price', required=True, help='买入价格')
    pos_add.add_argument('--entry-date', required=True, help='买入日期 YYYY-MM-DD')
    pos_add.add_argument('--shares', type=int, default=0, help='持仓数量')
    pos_add.add_argument('--position-type', default='趋势', help='持仓类型: 趋势/波段/短线')
    pos_add.add_argument('--stop-loss', help='止损价')
    pos_add.add_argument('--take-profit', help='止盈价')
    pos_add.add_argument('--notes', help='备注')
    pos_add.set_defaults(func=cmd_position_add)
    
    # 删除持仓
    pos_remove = position_subparsers.add_parser('remove', help='删除持仓')
    pos_remove.add_argument('--code', required=True, help='股票代码')
    pos_remove.set_defaults(func=cmd_position_remove)
    
    # 更新持仓
    pos_update = position_subparsers.add_parser('update', help='更新持仓')
    pos_update.add_argument('--code', required=True, help='股票代码')
    pos_update.add_argument('--stop-loss', help='止损价')
    pos_update.add_argument('--take-profit', help='止盈价')
    pos_update.add_argument('--shares', type=int, help='持仓数量')
    pos_update.add_argument('--position-type', help='持仓类型')
    pos_update.add_argument('--notes', help='备注')
    pos_update.set_defaults(func=cmd_position_update)
    
    # 列出持仓
    pos_list = position_subparsers.add_parser('list', help='列出所有持仓')
    pos_list.set_defaults(func=cmd_position_list)
    
    # 查看持仓详情
    pos_detail = position_subparsers.add_parser('detail', help='查看持仓详情')
    pos_detail.add_argument('--code', required=True, help='股票代码')
    pos_detail.set_defaults(func=cmd_position_detail)
    
    # CSV导入持仓
    pos_import = position_subparsers.add_parser('import', help='从CSV导入持仓')
    pos_import.add_argument('--file', required=True, help='CSV文件路径')
    pos_import.set_defaults(func=cmd_position_import)
    
    # CSV导出持仓
    pos_export = position_subparsers.add_parser('export', help='导出持仓到CSV')
    pos_export.add_argument('--file', required=True, help='CSV文件路径')
    pos_export.set_defaults(func=cmd_position_export)
    
    # ========== 候选池命令 ==========
    candidate_parser = subparsers.add_parser('candidate', help='候选池操作')
    candidate_subparsers = candidate_parser.add_subparsers(dest='action', help='操作类型')
    
    # 添加候选
    cand_add = candidate_subparsers.add_parser('add', help='添加候选')
    cand_add.add_argument('--code', required=True, help='股票代码')
    cand_add.add_argument('--name', required=True, help='股票名称')
    cand_add.add_argument('--watch-price', required=True, help='关注价格')
    cand_add.add_argument('--watch-date', required=True, help='关注日期 YYYY-MM-DD')
    cand_add.add_argument('--target-price', help='目标买入价')
    cand_add.add_argument('--watch-type', default='趋势回调', help='关注类型: 趋势回调/底部反转/突破回踩')
    cand_add.add_argument('--watch-reason', help='关注原因')
    cand_add.add_argument('--notes', help='备注')
    cand_add.set_defaults(func=cmd_candidate_add)
    
    # 删除候选
    cand_remove = candidate_subparsers.add_parser('remove', help='删除候选')
    cand_remove.add_argument('--code', required=True, help='股票代码')
    cand_remove.set_defaults(func=cmd_candidate_remove)
    
    # 更新候选
    cand_update = candidate_subparsers.add_parser('update', help='更新候选')
    cand_update.add_argument('--code', required=True, help='股票代码')
    cand_update.add_argument('--target-price', help='目标买入价')
    cand_update.add_argument('--watch-type', help='关注类型')
    cand_update.add_argument('--watch-reason', help='关注原因')
    cand_update.add_argument('--notes', help='备注')
    cand_update.set_defaults(func=cmd_candidate_update)
    
    # 列出候选
    cand_list = candidate_subparsers.add_parser('list', help='列出所有候选')
    cand_list.set_defaults(func=cmd_candidate_list)
    
    # 查看候选详情
    cand_detail = candidate_subparsers.add_parser('detail', help='查看候选详情')
    cand_detail.add_argument('--code', required=True, help='股票代码')
    cand_detail.set_defaults(func=cmd_candidate_detail)
    
    # CSV导入候选
    cand_import = candidate_subparsers.add_parser('import', help='从CSV导入候选')
    cand_import.add_argument('--file', required=True, help='CSV文件路径')
    cand_import.set_defaults(func=cmd_candidate_import)
    
    # CSV导出候选
    cand_export = candidate_subparsers.add_parser('export', help='导出候选到CSV')
    cand_export.add_argument('--file', required=True, help='CSV文件路径')
    cand_export.set_defaults(func=cmd_candidate_export)
    
    # ========== 技术指标测试命令 ==========
    test_parser = subparsers.add_parser('test-indicators', help='测试股票技术指标计算（不传code则计算持仓池所有股票）')
    test_parser.add_argument('--code', help='股票代码（不传则计算持仓池所有股票）')
    test_parser.add_argument('--save', action='store_true', help='保存计算结果到数据库')
    test_parser.set_defaults(func=cmd_test_indicators)
    
    # ========== 初始化命令 ==========
    init_parser = subparsers.add_parser('init', help='初始化数据库表')
    init_parser.set_defaults(func=cmd_init)
    
    # ========== CSV模板生成命令 ==========
    template_parser = subparsers.add_parser('generate-template', help='生成CSV模板文件')
    template_parser.add_argument('--dir', help='模板输出目录，默认/mnt/hgfs/shares')
    template_parser.set_defaults(func=cmd_generate_template)
    
    # 解析命令
    args = parser.parse_args()
    
    # 执行命令
    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()