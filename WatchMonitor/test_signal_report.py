#!/usr/bin/env python3
"""
测试信号检测引擎和报告生成
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from datetime import datetime
from signal_detector import detect_all_signals
from core.storage import save_report

def test_signal_detection():
    """测试信号检测功能"""
    print("\n" + "="*60)
    print("测试信号检测引擎")
    print("="*60)
    
    result = detect_all_signals()
    
    # 持仓池风险信号
    print("\n【持仓池风险信号】")
    for pr in result['position_risks']:
        code = pr['code']
        name = pr['name']
        signals = pr['signals']
        current_price = pr['current_price']
        profit_pct = pr['profit_pct']
        
        print(f"\n{name} ({code}): 现价{current_price:.2f}, 盈亏{profit_pct:.1f}%")
        if not signals:
            print("  ✅ 无风险信号")
        else:
            for sig in signals:
                severity = sig.get('severity', 'info')
                message = sig.get('message', '')
                icon = {'fatal': '🔴🔴', 'critical': '🔴', 'high': '🔴', 'medium': '⚠️', 'warning': '⚠️', 'positive': '✅', 'info': '💡'}.get(severity, '💡')
                print(f"  {icon} {message}")
    
    # 候选池抄底信号
    print("\n【候选池抄底信号】")
    for cs in result['candidate_signals']:
        code = cs['code']
        name = cs['name']
        score = cs['score']
        stars = cs['stars']
        score_level = cs['score_level']
        signals = cs['signals']
        current_price = cs['current_price']
        
        print(f"\n{name} ({code}): 现价{current_price:.2f}")
        print(f"  {stars} 评分: {score}分 ({score_level})")
        if signals:
            for sig in signals:
                if sig['type'] not in ['no_data', 'mine_warning']:
                    print(f"  {sig['message']}")
    
    return result


def generate_test_report():
    """生成测试报告"""
    print("\n" + "="*60)
    print("生成测试报告")
    print("="*60)
    
    result = detect_all_signals()
    date_str = datetime.now().strftime('%Y-%m-%d')
    
    lines = []
    lines.append(f"📊 盯盘助手测试报告 — {date_str}")
    lines.append("")
    lines.append("***")
    lines.append("")
    
    # 持仓池风险信号
    lines.append("## 🚨 持仓池风险信号")
    lines.append("")
    
    for pr in result['position_risks']:
        code = pr['code']
        name = pr['name']
        position_type = pr['position_type']
        entry_price = pr['entry_price']
        current_price = pr['current_price']
        profit_pct = pr['profit_pct']
        signals = pr['signals']
        
        profit_sign = '+' if profit_pct >= 0 else ''
        lines.append(f"> **{name} ({code})** — {position_type}持仓 | 成本{entry_price:.2f} | 现价{current_price:.2f} ({profit_sign}{profit_pct:.1f}%)")
        lines.append("> ")
        
        if not signals:
            lines.append("> ✅ 暂无风险信号")
        else:
            severity_order = {'fatal': 0, 'critical': 1, 'high': 2, 'medium': 3, 'warning': 4, 'info': 5, 'positive': 6}
            sorted_signals = sorted(signals, key=lambda x: severity_order.get(x.get('severity', 'info'), 99))
            
            for sig in sorted_signals:
                severity = sig.get('severity', 'info')
                message = sig.get('message', '')
                icon = {'fatal': '🔴🔴', 'critical': '🔴', 'high': '🔴', 'medium': '⚠️', 'warning': '⚠️', 'positive': '✅', 'info': '💡'}.get(severity, '💡')
                lines.append(f"> {icon} {message}")
        
        lines.append("> ")
        lines.append("")
    
    # 候选池抄底信号
    lines.append("## 🎯 候选池抄底信号")
    lines.append("")
    
    for cs in result['candidate_signals']:
        code = cs['code']
        name = cs['name']
        watch_type = cs['watch_type']
        watch_price = cs['watch_price']
        current_price = cs['current_price']
        drop_pct = cs['drop_pct']
        score = cs['score']
        stars = cs['stars']
        score_level = cs['score_level']
        signals = cs['signals']
        mine_result = cs['mine_result']
        
        drop_sign = '+' if drop_pct >= 0 else ''
        lines.append(f"> **{name} ({code})** — {watch_type} | 关注价{watch_price:.2f} | 现价{current_price:.2f} ({drop_sign}{drop_pct:.1f}%)")
        lines.append("> ")
        
        if stars:
            lines.append(f"> {stars} 抄底评分：**{score}分**（{score_level})")
        else:
            lines.append(f"> 抄底评分：{score}分（{score_level})")
        lines.append("> ")
        
        if signals:
            for sig in signals:
                if sig['type'] not in ['no_data', 'mine_warning']:
                    lines.append(f"> {sig['message']}")
        
        if mine_result and mine_result.get('has_mine'):
            lines.append(f"> ⚠️ 扫雷检测有风险，请谨慎")
        
        lines.append("> ")
        lines.append("")
    
    lines.append("***")
    lines.append(f"检测时间: {result['detect_time']}")
    
    report_text = '\n'.join(lines)
    
    # 保存报告
    report_path = save_report(date_str, report_text)
    print(f"\n✅ 报告已保存: {report_path}")
    
    return report_text


if __name__ == '__main__':
    # 测试信号检测
    test_signal_detection()
    
    # 生成测试报告
    report = generate_test_report()
    
    print("\n" + "="*60)
    print("测试完成")
    print("="*60)