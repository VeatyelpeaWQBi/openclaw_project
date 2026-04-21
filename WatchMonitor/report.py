"""
盯盘助手报告生成模块
拆分成3个独立部分：大盘分析 + 持仓池风险 + 候选池抄底
"""

import logging
import os
import sys

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.data_access import get_index_realtime, get_market_sentiment, get_market_volume_compare
from core.paths import REPORTS_DIR
from signal_detector import detect_all_signals

logger = logging.getLogger(__name__)


def generate_market_report(result):
    """
    生成大盘分析报告（部分1）
    
    参数:
        result: WatchMonitorStrategy.run() 的返回值
        
    返回:
        str: 大盘分析报告文本
    """
    date_str = result['date_str']
    top_sectors = result['top_sectors']
    has_signal = result['has_signal']
    skip_reason = result.get('skip_reason', '')
    sector_details = result.get('sector_details', [])

    lines = []
    lines.append(f"📊 盯盘助手 — {date_str}")
    lines.append("")
    lines.append("***")
    lines.append("")

    # ========== 市场概况 ==========
    try:
        indices = get_index_realtime()
        sentiment = get_market_sentiment()
        volume = get_market_volume_compare()

        # 主要指数
        if indices:
            lines.append("## 📈 主要指数")
            lines.append("")
            show_indices = ['上证指数', '深证成指', '创业板指', '沪深300', '中证500', '中证1000']
            for name in show_indices:
                if name in indices:
                    idx = indices[name]
                    sign = '+' if idx['change_pct'] >= 0 else ''
                    if idx['change_pct'] > 0:
                        lines.append(f"**<font color=\"red\">{name}</font>**: {idx['price']:.2f} **<font color=\"red\">({sign}{idx['change_pct']:.2f}%)</font>**")
                    elif idx['change_pct'] < 0:
                        lines.append(f"**<font color=\"green\">{name}</font>**: {idx['price']:.2f} **<font color=\"green\">({sign}{idx['change_pct']:.2f}%)</font>**")
                    else:
                        lines.append(f"**{name}**: {idx['price']:.2f} **({sign}{idx['change_pct']:.2f}%)**")
            lines.append("")

        # 市场分化
        if indices and '上证指数' in indices and '中证全指' in indices:
            lines.append("## 🔀 市场分化")
            lines.append("")
            sh_change = indices['上证指数']['change_pct']
            zz_change = indices['中证全指']['change_pct']
            if sh_change != 0:
                ratio = zz_change / sh_change
                # 根据涨跌趋势和分化系数判断市场状态
                if ratio < 0:  # 分化系数为负：上证和中证全指方向相反
                    if sh_change > 0 and zz_change < 0:
                        desc = '🐊 鳄鱼张嘴（指数红个股跌，主力拉指数掩护出货，危险！）'
                    else:
                        desc = '🦅 题材先行（权重跌，题材涨，风格可能在切换）'
                elif sh_change > 0.3:  # 上涨趋势
                    if ratio > 1.2:
                        desc = '🟢 权重搭台，题材唱戏（大盘稳，题材涨，赚钱效应明显）'
                    elif ratio < 0.8:
                        desc = '🟡 只赚指数不赚钱（涨指数不涨题材，赚钱效应弱）'
                    else:
                        desc = '🟢 普涨行情'
                elif sh_change < -0.3:  # 下跌趋势
                    if ratio > 1.2:
                        desc = '🔴 题材踩踏（散户恐慌抛售题材股，亏钱效应炸裂）'
                    elif ratio < 0.8:
                        desc = '🟡 权重补跌（大盘跌，题材稳，可能见底）'
                    else:
                        desc = '🟠 市场弱势，单边下跌，小心系统性风险'
                else:  # 震荡
                    if abs(ratio - 1) > 0.3:
                        desc = '🜲 二八分化（板块轮动快，板块选对吃肉选错吃面）'
                    else:
                        desc = '⚪ 横盘磨叽（多空僵持，观望为主）'
                lines.append(f"- 上证(加权): {sh_change:+.2f}% | 中证全指(等权): {zz_change:+.2f}%")
                lines.append(f"- 分化系数: **{ratio:.2f}**")
                lines.append(f"- {desc}")
            lines.append("")

        # 市场情绪
        if sentiment:
            lines.append("## 💭 市场情绪")
            lines.append("")
            up = sentiment.get('up', 0)
            down = sentiment.get('down', 0)
            limit_up = sentiment.get('limit_up', 0)
            limit_down = sentiment.get('limit_down', 0)
            if up + down > 0:
                ratio_str = f"1:{down/up:.1f}" if up > 0 else "-"
                if down > up * 2:
                    mood = '🔴偏空'
                elif up > down:
                    mood = '🟢偏多'
                else:
                    mood = '🟡震荡'
                lines.append(f"- 上涨: **{up}**只({limit_up}涨停) | 下跌: **{down}**只({limit_down}跌停)")
                lines.append(f"- 涨跌比: **{ratio_str}** → {mood}")
                activity_rate = sentiment.get('activity_rate', 0)
                if activity_rate:
                    lines.append(f"- 活跃度: **{activity_rate:.2f}%**")

        # 成交量
        if volume:
            today = volume.get('today_amount', 0)
            yesterday = volume.get('yesterday_amount', 0)
            change_pct = volume.get('change_pct', 0)
            is_fang = volume.get('is_fangliang', False)
            direction = '放量' if is_fang else '缩量'
            lines.append(f"大盘成交额: **{today:.0f}亿** vs 昨日{yesterday:.0f}亿 — {direction}{abs(change_pct):.1f}%")
            lines.append("")

    except Exception as e:
        logger.warning(f"获取市场概况失败: {e}")
        lines.append("")

    # ========== 热门板块 ==========
    if not top_sectors:
        lines.append("今日热门板块获取失败。")
        lines.append("")
    else:
        lines.append("## 🔥 今日热门板块TOP10")
        lines.append("")
        top10_attack_names = set()
        for atk in result.get('top10_attack', []):
            top10_attack_names.add(atk['name'])
        for i, s in enumerate(top_sectors[:10], 1):
            name = s['name']
            pct = round(s.get('change_percent', 0), 2)
            sign = '+' if pct > 0 else ''
            if name in top10_attack_names:
                lines.append(f"- {i}. **{name}**: **{sign}{pct}%**")
            else:
                lines.append(f"- {i}. {name}: **{sign}{pct}%**")
        lines.append("")

        # 进攻型板块
        top10_attack = result.get('top10_attack', [])
        if top10_attack:
            attack_names = [s['name'] for s in top10_attack]
            lines.append(f"- 🎯 进攻型板块: {', '.join(attack_names)}")
            lines.append("")

        # 板块详细数据
        if sector_details:
            lines.append("## 板块详细数据")
            lines.append("")
            for detail in sector_details:
                name = detail['name']
                change_pct = detail['change_percent']
                stock_count = detail['stock_count']
                is_attack = detail['is_attack']
                lead_stocks = detail['lead_stocks']
                
                sign = '+' if change_pct > 0 else ''
                attack_flag = '进攻型' if is_attack else ''
                
                lines.append(f"- **{name}** ({sign}{change_pct:.2f}%) {attack_flag}")
                lines.append(f"     成分股: **{stock_count}**只")
                
                if lead_stocks:
                    lead_str = ', '.join([f"{s['name']}({s['change_pct']:+.2f}%)" for s in lead_stocks])
                    lines.append(f"     领涨股: {lead_str}")
                lines.append("")

        # 信号状态
        if not has_signal:
            reason_text = f"（{skip_reason}）" if skip_reason else ""
            lines.append(f"今日{reason_text}，暂无进攻型板块信号。")
        else:
            lines.append(f"今日有进攻型板块信号。")

    return '\n'.join(lines)


def generate_position_report():
    """
    生成持仓池风险信号报告（部分2）
    - 无风险信号时不显示扫雷警告
    - 无风险信号个股展示技术概要分析
    
    返回:
        str: 持仓池风险信号报告文本
    """
    lines = []
    lines.append("## 🚨 持仓池风险信号")
    lines.append("")
    
    try:
        signals_result = detect_all_signals()
        position_risks = signals_result['position_risks']
        
        if not position_risks:
            lines.append("持仓池为空")
            return '\n'.join(lines)
        
        for pr in position_risks:
            code = pr['code']
            name = pr['name']
            position_type = pr['position_type']
            entry_price = pr['entry_price']
            current_price = pr['current_price']
            profit_pct = pr['profit_pct']
            signals = pr['signals']
            mine_result = pr['mine_result']
            indicators = pr.get('indicators', {})  # 技术指标
            
            # 持仓基本信息
            profit_sign = '+' if profit_pct >= 0 else ''
            profit_color = 'red' if profit_pct > 0 else 'green' if profit_pct < 0 else ''
            
            lines.append(f"- **{name} ({code})** — {position_type}持仓")
            if profit_color:
                lines.append(f"  - 成本{entry_price:.2f} | 现价<font color=\"{profit_color}\">{current_price:.2f} ({profit_sign}{profit_pct:.1f}%)</font>")
            else:
                lines.append(f"  - 成本{entry_price:.2f} | 现价{current_price:.2f} ({profit_sign}{profit_pct:.1f}%)")
            lines.append("")
            
            # 筛选真实风险信号（排除扫雷）
            risk_signals = [s for s in signals if s['type'] != 'mine_warning']
            
            # 先显示风险信号（如果有）
            if risk_signals:
                # 按严重度排序显示
                severity_order = {'fatal': 0, 'critical': 1, 'high': 2, 'medium': 3, 'warning': 4, 'info': 5, 'positive': 6}
                sorted_signals = sorted(risk_signals, key=lambda x: severity_order.get(x.get('severity', 'info'), 99))
                
                for sig in sorted_signals:
                    severity = sig.get('severity', 'info')
                    message = sig.get('message', '')
                    
                    # 根据严重度选择图标
                    if severity == 'fatal':
                        icon = '🔴🔴'
                    elif severity == 'critical':
                        icon = '🔴'
                    elif severity == 'high':
                        icon = '🔴'
                    elif severity == 'medium':
                        icon = '⚠️'
                    elif severity == 'warning':
                        icon = '⚠️'
                    elif severity == 'positive':
                        icon = '✅'
                    else:
                        icon = '💡'
                    
                    lines.append(f"  - {icon} {message}")
            else:
                lines.append("  - ✅ 暂无风险信号")
            lines.append("")
            
            # ========== 技术概要分析（所有情况下都显示） ==========
            if indicators:
                lines.append("  - 📊 技术概要:")
                
                # SuperTrend状态（简化为SuTd）+ 反转预警
                st_dir = indicators.get('st_direction')
                st_upper = indicators.get('st_upper_band')  # 阻力线
                st_lower = indicators.get('st_lower_band')  # 支撑线
                
                st_dir_text = '多头⬆' if st_dir == 1 else '空头⬇' if st_dir == -1 else 'N/A'
                
                # SuperTrend反转预警
                st_warning = ''
                if st_dir == 1 and st_lower and current_price:  # 多头状态
                    # 多空切换点是支撑线，跌破转为空头
                    gap_pct = (current_price - st_lower) / st_lower * 100
                    if gap_pct > 0:
                        st_warning = f"（多→空切换点{st_lower:.2f}，距-{gap_pct:.1f}%）"
                    else:
                        st_warning = f"（⚠️已跌破多→空切换{st_lower:.2f}）"
                elif st_dir == -1 and st_upper and current_price:  # 空头状态
                    # 多空切换点是阻力线，突破转为多头
                    gap_pct = (current_price - st_upper) / st_upper * 100
                    if gap_pct < 0:
                        st_warning = f"（空→多切换点{st_upper:.2f}，距+{-gap_pct:.1f}%）"
                    else:
                        st_warning = f"（⚠️已突破空→多切换点{st_upper:.2f}）"
                
                lines.append(f"    - SuTd: {st_dir_text} {st_warning}")
                
                # RSI状态
                rsi = indicators.get('rsi_14')
                rsi_text = f"RSI: {rsi:.1f}" if rsi else "RSI: N/A"
                rsi_status = ''
                if rsi:
                    if rsi > 70:
                        rsi_status = '(超买)'
                    elif rsi < 30:
                        rsi_status = '(超卖)'
                    else:
                        rsi_status = '(中性)'
                lines.append(f"    - {rsi_text}{rsi_status}")
                
                # 均线关系
                ma5 = indicators.get('ma5')
                ma10 = indicators.get('ma10')
                ma20 = indicators.get('ma20')
                ma60 = indicators.get('ma60')
                
                ma_status = []
                if ma5 and current_price:
                    ma_status.append('MA5上方' if current_price > ma5 else 'MA5下方')
                if ma10 and current_price:
                    ma_status.append('MA10上方' if current_price > ma10 else 'MA10下方')
                if ma20 and current_price:
                    ma_status.append('MA20上方' if current_price > ma20 else 'MA20下方')
                if ma60 and current_price:
                    ma_status.append('MA60上方' if current_price > ma60 else 'MA60下方')
                
                lines.append(f"    - 均线位置: {' '.join(ma_status) if ma_status else 'N/A'}")
                
                # 均线斜率
                ma5_slope = indicators.get('ma5_slope')
                ma10_slope = indicators.get('ma10_slope')
                slope_status = []
                if ma5_slope:
                    slope_status.append('MA5⬆' if ma5_slope == 1 else 'MA5⬇' if ma5_slope == -1 else 'MA5→')
                if ma10_slope:
                    slope_status.append('MA10⬆' if ma10_slope == 1 else 'MA10⬇' if ma10_slope == -1 else 'MA10→')
                
                if slope_status:
                    lines.append(f"    - 均线趋势: {' '.join(slope_status)}")
                
                # 综合判断
                if st_dir == 1 and 'MA5上方' in ma_status and 'MA10上方' in ma_status:
                    lines.append(f"    - 📈 短期趋势向上")
                elif st_dir == -1 and 'MA5下方' in ma_status:
                    lines.append(f"    - 📉 短期趋势向下")
                else:
                    lines.append(f"    - 📊 趋势震荡")
            
            # 扫雷警告（从signals中获取详细信息）
            mine_signal = [s for s in signals if s['type'] == 'mine_warning']
            if mine_signal:
                lines.append(f"  - {mine_signal[0]['message']}")

            lines.append("")
            
    except Exception as e:
        logger.warning(f"检测持仓池风险信号失败: {e}")
        lines.append("检测失败，请查看日志")
    
    return '\n'.join(lines)


def generate_candidate_report():
    """
    生成候选池抄底信号报告（部分3）
    
    返回:
        str: 候选池抄底信号报告文本
    """
    lines = []
    lines.append("## 🎯 候选池抄底信号")
    lines.append("")
    
    try:
        signals_result = detect_all_signals()
        candidate_signals = signals_result['candidate_signals']
        
        if not candidate_signals:
            lines.append("候选池为空")
            return '\n'.join(lines)
        
        for cs in candidate_signals:
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
            
            # 基本信息行
            drop_sign = '+' if drop_pct >= 0 else ''
            drop_color = 'red' if drop_pct > 0 else 'green' if drop_pct < 0 else ''
            
            lines.append(f"- **{name} ({code})** — {watch_type}")
            if drop_color:
                lines.append(f"  关注价{watch_price:.2f} | 现价<font color=\"{drop_color}\">{current_price:.2f} ({drop_sign}{drop_pct:.1f}%)</font>")
            else:
                lines.append(f"  关注价{watch_price:.2f} | 现价{current_price:.2f} ({drop_sign}{drop_pct:.1f}%)")
            
            # 评分行
            if stars:
                lines.append(f"  {stars} 抄底评分: **{score}分**（{score_level})")
            else:
                lines.append(f"  抄底评分: {score}分（{score_level})")
            
            # 信号列表（只显示关键信号）
            if signals:
                for sig in signals:
                    if sig['type'] in ['no_data', 'mine_warning']:
                        continue
                    message = sig.get('message', '')
                    lines.append(f"  - {message}")
            
            lines.append("")
            
    except Exception as e:
        logger.warning(f"检测候选池抄底信号失败: {e}")
        lines.append("检测失败，请查看日志")
    
    return '\n'.join(lines)


def generate_report(result):
    """
    生成完整报告（备用，合并3个部分）
    
    参数:
        result: WatchMonitorStrategy.run() 的返回值
        
    返回:
        str: 完整报告文本
    """
    lines = []
    
    # 部分1：大盘分析
    market_report = generate_market_report(result)
    lines.append(market_report)
    lines.append("")
    lines.append("***")
    lines.append("")
    
    # 部分2：持仓池风险
    position_report = generate_position_report()
    lines.append(position_report)
    lines.append("")
    lines.append("***")
    lines.append("")
    
    # 部分3：候选池抄底
    candidate_report = generate_candidate_report()
    lines.append(candidate_report)
    lines.append("")
    lines.append("***")
    lines.append(f"报告已保存")
    
    return '\n'.join(lines)


def save_report_parts(date_str, part1, part2, part3):
    """
    保存3个部分的报告文件
    
    参数:
        date_str: 日期字符串
        part1: 大盘分析报告
        part2: 持仓池风险报告
        part3: 候选池抄底报告
        
    返回:
        tuple: (path1, path2, path3)
    """
    from core.storage import ensure_dirs
    ensure_dirs()
    
    path1 = os.path.join(REPORTS_DIR, f'report_{date_str}_part1.md')
    path2 = os.path.join(REPORTS_DIR, f'report_{date_str}_part2.md')
    path3 = os.path.join(REPORTS_DIR, f'report_{date_str}_part3.md')
    
    with open(path1, 'w', encoding='utf-8') as f:
        f.write(part1)
    
    with open(path2, 'w', encoding='utf-8') as f:
        f.write(part2)
    
    with open(path3, 'w', encoding='utf-8') as f:
        f.write(part3)
    
    return (path1, path2, path3)