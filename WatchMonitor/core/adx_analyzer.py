"""
ADX分析模块 - 盯盘助手专用
用于分析市场ADX情绪分布、持仓股/候选股ADX状态

数据来源：adx_score表（共享stock_data.db）
"""

import sqlite3
import logging
from datetime import datetime
from typing import Optional

from core.paths import DB_PATH

logger = logging.getLogger(__name__)


# ==================== ADX趋势分类 ====================

def classify_trend(adx: float, plus_di: float, minus_di: float) -> tuple[str, str, str]:
    """
    根据ADX和DI值分类趋势状态
    
    返回:
        tuple: (趋势类型, 方向符号, 概要评价)
        
    分类标准:
        - 强多头: ADX≥50, +DI>-DI+5
        - 中等多头: 25-50, +DI>-DI+5
        - 弱多头: 15-25, +DI>-DI
        - 强空头: ADX≥50, -DI>+DI+5
        - 中等空头: 25-50, -DI>+DI+5
        - 弱空头: 15-25, -DI>+DI
        - 趋势不明: ADX≥25, |+-DI|≤5
        - 无趋势: ADX<15
    """
    di_diff = plus_di - minus_di
    abs_di_diff = abs(di_diff)
    
    # 无趋势（最弱）
    if adx < 15:
        return ('无趋势', '⚪', '横盘震荡，无明确方向')
    
    # 趋势不明（ADX≥25但多空僵持）
    if adx >= 25 and abs_di_diff <= 5:
        return ('趋势不明', '🟡', '趋势不明，多空僵持')
    
    # 有趋势且有方向
    if di_diff > 5:  # 多头方向明显
        direction = '↑'
        color = '🟢'
        if adx >= 50:
            return ('强多头', color, '强趋势多头，短期强势向上')
        elif adx >= 25:
            return ('中等多头', color, '趋势中等偏强，短期向上')
        else:  # 15-25
            return ('弱多头', color, '趋势萌芽，方向偏多')
    elif di_diff < -5:  # 空头方向明显
        direction = '↓'
        color = '🔴'
        if adx >= 50:
            return ('强空头', color, '强趋势空头，短期强势向下')
        elif adx >= 25:
            return ('中等空头', color, '趋势中等偏弱，短期向下')
        else:  # 15-25
            return ('弱空头', color, '趋势萌芽，方向偏空')
    else:  # 方向不明显
        return ('趋势不明', '🟡', '趋势强度适中，方向不清晰')


def format_adx_display(adx: float, plus_di: float, minus_di: float) -> str:
    """
    格式化ADX展示（用于持仓/候选池）
    
    返回:
        str: 如 "ADX: 35↑ 多头" 或 "ADX: 18⚪ 无趋势"
    """
    trend_type, color, summary = classify_trend(adx, plus_di, minus_di)
    
    # 方向箭头
    if trend_type in ('强多头', '中等多头', '弱多头'):
        arrow = '↑'
        direction = '多头'
    elif trend_type in ('强空头', '中等空头', '弱空头'):
        arrow = '↓'
        direction = '空头'
    elif trend_type == '无趋势':
        arrow = ''
        direction = '无趋势'
    else:
        arrow = ''
        direction = '不明'
    
    # 展示格式：颜色 + ADX值+箭头+方向
    return f"{color} ADX: {int(adx)}{arrow} {direction}"


# ==================== 市场ADX情绪统计 ====================

def get_market_adx_distribution(calc_date: Optional[str] = None) -> dict:
    """
    获取市场ADX趋势强度+方向分布
    
    参数:
        calc_date: 计算日期，默认取今日
        
    返回:
        dict: {
            'date': str,
            'distribution': [
                {'type': '强多头', 'count': 431, 'pct': 8.47, 'color': '🟢'},
                ...
            ],
            'summary': str,  # 市场整体评价
        }
    """
    if calc_date is None:
        calc_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # 先检查今日数据是否存在
        cursor.execute("SELECT COUNT(*) FROM adx_score WHERE calc_date = ?", (calc_date,))
        total = cursor.fetchone()[0]
        
        if total == 0:
            logger.warning(f"ADX数据不存在: {calc_date}")
            conn.close()
            return {
                'date': calc_date,
                'distribution': [],
                'summary': 'ADX数据未更新',
            }
        
        # 查询所有数据并分类
        cursor.execute("""
            SELECT adx, plus_di, minus_di
            FROM adx_score
            WHERE calc_date = ?
        """, (calc_date,))
        
        rows = cursor.fetchall()
        conn.close()
        
        # 分类统计
        counts = {
            '强多头': 0,
            '中等多头': 0,
            '弱多头': 0,
            '弱空头': 0,
            '中等空头': 0,
            '强空头': 0,
            '趋势不明': 0,
            '无趋势': 0,
        }
        
        colors = {
            '强多头': '🟢',
            '中等多头': '🟢',
            '弱多头': '🟢',
            '弱空头': '🔴',
            '中等空头': '🔴',
            '强空头': '🔴',
            '趋势不明': '🟡',
            '无趋势': '⚪',
        }
        
        for adx, plus_di, minus_di in rows:
            trend_type, _, _ = classify_trend(adx, plus_di, minus_di)
            counts[trend_type] += 1
        
        # 构建分布列表（按强弱顺序）
        order = ['强多头', '中等多头', '弱多头', '弱空头', '中等空头', '强空头', '趋势不明', '无趋势']
        distribution = []
        for t in order:
            cnt = counts[t]
            pct = round(cnt * 100.0 / total, 2)
            distribution.append({
                'type': t,
                'count': cnt,
                'pct': pct,
                'color': colors[t],
            })
        
        # 市场整体评价
        bullish_count = counts['强多头'] + counts['中等多头'] + counts['弱多头']
        bearish_count = counts['强空头'] + counts['中等空头'] + counts['弱空头']
        trend_count = counts['强多头'] + counts['中等多头'] + counts['强空头'] + counts['中等空头']
        
        if bullish_count > bearish_count * 1.5:
            summary = f"市场偏多，多头{bullish_count}只 > 空头{bearish_count}只"
        elif bearish_count > bullish_count * 1.5:
            summary = f"市场偏空，空头{bearish_count}只 > 多头{bullish_count}只"
        elif trend_count > total * 0.3:
            summary = f"市场分化明显，趋势股{trend_count}只({round(trend_count*100/total,1)}%)"
        else:
            summary = f"市场无明确方向，无趋势股{counts['无趋势']}只({round(counts['无趋势']*100/total,1)}%)"
        
        logger.info(f"ADX分布: 多头{bullish_count}只, 空头{bearish_count}只, 无趋势{counts['无趋势']}只")
        
        return {
            'date': calc_date,
            'distribution': distribution,
            'summary': summary,
            'total': total,
            'bullish_count': bullish_count,
            'bearish_count': bearish_count,
        }
        
    except Exception as e:
        logger.error(f"获取ADX分布失败: {e}")
        return {
            'date': calc_date,
            'distribution': [],
            'summary': f'获取失败: {e}',
        }


# ==================== 持仓/候选股ADX查询 ====================

def get_stock_adx(code: str, calc_date: Optional[str] = None) -> Optional[dict]:
    """
    获取单只股票的ADX数据
    
    参数:
        code: 股票代码
        calc_date: 计算日期，默认今日
        
    返回:
        dict or None: {adx, plus_di, minus_di, trend_type, display, summary}
    """
    if calc_date is None:
        calc_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT adx, plus_di, minus_di
            FROM adx_score
            WHERE code = ? AND calc_date = ?
            ORDER BY calc_date DESC LIMIT 1
        """, (code, calc_date))
        
        row = cursor.fetchone()
        conn.close()
        
        if row is None:
            logger.warning(f"[{code}] ADX数据不存在: {calc_date}")
            return None
        
        adx, plus_di, minus_di = row
        trend_type, color, summary = classify_trend(adx, plus_di, minus_di)
        display = format_adx_display(adx, plus_di, minus_di)
        
        return {
            'code': code,
            'adx': round(adx, 1),
            'plus_di': round(plus_di, 1),
            'minus_di': round(minus_di, 1),
            'trend_type': trend_type,
            'color': color,
            'display': display,
            'summary': summary,
        }
        
    except Exception as e:
        logger.error(f"[{code}] 获取ADX失败: {e}")
        return None


def get_positions_with_adx(calc_date: Optional[str] = None) -> list[dict]:
    """
    获取活跃持仓及其ADX状态
    
    返回:
        list: [{code, name, adx_info}]
    """
    if calc_date is None:
        calc_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # 查询活跃持仓（status != 'CLOSED'）
        cursor.execute("""
            SELECT code, name
            FROM positions
            WHERE status != 'CLOSED'
            ORDER BY opened_at DESC
        """)
        
        positions = cursor.fetchall()
        conn.close()
        
        result = []
        for code, name in positions:
            adx_info = get_stock_adx(code, calc_date)
            result.append({
                'code': code,
                'name': name,
                'adx_info': adx_info,
            })
        
        logger.info(f"持仓ADX查询: {len(result)}只")
        return result
        
    except Exception as e:
        logger.error(f"获取持仓ADX失败: {e}")
        return []


def get_candidates_with_adx(calc_date: Optional[str] = None) -> list[dict]:
    """
    获取候选池及其ADX状态
    
    返回:
        list: [{code, name, watch_type, watch_reason, adx_info}]
    """
    if calc_date is None:
        calc_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # 查询候选池
        cursor.execute("""
            SELECT code, name, watch_type, watch_reason
            FROM candidate_pool
            ORDER BY watch_date DESC
        """)
        
        candidates = cursor.fetchall()
        conn.close()
        
        result = []
        for code, name, watch_type, watch_reason in candidates:
            adx_info = get_stock_adx(code, calc_date)
            result.append({
                'code': code,
                'name': name,
                'watch_type': watch_type or '',
                'watch_reason': watch_reason or '',
                'adx_info': adx_info,
            })
        
        logger.info(f"候选池ADX查询: {len(result)}只")
        return result
        
    except Exception as e:
        logger.error(f"获取候选池ADX失败: {e}")
        return []