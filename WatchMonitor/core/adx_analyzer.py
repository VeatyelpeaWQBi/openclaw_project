"""
ADX分析模块 - 盯盘助手专用
用于分析市场ADX情绪分布、持仓股/候选股ADX状态

数据来源：adx_score表（共享stock_data.db）
"""

import sqlite3
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Optional

from core.paths import DB_PATH

logger = logging.getLogger(__name__)

# 延迟导入以避免循环依赖
_index_adx_config = None
_data_access = None

def _get_index_adx_config():
    global _index_adx_config
    if _index_adx_config is None:
        from core.index_adx_config import (
            INDEX_ADX_CONFIG, INDEX_ADX_VISUAL_COEFFICIENT,
            DI_DIRECTION_THRESHOLD, COMMENT_CONFIG,
            get_index_thresholds
        )
        _index_adx_config = {
            'INDEX_ADX_CONFIG': INDEX_ADX_CONFIG,
            'INDEX_ADX_VISUAL_COEFFICIENT': INDEX_ADX_VISUAL_COEFFICIENT,
            'DI_DIRECTION_THRESHOLD': DI_DIRECTION_THRESHOLD,
            'COMMENT_CONFIG': COMMENT_CONFIG,
            'get_index_thresholds': get_index_thresholds,
        }
    return _index_adx_config

def _get_data_access():
    global _data_access
    if _data_access is None:
        from core.data_access import get_index_daily_kline_from_db
        _data_access = {'get_index_daily_kline_from_db': get_index_daily_kline_from_db}
    return _data_access


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

    颜色表示趋势强度，多头和空头颜色完全区分：
    - 多头：强🟢 / 中🟡 / 弱⚪
    - 空头：强🔴 / 中🟠 / 弱🟤
    箭头表示方向：↑多头 / ↓空头

    返回:
        str: 如 "🟢 ADX: 35↑" 或 "🔴 ADX: 42↓"
    """
    trend_type, _, _ = classify_trend(adx, plus_di, minus_di)

    # 颜色根据方向+强度决定（多头空头完全区分）
    if trend_type in ('强多头', '中等多头', '弱多头'):
        # 多头：强🟢 / 中🟡 / 弱⚪
        if adx >= 50:
            color = '🟢'
        elif adx >= 25:
            color = '🟡'
        else:
            color = '⚪'
        arrow = '↑'
    elif trend_type in ('强空头', '中等空头', '弱空头'):
        # 空头：强🔴 / 中🟠 / 弱🟤
        if adx >= 50:
            color = '🔴'
        elif adx >= 25:
            color = '🟠'
        else:
            color = '🟤'
        arrow = '↓'
    else:
        # 无趋势或趋势不明
        color = '⚪'
        arrow = ''

    # 展示格式：颜色 + ADX值 + 箭头
    return f"{color} ADX: {int(adx)}{arrow}"


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

        # 先尝试查询指定日期的数据
        cursor.execute("""
            SELECT adx, plus_di, minus_di
            FROM adx_score
            WHERE code = ? AND calc_date = ?
            ORDER BY calc_date DESC LIMIT 1
        """, (code, calc_date))

        row = cursor.fetchone()

        # 如果指定日期没有数据，查询最新的数据
        if row is None:
            cursor.execute("""
                SELECT adx, plus_di, minus_di, calc_date
                FROM adx_score
                WHERE code = ?
                ORDER BY calc_date DESC LIMIT 1
            """, (code,))
            result = cursor.fetchone()
            if result:
                adx, plus_di, minus_di, actual_date = result
                row = (adx, plus_di, minus_di)
                logger.debug(f"[{code}] 使用最新ADX数据: {actual_date}")

        conn.close()

        if row is None:
            # ETF无ADX数据是正常的，用debug级别
            is_etf = code.startswith(('51', '159', '56', '58'))
            if is_etf:
                logger.debug(f"[{code}] ETF无ADX数据")
            else:
                logger.warning(f"[{code}] ADX数据不存在")
            return None
        
        adx, plus_di, minus_di = row
        trend_type, color, summary = classify_trend(adx, plus_di, minus_di)
        display = format_adx_display(adx, plus_di, minus_di)
        
        # 查询近4天ADX历史用于趋势展示
        history = get_stock_adx_history(code, days=4, calc_date=calc_date)
        trend = format_adx_trend(history)

        return {
            'code': code,
            'adx': round(adx, 1),
            'plus_di': round(plus_di, 1),
            'minus_di': round(minus_di, 1),
            'trend_type': trend_type,
            'color': color,
            'display': display,
            'summary': summary,
            'history': history,
            'trend': trend,
        }

    except Exception as e:
        logger.error(f"[{code}] 获取ADX失败: {e}")
        return None


def get_stock_adx_history(code: str, days: int = 4, calc_date: Optional[str] = None) -> list[dict]:
    """
    获取个股近N天的ADX历史数据

    参数:
        code: 股票代码
        days: 取最近N天（默认4天，用于计算3天变化）
        calc_date: 截止日期，默认今日

    返回:
        list[dict]: 按日期从新到旧排序，每个元素包含 calc_date/adx/plus_di/minus_di
    """
    if calc_date is None:
        calc_date = datetime.now().strftime('%Y-%m-%d')

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 查询截止日期前最近N天的ADX数据
        cursor.execute("""
            SELECT calc_date, adx, plus_di, minus_di
            FROM adx_score
            WHERE code = ? AND calc_date <= ?
            ORDER BY calc_date DESC
            LIMIT ?
        """, (code, calc_date, days))

        rows = cursor.fetchall()
        conn.close()

        history = []
        for row in rows:
            history.append({
                'calc_date': row[0],
                'adx': round(row[1], 1) if row[1] is not None else None,
                'plus_di': round(row[2], 1) if row[2] is not None else None,
                'minus_di': round(row[3], 1) if row[3] is not None else None,
            })

        return history

    except Exception as e:
        logger.warning(f"[{code}] 获取ADX历史失败: {e}")
        return []


def format_adx_trend(history: list[dict]) -> str:
    """
    格式化ADX近3天变化趋势

    参数:
        history: 近4天ADX数据（新→旧），来自 get_stock_adx_history 或 get_index_adx

    返回:
        str: （↑+0.1，↓-0.2，→0.0），数据不足时返回空字符串
    """
    if not history or len(history) < 2:
        return ''

    parts = []
    # 计算近 min(3, len(history)-1) 天的变化
    for i in range(min(3, len(history) - 1)):
        curr_adx = history[i].get('adx')
        prev_adx = history[i + 1].get('adx')

        if curr_adx is None or prev_adx is None:
            continue

        delta = round(curr_adx - prev_adx, 1)

        if abs(delta) < 0.05:
            arrow = '→'
            delta_str = f'{delta:.1f}'
        elif delta > 0:
            arrow = '↑'
            delta_str = f'+{delta:.1f}'
        else:
            arrow = '↓'
            delta_str = f'{delta:.1f}'

        parts.append(f'{arrow}{delta_str}')

    if not parts:
        return ''

    return '（' + '，'.join(parts) + '）'


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


# ==================== 本地ADX计算（避免跨包导入问题） ====================

def _wilder_rma(series: pd.Series, period: int) -> pd.Series:
    """怀尔德平滑法（RMA）"""
    import numpy as np
    result = pd.Series(np.nan, index=series.index, dtype=float)
    if len(series) < period:
        return result
    first_sma = series.iloc[:period].mean()
    values = series.values
    rma_val = first_sma
    result_arr = np.full(len(values), np.nan)
    result_arr[period - 1] = first_sma
    alpha = 1.0 / period
    for i in range(period, len(values)):
        rma_val = rma_val * (1.0 - alpha) + values[i] * alpha
        result_arr[i] = rma_val
    return pd.Series(result_arr, index=series.index)


def _calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """计算完整ADX系统（+DI, -DI, DX, ADX）"""
    import numpy as np
    high = df['high']
    low = df['low']
    close = df['close']
    prev_close = close.shift(1)

    # Step 1: True Range
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)

    # Step 2: Directional Movement
    up_move = high - high.shift(1)
    down_move = low.shift(1) - low
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    # Step 3: Wilder Smoothing
    str_smooth = _wilder_rma(tr, period)
    plus_dm_smooth = _wilder_rma(pd.Series(plus_dm, index=df.index), period)
    minus_dm_smooth = _wilder_rma(pd.Series(minus_dm, index=df.index), period)

    # Step 4: Directional Indicators
    di_denom = str_smooth.replace(0, np.nan)
    plus_di = 100.0 * plus_dm_smooth / di_denom
    minus_di = 100.0 * minus_dm_smooth / di_denom

    # Step 5: DX
    di_diff = (plus_di - minus_di).abs()
    di_sum = (plus_di + minus_di).replace(0, np.nan)
    dx = 100.0 * di_diff / di_sum

    # Step 6: ADX
    adx = _wilder_rma(dx, period)

    # 预热期掩码
    warmup = 2 * period - 1
    if len(adx) >= warmup:
        adx.iloc[:warmup] = np.nan

    result = df.copy()
    result['plus_di'] = plus_di
    result['minus_di'] = minus_di
    result['dx'] = dx
    result['adx'] = adx
    return result


# ==================== 指数ADX计算与展示 ====================

def format_index_adx_display(adx: float, plus_di: float, minus_di: float) -> str:
    """
    格式化指数ADX展示（大盘视觉补偿版）

    与个股format_adx_display格式一致，但使用1.2x系数进行强度分类：
    - 调整后ADX值仅用于EMOJI判断，展示数值保持原始ADX
    - 方向箭头仍基于原始+DI/-DI判断

    返回:
        str: 如 "🟢 ADX: 57↑"
    """
    cfg = _get_index_adx_config()
    coef = cfg['INDEX_ADX_VISUAL_COEFFICIENT']
    threshold = cfg['DI_DIRECTION_THRESHOLD']
    adjusted_adx = adx * coef

    # 方向判定（使用原始DI）
    di_diff = plus_di - minus_di

    # 颜色根据方向+强度决定（基于adjusted_adx）
    if di_diff > threshold:  # 多头方向明显
        if adjusted_adx >= 50:
            color = '🟢'
        elif adjusted_adx >= 25:
            color = '🟡'
        else:
            color = '⚪'
        arrow = '↑'
    elif di_diff < -threshold:  # 空头方向明显
        if adjusted_adx >= 50:
            color = '🔴'
        elif adjusted_adx >= 25:
            color = '🟠'
        else:
            color = '🟤'
        arrow = '↓'
    else:  # 方向不明显
        if adjusted_adx >= 25:
            color = '🟡'
        else:
            color = '⚪'
        arrow = ''

    return f"{color} ADX: {int(adx)}{arrow}"


def _get_di_direction(plus_di: float, minus_di: float, threshold: float = 5.0) -> str:
    """获取DI方向分类：多头/空头/横盘"""
    di_diff = plus_di - minus_di
    if di_diff > threshold:
        return '多头'
    elif di_diff < -threshold:
        return '空头'
    else:
        return '横盘'


def generate_index_short_comment(adx_info: dict, thresholds: dict) -> str:
    """
    指数ADX短评生成引擎

    按优先级选择一条短评：
    1. 方向转换检测（对比前日状态）
    2. 历史极值检测
    3. ADX连续变化（3日）
    4. 中位对比
    5. 默认无特殊状态时返回空字符串

    参数:
        adx_info: get_index_adx返回的dict
        thresholds: 该指数3方向统计阈值

    返回:
        str: 短评文本，或空字符串
    """
    cfg = _get_index_adx_config()
    comment_cfg = cfg['COMMENT_CONFIG']
    di_threshold = cfg['DI_DIRECTION_THRESHOLD']

    adx = adx_info.get('adx', 0)
    plus_di = adx_info.get('plus_di', 0)
    minus_di = adx_info.get('minus_di', 0)
    prev_adx = adx_info.get('prev_adx')
    prev_plus_di = adx_info.get('prev_plus_di')
    prev_minus_di = adx_info.get('prev_minus_di')
    recent_adx_series = adx_info.get('recent_adx_series', [])

    # 需要前日数据才能做的检测
    if prev_plus_di is not None and prev_minus_di is not None:
        prev_dir = _get_di_direction(prev_plus_di, prev_minus_di, di_threshold)
        curr_dir = _get_di_direction(plus_di, minus_di, di_threshold)

        # 1. 方向转换检测（最高优先级）
        if prev_dir != curr_dir:
            transitions = {
                ('多头', '空头'): '⚠️ 多转空，趋势转向',
                ('多头', '横盘'): '⚠️ 多转平，趋势放缓',
                ('空头', '多头'): '⚠️ 空转多，趋势反转',
                ('空头', '横盘'): '⚠️ 空转平，跌势放缓',
                ('横盘', '多头'): '⚠️ 平转多，趋势启动',
                ('横盘', '空头'): '⚠️ 平转空，趋势转弱',
            }
            comment = transitions.get((prev_dir, curr_dir))
            if comment:
                return comment

    # 2. 历史极值检测
    curr_dir = _get_di_direction(plus_di, minus_di, di_threshold)
    dir_thresholds = thresholds.get(curr_dir, {})
    if dir_thresholds:
        hist_max = dir_thresholds.get('max', 0)
        hist_median = dir_thresholds.get('median', 0)
        extreme_threshold = comment_cfg['extreme_threshold']

        if hist_max > 0 and adx > hist_max * extreme_threshold:
            return '🔥 ADX接近历史极值'

        # 4. 中位对比（在极值之后、连续变化之后）
        median_high = comment_cfg['median_high_threshold']
        median_low = comment_cfg['median_low_threshold']

        if hist_median > 0:
            if adx > hist_median * median_high:
                return '📊 高于历史中位'
            elif adx < hist_median * median_low:
                return '📊 低于历史中位'

    # 3. ADX连续变化检测（需要至少3日序列）
    if len(recent_adx_series) >= comment_cfg['consecutive_days']:
        # 连续上升检测
        consecutive_up = all(
            recent_adx_series[i] > recent_adx_series[i - 1]
            for i in range(1, len(recent_adx_series))
        )
        # 连续下降检测
        consecutive_down = all(
            recent_adx_series[i] < recent_adx_series[i - 1]
            for i in range(1, len(recent_adx_series))
        )

        if consecutive_up and adx > comment_cfg['adx_strengthen_min']:
            return '📈 趋势在加强'
        if consecutive_down and prev_adx and prev_adx > comment_cfg['adx_weaken_max']:
            return '📉 趋势在减弱'

    # 5. 默认无特殊状态
    return ''


def get_index_adx(index_code: str, calc_date: Optional[str] = None) -> Optional[dict]:
    """
    获取单只指数的ADX数据（实时计算）

    参数:
        index_code: 指数代码（如 '000001'）
        calc_date: 计算日期 'YYYY-MM-DD'，默认取最新日期

    返回:
        dict or None: {
            'adx': float,           # 原始ADX值（保留1位小数）
            'plus_di': float,       # +DI
            'minus_di': float,      # -DI
            'trend_type': str,      # classify_trend结果
            'display': str,         # format_index_adx_display结果
            'summary': str,         # classify_trend的summary
            'short_comment': str,   # 生成的短评
            'prev_adx': float,      # 前日ADX
            'prev_plus_di': float,  # 前日+DI
            'prev_minus_di': float, # 前日-DI
            'recent_adx_series': list,  # 最近N日ADX序列（用于连续变化检测）
        }
    """
    try:
        da = _get_data_access()
        get_kline = da['get_index_daily_kline_from_db']
        cfg = _get_index_adx_config()
        get_thresholds = cfg['get_index_thresholds']

        # 1. 获取指数K线（60天，留足ADX预热期27根+余量）
        df = get_kline(index_code, days=60)
        if df.empty or len(df) < 27:
            logger.warning(f"[{index_code}] 指数K线不足{len(df)}条，无法计算ADX")
            return None

        # 2. 计算ADX
        result_df = _calculate_adx(df, period=14)
        valid = result_df[result_df['adx'].notna()].copy()
        if valid.empty:
            logger.warning(f"[{index_code}] ADX计算结果为空")
            return None

        # 3. 取目标日期数据（默认最新）
        if calc_date:
            target = valid[valid['date'].astype(str).str[:10] == calc_date]
            if target.empty:
                logger.warning(f"[{index_code}] 指定日期{calc_date}无ADX数据")
                return None
        else:
            target = valid.iloc[[-1]]

        row = target.iloc[-1]
        adx = float(row['adx'])
        plus_di = float(row['plus_di'])
        minus_di = float(row['minus_di'])

        # 4. 取前日数据（用于方向转换检测）
        prev_adx = prev_plus_di = prev_minus_di = None
        if len(valid) >= 2:
            prev_row = valid.iloc[-2]
            prev_adx = float(prev_row['adx'])
            prev_plus_di = float(prev_row['plus_di'])
            prev_minus_di = float(prev_row['minus_di'])

        # 5. 取最近3日ADX序列（用于连续变化检测，保持原有逻辑不变）
        recent_adx_series = []
        if len(valid) >= 3:
            recent_adx_series = valid['adx'].tail(3).tolist()

        # 5b. 取最近4日完整ADX历史（用于近3天趋势展示）
        history = []
        for i in range(min(4, len(valid))):
            row_hist = valid.iloc[-(i + 1)]
            history.append({
                'calc_date': str(row_hist['date'])[:10] if 'date' in row_hist else '',
                'adx': float(row_hist['adx']) if pd.notna(row_hist['adx']) else None,
                'plus_di': float(row_hist['plus_di']) if pd.notna(row_hist['plus_di']) else None,
                'minus_di': float(row_hist['minus_di']) if pd.notna(row_hist['minus_di']) else None,
            })
        trend = format_adx_trend(history)

        # 6. 趋势分类与展示
        trend_type, color, summary = classify_trend(adx, plus_di, minus_di)
        display = format_index_adx_display(adx, plus_di, minus_di)

        # 7. 生成短评
        thresholds = get_thresholds(index_code)
        adx_info = {
            'adx': adx,
            'plus_di': plus_di,
            'minus_di': minus_di,
            'prev_adx': prev_adx,
            'prev_plus_di': prev_plus_di,
            'prev_minus_di': prev_minus_di,
            'recent_adx_series': recent_adx_series,
        }
        short_comment = generate_index_short_comment(adx_info, thresholds)

        return {
            'index_code': index_code,
            'adx': round(adx, 1),
            'plus_di': round(plus_di, 1),
            'minus_di': round(minus_di, 1),
            'trend_type': trend_type,
            'color': color,
            'display': display,
            'summary': summary,
            'short_comment': short_comment,
            'prev_adx': round(prev_adx, 1) if prev_adx else None,
            'prev_plus_di': round(prev_plus_di, 1) if prev_plus_di else None,
            'prev_minus_di': round(prev_minus_di, 1) if prev_minus_di else None,
            'recent_adx_series': [round(x, 1) for x in recent_adx_series],
            'history': history,
            'trend': trend,
        }

    except Exception as e:
        logger.error(f"[{index_code}] 获取指数ADX失败: {e}")
        return None