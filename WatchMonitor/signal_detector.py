"""
信号检测引擎 - 盯盘助手专用
用于检测持仓池风险信号和候选池抄底信号
"""

import logging
import pandas as pd
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from typing import List, Dict, Optional

from core.storage import (
    get_daily_data_from_sqlite,
    get_all_positions,
    get_all_candidates
)
from core.indicators import (
    calculate_all_indicators,
    calculate_ma,
    calculate_ma_slope,
    check_ma_breakdown,
    check_ma_cross,
    calculate_supertrend,
    check_supertrend_flip,
    get_weekly_kline,
    check_divergence,
    calculate_macd,
    calculate_rsi,
    calculate_volume_ratio,
    identify_candle_patterns,
    check_volume_stagnation,
    check_high_long_upper_shadow,
    check_breakdown_big_bull_candle
)

logger = logging.getLogger(__name__)


# ==================== 扫雷接口 ====================

def call_mine_clearance(code: str) -> Optional[Dict]:
    """
    调用扫雷接口
    
    参数:
        code: 股票代码
        
    返回:
        dict: 扫雷结果 或 None（调用失败）
    """
    try:
        import adata
        result = adata.sentiment.mine.mine_clearance_tdx(code)
        # adata返回DataFrame，需要正确判断
        if result is not None and not result.empty:
            # 如果有数据，说明有风险
            return {
                'code': code,
                'has_mine': True,
                'mine_details': result.to_dict() if hasattr(result, 'to_dict') else str(result)
            }
        return {'code': code, 'has_mine': False}
    except Exception as e:
        logger.warning(f"扫雷接口调用失败 [{code}]: {e}")
        return None


# ==================== 持仓池风险信号检测 ====================

def detect_position_risk_signals(position: Dict) -> Dict:
    """
    检测单只持仓的风险信号
    
    参数:
        position: 持仓信息 dict
        
    返回:
        dict: {
            'code': str,
            'name': str,
            'signals': list,  # 信号列表
            'current_price': float,
            'profit_pct': float,
            'mine_result': dict  # 扫雷结果
        }
    """
    code = position['code']
    name = position['name']
    entry_price = position['entry_price']
    position_type = position.get('position_type', '趋势')
    stop_loss = position.get('stop_loss')
    take_profit = position.get('take_profit')
    
    result = {
        'code': code,
        'name': name,
        'position_type': position_type,
        'entry_price': entry_price,
        'signals': [],
        'current_price': None,
        'profit_pct': None,
        'mine_result': None,
        'indicators': {}  # 新增：技术指标数据
    }
    
    # 获取日K数据
    df = get_daily_data_from_sqlite(code, days=340)
    if df.empty:
        logger.warning(f"未找到 {code} 的日K数据")
        result['signals'].append({
            'type': 'no_data',
            'severity': 'error',
            'message': '无日K数据'
        })
        return result
    
    # 计算技术指标
    indicators = calculate_all_indicators(df)
    current_price = df['close'].iloc[-1]
    result['current_price'] = current_price
    result['profit_pct'] = (current_price - entry_price) / entry_price * 100 if entry_price else 0
    result['indicators'] = indicators  # 新增：保存技术指标
    
    # ========== 1. 均线破位检测（根据持仓类型选择关注级别）==========
    ma_periods_by_type = {
        '短线': [5, 10],
        '波段': [10, 20],
        '趋势': [20, 60, 120]
    }
    
    focus_periods = ma_periods_by_type.get(position_type, [20, 60])
    breakdown_signals = check_ma_breakdown(df, focus_periods)
    
    for sig in breakdown_signals:
        period = sig['period']
        severity = sig['severity']
        ma_val = sig['ma_value']
        
        # 根据持仓类型调整严重度
        if position_type == '趋势' and period >= 60:
            severity = 'critical'
        elif position_type == '波段' and period == 20:
            severity = 'high'
        elif position_type == '短线' and period == 5:
            severity = 'medium'
        
        result['signals'].append({
            'type': 'ma_breakdown',
            'period': period,
            'severity': severity,
            'message': f"跌破{period}日均线（MA{period}={ma_val:.2f}）",
            'value': ma_val
        })
    
    # ========== 2. 均线死叉检测 ==========
    for fast, slow in [(5, 10), (5, 20)]:
        cross_signal = check_ma_cross(df, fast, slow)
        if cross_signal and cross_signal['type'] == 'death_cross':
            result['signals'].append({
                'type': 'death_cross',
                'severity': 'medium',
                'message': f"MA{fast}下穿MA{slow}（死叉）",
                'fast': fast,
                'slow': slow
            })
    
    # ========== 3. SuperTrend翻空检测 ==========
    st_flip = check_supertrend_flip(df)
    if st_flip and st_flip['type'] == 'flip_to_bear':
        severity = 'high'
        
        # 周线SuperTrend检测（更高优先级）
        weekly_df = get_weekly_kline(df)
        if not weekly_df.empty:
            weekly_st_flip = check_supertrend_flip(weekly_df)
            if weekly_st_flip and weekly_st_flip['type'] == 'flip_to_bear':
                severity = 'critical'
                result['signals'].append({
                    'type': 'weekly_st_flip_bear',
                    'severity': 'critical',
                    'message': '⚠️ 周线SuperTrend翻空（极高风险）'
                })
        
        result['signals'].append({
            'type': 'st_flip_bear',
            'severity': severity,
            'message': f"日线SuperTrend翻空",
            'upper_band': st_flip.get('upper_band')
        })
    
    # ========== 4. 顶部结构检测 ==========
    
    # 放量滞涨
    stagnation = check_volume_stagnation(df)
    if stagnation:
        result['signals'].append({
            'type': 'volume_stagnation',
            'severity': 'medium',
            'message': f"放量滞涨（量比{stagnation['volume_ratio']:.2f}，涨幅{stagnation['change_pct']:.1f}%）"
        })
    
    # 高位长上影线
    long_upper = check_high_long_upper_shadow(df)
    if long_upper:
        result['signals'].append({
            'type': 'high_long_upper_shadow',
            'severity': 'medium',
            'message': '高位长上影线'
        })
    
    # 跌破新高大阳线底部
    breakdown_bull = check_breakdown_big_bull_candle(df)
    if breakdown_bull:
        result['signals'].append({
            'type': 'breakdown_big_bull_candle',
            'severity': 'high',
            'message': breakdown_bull['message'],
            'big_bull_low': breakdown_bull.get('big_bull_low'),
            'big_bull_change': breakdown_bull.get('big_bull_change')
        })
    
    # 顶背离
    macd_divergence = check_divergence(df, indicator='macd')
    if macd_divergence and macd_divergence['type'] == 'top_divergence':
        result['signals'].append({
            'type': 'top_divergence',
            'severity': 'high',
            'message': 'MACD顶背离（价格新高但指标未新高）'
        })
    
    rsi_divergence = check_divergence(df, indicator='rsi')
    if rsi_divergence and rsi_divergence['type'] == 'top_divergence':
        result['signals'].append({
            'type': 'rsi_top_divergence',
            'severity': 'high',
            'message': 'RSI顶背离'
        })
    
    # ========== 5. 止损止盈检测 ==========
    
    # 接近止损价（3%以内）
    if stop_loss and current_price <= stop_loss * 1.03:
        distance_pct = (current_price - stop_loss) / stop_loss * 100
        if current_price <= stop_loss:
            result['signals'].append({
                'type': 'hit_stop_loss',
                'severity': 'fatal',
                'message': f'🔴🔴 已触及止损价（{stop_loss:.2f}）！'
            })
        else:
            result['signals'].append({
                'type': 'near_stop_loss',
                'severity': 'high',
                'message': f"接近止损价（止损{stop_loss:.2f}，距离{distance_pct:.1f}%）"
            })
    
    # 接近止盈价
    if take_profit and current_price >= take_profit * 0.97:
        if current_price >= take_profit:
            result['signals'].append({
                'type': 'hit_take_profit',
                'severity': 'positive',
                'message': f'✅ 已触及止盈价（{take_profit:.2f}）'
            })
        else:
            result['signals'].append({
                'type': 'near_take_profit',
                'severity': 'info',
                'message': f"接近止盈价（{take_profit:.2f}）"
            })
    
    # 利润回撤超50%
    if result['profit_pct'] > 0:
        # 需要追踪最高利润（简化处理：假设最高利润就是当前profit_pct）
        # 实际应该追踪历史最高点
        pass  # TODO: 需要历史数据追踪
    
    # ========== 6. 扫雷检测 ==========
    mine_result = call_mine_clearance(code)
    result['mine_result'] = mine_result
    if mine_result and mine_result.get('has_mine'):
        result['signals'].append({
            'type': 'mine_warning',
            'severity': 'warning',
            'message': '⚠️ 扫雷检测有风险'
        })
    
    return result


def detect_all_position_risks() -> List[Dict]:
    """
    检测所有持仓的风险信号
    
    返回:
        list: 所有持仓的风险检测结果
    """
    positions = get_all_positions()
    results = []
    
    for pos in positions:
        result = detect_position_risk_signals(pos)
        results.append(result)
    
    # 按风险严重度排序
    severity_order = {'fatal': 0, 'critical': 1, 'high': 2, 'medium': 3, 'warning': 4, 'info': 5, 'positive': 6}
    
    def get_max_severity(signals):
        if not signals:
            return 99
        severities = [s.get('severity', 'info') for s in signals]
        return min([severity_order.get(s, 99) for s in severities])
    
    results.sort(key=lambda x: get_max_severity(x['signals']))
    
    return results


# ==================== 候选池抄底信号检测 ====================

def detect_candidate_bottom_signals(candidate: Dict) -> Dict:
    """
    检测单只候选的抄底信号
    
    参数:
        candidate: 候选信息 dict
        
    返回:
        dict: {
            'code': str,
            'name': str,
            'signals': list,
            'score': int,  # 综合评分 0-100
            'mine_result': dict
        }
    """
    code = candidate['code']
    name = candidate['name']
    watch_price = candidate['watch_price']
    watch_type = candidate.get('watch_type', '趋势回调')
    
    result = {
        'code': code,
        'name': name,
        'watch_type': watch_type,
        'watch_price': watch_price,
        'signals': [],
        'score': 0,
        'current_price': None,
        'drop_pct': None,
        'mine_result': None
    }
    
    # 获取日K数据
    df = get_daily_data_from_sqlite(code, days=340)
    if df.empty:
        logger.warning(f"未找到 {code} 的日K数据")
        result['signals'].append({
            'type': 'no_data',
            'severity': 'error',
            'message': '无日K数据'
        })
        return result
    
    # 计算技术指标
    indicators = calculate_all_indicators(df)
    current_price = df['close'].iloc[-1]
    result['current_price'] = current_price
    result['drop_pct'] = (current_price - watch_price) / watch_price * 100 if watch_price else 0
    
    score = 0
    
    # ========== 1. 企稳信号检测 ==========
    
    # 3日不新低
    recent_low_3d = df['low'].iloc[-4:-1].min()
    today_low = df['low'].iloc[-1]
    if today_low > recent_low_3d:
        score += 10
        result['signals'].append({
            'type': 'no_new_low_3d',
            'strength': 'weak',
            'message': '✅ 3日不新低',
            'score_add': 10
        })
    
    # 5日不新低
    recent_low_5d = df['low'].iloc[-6:-1].min()
    if today_low > recent_low_5d:
        score += 15
        result['signals'].append({
            'type': 'no_new_low_5d',
            'strength': 'medium',
            'message': '✅ 5日不新低',
            'score_add': 15
        })
    
    # 关键均线企稳（根据关注类型选择均线）
    focus_ma_by_type = {
        '趋势回调': [20, 60],
        '底部反转': [60, 120, 250],
        '突破回踩': [5, 10]
    }
    
    focus_mas = focus_ma_by_type.get(watch_type, [60])
    for period in focus_mas:
        ma_val = indicators.get(f'ma{period}')
        if ma_val:
            # 判断是否在均线附近震荡（上下5%）
            upper_bound = ma_val * 1.05
            lower_bound = ma_val * 0.95
            if current_price >= lower_bound and current_price <= upper_bound:
                score += 20
                result['signals'].append({
                    'type': 'ma_support',
                    'period': period,
                    'strength': 'strong',
                    'message': f'✅ {period}日均线企稳（MA{period}={ma_val:.2f}）',
                    'score_add': 20
                })
                break  # 只加一次
    
    # 温和放量企稳
    vr5 = indicators.get('volume_ratio_5', 1.0)
    vr20 = indicators.get('volume_ratio_20', 1.0)
    if 1.2 <= vr5 <= 1.6:
        score += 10
        result['signals'].append({
            'type': 'moderate_volume',
            'strength': 'strong',
            'message': f'✅ 温和放量（量比{vr5:.2f}）',
            'score_add': 10
        })
    
    # ========== 2. 均线转向信号检测 ==========
    
    # MA5转向向上
    ma5_slope = indicators.get('ma5_slope')
    if ma5_slope == 1:
        score += 10
        result['signals'].append({
            'type': 'ma5_turning_up',
            'strength': 'medium',
            'message': '✅ 5日均线转向向上',
            'score_add': 10
        })
    
    # MA10转向向上
    ma10_slope = indicators.get('ma10_slope')
    if ma10_slope == 1:
        score += 15
        result['signals'].append({
            'type': 'ma10_turning_up',
            'strength': 'strong',
            'message': '✅ 10日均线转向向上',
            'score_add': 15
        })
    
    # 金叉检测
    for fast, slow in [(5, 10), (5, 20)]:
        cross_signal = check_ma_cross(df, fast, slow)
        if cross_signal and cross_signal['type'] == 'golden_cross':
            add_score = 15 if slow == 10 else 20
            score += add_score
            result['signals'].append({
                'type': 'golden_cross',
                'fast': fast,
                'slow': slow,
                'strength': 'strong',
                'message': f'✅ MA{fast}金叉MA{slow}',
                'score_add': add_score
            })
    
    # K线实体上穿均线
    ma5 = indicators.get('ma5')
    ma10 = indicators.get('ma10')
    ma20 = indicators.get('ma20')
    
    prev_close = df['close'].iloc[-2] if len(df) > 1 else current_price
    
    if ma5 and prev_close < ma5 and current_price > ma5:
        score += 10
        result['signals'].append({
            'type': 'cross_ma5',
            'strength': 'strong',
            'message': '✅ K线实体上穿MA5',
            'score_add': 10
        })
    
    if ma10 and prev_close < ma10 and current_price > ma10:
        score += 15
        result['signals'].append({
            'type': 'cross_ma10',
            'strength': 'strong',
            'message': '✅ K线实体上穿MA10',
            'score_add': 15
        })
    
    if ma20 and prev_close < ma20 and current_price > ma20:
        score += 20
        result['signals'].append({
            'type': 'cross_ma20',
            'strength': 'very_strong',
            'message': '✅ K线实体上穿MA20',
            'score_add': 20
        })
    
    # ========== 3. 底部结构信号检测 ==========
    
    # 底背离
    macd_divergence = check_divergence(df, indicator='macd')
    if macd_divergence and macd_divergence['type'] == 'bottom_divergence':
        score += 25
        result['signals'].append({
            'type': 'bottom_divergence',
            'strength': 'very_strong',
            'message': '✅ MACD底背离（价格新低但指标未新低）',
            'score_add': 25
        })
    
    rsi_divergence = check_divergence(df, indicator='rsi')
    if rsi_divergence and rsi_divergence['type'] == 'bottom_divergence':
        score += 25
        result['signals'].append({
            'type': 'rsi_bottom_divergence',
            'strength': 'very_strong',
            'message': '✅ RSI底背离',
            'score_add': 25
        })
    
    # 缩量至极致后放量
    vr20 = indicators.get('volume_ratio_20', 1.0)
    if vr20 < 0.5:  # 极度缩量
        # 检查最近是否放量回升
        recent_vr = calculate_volume_ratio(df, 5)
        if recent_vr > 1.0:
            score += 15
            result['signals'].append({
                'type': 'volume_shrink_extreme',
                'strength': 'strong',
                'message': '✅ 缩量至极致后放量',
                'score_add': 15
            })
    
    # 长下影线
    patterns = identify_candle_patterns(df)
    if patterns.get('is_long_lower_shadow'):
        score += 10
        result['signals'].append({
            'type': 'long_lower_shadow',
            'strength': 'medium',
            'message': '✅ 长下影线',
            'score_add': 10
        })
    
    # ========== 4. 综合评分 ==========
    result['score'] = min(score, 100)  # 上限100
    
    # 评分等级
    if score >= 90:
        result['score_level'] = '极强信号，建议果断入场'
        result['stars'] = '⭐⭐⭐⭐'
    elif score >= 70:
        result['score_level'] = '强信号，可考虑入场'
        result['stars'] = '⭐⭐⭐'
    elif score >= 50:
        result['score_level'] = '中等信号，可关注'
        result['stars'] = '⭐⭐'
    elif score >= 30:
        result['score_level'] = '弱信号，继续观察'
        result['stars'] = '⭐'
    else:
        result['score_level'] = '无明确信号'
        result['stars'] = ''
    
    # ========== 5. 扫雷检测（仅对有抄底信号的股票）==========
    if result['score'] >= 50:
        mine_result = call_mine_clearance(code)
        result['mine_result'] = mine_result
        if mine_result and mine_result.get('has_mine'):
            result['signals'].append({
                'type': 'mine_warning',
                'severity': 'warning',
                'message': '⚠️ 扫雷检测有风险'
            })
    
    return result


def detect_all_candidate_signals() -> List[Dict]:
    """
    检测所有候选的抄底信号
    
    返回:
        list: 所有候选的抄底检测结果
    """
    candidates = get_all_candidates()
    results = []
    
    for cand in candidates:
        result = detect_candidate_bottom_signals(cand)
        results.append(result)
    
    # 按评分排序
    results.sort(key=lambda x: x['score'], reverse=True)
    
    return results


# ==================== 综合信号检测 ====================

def detect_all_signals() -> Dict:
    """
    检测所有信号（持仓池风险 + 候选池抄底）
    
    返回:
        dict: {
            'position_risks': list,
            'candidate_signals': list,
            'has_position_risk': bool,
            'has_candidate_signal': bool,
            'detect_time': str
        }
    """
    detect_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    position_risks = detect_all_position_risks()
    candidate_signals = detect_all_candidate_signals()
    
    # 判断是否有风险信号
    has_position_risk = False
    for pr in position_risks:
        for sig in pr['signals']:
            if sig.get('severity') in ['fatal', 'critical', 'high', 'medium']:
                has_position_risk = True
                break
    
    # 判断是否有抄底信号
    has_candidate_signal = False
    for cs in candidate_signals:
        if cs['score'] >= 50:
            has_candidate_signal = True
            break
    
    return {
        'position_risks': position_risks,
        'candidate_signals': candidate_signals,
        'has_position_risk': has_position_risk,
        'has_candidate_signal': has_candidate_signal,
        'detect_time': detect_time
    }