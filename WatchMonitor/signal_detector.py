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
from core.indicator_funcs import (
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
    check_breakdown_big_bull_candle,
    check_breakdown_medium_bull_candle
)
from core.indicators.manager import IndicatorManager

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
    
    # ========== 2. MA5斜率预警检测 ==========
    ma5_slope = indicators.get('ma5_slope')
    
    # 2.1 MA5即将拐头向下（当前向上，但明天可能向下）
    if ma5_slope == 1:  # MA5斜率向上
        if len(df) >= 5:
            deduct_price = df['close'].iloc[-5]  # 5天前收盘价（抵扣价）
            current_close = df['close'].iloc[-1]
            
            # 如果当前收盘价低于抵扣价，明天MA5会下降
            if current_close < deduct_price:
                # 计算需要涨多少才能维持MA5向上
                required_change_pct = ((deduct_price - current_close) / current_close * 100) if current_close > 0 else 0
                
                message = f"⚠️ MA5即将拐头向下（当前向上，但如果明天跌破{deduct_price:.2f}（需跌{-required_change_pct:+.1f}%），MA5将拐头向下）"
                severity = 'warning'
                
                result['signals'].append({
                    'type': 'ma5_turning_down_warning',
                    'severity': severity,
                    'message': message,
                    'deduct_price': deduct_price,
                    'required_change_pct': required_change_pct
                })
    
    # 2.2 MA5斜率向下检测
    if ma5_slope == -1:  # MA5斜率向下
        # 计算抵扣价：5天前的收盘价
        if len(df) >= 5:
            deduct_price = df['close'].iloc[-5]  # 5天前收盘价
            current_close = df['close'].iloc[-1]
            
            # 计算明天需要涨多少才能达到抵扣价
            required_change_pct = ((deduct_price - current_close) / current_close * 100) if current_close > 0 else 0
            
            # 获取MA5和MA10当前值
            ma5 = indicators.get('ma5')
            ma10 = indicators.get('ma10')
            
            # 如果MA5接近MA10（差距小于3%），预警即将死叉
            if ma5 and ma10 and abs(ma5 - ma10) / ma10 < 0.03:
                # 计算明天避免死叉需要的价格
                # 需要计算：MA5_new >= MA10_new
                # MA5_new = (close[-4]+close[-3]+close[-2]+close[-1]+close_tomorrow)/5
                # MA10_new = (close[-9]+...+close[-1]+close_tomorrow)/10
                # 简化：close_tomorrow需要>=某个值让MA5_new>=MA10_new
                
                # 避免死叉需要的明天收盘价（近似计算）
                # 如果MA5略高于MA10，需要close_tomorrow足够高才能维持MA5>=MA10
                # 简化：取抵扣价作为预警价格
                
                message = f"⚠️ MA5斜率向下，如果明天无法涨到{deduct_price:.2f}之上（需涨{required_change_pct:+.1f}%），将持续向下，即将死叉MA10"
                severity = 'medium'
            else:
                # 只是MA5向下，没有即将死叉
                message = f"⚠️ MA5斜率向下，如果明天无法涨到{deduct_price:.2f}之上（需涨{required_change_pct:+.1f}%），将持续向下"
                severity = 'warning'
            
            result['signals'].append({
                'type': 'ma5_turning_down',
                'severity': severity,
                'message': message,
                'deduct_price': deduct_price,
                'required_change_pct': required_change_pct
            })
    
    # ========== 3. 均线死叉检测 ==========
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
    
    # 跌破新高大阳线开盘价（5%以上）
    breakdown_bull = check_breakdown_big_bull_candle(df)
    if breakdown_bull:
        result['signals'].append({
            'type': 'breakdown_big_bull_candle',
            'severity': 'high',
            'message': breakdown_bull['message'],
            'big_bull_open': breakdown_bull.get('big_bull_open'),
            'big_bull_change': breakdown_bull.get('big_bull_change')
        })
    
    # 跌破新高中阳线开盘价（2.5%~5%）
    breakdown_medium_bull = check_breakdown_medium_bull_candle(df)
    if breakdown_medium_bull:
        result['signals'].append({
            'type': 'breakdown_medium_bull_candle',
            'severity': 'medium',
            'message': breakdown_medium_bull['message'],
            'medium_bull_open': breakdown_medium_bull.get('medium_bull_open'),
            'medium_bull_change': breakdown_medium_bull.get('medium_bull_change')
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
        details = mine_result.get('mine_details', {})
        
        # 提取风险类型和原因
        risk_types = []
        risk_reasons = []
        risk_reason_detail = []
        
        # 处理DataFrame返回的数据
        if isinstance(details, dict):
            f_type = details.get('f_type', {})
            t_type = details.get('t_type', {})
            reason = details.get('reason', {})
            
            # 过滤无效值的列表
            invalid_values = ['暂无风险项', 'N/A', '无', 'nan', 'NaN', '', None]
            
            # 如果是字典（DataFrame.to_dict()的结果），转为列表并过滤无效值
            if isinstance(f_type, dict):
                risk_types = list(set([v for k, v in f_type.items() if pd.notna(v) and v not in invalid_values]))
            if isinstance(t_type, dict):
                risk_reasons = list(set([v for k, v in t_type.items() if pd.notna(v) and v not in invalid_values]))
            if isinstance(reason, dict):
                risk_reason_detail = list(set([v for k, v in reason.items() if pd.notna(v) and v not in invalid_values]))
        
        # 构造详细消息（最多5条）
        message_parts = []
        
        if risk_types:
            message_parts.append(f"类型: {', '.join(risk_types[:5])}")
        if risk_reasons:
            message_parts.append(f"风险: {', '.join(risk_reasons[:5])}")
        if risk_reason_detail and len(message_parts) < 5:
            # 如果还有空间，添加详细原因
            remaining = 5 - len(message_parts)
            message_parts.extend([f"原因: {r}" for r in risk_reason_detail[:remaining]])
        
        # 只有当有实际风险项时才添加扫雷警告
        if message_parts:
            message = '⚠️ 扫雷检测有风险 — ' + ' | '.join(message_parts)
            result['signals'].append({
                'type': 'mine_warning',
                'severity': 'warning',
                'message': message
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
    result['drop_pct'] = (current_price - watch_price) / watch_price * 100 if watch_price else 0
    result['indicators'] = indicators  # 新增：保存技术指标
    
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


# ==================== 模块化信号检测（新） ====================

def detect_signals_with_manager(code: str, df: pd.DataFrame, context: Dict) -> Dict:
    """
    使用IndicatorManager检测信号（模块化方式）

    参数:
        code: 股票代码
        df: 日K数据DataFrame
        context: 上下文信息（包含is_position, is_candidate, current_price等）

    返回:
        Dict: {
            'indicators_data': dict,  # 所有指标数据
            'signals': list,          # 所有信号
            'score': float,           # 综合评分
            'reasons': list           # 评分原因
        }
    """
    manager = IndicatorManager.get_instance()

    # 计算所有指标
    indicators_data = manager.calculate_all(df, code=code)

    # 检测信号
    context['df'] = df
    context['code'] = code
    signals = manager.detect_all_signals(indicators_data, context)

    # 计算评分
    score, reasons = manager.calculate_total_score(indicators_data, context)

    return {
        'indicators_data': indicators_data,
        'signals': signals,
        'score': score,
        'reasons': reasons
    }


def generate_indicator_report_with_manager(code: str, df: pd.DataFrame, context: Dict) -> List[str]:
    """
    使用IndicatorManager生成指标报告内容

    参数:
        code: 股票代码
        df: 日K数据DataFrame
        context: 上下文信息

    返回:
        List[str]: 报告展示行列表
    """
    manager = IndicatorManager.get_instance()

    # 计算所有指标（如果context中没有）
    if 'indicators_data' not in context:
        indicators_data = manager.calculate_all(df, code=code)
        context['indicators_data'] = indicators_data
    else:
        indicators_data = context['indicators_data']

    # 生成报告
    return manager.generate_report_lines(indicators_data, context)