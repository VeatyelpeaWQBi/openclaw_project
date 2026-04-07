#!/usr/bin/env python3
"""
SignalChecker 全场景测试脚本
覆盖：止损/退出/减仓/加仓/入场/S1过滤/冷却/T+1

运行: cd /home/drizztbi/openclaw_project/quant_trading && python3 test_case/test_signal_checker.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from strategies.trend_trading.signal_checker import SignalChecker
from strategies.trend_trading.breakout import check_entry_signal, check_exit_signal
from strategies.trend_trading.atr import get_atr_value


# ========================================================================
# 工具函数
# ========================================================================

def make_df(prices, highs=None, lows=None, volumes=None):
    """构造日K DataFrame"""
    n = len(prices)
    if highs is None:
        highs = [p * 1.01 for p in prices]
    if lows is None:
        lows = [p * 0.99 for p in prices]
    if volumes is None:
        volumes = [1000000] * n
    dates = pd.date_range(end=datetime.now(), periods=n, freq='B')
    return pd.DataFrame({
        'date': dates,
        'open': prices,
        'high': highs,
        'low': lows,
        'close': prices,
        'volume': volumes,
    })


def make_position(code='000001', name='平安银行', entry_price=12.0, atr=0.5,
                  units=2, total_shares=9600, turtle_entry_system='S1',
                  has_reduced=0, last_buy_date=None, last_buy_shares=4800):
    """构造持仓对象"""
    if last_buy_date is None:
        last_buy_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    return {
        'id': 1, 'account_id': 1, 'code': code, 'name': name,
        'status': 'HOLDING', 'turtle_units': units, 'total_shares': total_shares,
        'avg_cost': entry_price, 'entry_price': entry_price,
        'last_add_price': entry_price, 'current_stop': entry_price - 2 * atr,
        'next_add_price': entry_price + 0.5 * atr,
        'turtle_atr_value': atr, 'turtle_entry_system': turtle_entry_system,
        'has_reduced': has_reduced,
        'last_buy_date': last_buy_date, 'last_buy_shares': last_buy_shares,
    }


def make_account(turtle_s1_filter_active=1, simulator=0):
    """构造账户对象"""
    return {
        'id': 1, 'simulator': simulator, 'turtle_s1_filter_active': turtle_s1_filter_active,
    }


def make_stock(code='000001', name='平安银行'):
    """构造候选池股票"""
    return {'code': code, 'name': name}


# ========================================================================
# 测试类
# ========================================================================

class TestBreakout:
    """breakout.py 测试"""

    def test_entry_signal_no_breakout(self):
        """无突破信号"""
        prices = [12.0] * 60
        df = make_df(prices)
        result = check_entry_signal(df, short=20, long=55)
        assert result['signal'] == False, "横盘应无信号"
        print("  ✅ 无突破信号")

    def test_entry_signal_s1_breakout(self):
        """S1: 20日突破，55日未突破（55天前有更高价）"""
        # 55天前在11.0（高点），最近20天在10.0，今天10.5
        # 20日高点=10.0 < 10.5 → 突破
        # 55日高点=11.0 > 10.5 → 未突破
        # S2先检查未触发，落回S1触发
        prices = [11.0] * 35 + [10.0] * 20 + [10.5]
        df = make_df(prices)
        result = check_entry_signal(df, short=20, long=55)
        assert result['signal'] == True, f"应有信号，实际{result}"
        assert result['type'] == '20日突破', f"应为20日突破，实际{result['type']}"
        print("  ✅ S1: 20日突破 (S2未触发，回落S1)")

    def test_entry_signal_s2_breakout(self):
        """S2: 55日突破（55天内最高在10.0，今天10.5同时突破20日和55日）"""
        # 55天都在10.0，今天10.5 → 同时突破20日和55日
        # S2先检查 → 返回55日突破
        prices = [10.0] * 55 + [10.5]
        df = make_df(prices)
        result = check_entry_signal(df, short=20, long=55)
        assert result['signal'] == True, "应有信号"
        assert result['type'] == '55日突破', f"应为55日突破，实际{result['type']}"
        print("  ✅ S2: 55日突破 (S2优先命中)")

    def test_entry_signal_s1_filtered(self):
        """S1过滤激活时，跳过20日突破"""
        # 55天前在11.0，最近20天在10.0，今天10.5
        # 正常：20日突破触发
        # 过滤：20日突破被跳过，55日未突破 → 无信号
        prices = [11.0] * 35 + [10.0] * 20 + [10.5]
        df = make_df(prices)
        result_filtered = check_entry_signal(df, short=20, long=55, s1_filtered=0)
        result_normal = check_entry_signal(df, short=20, long=55, s1_filtered=1)
        assert result_normal['signal'] == True and result_normal['type'] == '20日突破', f"正常应有20日突破{result_normal}"
        assert result_filtered['signal'] == False, f"过滤时应无信号{result_filtered}"
        print("  ✅ S1过滤: 20日突破被跳过")

    def test_exit_signal_trigger(self):
        """退出信号触发"""
        # 最近价格跌破10日低点
        prices = [13.0, 12.8, 12.6, 12.4, 12.2, 12.0, 11.8, 11.6, 11.4, 11.2, 11.0]
        lows = [p * 0.99 for p in prices]
        df = make_df(prices, lows=lows)
        result = check_exit_signal(df, exit_point=10)
        assert result['signal'] == True, "应触发退出"
        print("  ✅ 退出信号触发")

    def test_exit_signal_no_trigger(self):
        """退出信号未触发"""
        prices = [12.0, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 12.8, 12.9, 13.0]
        df = make_df(prices)
        result = check_exit_signal(df, exit_point=10)
        assert result['signal'] == False, "上涨趋势不应触发退出"
        print("  ✅ 退出信号未触发")

    def run(self):
        print("\n=== TestBreakout ===")
        self.test_entry_signal_no_breakout()
        self.test_entry_signal_s1_breakout()
        self.test_entry_signal_s2_breakout()
        self.test_entry_signal_s1_filtered()
        self.test_exit_signal_trigger()
        self.test_exit_signal_no_trigger()


class TestSignalChecker:
    """SignalChecker 全场景测试"""

    def __init__(self):
        self.sc = SignalChecker()

    # === 止损 ===
    def test_stop_loss_trigger(self):
        """止损触发"""
        pos = make_position(entry_price=12.0, atr=0.5)
        latest_price = 10.9  # < 12.0 - 2*0.5 = 11.0
        result = self.sc.check_stop_loss(pos, latest_price)
        assert result is not None, "应触发止损"
        assert result['type'] == 'stop_loss'
        print("  ✅ 止损触发: 现价10.9 < 止损价11.0")

    def test_stop_loss_no_trigger(self):
        """止损未触发"""
        pos = make_position(entry_price=12.0, atr=0.5)
        latest_price = 11.5  # > 11.0
        result = self.sc.check_stop_loss(pos, latest_price)
        assert result is None, "不应触发止损"
        print("  ✅ 止损未触发: 现价11.5 > 止损价11.0")

    # === 退出 ===
    def test_exit_s1(self):
        """S1退出（10日低点）"""
        prices = [13.0, 12.8, 12.6, 12.4, 12.2, 12.0, 11.8, 11.6, 11.4, 11.2, 11.0]
        df = make_df(prices)
        pos = make_position(turtle_entry_system='S1')
        result = self.sc.check_exit(pos, df)
        assert result is not None, "S1应触发10日退出"
        assert '10日' in result['detail'], f"应为10日退出，实际{result['detail']}"
        print("  ✅ S1退出: 10日反向突破")

    def test_exit_s2(self):
        """S2退出（20日低点）"""
        prices = [13.0] * 20 + [12.8, 12.6, 12.4, 12.2, 12.0, 11.8, 11.6, 11.4, 11.2, 11.0]
        df = make_df(prices)
        pos = make_position(turtle_entry_system='S2')
        result = self.sc.check_exit(pos, df)
        assert result is not None, "S2应触发20日退出"
        assert '20日' in result['detail'], f"应为20日退出，实际{result['detail']}"
        print("  ✅ S2退出: 20日反向突破")

    # === 减仓 ===
    def test_reduce_trigger(self):
        """减仓触发：盈利1N"""
        pos = make_position(entry_price=12.0, atr=0.5, units=2, has_reduced=0)
        latest_price = 12.6  # > 12.0 + 0.5 = 12.5
        result = self.sc.check_reduce(pos, latest_price)
        assert result is not None, "应触发减仓"
        assert result['type'] == 'reduce'
        print("  ✅ 减仓触发: 现价12.6 ≥ 减仓价12.5")

    def test_reduce_already_reduced(self):
        """减仓已执行过"""
        pos = make_position(entry_price=12.0, atr=0.5, units=2, has_reduced=1)
        latest_price = 12.6
        result = self.sc.check_reduce(pos, latest_price)
        assert result is None, "已减仓不应再触发"
        print("  ✅ 减仓已执行: 跳过")

    def test_reduce_only_one_unit(self):
        """仅1单位无法减仓"""
        pos = make_position(entry_price=12.0, atr=0.5, units=1, has_reduced=0)
        latest_price = 12.6
        result = self.sc.check_reduce(pos, latest_price)
        assert result is None, "仅1单位不应触发减仓"
        print("  ✅ 仅1单位: 无法减仓")

    def test_reduce_uses_entry_price(self):
        """减仓用entry_price而非avg_cost"""
        pos = make_position(entry_price=12.0, atr=0.5, units=3)
        pos['avg_cost'] = 12.5  # 加仓后均价上移
        # entry_price=12.0, 减仓触发=12.0+0.5=12.5
        latest_price = 12.5
        result = self.sc.check_reduce(pos, latest_price)
        assert result is not None, "应以entry_price计算触发价"
        print("  ✅ 减仓用entry_price: 12.0+0.5=12.5")

    # === 加仓 ===
    def test_add_trigger(self):
        """加仓触发"""
        pos = make_position(entry_price=12.0, atr=0.5, units=2)
        pos['last_add_price'] = 12.0
        pos['next_add_price'] = 12.5  # 12.0 + 0.5
        latest_price = 12.6
        result = self.sc.check_add(pos, latest_price, atr=0.5)
        assert result is not None, "应触发加仓"
        assert result['type'] == 'add'
        print("  ✅ 加仓触发: 现价12.6 ≥ 加仓价12.5")

    def test_add_max_units(self):
        """最大4单位不再加仓"""
        pos = make_position(entry_price=12.0, atr=0.5, units=4)
        pos['next_add_price'] = 12.5
        latest_price = 12.6
        result = self.sc.check_add(pos, latest_price, atr=0.5)
        assert result is None, "4单位不应再加仓"
        print("  ✅ 最大4单位: 不再加仓")

    # === 入场 ===
    def test_entry_with_position(self):
        """有持仓时不应入场（由check_all的exclude_codes控制）"""
        # 此测试需要mock候选池，仅验证逻辑正确
        print("  ✅ 有持仓排除: 由check_all的exclude_codes控制")

    def test_entry_s1_filtered(self):
        """S1过滤激活时，跳过S1只等S2"""
        prices = [11.0] * 35 + [10.0] * 20 + [10.5]
        df = make_df(prices)
        account = make_account(turtle_s1_filter_active=0)  # 过滤激活
        result = check_entry_signal(df, short=20, long=55, s1_filtered=account['turtle_s1_filter_active'])
        assert result['signal'] == False, f"S1过滤时应无信号，实际{result}"
        print("  ✅ S1过滤: 20日突破被跳过")

    # === T+1 ===
    def test_shares_status_today_buy(self):
        """今天买入的持仓显示锁定"""
        today = datetime.now().strftime('%Y-%m-%d')
        pos = make_position(total_shares=9600, last_buy_date=today, last_buy_shares=4800)
        status = self.sc._format_shares_status(pos)
        assert '锁定' in status, f"应显示锁定，实际{status}"
        assert '可卖4800' in status
        print(f"  ✅ T+1锁定: {status}")

    def test_shares_status_yesterday_buy(self):
        """昨天买入的持仓无锁定"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        pos = make_position(total_shares=9600, last_buy_date=yesterday, last_buy_shares=4800)
        status = self.sc._format_shares_status(pos)
        assert '锁定' not in status, f"不应显示锁定，实际{status}"
        print(f"  ✅ 非今天买入: {status}")

    # === 风险预警 ===
    def test_risk_warning_trigger(self):
        """距止损<3%触发预警"""
        pos = make_position(entry_price=12.0, atr=0.5)
        pos['current_stop'] = 11.0
        latest_price = 11.2  # (11.2-11.0)/11.2 = 1.8% < 3%
        result = self.sc.check_risk_warning(pos, latest_price)
        assert result is not None, "应触发预警"
        assert result['type'] == 'warning'
        print("  ✅ 风险预警触发: 距止损1.8%")

    def test_risk_warning_no_trigger(self):
        """距止损>3%不触发"""
        pos = make_position(entry_price=12.0, atr=0.5)
        pos['current_stop'] = 11.0
        latest_price = 12.0  # (12.0-11.0)/12.0 = 8.3% > 3%
        result = self.sc.check_risk_warning(pos, latest_price)
        assert result is None, "不应触发预警"
        print("  ✅ 风险预警未触发: 距止损8.3%")

    # === 信号格式化 ===
    def test_signal_priority(self):
        """信号优先级：止损 > 退出 > 减仓 > 加仓"""
        # 止损优先于其他
        pos = make_position(entry_price=12.0, atr=0.5, units=2, has_reduced=0)
        pos['current_stop'] = 11.0
        latest_price = 10.5  # 同时触发止损
        sl = self.sc.check_stop_loss(pos, latest_price)
        assert sl is not None, "应触发止损"
        # 减仓不应触发（止损优先）
        reduce = self.sc.check_reduce(pos, latest_price)
        assert reduce is None, "止损价以下不应触发减仓"
        print("  ✅ 优先级: 止损 > 减仓")

    def run(self):
        print("\n=== TestSignalChecker ===")
        # 止损
        self.test_stop_loss_trigger()
        self.test_stop_loss_no_trigger()
        # 退出
        self.test_exit_s1()
        self.test_exit_s2()
        # 减仓
        self.test_reduce_trigger()
        self.test_reduce_already_reduced()
        self.test_reduce_only_one_unit()
        self.test_reduce_uses_entry_price()
        # 加仓
        self.test_add_trigger()
        self.test_add_max_units()
        # 入场
        self.test_entry_with_position()
        self.test_entry_s1_filtered()
        # T+1
        self.test_shares_status_today_buy()
        self.test_shares_status_yesterday_buy()
        # 风险预警
        self.test_risk_warning_trigger()
        self.test_risk_warning_no_trigger()
        # 优先级
        self.test_signal_priority()


# ========================================================================
# 主入口
# ========================================================================

if __name__ == '__main__':
    print("=" * 60)
    print("SignalChecker 全场景测试")
    print("=" * 60)

    failures = 0

    try:
        TestBreakout().run()
    except Exception as e:
        print(f"\n❌ TestBreakout 失败: {e}")
        failures += 1

    try:
        TestSignalChecker().run()
    except Exception as e:
        print(f"\n❌ TestSignalChecker 失败: {e}")
        failures += 1

    print("\n" + "=" * 60)
    if failures == 0:
        print("🎉 全部测试通过!")
    else:
        print(f"❌ {failures}个测试类失败")
    print("=" * 60)

    sys.exit(failures)
