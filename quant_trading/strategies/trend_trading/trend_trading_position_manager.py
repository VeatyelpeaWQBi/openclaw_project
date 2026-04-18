"""
趋势交易策略 — 持仓管理层
封装趋势交易特有的持仓计算逻辑，底层CRUD委托给 infra PositionManager

职责：
  - ATR仓位计算（shares_per_unit）
  - 止损价/加仓价/退出价计算
  - 加仓/平仓的策略前置校验
  - S1/S2冷却天数决策
  - S1过滤激活/清除
"""

import logging

from infra.position_manager import PositionManager
from strategies.trend_trading.atr import calc_unit_size, calc_stop_price, calc_add_price, calc_exit_price

logger = logging.getLogger(__name__)


class TrendTradingPositionManager:
    """趋势交易策略持仓管理器"""

    def __init__(self):
        self.pm = PositionManager()

    # ==================== 代理通用查询方法 ====================

    def get_active_positions(self, account_id):
        return self.pm.get_active_positions(account_id)

    def get_cooling_positions(self, account_id):
        return self.pm.get_cooling_positions(account_id)

    def get_position(self, account_id, code):
        return self.pm.get_position(account_id, code)

    def count_today_opens(self, account_id, target_date=None):
        return self.pm.count_today_opens(account_id, target_date=target_date)

    def check_cooldown_release(self, account_id, target_date=None):
        return self.pm.check_cooldown_release(account_id, target_date=target_date)

    def get_total_units(self, account_id):
        return self.pm.get_total_units(account_id)

    def get_position_status(self, account_id, code, target_date=None):
        return self.pm.get_position_status(account_id, code, target_date=target_date)

    def get_position_flow(self, account_id, code=None, limit=20):
        return self.pm.get_position_flow(account_id, code, limit)

    # ==================== turtle特有：仓位计算 ====================

    def calc_shares_per_unit(self, capital, atr, price):
        """
        计算每单位股数（趋势法则：1%风险）
        
        参数:
            capital: 总资金
            atr: ATR值
            price: 当前价格
        
        返回:
            int: 每单位股数
        """
        return calc_unit_size(capital, atr, price)

    # ==================== 开仓 ====================

    def open_position(self, account_id, code, name, price, atr, units=1,
                      account_manager=None, capital=100000, target_date=None):
        """
        开仓（turtle策略）
        
        参数:
            account_id: 账户ID
            code: 股票代码
            name: 股票名称
            price: 入场价
            atr: ATR值
            units: 单位数，默认1
            account_manager: AccountManager实例
            capital: 账户总资金（用于计算仓位）
            target_date: 业务日期（回测时传入）
        """
        # turtle特有计算
        shares_per_unit = calc_unit_size(capital, atr, price)
        total_shares = shares_per_unit * units
        stop_price = calc_stop_price(price, atr)
        next_add_price = calc_add_price(price, atr)
        
        # 委托 infra 层执行 CRUD
        return self.pm.open_position(
            account_id=account_id, code=code, name=name, price=price,
            total_shares=total_shares, stop_price=stop_price,
            next_add_price=next_add_price, shares_per_unit=shares_per_unit,
            account_manager=account_manager, units=units, atr=atr,
            entry_system='S1',  # 默认S1，外部可覆盖
            target_date=target_date,
        )

    def open_position_with_system(self, account_id, code, name, price, atr,
                                   entry_system='S1', units=1,
                                   account_manager=None, capital=100000, target_date=None):
        """
        指定入场系统的开仓
        """
        shares_per_unit = calc_unit_size(capital, atr, price)
        total_shares = shares_per_unit * units
        stop_price = calc_stop_price(price, atr)
        next_add_price = calc_add_price(price, atr)
        
        return self.pm.open_position(
            account_id=account_id, code=code, name=name, price=price,
            total_shares=total_shares, stop_price=stop_price,
            next_add_price=next_add_price, shares_per_unit=shares_per_unit,
            account_manager=account_manager, units=units, atr=atr,
            entry_system=entry_system,
            target_date=target_date,
        )

    # ==================== 加仓 ====================

    def add_position(self, account_id, code, new_price, atr, account_manager=None, target_date=None):
        """
        加仓（turtle策略：计算新止损/加仓价）
        """
        params = self._calc_add_params(account_id, code, new_price, atr)
        if not params:
            return None
        
        return self.pm.add_position(
            account_id=account_id, code=code, new_price=new_price,
            shares_per_unit=params['shares_per_unit'],
            new_stop_price=params['new_stop_price'],
            new_next_add_price=params['new_next_add_price'],
            account_manager=account_manager, atr=atr,
            target_date=target_date,
        )

    def _calc_add_params(self, account_id, code, new_price, atr):
        """
        计算加仓参数（不执行加仓）
        
        返回:
            dict: {
                'shares_per_unit': int,
                'new_stop_price': float,
                'new_next_add_price': float,
                'new_avg_cost': float,
            }
        """
        pos = self.pm.get_position(account_id, code)
        if not pos:
            return None
        
        # turtle特有：4单位上限
        if pos['turtle_units'] >= 4:
            logger.warning(f"[{code}] 已达4单位上限，不可加仓")
            return None
        
        shares_per_unit = pos.get('shares_per_unit', 0)
        if shares_per_unit <= 0:
            shares_per_unit = pos['total_shares'] // pos['turtle_units'] if pos['turtle_units'] > 0 else 0
        
        # turtle特有：计算新平均成本
        from strategies.trend_trading.atr import calc_avg_cost_after_add
        
        old_avg_cost = pos.get('avg_cost', new_price)
        old_shares = pos.get('total_shares', 0)
        # 加仓手续费估算（买入金额的万分之三）
        est_fees = new_price * shares_per_unit * 0.0003
        new_avg_cost = calc_avg_cost_after_add(old_avg_cost, old_shares, new_price, shares_per_unit, est_fees)
        
        # turtle改良：基于平均成本计算止损价，加仓线仍用加仓价
        new_stop_price = calc_stop_price(new_avg_cost, atr)
        new_next_add_price = calc_add_price(new_price, atr)
        
        return {
            'shares_per_unit': shares_per_unit,
            'new_stop_price': new_stop_price,
            'new_next_add_price': new_next_add_price,
            'new_avg_cost': new_avg_cost,
        }

    # ==================== 平仓 ====================

    def close_position(self, account_id, code, reason, sell_price, cooldown_days=10, account_manager=None, target_date=None):
        """
        平仓（纯委托，冷却天数由调用方决定）
        """
        return self.pm.close_position(
            account_id=account_id, code=code, reason=reason,
            sell_price=sell_price, cooldown_days=cooldown_days,
            account_manager=account_manager,
            target_date=target_date,
        )

    # ==================== 开仓参数计算 ====================

    def _calc_open_params(self, capital, price, atr, units=1):
        """
        计算开仓参数（不执行开仓）
        
        turtle策略：
          shares_per_unit = capital / (price × ATR × 1%风险系数)
          stop_price = 入场价 - 2×ATR
          next_add_price = 入场价 + 0.5×ATR
        
        参数:
            capital: 总资金
            price: 入场价
            atr: ATR值
            units: 开仓单位数（默认1）
        
        返回:
            dict: {
                'shares_per_unit': int,
                'total_shares': int,
                'stop_price': float,
                'next_add_price': float,
            }
        """
        from strategies.trend_trading.atr import calc_unit_size, calc_stop_price, calc_add_price
        
        shares_per_unit = calc_unit_size(capital, atr, price)
        if shares_per_unit <= 0:
            return None  # 1手超5%仓位，不可开仓
        
        total_shares = shares_per_unit * units
        stop_price = calc_stop_price(price, atr)
        next_add_price = calc_add_price(price, atr)
        
        return {
            'shares_per_unit': shares_per_unit,
            'total_shares': total_shares,
            'stop_price': stop_price,
            'next_add_price': next_add_price,
        }

    # ==================== 每日ATR更新 ====================

    def update_atr_values(self, account_id, kline_data):
        """
        每日更新持仓股的ATR值

        参数:
            account_id: 账户ID
            kline_data: {code: DataFrame} 当日K线数据

        返回:
            dict: {code: new_atr} 更新结果
        """
        from strategies.trend_trading.atr import get_atr_value

        positions = self.get_active_positions(account_id)
        if not positions:
            return {}

        updates = {}
        for pos in positions:
            code = pos['code']
            df = kline_data.get(code)
            if df is None or df.empty:
                continue

            new_atr = get_atr_value(df)
            if new_atr <= 0:
                continue

            # 更新DB
            self.pm._update_atr_value(account_id, code, new_atr)
            updates[code] = new_atr

        if updates:
            logger.info(f"[账户{account_id}] 更新ATR: {len(updates)}只")

        return updates
