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
        
        # turtle特有：计算新止损价和下次加仓价
        new_stop_price = calc_stop_price(new_price, atr)
        new_next_add_price = calc_add_price(new_price, atr)
        
        return self.pm.add_position(
            account_id=account_id, code=code, new_price=new_price,
            shares_per_unit=shares_per_unit,
            new_stop_price=new_stop_price,
            new_next_add_price=new_next_add_price,
            account_manager=account_manager, atr=atr,
            target_date=target_date,
        )

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
