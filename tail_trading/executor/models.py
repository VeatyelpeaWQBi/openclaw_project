"""
交易执行器 — 数据模型
定义交易指令和执行结果的结构
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class TradeAction(Enum):
    """交易动作枚举"""
    OPEN = "开仓"          # 开仓买入
    ADD = "加仓"           # 加仓买入
    REDUCE = "减仓"        # 减仓卖出（部分）
    CLOSE = "平仓"         # 清仓卖出（全部）
    CLOSE_STOP_LOSS = "止损平仓"
    CLOSE_TAKE_PROFIT = "止盈平仓"


class TradeStatus(Enum):
    """交易执行状态"""
    SUCCESS = "成功"
    FAILED = "失败"
    SKIPPED = "跳过"       # 因条件不满足跳过（如T+1锁定）
    PARTIAL = "部分成交"    # 减仓场景可能部分成交


@dataclass
class TradeCommand:
    """
    交易指令 — 一条完整的交易命令

    属性:
        action: 交易动作
        code: 股票代码
        name: 股票名称
        price: 执行价格（目标价）
        units: 交易单位数（开仓/加仓时指定，默认1）
        shares: 具体股数（减仓/平仓时可指定，None=自动计算）
        atr: ATR值（开仓/加仓时需要）
        reason: 操作原因
        system_type: 系统类型 'S1' 或 'S2'
        source: 指令来源（信号类型/人工指令等）
        metadata: 额外元数据
    """
    action: TradeAction
    code: str
    name: str = ""
    price: float = 0.0
    units: int = 1
    shares: Optional[int] = None
    atr: float = 0.0
    reason: str = ""
    system_type: Optional[str] = None
    source: str = ""
    metadata: dict = field(default_factory=dict)

    def __str__(self):
        return f"[{self.action.value}] {self.code} {self.name} @ {self.price:.2f} ({self.reason})"


@dataclass
class TradeResult:
    """
    交易执行结果

    属性:
        success: 是否执行成功
        status: 执行状态
        command: 原始指令
        executed_shares: 实际成交股数
        executed_price: 实际成交价
        executed_amount: 实际成交金额
        fees: 费用明细
        profit: 本次盈亏（平仓/减仓时有值）
        units_before: 操作前持仓单位数
        units_after: 操作后持仓单位数
        position_after: 操作后持仓快照
        message: 人可读的执行描述
        error: 错误信息（失败时）
    """
    success: bool
    status: TradeStatus
    command: TradeCommand
    executed_shares: int = 0
    executed_price: float = 0.0
    executed_amount: float = 0.0
    fees: dict = field(default_factory=dict)
    profit: float = 0.0
    units_before: int = 0
    units_after: int = 0
    position_after: Optional[dict] = None
    message: str = ""
    error: str = ""

    def __str__(self):
        icon = "✅" if self.success else ("⏭️" if self.status == TradeStatus.SKIPPED else "❌")
        return f"{icon} {self.command.code} {self.command.action.value}: {self.message}"

    def to_dict(self) -> dict:
        """转换为字典（便于序列化/日志）"""
        return {
            'success': self.success,
            'status': self.status.value,
            'action': self.command.action.value,
            'code': self.command.code,
            'name': self.command.name,
            'executed_shares': self.executed_shares,
            'executed_price': self.executed_price,
            'executed_amount': self.executed_amount,
            'fees': self.fees,
            'profit': self.profit,
            'units_before': self.units_before,
            'units_after': self.units_after,
            'message': self.message,
            'error': self.error,
        }
