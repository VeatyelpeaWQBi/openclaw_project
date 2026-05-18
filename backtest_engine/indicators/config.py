"""
回测策略配置

集中管理所有可配置参数，方便调整策略
"""


class BacktestConfig:
    """回测配置类"""

    # ========== 回测时间范围 ==========
    START_DATE = '2020-01-01'
    END_DATE = '2026-01-01'

    # ========== 资金配置 ==========
    INITIAL_CAPITAL = 200_000

    # ========== 信号检测窗口 ==========
    SIGNAL_WINDOW = 5  # 滑动窗口大小（3/5/7）

    # ========== MACD参数 ==========
    MACD_FAST = 12
    MACD_SLOW = 26
    MACD_SIGNAL = 9

    # ========== EMA参数 ==========
    EMA_FAST = 18
    EMA_SLOW = 40

    # ========== OBV参数 ==========
    OBV_MA_PERIOD = 30  # MAOBV周期

    # ========== 仓位分配 ==========
    MIN_POSITION_PCT = 0.05   # 5%
    MAX_POSITION_PCT = 0.15   # 15%
    POSITION_FUZZY = 0.03     # ±3%波动
    MAX_DAILY_OPEN = 3        # 单日最大建仓数

    # ========== DIF走平判断 ==========
    DIF_FLAT_WINDOW = 3        # 滑动窗口天数
    DIF_FLAT_THRESHOLD = 0.02  # 走平阈值

    # ========== 即将交叉判断 ==========
    SOON_CROSS_SLOPE_MIN = 0.001  # 最小斜率
    SOON_CROSS_GAP_RATIO = 0.1    # 差值比例阈值（与DEA的相对差值）
    SOON_CROSS_OBV_RATIO = 0.05   # OBV上叉MAOBV的相对差值

    # ========== OBV斜率判断 ==========
    OBV_UPTREND_WINDOW = 5       # OBV上升趋势判断窗口
    OBV_UPTREND_MIN_SLOPE = 0.01  # OBV上升趋势最小斜率

    # ========== 滑点和手续费 ==========
    SLIPPAGE = 0.0   # 滑点（暂不计算）
    COMMISSION = 0.0 # 手续费（暂不计算）

    # ========== 输出配置 ==========
    OUTPUT_DIR = 'backtest_results'  # 输出目录

    def __repr__(self):
        return f"BacktestConfig(Start={self.START_DATE}, End={self.END_DATE}, Capital={self.INITIAL_CAPITAL})"


# 默认配置实例
DEFAULT_CONFIG = BacktestConfig()