"""
指数ADX统计阈值配置
基于2014-2026年全部历史日K按DI方向分类统计结果
所有阈值可配置化修改
"""

# 指数代码映射
INDEX_CODE_MAP = {
    '000001': '上证指数',
    '399001': '深证成指',
    '399006': '创业板指',
    '000300': '沪深300',
    '000905': '中证500',
    '000852': '中证1000',
    '000985': '中证全指',
}

# 展示指数列表（盯盘助手主要展示）
SHOW_INDEX_NAMES = ['上证指数', '深证成指', '创业板指', '沪深300', '中证500', '中证1000']

# ADX视觉补偿系数（大盘波动天然小于个股）
INDEX_ADX_VISUAL_COEFFICIENT = 1.2

# DI方向判定阈值
DI_DIRECTION_THRESHOLD = 5.0

# 短评生成阈值配置
COMMENT_CONFIG = {
    # 历史极值检测
    'extreme_threshold': 0.9,      # ADX > 历史最高 * 0.9 视为接近极值
    'median_high_threshold': 1.0,  # ADX > 中位数 视为高于中位
    'median_low_threshold': 0.7,   # ADX < 中位数 * 0.7 视为低于中位

    # ADX连续变化检测
    'consecutive_days': 3,         # 连续N日变化才提示
    'adx_strengthen_min': 15,      # 趋势加强时ADX最小值（排除无趋势区噪音）
    'adx_weaken_max': 25,          # 趋势减弱时前日ADX最大值（排除本身就很弱的情况）
}

# 各指数各方向ADX统计阈值
# 数据来源: WatchMonitor/test_case/index_adx_direction_stats.csv
INDEX_ADX_CONFIG = {
    '000001': {
        'name': '上证指数',
        '多头': {'max': 67.77, 'min': 10.43, 'mean': 28.09, 'median': 24.18},
        '空头': {'max': 59.53, 'min': 10.28, 'mean': 27.46, 'median': 25.96},
        '横盘': {'max': 51.22, 'min': 10.07, 'mean': 19.38, 'median': 17.77},
    },
    '399001': {
        'name': '深证成指',
        '多头': {'max': 65.80, 'min': 9.04, 'mean': 27.65, 'median': 24.38},
        '空头': {'max': 59.65, 'min': 9.64, 'mean': 29.46, 'median': 27.88},
        '横盘': {'max': 56.65, 'min': 8.35, 'mean': 19.34, 'median': 17.81},
    },
    '399006': {
        'name': '创业板指',
        '多头': {'max': 63.78, 'min': 10.15, 'mean': 28.07, 'median': 25.81},
        '空头': {'max': 56.01, 'min': 9.82, 'mean': 28.52, 'median': 26.84},
        '横盘': {'max': 45.74, 'min': 8.10, 'mean': 19.59, 'median': 17.96},
    },
    '000300': {
        'name': '沪深300',
        '多头': {'max': 68.83, 'min': 8.82, 'mean': 27.57, 'median': 24.55},
        '空头': {'max': 57.38, 'min': 9.32, 'mean': 27.58, 'median': 26.68},
        '横盘': {'max': 48.27, 'min': 8.28, 'mean': 19.21, 'median': 17.71},
    },
    '000905': {
        'name': '中证500',
        '多头': {'max': 63.47, 'min': 8.36, 'mean': 28.05, 'median': 24.86},
        '空头': {'max': 64.70, 'min': 9.10, 'mean': 29.28, 'median': 27.68},
        '横盘': {'max': 55.46, 'min': 8.03, 'mean': 19.43, 'median': 17.42},
    },
    '000852': {
        'name': '中证1000',
        '多头': {'max': 63.04, 'min': 8.00, 'mean': 27.78, 'median': 24.63},
        '空头': {'max': 61.78, 'min': 8.69, 'mean': 30.51, 'median': 28.26},
        '横盘': {'max': 53.26, 'min': 7.56, 'mean': 19.81, 'median': 18.47},
    },
    '000985': {
        'name': '中证全指',
        '多头': {'max': 65.04, 'min': 9.89, 'mean': 27.96, 'median': 24.27},
        '空头': {'max': 62.78, 'min': 9.61, 'mean': 29.04, 'median': 27.53},
        '横盘': {'max': 43.49, 'min': 8.96, 'mean': 19.44, 'median': 17.76},
    },
}


def get_index_thresholds(index_code: str) -> dict:
    """返回指定指数3方向的统计阈值"""
    return INDEX_ADX_CONFIG.get(index_code, {})


def get_index_name(index_code: str) -> str:
    """返回指数展示名称"""
    return INDEX_CODE_MAP.get(index_code, index_code)


def get_index_code_by_name(name: str) -> str | None:
    """根据展示名称返回指数代码"""
    for code, n in INDEX_CODE_MAP.items():
        if n == name:
            return code
    return None
