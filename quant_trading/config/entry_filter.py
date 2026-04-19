"""
开仓信号二次筛选配置

配置项说明：
- weak_market_threshold: 弱势市场判定阈值（平均分低于此值为弱势市场）
- strong_market_threshold: 强势市场判定阈值（平均分高于此值为强势市场）
- weak_market_score_limit: 弱势市场个股综合分门槛（绝对值）
- normal_market_bonus: 正常市场个股分相对于平均分的加分值

设计文档：shares/开仓信号二次筛选算法设计文档.md
"""

ENTRY_FILTER_CONFIG = {
    # 市场状态划分阈值
    'weak_market_threshold': 28.65,      # 弱势市场平均分阈值（<此值为弱势）
    'strong_market_threshold': 35,    # 强势市场平均分阈值（>此值为强势）
    
    # 弱势市场个股门槛
    'weak_market_score_limit': 70.0,    # 弱势市场个股综合分门槛
    
    # 正常市场个股门槛
    'normal_market_bonus': 20.0,        # 正常市场个股分 = 平均分 + 此值
}
