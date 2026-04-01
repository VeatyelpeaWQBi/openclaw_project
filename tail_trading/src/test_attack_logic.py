#!/usr/bin/env python3
"""
验证尾盘T+1进攻型板块检查逻辑
"""

import sys
import logging
sys.path.insert(0, '***REMOVED***/tail_trading/config')
from sectors import is_attack_sector, filter_attack_sectors

logger = logging.getLogger(__name__)

def test_is_attack_sector():
    """测试 is_attack_sector 函数"""
    attack_names = ['半导体', '人工智能', '券商', '军工', '光伏', '创新药']
    defense_names = ['银行', '煤炭', '食品饮料', '高速公路']
    neutral_names = ['环保', '商贸', '旅游']

    logger.info("=== 测试 is_attack_sector ===")
    for name in attack_names:
        result = is_attack_sector(name)
        assert result, f"{name} 应该是进攻型板块"
        logger.info(f"  {name}: 进攻型")
    for name in defense_names:
        result = is_attack_sector(name)
        if not result:
            logger.info(f"  {name}: 非进攻型")
        else:
            logger.warning(f"  {name}: 进攻型(异常，应为非进攻型)")
    for name in neutral_names:
        result = is_attack_sector(name)
        logger.debug(f"  {name}: {'进攻型' if result else '非进攻型'}")

def test_attack_filter_logic():
    """模拟测试进攻型板块过滤逻辑"""
    logger.info("=== 测试进攻型板块过滤逻辑 ===")

    # 场景1: 前10有进攻型，前5有进攻型 → 正常选股
    sectors_case1 = [
        {'name': '半导体', 'change_percent': 5.0},
        {'name': '银行', 'change_percent': 4.5},
        {'name': '人工智能', 'change_percent': 4.0},
        {'name': '煤炭', 'change_percent': 3.5},
        {'name': '券商', 'change_percent': 3.0},
        {'name': '食品饮料', 'change_percent': 2.5},
        {'name': '军工', 'change_percent': 2.0},
        {'name': '环保', 'change_percent': 1.5},
        {'name': '光伏', 'change_percent': 1.0},
        {'name': '旅游', 'change_percent': 0.5},
    ]
    top10 = sectors_case1[:10]
    top10_attack = [s for s in top10 if is_attack_sector(s['name'])]
    top5 = sectors_case1[:5]
    top5_attack = [s for s in top5 if is_attack_sector(s['name'])]
    logger.info(f"  场景1 - 前10进攻型: {[s['name'] for s in top10_attack]}")
    logger.info(f"  场景1 - 前5进攻型: {[s['name'] for s in top5_attack]}")
    assert len(top10_attack) > 0 and len(top5_attack) > 0, "场景1应该通过检查"
    logger.info("  场景1: 通过检查，可选股")

    # 场景2: 前10有进攻型，前5无进攻型 → 跳过
    sectors_case2 = [
        {'name': '银行', 'change_percent': 5.0},
        {'name': '煤炭', 'change_percent': 4.5},
        {'name': '食品饮料', 'change_percent': 4.0},
        {'name': '高速公路', 'change_percent': 3.5},
        {'name': '农业', 'change_percent': 3.0},
        {'name': '半导体', 'change_percent': 2.5},
        {'name': '军工', 'change_percent': 2.0},
        {'name': '环保', 'change_percent': 1.5},
        {'name': '光伏', 'change_percent': 1.0},
        {'name': '旅游', 'change_percent': 0.5},
    ]
    top10 = sectors_case2[:10]
    top10_attack = [s for s in top10 if is_attack_sector(s['name'])]
    top5 = sectors_case2[:5]
    top5_attack = [s for s in top5 if is_attack_sector(s['name'])]
    logger.info(f"  场景2 - 前10进攻型: {[s['name'] for s in top10_attack]}")
    logger.info(f"  场景2 - 前5进攻型: {[s['name'] for s in top5_attack]}")
    assert len(top10_attack) > 0, "场景2前10应该有进攻型"
    assert len(top5_attack) == 0, "场景2前5应该无进攻型"
    logger.info("  场景2: 前5无进攻型，应跳过")

    # 场景3: 前10无进攻型 → 跳过
    sectors_case3 = [
        {'name': '银行', 'change_percent': 5.0},
        {'name': '煤炭', 'change_percent': 4.5},
        {'name': '食品饮料', 'change_percent': 4.0},
        {'name': '高速公路', 'change_percent': 3.5},
        {'name': '农业', 'change_percent': 3.0},
        {'name': '纺织', 'change_percent': 2.5},
        {'name': '家电', 'change_percent': 2.0},
        {'name': '环保', 'change_percent': 1.5},
        {'name': '旅游', 'change_percent': 1.0},
        {'name': '商贸', 'change_percent': 0.5},
    ]
    top10 = sectors_case3[:10]
    top10_attack = [s for s in top10 if is_attack_sector(s['name'])]
    logger.info(f"  场景3 - 前10进攻型: {[s['name'] for s in top10_attack]}")
    assert len(top10_attack) == 0, "场景3前10应该无进攻型"
    logger.info("  场景3: 前10无进攻型，应跳过")

    logger.info("所有测试通过！")

if __name__ == '__main__':
    test_is_attack_sector()
    test_attack_filter_logic()
