"""
进攻型/防御型板块关键词定义
从 sectors_config.yaml 读取配置
"""

import os
import yaml

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'sectors_config.yaml')


def _load_config():
    """加载YAML配置"""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


# 加载配置
_config = _load_config()

# 从配置中提取关键词列表
ATTACK_KEYWORDS = []
for category in _config.get('attack_sectors', {}).values():
    ATTACK_KEYWORDS.extend(category.get('keywords', []))
# 去重
ATTACK_KEYWORDS = list(set(ATTACK_KEYWORDS))

DEFENSE_KEYWORDS = []
for category in _config.get('defense_sectors', {}).values():
    DEFENSE_KEYWORDS.extend(category.get('keywords', []))
DEFENSE_KEYWORDS = list(set(DEFENSE_KEYWORDS))

EXCLUDE_KEYWORDS = _config.get('exclude_keywords', [])


def is_attack_sector(sector_name):
    """判断是否为进攻型板块"""
    return any(kw in sector_name for kw in ATTACK_KEYWORDS)


def is_defense_sector(sector_name):
    """判断是否为防御型板块"""
    return any(kw in sector_name for kw in DEFENSE_KEYWORDS)


def is_excluded_sector(sector_name):
    """判断是否为需要排除的统计性概念"""
    return any(kw in sector_name for kw in EXCLUDE_KEYWORDS)


def filter_attack_sectors(sectors):
    """从板块列表中筛选进攻型板块"""
    return [s for s in sectors
            if is_attack_sector(s['name'])
            and not is_defense_sector(s['name'])
            and not is_excluded_sector(s['name'])]


def get_attack_categories():
    """获取进攻型板块分类信息（用于报告）"""
    return _config.get('attack_sectors', {})


def get_defense_categories():
    """获取防御型板块分类信息"""
    return _config.get('defense_sectors', {})
