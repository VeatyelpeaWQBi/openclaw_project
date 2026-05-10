"""
路径配置模块

优先使用根目录下的config目录
"""

import os
import json
import yaml


# 项目根目录
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))


def get_config_dir():
    """获取配置目录路径"""
    return os.path.join(_PROJECT_ROOT, '..', 'config')


def load_config(config_name='paths'):
    """加载配置文件

    参数:
        config_name: 配置文件名（不含.yaml扩展）

    返回:
        dict: 配置字典，如果配置不存在则返回默认值
    """
    config_dir = get_config_dir()

    # 配置文件路径
    config_file = os.path.join(config_dir, f'{config_name}.yaml')

    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            # 配置文件不存在或读取失败，返回空配置
            return {}

    return {}
