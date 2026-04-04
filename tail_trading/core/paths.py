"""
路径配置加载模块
从 paths.json 加载所有路径配置
"""
import os
import json

# core/ 的上级目录就是项目根目录
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_config_path = os.path.join(_PROJECT_ROOT, 'config', 'paths.json')


def _load_paths():
    if os.path.exists(_config_path):
        with open(_config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    # fallback：使用项目根目录推算
    return {
        'project_root': _PROJECT_ROOT,
        'data_dir': os.path.join(_PROJECT_ROOT, 'data'),
        'daily_data_dir': os.path.join(_PROJECT_ROOT, 'daily_data'),
        'minute_data_dir': os.path.join(_PROJECT_ROOT, 'minute_data'),
        'index_history_dir': os.path.join(_PROJECT_ROOT, 'index_history'),
        'reports_dir': os.path.join(_PROJECT_ROOT, '..', 'shares', 'reports'),
        'db_path': os.path.join(_PROJECT_ROOT, 'data', 'stock_data.db')
    }

PATHS = _load_paths()

# 导出常用路径
PROJECT_ROOT = PATHS['project_root']
DATA_DIR = PATHS['data_dir']
DAILY_DATA_DIR = PATHS['daily_data_dir']
MINUTE_DATA_DIR = PATHS['minute_data_dir']
INDEX_HISTORY_DIR = PATHS['index_history_dir']
REPORTS_DIR = PATHS['reports_dir']
DB_PATH = PATHS['db_path']
