"""
路径配置加载模块
根据环境变量 test_env 选择配置文件：
  test_env=physical → paths_windows.json（Windows 物理机）
  test_env=virtual 或未设置 → paths.json（Ubuntu 虚拟机，默认）
"""
import os
import json

# core/ 的上级目录就是项目根目录
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_config_dir = os.path.join(os.path.dirname(_PROJECT_ROOT), 'config')


def _load_paths():
    try:
        env = os.environ.get('test_env', 'virtual').strip().lower()
    except Exception:
        env = 'virtual'

    if env == 'physical':
        config_file = os.path.join(_config_dir, 'paths_windows.json')
    else:
        config_file = os.path.join(_config_dir, 'paths.json')

    if os.path.exists(config_file):
        with open(config_file, 'r', encoding='utf-8') as f:
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
