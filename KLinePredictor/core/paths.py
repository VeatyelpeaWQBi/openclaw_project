"""
路径配置加载模块
读取根目录config下的配置文件
根据环境变量 test_env 选择配置文件：
  test_env=physical → paths_windows.json（Windows 物理机）
  test_env=virtual 或未设置 → paths.json（Ubuntu 虚拟机，默认）
"""
import os
import json

# KLinePredictor 的项目根目录
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 根目录的config（openclaw_project/config）
_ROOT_CONFIG_DIR = os.path.join(os.path.dirname(_PROJECT_ROOT), 'config')


def _load_paths():
    try:
        env = os.environ.get('test_env', 'virtual').strip().lower()
    except Exception:
        env = 'virtual'

    if env == 'physical':
        config_file = os.path.join(_ROOT_CONFIG_DIR, 'paths_windows.json')
    else:
        config_file = os.path.join(_ROOT_CONFIG_DIR, 'paths.json')

    if os.path.exists(config_file):
        with open(config_file, 'r', encoding='utf-8') as f:
            paths = json.load(f)
            # 覆盖project_root为KLinePredictor目录
            paths['project_root'] = _PROJECT_ROOT
            return paths

    # fallback：使用项目根目录推算
    return {
        'project_root': _PROJECT_ROOT,
        'data_dir': os.path.join(os.path.dirname(_PROJECT_ROOT), 'DATA'),
        'reports_dir': os.path.join(os.path.dirname(_PROJECT_ROOT), 'shares', 'reports'),
        'db_path': os.path.join(os.path.dirname(_PROJECT_ROOT), 'DATA', 'stock_data.db')
    }

PATHS = _load_paths()

# 导出常用路径
PROJECT_ROOT = PATHS['project_root']
DATA_DIR = PATHS['data_dir']
REPORTS_DIR = PATHS['reports_dir']
DB_PATH = PATHS['db_path']
CHARTS_DIR = os.path.join(PROJECT_ROOT, 'charts')