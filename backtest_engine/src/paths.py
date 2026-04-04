"""
路径配置加载模块（回测引擎版）
从 tail_trading/config/paths.json 加载所有路径配置
"""
import os
import json

# 回测引擎的上一级目录是项目根目录下的 backtest_engine/
# 项目根目录 = 当前文件上两级
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_config_path = os.path.join(_project_root, 'tail_trading', 'config', 'paths.json')

def _load_paths():
    if os.path.exists(_config_path):
        with open(_config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    # fallback：使用项目根目录推算
    root = _project_root
    return {
        'project_root': root,
        'data_dir': os.path.join(root, 'tail_trading', 'data'),
        'daily_data_dir': os.path.join(root, 'tail_trading', 'daily_data'),
        'minute_data_dir': os.path.join(root, 'tail_trading', 'minute_data'),
        'index_history_dir': os.path.join(root, 'tail_trading', 'index_history'),
        'reports_dir': os.path.join(root, '..', 'shares', 'reports'),
        'db_path': os.path.join(root, 'DATA', 'stock_data.db')
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
