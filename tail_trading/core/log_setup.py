"""
日志初始化模块
从 config/logging.yaml 读取配置，设置根logger
"""
import logging
import logging.handlers
import os
import yaml
from datetime import datetime

from core.paths import PROJECT_ROOT


def setup_logging(config_path=None, log_dir=None):
    """
    初始化日志系统

    参数:
        config_path: logging.yaml 路径，默认 config/logging.yaml
        log_dir: 日志目录覆盖（prod模式用）
    """
    if config_path is None:
        config_path = os.path.join(PROJECT_ROOT, 'config', 'logging.yaml')

    # 默认配置
    log_level = 'DEBUG'
    file_enabled = True
    console_enabled = True
    console_level = 'INFO'

    # 读取yaml
    if os.path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f).get('logging', {})
        log_level = cfg.get('level', 'DEBUG')
        file_cfg = cfg.get('file', {})
        file_enabled = file_cfg.get('enabled', True)
        if log_dir is None:
            log_dir = file_cfg.get('dir', 'logs')
        console_cfg = cfg.get('console', {})
        console_enabled = console_cfg.get('enabled', True)
        console_level = console_cfg.get('level', 'INFO')

    # 确保日志目录存在
    log_dir = os.path.abspath(os.path.join(PROJECT_ROOT, log_dir))
    os.makedirs(log_dir, exist_ok=True)

    # 配置根logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.DEBUG))

    # 清除已有handler
    root_logger.handlers.clear()

    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 文件handler（按天轮转）
    if file_enabled:
        today = datetime.now().strftime('%Y-%m-%d')
        log_file = os.path.join(log_dir, f'tail_trading_{today}.log')
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
        )
        file_handler.setLevel(getattr(logging, log_level.upper(), logging.DEBUG))
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # 控制台handler
    if console_enabled:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, console_level.upper(), logging.INFO))
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    return root_logger
