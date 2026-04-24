"""
技术指标管理器

设计原则：
- 每次分析一只股票时，动态实例化各指标对象
- 通过配置循环调用（责任链模式）
- 汇总评分和报告，返回完整分析结果
- 分析完毕后释放所有指标对象
"""

import os
import yaml
import logging
import importlib
from typing import List, Dict, Tuple, Any, Optional, Type
from pandas import DataFrame

from .base import BaseIndicator

logger = logging.getLogger(__name__)


class IndicatorManager:
    """
    指标管理器 - 动态实例化、循环调用、汇总结果

    使用方式：
        manager = IndicatorManager(config_path)
        result = manager.analyze_stock(code, df, context)
        # result包含: signals, report_lines, total_score, score_reasons
    """

    # 指标注册表（类引用）
    _indicator_registry: Dict[str, Type[BaseIndicator]] = {}

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化管理器

        参数:
            config_path: YAML配置文件路径
        """
        if config_path is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            config_path = os.path.join(base_dir, 'config', 'indicators.yaml')

        self.config_path = config_path
        self.config: Dict = {}
        self._load_config()
        self._load_registry()

    def _load_config(self) -> None:
        """加载YAML配置"""
        if not os.path.exists(self.config_path):
            logger.warning(f"指标配置文件不存在: {self.config_path}")
            self.config = {'indicators': []}
            return

        with open(self.config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f) or {'indicators': []}

        logger.info(f"加载指标配置: {len(self.config.get('indicators', []))}个指标")

    def _load_registry(self) -> None:
        """加载指标类注册表"""
        registry_map = {
            'supertrend': ('supertrend_indicator', 'SuperTrendIndicator'),
            'rsi': ('rsi_indicator', 'RSIIndicator'),
            'macd': ('macd_indicator', 'MACDIndicator'),
            'ma': ('ma_indicator', 'MAIndicator'),
            'adx': ('adx_indicator', 'ADXIndicator'),
            'volume': ('volume_indicator', 'VolumeIndicator'),
            'candle': ('candle_indicator', 'CandleIndicator'),
        }

        for name, (module_name, class_name) in registry_map.items():
            try:
                module = importlib.import_module(f'core.indicators.{module_name}')
                cls = getattr(module, class_name)
                self._indicator_registry[name] = cls
            except Exception as e:
                logger.error(f"加载指标类 {name} 失败: {e}")

    def analyze_stock(self, code: str, df: DataFrame, context: Dict) -> Dict:
        """
        分析单只股票（一站式接口）

        流程：
        1. 根据配置动态实例化各指标对象
        2. 循环调用获取信号、报告、评分
        3. 汇总结果
        4. 释放所有指标对象

        参数:
            code: 股票代码
            df: 日K数据DataFrame
            context: 上下文信息 {
                'current_price': float,
                'is_position': bool,
                'is_candidate': bool,
                'position_type': str,
                'entry_price': float (持仓时),
                'watch_price': float (候选时),
            }

        返回:
            Dict: {
                'code': str,
                'signals': List[Dict],      # 所有指标信号汇总
                'report_lines': List[str],  # 所有指标报告内容汇总
                'total_score': float,       # 加权综合评分
                'score_reasons': List[str], # 评分原因汇总
                'indicator_results': Dict,  # 各指标单独结果（可选）
            }
        """
        result = {
            'code': code,
            'signals': [],
            'report_lines': [],
            'total_score': 0.0,
            'score_reasons': [],
            'indicator_results': {},
        }

        if df is None or df.empty:
            logger.warning(f"[{code}] 日K数据为空，跳过分析")
            return result

        # 添加code到context
        context['code'] = code

        # 获取启用的指标配置列表
        indicators_config = self.config.get('indicators', [])
        enabled_configs = [c for c in indicators_config if c.get('enabled', True)]

        # 按report_order排序
        enabled_configs.sort(key=lambda x: x.get('report_order', 99))

        # 存储本次分析的指标实例（用于释放）
        indicator_instances: List[BaseIndicator] = []

        try:
            # 循环实例化并调用各指标（责任链模式）
            for indicator_config in enabled_configs:
                name = indicator_config.get('name')

                # 获取指标类
                cls = self._indicator_registry.get(name)
                if cls is None:
                    logger.warning(f"[{code}] 未注册指标类: {name}")
                    continue

                try:
                    # 动态实例化指标对象
                    indicator = cls(df, indicator_config, context)
                    indicator_instances.append(indicator)

                    # 获取信号
                    signals = indicator.get_signals()
                    result['signals'].extend(signals)

                    # 获取报告内容
                    report_lines = indicator.get_report_lines()
                    result['report_lines'].extend(report_lines)

                    # 获取评分
                    weighted_score = indicator.get_weighted_score()
                    score, reasons = indicator.get_score()
                    result['total_score'] += weighted_score
                    result['score_reasons'].extend(reasons)

                    # 存储各指标单独结果
                    result['indicator_results'][name] = {
                        'signals': signals,
                        'report_lines': report_lines,
                        'score': score,
                        'weighted_score': weighted_score,
                        'reasons': reasons,
                        'data': indicator.get_data(),
                    }

                    logger.debug(f"[{code}] {name}: score={score}, weighted={weighted_score}")

                except Exception as e:
                    logger.warning(f"[{code}] 指标 {name} 分析失败: {e}")

        finally:
            # 释放所有指标对象
            for indicator in indicator_instances:
                try:
                    indicator.cleanup()
                except Exception:
                    pass
            indicator_instances.clear()

        logger.info(f"[{code}] 分析完成: total_score={result['total_score']:.1f}, signals={len(result['signals'])}")

        return result

    def get_enabled_indicator_names(self) -> List[str]:
        """获取启用的指标名称列表"""
        indicators_config = self.config.get('indicators', [])
        return [c.get('name') for c in indicators_config if c.get('enabled', True)]

    def get_indicator_config(self, name: str) -> Optional[Dict]:
        """获取单个指标的配置"""
        for c in self.config.get('indicators', []):
            if c.get('name') == name:
                return c
        return None

    def reload_config(self) -> None:
        """重新加载配置（热更新）"""
        self._load_config()
        logger.info("配置已重新加载")