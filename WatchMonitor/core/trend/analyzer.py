"""
趋势分析管理器

功能：统一管理趋势检测和场景分析
- 趋势检测：判断上升/下降/横盘
- 场景分析：根据趋势类型执行对应分析
- 结果汇总：整合所有信号和报告
"""

import logging
from typing import Dict, List, Optional
from pandas import DataFrame

from .detector import TrendDetector
from .scenarios import UptrendScenario, DowntrendScenario, SidewaysScenario

logger = logging.getLogger(__name__)


class TrendAnalyzer:
    """趋势分析管理器"""

    # 场景注册表
    _scenario_registry = {
        'uptrend': UptrendScenario,
        'downtrend': DowntrendScenario,
        'sideways': SidewaysScenario,
    }

    def __init__(self, config: Dict = None):
        """
        初始化趋势分析管理器

        参数:
            config: 配置参数（可选）
        """
        self.config = config or {}

    def _get_detector_config(self) -> Dict:
        """获取TrendDetector的配置"""
        # 从config中提取trend_detection部分，如果没有则返回None让detector自己加载
        if self.config and 'trend_detection' in self.config:
            return self.config['trend_detection']
        return None

    def _get_history_days(self) -> int:
        """获取历史数据天数"""
        # 优先使用配置，其次使用默认值120天
        if self.config and 'trend_detection' in self.config:
            return self.config['trend_detection'].get('max_history_days', 120)
        return 120

    def analyze_stock(self, code: str, df: DataFrame, context: Dict) -> Dict:
        """
        分析单只股票的趋势和场景

        参数:
            code: 股票代码
            df: 日K数据
            context: 上下文信息 {
                'code': str,
                'name': str,
                'current_price': float,
                'is_position': bool,
                'is_candidate': bool,
                'entry_price': float (持仓时),
                'watch_price': float (候选时),
                # ... 其他上下文
            }

        返回:
            dict: {
                'code': str,
                'trend': dict,           # 趋势检测结果
                'scenario': dict,        # 场景分析结果
                'signals': list,         # 所有信号
                'report_lines': list,   # 报告内容
            }
        """
        if df is None or df.empty:
            return self._empty_result(code, context)

        # 1. 检测趋势类型
        detector_config = self._get_detector_config()
        detector = TrendDetector(config=detector_config)
        trend_result = detector.detect_trend(df, code)

        # 2. 根据趋势类型选择场景分析器
        scenario_cls = self._scenario_registry.get(trend_result['trend_type'])
        if scenario_cls is None:
            logger.warning(f"[{code}] 未找到场景分析器: {trend_result['trend_type']}")
            return self._empty_result(code, context, trend_result=trend_result)

        # 3. 执行场景分析（不修改原始context，创建新字典）
        scenario_context = {**context, **trend_result}
        scenario = scenario_cls(df, scenario_context)

        # ✅ 显式调用场景分析的执行流程
        scenario.execute()

        # 4. 汇总结果
        result = {
            'code': code,
            'name': context.get('name', ''),
            'trend': trend_result,
            'scenario': {
                'type': scenario.scenario_type,
                'name': scenario.scenario_name,
                'analysis': scenario.get_analysis_result(),
                'summary': scenario.get_summary(),
            },
            'signals': scenario.get_signals(),
            'report_lines': scenario.get_report_lines(),
        }

        logger.debug(f"[{code}] 趋势分析完成: {trend_result['trend_name']}, 信号{len(result['signals'])}个")
        return result

    def analyze_pool(self, items: List[Dict], pool_type: str = 'position') -> List[Dict]:
        """
        批量分析持仓池/候选池

        参数:
            items: 持仓/候选列表
            pool_type: 'position' 或 'candidate'

        返回:
            list: 分析结果列表
        """
        from core.storage import get_daily_data_from_sqlite

        results = []
        history_days = self._get_history_days()

        for item in items:
            code = item['code']

            # 获取日K数据（使用配置的历史天数）
            df = get_daily_data_from_sqlite(code, days=history_days)
            if df.empty:
                logger.debug(f"[{code}] 无日K数据，跳过分析")
                continue

            # 构建上下文
            context = self._build_context(item, pool_type, df)

            # 执行分析
            result = self.analyze_stock(code, df, context)
            results.append(result)

        logger.info(f"趋势分析完成: {pool_type}池 {len(results)}只股票")
        return results

    def _build_context(self, item: Dict, pool_type: str, df: DataFrame) -> Dict:
        """
        构建上下文信息

        参数:
            item: 持仓/候选信息
            pool_type: 'position' 或 'candidate'
            df: 日K数据

        返回:
            dict: 上下文信息
        """
        context = {
            'code': item.get('code'),
            'name': item.get('name'),
            'current_price': df['close'].iloc[-1] if not df.empty else None,
            'is_position': pool_type == 'position',
            'is_candidate': pool_type == 'candidate',
        }

        if pool_type == 'position':
            context.update({
                'entry_price': item.get('entry_price'),
                'position_type': item.get('position_type'),
                'stop_loss': item.get('stop_loss'),
                'take_profit': item.get('take_profit'),
            })
        elif pool_type == 'candidate':
            context.update({
                'watch_price': item.get('watch_price'),
                'watch_type': item.get('watch_type'),
                'watch_reason': item.get('watch_reason'),
            })

        return context

    def _empty_result(self, code: str, context: Dict = None, trend_result: Dict = None) -> Dict:
        """返回空结果"""
        if context is None:
            context = {}

        return {
            'code': code,
            'trend': trend_result or {
                'code': code,
                'trend_type': 'sideways',
                'trend_name': '数据不足',
                'score': 50.0,
                'strength': 'weak',
                'details': {'reason': '数据不足，无法判断趋势'},
            },
            'scenario': {
                'type': 'sideways',
                'name': '无',
                'analysis': {},
                'summary': '数据不足，无法进行场景分析',
            },
            'signals': [],
            'report_lines': ['    - 数据不足，无法进行趋势分析'],
        }

    @staticmethod
    def format_trend_summary(result: Dict) -> str:
        """
        格式化趋势摘要（单行展示）

        参数:
            result: analyze_stock 的返回结果

        返回:
            str: 格式化摘要，如 "600000 浦发银行: 上升趋势(72分)"
        """
        code = result.get('code', '')
        name = result.get('name', '')
        trend = result.get('trend', {})
        scenario = result.get('scenario', {})

        trend_name = trend.get('trend_name', '未知')
        score = trend.get('score', 0)
        summary = scenario.get('summary', '')

        return f"{code} {name}: {trend_name}({score:.0f}分) - {summary}"
