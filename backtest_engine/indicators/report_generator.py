"""
回测报告生成模块

生成买卖记录明细、月度/年度盈利情况等报告
"""

import logging
import os
from datetime import datetime
from typing import Dict, List

import pandas as pd

from .config import BacktestConfig

logger = logging.getLogger(__name__)


class BacktestResult:
    """回测结果"""

    def __init__(self, initial_capital: float, trades: List,
                 daily_values: Dict[str, float], trading_dates: List[str]):
        self.initial_capital = initial_capital
        self.trades = trades
        self.daily_values = daily_values
        self.trading_dates = trading_dates

        # 计算指标
        self._calculate_metrics()

    def _calculate_metrics(self):
        """计算回测指标"""
        if not self.daily_values:
            self.final_value = self.initial_capital
            self.total_return = 0.0
            self.annualized_return = 0.0
            self.max_drawdown = 0.0
            self.buy_count = 0
            self.sell_count = 0
            return

        self.final_value = list(self.daily_values.values())[-1]
        self.total_return = (self.final_value - self.initial_capital) / self.initial_capital * 100

        # 年化收益率
        if len(self.daily_values) >= 2:
            start_date = list(self.daily_values.keys())[0]
            end_date = list(self.daily_values.keys())[-1]
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            years = (end_dt - start_dt).days / 365.25

            if years > 0:
                total_return_decimal = self.total_return / 100
                self.annualized_return = ((1 + total_return_decimal) ** (1 / years) - 1) * 100
            else:
                self.annualized_return = 0.0
        else:
            self.annualized_return = 0.0

        # 最大回撤
        values = list(self.daily_values.values())
        peak = values[0]
        self.max_drawdown = 0.0

        for value in values:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak * 100
            if drawdown > self.max_drawdown:
                self.max_drawdown = drawdown

        # 交易统计
        self.buy_count = sum(1 for t in self.trades if t.trade_type == 'buy')
        self.sell_count = sum(1 for t in self.trades if t.trade_type == 'sell')

    def to_dataframe(self) -> pd.DataFrame:
        """将交易记录转为DataFrame"""
        if not self.trades:
            return pd.DataFrame()

        rows = []
        for trade in self.trades:
            rows.append({
                'code': trade.code,
                'name': trade.name,
                'trade_type': trade.trade_type,
                'trade_date': trade.trade_date,
                'price': trade.price,
                'quantity': trade.quantity,
                'amount': trade.amount,
                'signal_strength': trade.signal_strength,
                'macd_rank': trade.macd_rank,
                'obv_slope': trade.obv_slope,
            })

        return pd.DataFrame(rows)

    def generate_monthly_report(self) -> pd.DataFrame:
        """生成月度盈利报告"""
        if not self.daily_values:
            return pd.DataFrame()

        # 按月分组
        monthly = {}
        for date, value in self.daily_values.items():
            year_month = date[:7]  # YYYY-MM
            if year_month not in monthly:
                monthly[year_month] = []
            monthly[year_month].append((date, value))

        # 计算每月收益
        report = []
        prev_value = self.initial_capital

        for year_month in sorted(monthly.keys()):
            dates, values = zip(*monthly[year_month])
            month_value = values[-1]

            month_return = (month_value - prev_value) / prev_value * 100 if prev_value > 0 else 0

            report.append({
                'year_month': year_month,
                'start_value': prev_value,
                'end_value': month_value,
                'return_pct': month_return,
            })

            prev_value = month_value

        return pd.DataFrame(report)

    def generate_yearly_report(self) -> pd.DataFrame:
        """生成年度盈利报告"""
        if not self.daily_values:
            return pd.DataFrame()

        # 按年分组
        yearly = {}
        for date, value in self.daily_values.items():
            year = date[:4]
            if year not in yearly:
                yearly[year] = []
            yearly[year].append((date, value))

        # 计算每年收益
        report = []
        prev_value = self.initial_capital

        for year in sorted(yearly.keys()):
            dates, values = zip(*yearly[year])
            year_value = values[-1]

            year_return = (year_value - prev_value) / prev_value * 100 if prev_value > 0 else 0

            # 计算该年度交易次数
            year_trades = [t for t in self.trades if t.trade_date.startswith(year)]
            buy_count = sum(1 for t in year_trades if t.trade_type == 'buy')
            sell_count = sum(1 for t in year_trades if t.trade_type == 'sell')

            report.append({
                'year': year,
                'start_value': prev_value,
                'end_value': year_value,
                'return_pct': year_return,
                'buy_count': buy_count,
                'sell_count': sell_count,
            })

            prev_value = year_value

        return pd.DataFrame(report)

    def generate_summary(self) -> dict:
        """生成汇总信息"""
        return {
            'initial_capital': self.initial_capital,
            'final_value': self.final_value,
            'total_return': self.total_return,
            'annualized_return': self.annualized_return,
            'max_drawdown': self.max_drawdown,
            'buy_count': self.buy_count,
            'sell_count': self.sell_count,
            'trading_days': len(self.trading_dates),
        }

    def save_to_file(self, output_dir: str = None):
        """
        保存报告到文件

        参数:
            output_dir: 输出目录，默认为 config.OUTPUT_DIR
        """
        if output_dir is None:
            from .config import BacktestConfig
            output_dir = BacktestConfig.OUTPUT_DIR

        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # 保存交易记录
        trades_df = self.to_dataframe()
        if not trades_df.empty:
            trades_file = os.path.join(output_dir, f'trades_{timestamp}.csv')
            trades_df.to_csv(trades_file, index=False, encoding='utf-8-sig')
            logger.info(f"交易记录已保存: {trades_file}")

        # 保存月度报告
        monthly_df = self.generate_monthly_report()
        if not monthly_df.empty:
            monthly_file = os.path.join(output_dir, f'monthly_{timestamp}.csv')
            monthly_df.to_csv(monthly_file, index=False, encoding='utf-8-sig')
            logger.info(f"月度报告已保存: {monthly_file}")

        # 保存年度报告
        yearly_df = self.generate_yearly_report()
        if not yearly_df.empty:
            yearly_file = os.path.join(output_dir, f'yearly_{timestamp}.csv')
            yearly_df.to_csv(yearly_file, index=False, encoding='utf-8-sig')
            logger.info(f"年度报告已保存: {yearly_file}")

        # 保存汇总信息
        summary = self.generate_summary()
        summary_file = os.path.join(output_dir, f'summary_{timestamp}.json')
        import json
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        logger.info(f"汇总信息已保存: {summary_file}")

        # 保存每日净值曲线
        if self.daily_values:
            daily_file = os.path.join(output_dir, f'portfolio_value_{timestamp}.csv')
            daily_df = pd.DataFrame([
                {'date': date, 'portfolio_value': value}
                for date, value in self.daily_values.items()
            ])
            daily_df.to_csv(daily_file, index=False, encoding='utf-8-sig')
            logger.info(f"每日净值已保存: {daily_file}")

    def print_summary(self):
        """打印汇总信息"""
        summary = self.generate_summary()

        print("\n" + "=" * 60)
        print("回测结果汇总")
        print("=" * 60)
        print(f"初始资金: {summary['initial_capital']:,.2f} 元")
        print(f"最终价值: {summary['final_value']:,.2f} 元")
        print(f"总收益率: {summary['total_return']:.2f}%")
        print(f"年化收益率: {summary['annualized_return']:.2f}%")
        print(f"最大回撤: {summary['max_drawdown']:.2f}%")
        print(f"买入次数: {summary['buy_count']}")
        print(f"卖出次数: {summary['sell_count']}")
        print(f"交易日数: {summary['trading_days']}")
        print("=" * 60)

        # 打印年度报告
        yearly_df = self.generate_yearly_report()
        if not yearly_df.empty:
            print("\n年度收益:")
            print("-" * 60)
            for _, row in yearly_df.iterrows():
                print(f"{row['year']}: {row['return_pct']:+.2f}% "
                      f"(买入{row['buy_count']}次, 卖出{row['sell_count']}次)")
            print("=" * 60)