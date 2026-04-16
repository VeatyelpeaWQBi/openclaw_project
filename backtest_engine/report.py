"""
回测报告生成器
计算回测指标、生成图表和HTML报告
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
from paths import REPORTS_DIR


class BacktestReport:
    """回测报告"""

    def __init__(self, results, initial_capital=100000):
        """
        参数:
            results: 交易结果列表 (list[dict]) 或 DataFrame
            initial_capital: 初始资金
        """
        if isinstance(results, pd.DataFrame):
            self.df = results
        elif isinstance(results, list):
            self.df = pd.DataFrame(results) if results else pd.DataFrame()
        else:
            self.df = pd.DataFrame()

        self.initial_capital = initial_capital

    def calculate_metrics(self):
        """计算回测指标"""
        if self.df.empty:
            return self._empty_metrics()

        returns = self.df['return_pct'].values / 100  # 转为小数
        n_trades = len(returns)
        wins = returns[returns > 0]
        losses = returns[returns <= 0]

        # 胜率
        win_rate = len(wins) / n_trades if n_trades > 0 else 0

        # 平均收益
        avg_return = np.mean(returns) if n_trades > 0 else 0

        # 累计收益
        total_return = np.prod(1 + returns) - 1

        # 年化收益（假设一年250个交易日，按交易次数估算）
        if 'buy_date' in self.df.columns and 'sell_date' in self.df.columns:
            try:
                first_date = pd.to_datetime(self.df['buy_date'].min())
                last_date = pd.to_datetime(self.df['sell_date'].max())
                days = (last_date - first_date).days
                if days > 0:
                    annual_return = (1 + total_return) ** (365 / days) - 1
                else:
                    annual_return = total_return
            except Exception:
                annual_return = total_return
        else:
            annual_return = total_return

        # 最大回撤（基于累计收益）
        cumulative = np.cumprod(1 + returns)
        peak = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - peak) / peak
        max_drawdown = abs(np.min(drawdown)) if len(drawdown) > 0 else 0

        # 盈亏比
        avg_win = np.mean(wins) if len(wins) > 0 else 0
        avg_loss = abs(np.mean(losses)) if len(losses) > 0 else 0.001
        profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 0

        # 夏普比率（假设无风险利率3%）
        risk_free = 0.03 / 250  # 日无风险利率
        if np.std(returns) > 0:
            sharpe_ratio = (np.mean(returns) - risk_free) / np.std(returns) * np.sqrt(250)
        else:
            sharpe_ratio = 0

        # 最大连续亏损次数
        max_consecutive_loss = self._max_consecutive(returns, lambda x: x <= 0)

        # 最大连续盈利次数
        max_consecutive_win = self._max_consecutive(returns, lambda x: x > 0)

        # 资金曲线
        if 'capital_after' in self.df.columns:
            final_capital = self.df['capital_after'].iloc[-1]
        else:
            final_capital = self.initial_capital * (1 + total_return)

        metrics = {
            'total_trades': n_trades,
            'winning_trades': len(wins),
            'losing_trades': len(losses),
            'win_rate': round(win_rate * 100, 2),
            'avg_return_pct': round(avg_return * 100, 2),
            'total_return_pct': round(total_return * 100, 2),
            'annual_return_pct': round(annual_return * 100, 2),
            'max_drawdown_pct': round(max_drawdown * 100, 2),
            'sharpe_ratio': round(sharpe_ratio, 2),
            'profit_loss_ratio': round(profit_loss_ratio, 2),
            'avg_win_pct': round(avg_win * 100, 2),
            'avg_loss_pct': round(-avg_loss * 100, 2),
            'max_consecutive_wins': max_consecutive_win,
            'max_consecutive_losses': max_consecutive_loss,
            'initial_capital': self.initial_capital,
            'final_capital': round(final_capital, 2),
            'net_profit': round(final_capital - self.initial_capital, 2),
        }

        return metrics

    def _empty_metrics(self):
        return {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'win_rate': 0,
            'avg_return_pct': 0,
            'total_return_pct': 0,
            'annual_return_pct': 0,
            'max_drawdown_pct': 0,
            'sharpe_ratio': 0,
            'profit_loss_ratio': 0,
            'avg_win_pct': 0,
            'avg_loss_pct': 0,
            'max_consecutive_wins': 0,
            'max_consecutive_losses': 0,
            'initial_capital': self.initial_capital,
            'final_capital': self.initial_capital,
            'net_profit': 0,
        }

    @staticmethod
    def _max_consecutive(arr, condition):
        """计算最大连续满足条件的次数"""
        max_count = 0
        current = 0
        for v in arr:
            if condition(v):
                current += 1
                max_count = max(max_count, current)
            else:
                current = 0
        return max_count

    def generate_charts(self, output_dir=None):
        """
        生成图表（收益曲线、回撤曲线等）

        参数:
            output_dir: 图表输出目录

        返回:
            dict: 图表文件路径
        """
        if self.df.empty:
            print("⚠️ 无数据，跳过图表生成")
            return {}

        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
            matplotlib.rcParams['axes.unicode_minus'] = False
        except ImportError:
            print("⚠️ matplotlib 未安装，跳过图表生成")
            return {}

        if output_dir is None:
            output_dir = REPORTS_DIR
        os.makedirs(output_dir, exist_ok=True)

        charts = {}
        returns = self.df['return_pct'].values / 100

        # 1. 资金曲线
        cumulative = np.cumprod(1 + returns)
        capital_curve = self.initial_capital * cumulative
        capital_curve = np.insert(capital_curve, 0, self.initial_capital)

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(range(len(capital_curve)), capital_curve, 'b-', linewidth=1.5)
        ax.set_title('Capital Curve', fontsize=14)
        ax.set_xlabel('Trade #')
        ax.set_ylabel('Capital (CNY)')
        ax.grid(True, alpha=0.3)
        path = os.path.join(output_dir, 'capital_curve.png')
        fig.savefig(path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        charts['capital_curve'] = path

        # 2. 收益分布
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.hist(returns * 100, bins=20, color='steelblue', edgecolor='white', alpha=0.8)
        ax.axvline(x=0, color='red', linestyle='--', linewidth=1)
        ax.set_title('Return Distribution', fontsize=14)
        ax.set_xlabel('Return (%)')
        ax.set_ylabel('Frequency')
        ax.grid(True, alpha=0.3)
        path = os.path.join(output_dir, 'return_distribution.png')
        fig.savefig(path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        charts['return_distribution'] = path

        # 3. 逐笔收益
        fig, ax = plt.subplots(figsize=(12, 5))
        colors = ['green' if r > 0 else 'red' for r in returns * 100]
        ax.bar(range(len(returns)), returns * 100, color=colors, alpha=0.7)
        ax.axhline(y=0, color='black', linewidth=0.5)
        ax.set_title('Trade Returns', fontsize=14)
        ax.set_xlabel('Trade #')
        ax.set_ylabel('Return (%)')
        ax.grid(True, alpha=0.3)
        path = os.path.join(output_dir, 'trade_returns.png')
        fig.savefig(path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        charts['trade_returns'] = path

        # 4. 回撤曲线
        peak = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - peak) / peak * 100
        fig, ax = plt.subplots(figsize=(12, 4))
        ax.fill_between(range(len(drawdown)), drawdown, 0, color='red', alpha=0.4)
        ax.set_title('Drawdown', fontsize=14)
        ax.set_xlabel('Trade #')
        ax.set_ylabel('Drawdown (%)')
        ax.grid(True, alpha=0.3)
        path = os.path.join(output_dir, 'drawdown.png')
        fig.savefig(path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        charts['drawdown'] = path

        print(f"📊 图表已保存至: {output_dir}")
        return charts

    def to_text(self):
        """生成文字报告"""
        metrics = self.calculate_metrics()
        lines = []
        lines.append("=" * 50)
        lines.append("  T+1 尾盘交易回测报告")
        lines.append("=" * 50)
        lines.append("")
        lines.append("📈 交易统计:")
        lines.append(f"  总交易次数:    {metrics['total_trades']}")
        lines.append(f"  盈利次数:      {metrics['winning_trades']}")
        lines.append(f"  亏损次数:      {metrics['losing_trades']}")
        lines.append(f"  胜率:          {metrics['win_rate']}%")
        lines.append("")
        lines.append("💰 收益指标:")
        lines.append(f"  平均收益:      {metrics['avg_return_pct']}%")
        lines.append(f"  平均盈利:      +{metrics['avg_win_pct']}%")
        lines.append(f"  平均亏损:      {metrics['avg_loss_pct']}%")
        lines.append(f"  盈亏比:        {metrics['profit_loss_ratio']}")
        lines.append(f"  累计收益:      {metrics['total_return_pct']}%")
        lines.append(f"  年化收益:      {metrics['annual_return_pct']}%")
        lines.append("")
        lines.append("⚠️ 风险指标:")
        lines.append(f"  最大回撤:      {metrics['max_drawdown_pct']}%")
        lines.append(f"  夏普比率:      {metrics['sharpe_ratio']}")
        lines.append(f"  最大连续盈利:  {metrics['max_consecutive_wins']}次")
        lines.append(f"  最大连续亏损:  {metrics['max_consecutive_losses']}次")
        lines.append("")
        lines.append("💵 资金:")
        lines.append(f"  初始资金:      ¥{metrics['initial_capital']:,.0f}")
        lines.append(f"  最终资金:      ¥{metrics['final_capital']:,.0f}")
        lines.append(f"  净利润:        ¥{metrics['net_profit']:,.0f}")
        lines.append("=" * 50)

        return '\n'.join(lines)

    def to_html(self, output_path=None, charts=None):
        """
        生成HTML报告

        参数:
            output_path: 输出文件路径
            charts: 图表文件路径字典

        返回:
            str: HTML报告内容
        """
        metrics = self.calculate_metrics()

        if output_path is None:
            report_dir = REPORTS_DIR
            os.makedirs(report_dir, exist_ok=True)
            output_path = os.path.join(report_dir, f'backtest_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html')

        # 交易明细表格
        trade_rows = ''
        if not self.df.empty:
            for _, row in self.df.iterrows():
                ret = row.get('return_pct', 0)
                color = 'green' if ret > 0 else 'red'
                trade_rows += f"""
                <tr>
                    <td>{row.get('code', '')}</td>
                    <td>{row.get('name', '')}</td>
                    <td>{row.get('sector', '')}</td>
                    <td>{row.get('buy_date', '')}</td>
                    <td>{row.get('sell_date', '')}</td>
                    <td>{row.get('buy_price', '')}</td>
                    <td>{row.get('sell_price', '')}</td>
                    <td style="color:{color}">{ret}%</td>
                    <td>{row.get('sell_reason', '')}</td>
                </tr>"""

        # 图表
        chart_html = ''
        if charts:
            for name, path in charts.items():
                chart_html += f'<img src="{os.path.basename(path)}" style="max-width:100%;margin:10px 0;">\n'

        html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>T+1 回测报告</title>
    <style>
        body {{ font-family: 'Microsoft YaHei', sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; border-bottom: 2px solid #4CAF50; padding-bottom: 10px; }}
        h2 {{ color: #555; margin-top: 30px; }}
        .metrics {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin: 20px 0; }}
        .metric {{ background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center; }}
        .metric .value {{ font-size: 24px; font-weight: bold; color: #333; }}
        .metric .label {{ font-size: 12px; color: #666; margin-top: 5px; }}
        .metric.positive .value {{ color: #4CAF50; }}
        .metric.negative .value {{ color: #f44336; }}
        table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
        th, td {{ padding: 8px 12px; border: 1px solid #ddd; text-align: center; font-size: 13px; }}
        th {{ background: #4CAF50; color: white; }}
        tr:nth-child(even) {{ background: #f9f9f9; }}
        .charts {{ margin: 20px 0; }}
    </style>
</head>
<body>
<div class="container">
    <h1>📊 T+1 尾盘交易回测报告</h1>
    <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

    <h2>📈 核心指标</h2>
    <div class="metrics">
        <div class="metric {'positive' if metrics['total_return_pct'] > 0 else 'negative'}">
            <div class="value">{metrics['total_return_pct']}%</div>
            <div class="label">累计收益率</div>
        </div>
        <div class="metric">
            <div class="value">{metrics['total_trades']}</div>
            <div class="label">总交易次数</div>
        </div>
        <div class="metric {'positive' if metrics['win_rate'] >= 50 else 'negative'}">
            <div class="value">{metrics['win_rate']}%</div>
            <div class="label">胜率</div>
        </div>
        <div class="metric {'positive' if metrics['profit_loss_ratio'] >= 1 else 'negative'}">
            <div class="value">{metrics['profit_loss_ratio']}</div>
            <div class="label">盈亏比</div>
        </div>
        <div class="metric negative">
            <div class="value">{metrics['max_drawdown_pct']}%</div>
            <div class="label">最大回撤</div>
        </div>
        <div class="metric">
            <div class="value">{metrics['sharpe_ratio']}</div>
            <div class="label">夏普比率</div>
        </div>
    </div>

    <h2>💰 收益详情</h2>
    <div class="metrics">
        <div class="metric">
            <div class="value">¥{metrics['initial_capital']:,.0f}</div>
            <div class="label">初始资金</div>
        </div>
        <div class="metric {'positive' if metrics['final_capital'] > metrics['initial_capital'] else 'negative'}">
            <div class="value">¥{metrics['final_capital']:,.0f}</div>
            <div class="label">最终资金</div>
        </div>
        <div class="metric {'positive' if metrics['net_profit'] > 0 else 'negative'}">
            <div class="value">¥{metrics['net_profit']:,.0f}</div>
            <div class="label">净利润</div>
        </div>
        <div class="metric">
            <div class="value">+{metrics['avg_win_pct']}% / {metrics['avg_loss_pct']}%</div>
            <div class="label">平均盈 / 亏</div>
        </div>
        <div class="metric">
            <div class="value">{metrics['annual_return_pct']}%</div>
            <div class="label">年化收益</div>
        </div>
        <div class="metric">
            <div class="value">{metrics['max_consecutive_wins']} / {metrics['max_consecutive_losses']}</div>
            <div class="label">最大连胜 / 连亏</div>
        </div>
    </div>

    <h2>📊 图表</h2>
    <div class="charts">
        {chart_html if chart_html else '<p>暂无图表</p>'}
    </div>

    <h2>📋 交易明细</h2>
    <table>
        <thead>
            <tr>
                <th>代码</th><th>名称</th><th>板块</th><th>买入日</th><th>卖出日</th>
                <th>买入价</th><th>卖出价</th><th>收益率</th><th>卖出原因</th>
            </tr>
        </thead>
        <tbody>
            {trade_rows if trade_rows else '<tr><td colspan="9">暂无交易记录</td></tr>'}
        </tbody>
    </table>
</div>
</body>
</html>"""

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)

        print(f"📄 HTML报告已保存至: {output_path}")
        return output_path

    def save_to_csv(self, output_path=None):
        """保存交易明细到CSV"""
        if output_path is None:
            report_dir = REPORTS_DIR
            os.makedirs(report_dir, exist_ok=True)
            output_path = os.path.join(report_dir, f'backtest_trades_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')

        if not self.df.empty:
            self.df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"📋 交易明细CSV已保存至: {output_path}")
        return output_path
