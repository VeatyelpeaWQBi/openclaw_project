#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
图表渲染工具模块
使用pyecharts生成K线预测对比图表
"""

import os
import pandas as pd
from typing import List, Optional
from datetime import datetime

from pyecharts.charts import Kline, Line, Bar, Grid
from pyecharts import options as opts


class KlineChartRenderer:
    """K线图表渲染器"""

    def __init__(self, output_dir: str = '', title: str = 'K线分析',
                 theme: str = 'dark'):
        """
        Args:
            output_dir: 输出目录，默认使用CHARTS_DIR
            title: 图表标题
            theme: 主题风格 ('dark' 或 'light')
        """
        self.output_dir = output_dir
        self.title = title
        self.theme = theme

        # 主题颜色配置
        self.colors = {
            'dark': {
                'bg': '#1a1a2e',
                'text': '#eee',
                'hist_up': '#00aa00',       # 历史涨：绿
                'hist_down': '#ff4444',     # 历史跌：红
                'pred_up': '#9933ff',       # 预测涨：紫
                'pred_down': '#4a90d9',     # 预测跌：蓝
                'split_line': '#ff9800',    # 分界线：橙
                'ma5': '#e0e0e0',
                'ma10': '#ffb347',
                'ma20': '#87ceeb',
                'vol_up': '#00aa00',
                'vol_down': '#ff4444'
            },
            'light': {
                'bg': '#ffffff',
                'text': '#333',
                'hist_up': '#00aa00',
                'hist_down': '#ff4444',
                'pred_up': '#9933ff',
                'pred_down': '#4a90d9',
                'split_line': '#ff9800',
                'ma5': '#888',
                'ma10': '#ff9800',
                'ma20': '#03a9f4',
                'vol_up': '#00aa00',
                'vol_down': '#ff4444'
            }
        }
        self.current_colors = self.colors.get(theme, self.colors['dark'])

    def _calc_ma(self, data: List, period: int) -> List:
        """计算均线"""
        result = []
        for i in range(len(data)):
            if i < period:
                result.append(None)
            else:
                sum_close = sum(data[j][1] for j in range(i - period + 1, i + 1))
                result.append(round(sum_close / period, 2))
        return result

    def _prepare_kline_data(self, df: pd.DataFrame, date_col: str = 'date') -> tuple:
        """准备K线数据"""
        dates = []
        if date_col in df.columns:
            dates = [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)[:10]
                     for d in df[date_col]]
        elif isinstance(df.index, pd.DatetimeIndex):
            dates = [d.strftime('%Y-%m-%d') for d in df.index]
        else:
            dates = [str(d)[:10] for d in df.index]

        kline_data = df[['open', 'close', 'low', 'high']].round(2).values.tolist()

        volumes = [int(v) for v in df['volume']]

        return dates, kline_data, volumes

    def render_prediction_chart(self,
                                 hist_df: pd.DataFrame,
                                 pred_df: pd.DataFrame,
                                 stock_code: str = '',
                                 save_path: str = '',
                                 show_ma: Optional[List[int]] = None,
                                 show_volume: bool = True,
                                 date_col: str = 'date') -> str:
        """生成K线预测对比图表（历史+预测）"""

        if show_ma is None:
            show_ma = [5, 10, 20]

        # 准备历史数据
        hist_dates, hist_kline, hist_volumes = self._prepare_kline_data(hist_df, date_col)
        hist_len = len(hist_dates)

        # 准备预测数据
        if pred_df.empty:
            pred_dates, pred_kline, pred_volumes = [], [], []
        else:
            pred_dates, pred_kline, pred_volumes = self._prepare_kline_data(pred_df, date_col)

        # 合并数据
        all_dates = hist_dates + pred_dates
        all_kline = hist_kline + pred_kline
        all_volumes = hist_volumes + pred_volumes
        split_idx = hist_len

        # 生成文件路径
        if not save_path:
            if not self.output_dir:
                from core.paths import CHARTS_DIR
                output_dir = CHARTS_DIR
            else:
                output_dir = self.output_dir
            os.makedirs(output_dir, exist_ok=True)

            last_date = hist_dates[-1] if hist_dates else 'unknown'
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'{stock_code}_pred_{last_date}_{timestamp}.html' if stock_code else f'kline_pred_{last_date}_{timestamp}.html'
            save_path = os.path.join(output_dir, filename)

        # 构建K线数据（带分界信息）
        # 格式: [open, close, low, high, is_prediction]
        kline_with_flag = []
        for i, k in enumerate(all_kline):
            is_pred = i >= split_idx
            kline_with_flag.append({
                'value': k,
                'itemStyle': {
                    'color': self.current_colors['pred_up'] if is_pred else self.current_colors['hist_up'],
                    'color0': self.current_colors['pred_down'] if is_pred else self.current_colors['hist_down'],
                    'borderColor': self.current_colors['pred_up'] if is_pred else self.current_colors['hist_up'],
                    'borderColor0': self.current_colors['pred_down'] if is_pred else self.current_colors['hist_down']
                }
            })

        # 根据涨跌动态调整颜色
        for i, item in enumerate(kline_with_flag):
            k = item['value']
            is_up = k[1] >= k[0]  # close >= open
            is_pred = i >= split_idx
            if is_pred:
                color = self.current_colors['pred_up'] if is_up else self.current_colors['pred_down']
            else:
                color = self.current_colors['hist_up'] if is_up else self.current_colors['hist_down']
            item['itemStyle']['color'] = color
            item['itemStyle']['borderColor'] = color
            # color0用于跌，这里统一处理
            if not is_up:
                item['itemStyle']['color0'] = color
                item['itemStyle']['borderColor0'] = color

        # 创建K线图 - 历史数据
        kline_hist = Kline()
        kline_hist.add_xaxis(hist_dates)
        kline_hist.add_yaxis(
            series_name="历史K线",
            y_axis=hist_kline,
            itemstyle_opts=opts.ItemStyleOpts(
                color=self.current_colors['hist_up'],
                color0=self.current_colors['hist_down'],
                border_color=self.current_colors['hist_up'],
                border_color0=self.current_colors['hist_down']
            )
        )

        # 创建K线图 - 预测数据
        if pred_kline:
            kline_pred = Kline()
            kline_pred.add_xaxis(pred_dates)
            kline_pred.add_yaxis(
                series_name="预测K线",
                y_axis=pred_kline,
                itemstyle_opts=opts.ItemStyleOpts(
                    color=self.current_colors['pred_up'],
                    color0=self.current_colors['pred_down'],
                    border_color=self.current_colors['pred_up'],
                    border_color0=self.current_colors['pred_down']
                )
            )

        # 合并所有K线数据用于tooltip和图表
        combined_kline = Kline()
        combined_kline.add_xaxis(all_dates)
        combined_kline.add_yaxis(
            series_name="K线",
            y_axis=all_kline,
            itemstyle_opts=opts.ItemStyleOpts(
                color=self.current_colors['hist_up'],
                color0=self.current_colors['hist_down'],
                border_color=self.current_colors['hist_up'],
                border_color0=self.current_colors['hist_down']
            )
        )

        title_text = f"{stock_code} K线预测分析" if stock_code else self.title

        # 全局配置
        global_opts = {
            'title_opts': opts.TitleOpts(
                title=title_text,
                pos_left="center",
                title_textstyle_opts=opts.TextStyleOpts(color=self.current_colors['text'])
            ),
            'legend_opts': opts.LegendOpts(
                pos_top="5%",
                selected_map={'历史K线': True, '预测K线': True, '分界线': True},
                textstyle_opts=opts.TextStyleOpts(color=self.current_colors['text'])
            ),
            'tooltip_opts': opts.TooltipOpts(
                trigger="axis",
                axis_pointer_type="cross",
                background_color="rgba(30,30,50,0.9)",
                border_color="#333",
                textstyle_opts=opts.TextStyleOpts(color="#fff")
            ),
            'datazoom_opts': [
                opts.DataZoomOpts(
                    is_show=True,
                    type_="slider",
                    pos_bottom="10%",
                    range_start=50,
                    range_end=100
                ),
                opts.DataZoomOpts(
                    type_="inside",
                    range_start=50,
                    range_end=100
                )
            ],
            'yaxis_opts': opts.AxisOpts(
                is_scale=True,
                splitarea_opts=opts.SplitAreaOpts(
                    is_show=True,
                    areastyle_opts=opts.AreaStyleOpts(opacity=1)
                ),
                axislabel_opts=opts.LabelOpts(color=self.current_colors['text'])
            ),
            'xaxis_opts': opts.AxisOpts(
                is_scale=True,
                axislabel_opts=opts.LabelOpts(rotate=45, color=self.current_colors['text'])
            )
        }

        combined_kline.set_global_opts(**global_opts)

        # 添加均线
        line = Line()
        line.add_xaxis(all_dates)

        ma_colors = {5: self.current_colors['ma5'],
                     10: self.current_colors['ma10'],
                     20: self.current_colors['ma20']}

        for period in show_ma:
            ma_data = self._calc_ma(all_kline, period)
            line.add_yaxis(
                series_name=f"MA{period}",
                y_axis=ma_data,
                is_smooth=True,
                linestyle_opts=opts.LineStyleOpts(width=1, color=ma_colors.get(period, '#888')),
                label_opts=opts.LabelOpts(is_show=False),
                symbol_size=0
            )

        # 成交量图
        if show_volume and all_volumes:
            bar = Bar()
            bar.add_xaxis(all_dates)
            # 根据涨跌设置成交量颜色
            vol_colors = []
            for i, k in enumerate(all_kline):
                is_up = k[1] >= k[0]
                is_pred = i >= split_idx
                if is_pred:
                    vol_colors.append(self.current_colors['pred_up'] if is_up else self.current_colors['pred_down'])
                else:
                    vol_colors.append(self.current_colors['vol_up'] if is_up else self.current_colors['vol_down'])

            bar.add_yaxis(
                series_name="成交量",
                y_axis=all_volumes,
                label_opts=opts.LabelOpts(is_show=False),
                itemstyle_opts=opts.ItemStyleOpts(color=self.current_colors['hist_up'])
            )
            bar.set_global_opts(
                tooltip_opts=opts.TooltipOpts(trigger="axis"),
                yaxis_opts=opts.AxisOpts(is_scale=True)
            )
        else:
            bar = None

        # 组合图表 - K线叠加均线和分界线
        kline_overlap = combined_kline.overlap(line)

        grid = Grid()
        grid.add(
            kline_overlap,
            grid_opts=opts.GridOpts(pos_left="10%", pos_right="10%", height="55%")
        )

        if bar:
            grid.add(
                bar,
                grid_opts=opts.GridOpts(pos_left="10%", pos_right="10%", pos_top="70%", height="20%")
            )

        # 在HTML中注入splitIdx变量
        grid.render(save_path)

        # 修改HTML文件，添加颜色区分和分界线
        with open(save_path, 'r', encoding='utf-8') as f:
            html_content = f.read()

        # 从HTML中提取图表ID
        import re
        chart_id_match = re.search(r'id="([a-f0-9]+)"', html_content)
        chart_id = chart_id_match.group(1) if chart_id_match else ''

        # 构建颜色变量（注入到head中，让formatter能访问）
        hist_up = self.current_colors['hist_up']
        hist_down = self.current_colors['hist_down']
        pred_up = self.current_colors['pred_up']
        pred_down = self.current_colors['pred_down']
        split_color = self.current_colors['split_line']

        # 先在head中注入全局变量
        global_vars = '''
<script>
var splitIdx = ''' + str(split_idx) + ''';
var histUpColor = "''' + hist_up + '''";
var histDownColor = "''' + hist_down + '''";
var predUpColor = "''' + pred_up + '''";
var predDownColor = "''' + pred_down + '''";
var splitLineColor = "''' + split_color + '''";
</script>
'''
        if '</head>' in html_content:
            html_content = html_content.replace('</head>', global_vars + '\n</head>')

        # 构建颜色修改和分界线的JavaScript代码
        inject_js = '''
<script>
(function() {
    var chartId = "''' + chart_id + '''";

    setTimeout(function() {
        var chartDiv = document.getElementById(chartId);
        if (!chartDiv) {
            chartDiv = document.querySelector('.chart-container');
        }
        if (!chartDiv) return;

        var chart = echarts.getInstanceByDom(chartDiv);
        if (!chart) return;

        var option = chart.getOption();

        // 设置tooltip formatter - 处理数组或对象形式
        var tooltipObj = Array.isArray(option.tooltip) ? option.tooltip[0] : option.tooltip;
        if (tooltipObj) {
            tooltipObj.formatter = function(params) {
                var klineParam = null;
                for (var i = 0; i < params.length; i++) {
                    if (params[i].seriesType === 'candlestick') {
                        klineParam = params[i];
                        break;
                    }
                }
                if (!klineParam) return '';

                var d = klineParam.data;
                // 数据格式: [open, close, low, high] 或 {value: [index, open, close, low, high], ...}
                var values;
                if (Array.isArray(d) && d.length === 4) {
                    values = d;
                } else if (d && d.value && Array.isArray(d.value)) {
                    // value数组可能包含[index, open, close, low, high]或[open, close, low, high]
                    values = d.value.length === 5 ? d.value.slice(1) : d.value;
                } else {
                    return '';
                }
                if (values.length < 4) return '';

                var date = klineParam.axisValue;
                var idx = klineParam.dataIndex;
                var isPred = idx >= splitIdx;
                var tag = isPred ? '【预测】' : '【历史】';
                var chg = ((values[1] - values[0]) / values[0] * 100).toFixed(2);
                var chgAmt = (values[1] - values[0]).toFixed(2);

                return tag + '<br/>' +
                    '日期: ' + date + '<br/>' +
                    '开盘: ' + values[0].toFixed(2) + '<br/>' +
                    '收盘: ' + values[1].toFixed(2) + '<br/>' +
                    '涨跌额: ' + (chgAmt >= 0 ? '+' : '') + chgAmt + '<br/>' +
                    '涨跌幅: ' + (chg >= 0 ? '+' : '') + chg + '%<br/>' +
                    '最低: ' + values[2].toFixed(2) + '<br/>' +
                    '最高: ' + values[3].toFixed(2);
            };
        }

        if (option.series) {
            option.series.forEach(function(series) {
                if (series.type === 'candlestick') {
                    var newData = [];
                    for (var idx = 0; idx < series.data.length; idx++) {
                        var k = series.data[idx];
                        if (!Array.isArray(k)) continue;
                        var isUp = k[1] >= k[0];
                        var isPred = idx >= splitIdx;
                        var color = isPred ? (isUp ? predUpColor : predDownColor) : (isUp ? histUpColor : histDownColor);
                        newData.push({
                            value: k,
                            itemStyle: { color: color, color0: color, borderColor: color, borderColor0: color }
                        });
                    }
                    series.data = newData;
                    series.markLine = {
                        symbol: ['none', 'none'],
                        label: { show: true, formatter: '预测起点', color: splitLineColor, position: 'end' },
                        lineStyle: { color: splitLineColor, width: 2, type: 'dashed' },
                        data: [{ xAxis: splitIdx }]
                    };
                }
            });
        }
        chart.setOption(option);
    }, 200);
})();
</script>
'''

        # 在body结束前注入
        if '</body>' in html_content:
            html_content = html_content.replace('</body>', inject_js + '\n</body>')

        with open(save_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"图表已保存: {save_path}")
        return save_path

    def render_single_chart(self,
                            df: pd.DataFrame,
                            title: str = '',
                            save_path: str = '',
                            show_ma: Optional[List[int]] = None,
                            show_volume: bool = True,
                            date_col: str = 'date') -> str:
        """生成单张K线图表（无预测对比）"""
        self.title = title if title else 'K线图表'
        return self.render_prediction_chart(
            hist_df=df,
            pred_df=pd.DataFrame(),
            stock_code='',
            save_path=save_path,
            show_ma=show_ma,
            show_volume=show_volume,
            date_col=date_col
        )

    def render_multi_comparison(self,
                                df_list: List[pd.DataFrame],
                                labels: List[str],
                                title: str = '多股票对比',
                                save_path: str = '',
                                date_col: str = 'date') -> str:
        """生成多股票收盘价对比图（折线图）"""

        if not save_path:
            if not self.output_dir:
                from core.paths import CHARTS_DIR
                output_dir = CHARTS_DIR
            else:
                output_dir = self.output_dir
            os.makedirs(output_dir, exist_ok=True)
            save_path = os.path.join(output_dir, f'comparison_{len(df_list)}.html')

        line = Line()
        line.set_global_opts(
            title_opts=opts.TitleOpts(title=title, pos_left="center"),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            datazoom_opts=[
                opts.DataZoomOpts(type_="slider", pos_bottom="10%"),
                opts.DataZoomOpts(type_="inside")
            ],
            yaxis_opts=opts.AxisOpts(is_scale=True),
            legend_opts=opts.LegendOpts(pos_top="5%")
        )

        colors = ['#00aa00', '#ff4444', '#4a90d9', '#ff6b6b', '#ffb347', '#87ceeb']

        for idx, (df, label) in enumerate(zip(df_list, labels)):
            dates = [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)[:10]
                     for d in df[date_col]]
            closes = df['close'].tolist()

            if idx == 0:
                line.add_xaxis(dates)

            line.add_yaxis(
                series_name=label,
                y_axis=closes,
                linestyle_opts=opts.LineStyleOpts(width=2, color=colors[idx % len(colors)]),
                label_opts=opts.LabelOpts(is_show=False)
            )

        line.render(save_path)
        print(f"对比图表已保存: {save_path}")
        return save_path


def create_renderer(output_dir: str = '', theme: str = 'dark') -> KlineChartRenderer:
    """创建图表渲染器的便捷函数"""
    return KlineChartRenderer(output_dir=output_dir, theme=theme)


# 示例用法
if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from core.paths import CHARTS_DIR

    renderer = create_renderer(CHARTS_DIR)

    hist_df = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=50),
        'open': [10 + i * 0.1 for i in range(50)],
        'close': [10 + i * 0.2 for i in range(50)],
        'low': [10 + i * 0.05 for i in range(50)],
        'high': [10 + i * 0.25 for i in range(50)],
        'volume': [1000000 for i in range(50)]
    })

    pred_df = pd.DataFrame({
        'date': pd.date_range('2025-02-20', periods=10),
        'open': [11 + i * 0.15 for i in range(10)],
        'close': [11 + i * 0.25 for i in range(10)],
        'low': [11 + i * 0.1 for i in range(10)],
        'high': [11 + i * 0.3 for i in range(10)],
        'volume': [1200000 for i in range(10)]
    })

    renderer.render_prediction_chart(hist_df, pred_df, stock_code='TEST001')