#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""KLinePredictor - K线预测工具"""

import sys
import os
import pandas as pd
from typing import Optional, List

# 添加Kronos路径
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Kronos'))

from model import Kronos, KronosTokenizer, KronosPredictor
from core.storage import get_daily_kline_range, get_trading_days_range
from core.paths import CHARTS_DIR
from core.chart_renderer import create_renderer


class KLinePredictorApp:
    """K线预测应用"""

    @staticmethod
    def set_proxy(proxy_url: str):
        """设置全局代理

        Args:
            proxy_url: 代理URL, 例如 "http://127.0.0.1:7890"
        """
        os.environ["HTTP_PROXY"] = proxy_url
        os.environ["HTTPS_PROXY"] = proxy_url
        print(f"代理已设置: {proxy_url}")

    def __init__(self):
        """初始化，加载Kronos模型"""
        self.tokenizer = KronosTokenizer.from_pretrained("NeoQuasar/Kronos-Tokenizer-base")
        self.model = Kronos.from_pretrained("NeoQuasar/Kronos-base")
        self.predictor = KronosPredictor(self.model, self.tokenizer, max_context=512)
        self.renderer = create_renderer(CHARTS_DIR)

    def predict_by_range(self, stock_code: str, start_date: str, end_date: str,
                         pred_len: int = 5, T: float = 1.5, top_p: float = 1.2,
                         sample_count: int = 1) -> pd.DataFrame:
        """
        通过日期范围查询日K数据并调用Kronos进行预测

        Args:
            stock_code: 股票代码（如 '000001'）
            start_date: 历史数据开始日期 'YYYY-MM-DD'
            end_date: 历史数据结束日期 'YYYY-MM-DD'
            pred_len: 预测未来多少个交易日（默认5）
            T: Temperature采样参数
            top_p: nucleus sampling参数
            sample_count: 采样次数

        Returns:
            DataFrame: 预测结果，包含open, high, low, close, volume, amount列
        """
        df = get_daily_kline_range(stock_code, start_date, end_date)

        if df.empty:
            print(f"未找到股票 {stock_code} 在 {start_date} 至 {end_date} 的日K数据")
            return pd.DataFrame()

        x_df = df[['open', 'high', 'low', 'close', 'volume', 'amount']].copy()
        x_timestamp = pd.Series(df['date'])

        trading_days = get_trading_days_range(end_date, '2099-12-31')
        if len(trading_days) < pred_len + 1:
            print(f"无法获取足够的未来交易日")
            return pd.DataFrame()

        future_dates = trading_days[1:pred_len + 1]
        y_timestamp = pd.Series(pd.to_datetime(future_dates))

        print(f"股票: {stock_code}, 历史数据: {len(df)}条, 预测未来 {pred_len} 个交易日")

        pred_df = self.predictor.predict(
            df=x_df,
            x_timestamp=x_timestamp,
            y_timestamp=y_timestamp,
            pred_len=pred_len,
            T=T,
            top_p=top_p,
            sample_count=sample_count
        )

        # 对预测数据保留2位小数
        if not pred_df.empty:
            for col in ['open', 'high', 'low', 'close']:
                if col in pred_df.columns:
                    pred_df[col] = pred_df[col].round(2)

        return pred_df

    def plot_prediction(self, stock_code: str, df: pd.DataFrame, pred_df: pd.DataFrame,
                        save_path: str = '', show_ma: Optional[List[int]] = None) -> str:
        """
        生成K线预测HTML图表

        Args:
            stock_code: 股票代码
            df: 历史日K数据DataFrame
            pred_df: 预测结果DataFrame
            save_path: 保存路径（可选，默认自动生成）
            show_ma: 显示哪些均线

        Returns:
            str: HTML文件路径
        """
        return self.renderer.render_prediction_chart(
            hist_df=df,
            pred_df=pred_df,
            stock_code=stock_code,
            save_path=save_path,
            show_ma=show_ma
        )

    def plot_single(self, df: pd.DataFrame, title: str = '', save_path: str = '') -> str:
        """
        生成单张K线图表（无预测）

        Args:
            df: K线数据DataFrame
            title: 图表标题
            save_path: 保存路径

        Returns:
            str: HTML文件路径
        """
        return self.renderer.render_single_chart(df, title, save_path)


def main():
    """示例：预测某股票并生成图表"""
    # KLinePredictorApp.set_proxy("http://127.0.0.1:10802")

    app = KLinePredictorApp()
    print("KLinePredictor 初始化完成")

    stock_code = '301183'
    start_date = '2024-01-01'
    end_date = '2026-04-15'
    pred_len = 40

    df = get_daily_kline_range(stock_code, start_date, end_date)
    pred_df = app.predict_by_range(stock_code, start_date, end_date, pred_len=pred_len)

    if not pred_df.empty and not df.empty:
        app.plot_prediction(stock_code, df, pred_df)


if __name__ == "__main__":
    main()