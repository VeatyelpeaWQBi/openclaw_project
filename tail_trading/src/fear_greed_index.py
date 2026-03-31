"""
A股恐贪指数（Fear & Greed Index）
多因子等权法，8个因子

因子列表:
1. 指数均线偏离度 (MA60) - 优先中证全A，fallback沪深300
2. 市场量能比 (20日均量) - 沪深300
3. 涨跌家数比 - 排除ST和退市股
4. 涨跌停比 - 排除ST和退市股
5. 市场振幅 (5日均值，反向) - 沪深300
6. RSI(14) - 沪深300
7. 高换手占比 (>5%)
8. 进攻/防守风格 - 领先指标(黄线)vs指数(白线)，fallback中证1000vs沪深300
"""

import os
import json
import sqlite3
import time as _time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from paths import DB_PATH

# 指数代码到SQLite index_code的映射
INDEX_CODE_MAP = {
    'hs300_kline.csv': ('000300', '沪深300'),
    'zz1000_kline.csv': ('000852', '中证1000'),
}


def _get_db_connection():
    """获取SQLite连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


class FearGreedIndex:
    def __init__(self, data_dir, index_history_dir):
        """
        data_dir: daily_data目录路径
        index_history_dir: index_history目录路径
        """
        self.data_dir = data_dir
        self.index_history_dir = index_history_dir

        # 加载指数数据（优先SQLite，CSV fallback用于指数历史文件）
        self.hs300 = self._load_index('hs300_kline.csv')
        self.zz1000 = self._load_index('zz1000_kline.csv')

        # 尝试加载中证全A指数 (sh.000985)
        # 如果本地没有数据文件，则后续用等权构造或沪深300近似
        self.all_a_index = self._try_load_full_a_index()

        # 加载所有个股数据（只从SQLite）
        self.all_stocks = {}
        self.stock_names = {}
        self._load_all_stocks()

        # 构造全A等权指数（用于因子1均线偏离度）
        # 如果已有中证全A数据则跳过
        self.equal_weight_index = None
        if self.all_a_index is None:
            self._build_equal_weight_index()

        # 预计算每日股票聚合数据（加速历史计算）
        self._daily_stock_agg = None

        # 缓存历史计算结果
        self._history_cache = None

    def _sqlite_load_index(self, index_code):
        """从SQLite加载指数K线数据"""
        try:
            conn = _get_db_connection()
            query = """SELECT date, open, high, low, close, volume, amount
                       FROM index_kline
                       WHERE index_code = ?
                       ORDER BY date"""
            df = pd.read_sql_query(query, conn, params=[index_code])
            conn.close()

            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                df = df.sort_values('date').reset_index(drop=True)
                return df
        except Exception as e:
            print(f"⚠️ SQLite加载指数 {index_code} 失败: {e}")
        return None

    def _csv_load_index(self, filename):
        """从CSV加载指数K线数据（fallback）"""
        filepath = os.path.join(self.index_history_dir, filename)
        if not os.path.exists(filepath):
            return None
        df = pd.read_csv(filepath)
        # 中文列名 -> 英文列名
        df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
        df['date'] = pd.to_datetime(df['date'])
        for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.sort_values('date').reset_index(drop=True)
        return df

    def _load_index(self, filename):
        """加载指数K线数据（优先SQLite，CSV fallback）"""
        # 优先从SQLite加载
        if filename in INDEX_CODE_MAP:
            index_code, _ = INDEX_CODE_MAP[filename]
            result = self._sqlite_load_index(index_code)
            if result is not None and not result.empty:
                return result

        # CSV fallback
        result = self._csv_load_index(filename)
        if result is not None:
            return result

        return pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume', 'amount'])

    def _sqlite_load_all_stocks(self):
        """从SQLite加载所有个股数据"""
        try:
            conn = _get_db_connection()
            # 获取所有股票代码和名称
            stocks_df = pd.read_sql_query(
                "SELECT DISTINCT code, name FROM daily_kline", conn
            )
            if stocks_df.empty:
                conn.close()
                return

            # 批量加载所有日K数据
            all_data = pd.read_sql_query(
                "SELECT code, name, date, open, high, low, close, volume, amount, turnover, "
                "pe_ratio, pb_ratio, ps_ratio, pcf_ratio, volume_ratio "
                "FROM daily_kline ORDER BY code, date",
                conn
            )
            conn.close()

            if all_data.empty:
                return

            all_data['date'] = pd.to_datetime(all_data['date'])

            # 按股票分组
            for code, group in all_data.groupby('code'):
                name = group['name'].iloc[0]
                df = group[['date', 'open', 'high', 'low', 'close',
                            'volume', 'amount', 'turnover']].copy()
                for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'turnover']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                df = df.sort_values('date').reset_index(drop=True)
                # 计算涨跌幅
                df['change_pct'] = df['close'].pct_change() * 100
                self.all_stocks[code] = df
                self.stock_names[code] = name

            print(f"✅ SQLite加载 {len(self.all_stocks)} 只个股数据")
            return True
        except Exception as e:
            print(f"⚠️ SQLite加载个股数据失败: {e}")
            return None


    def _load_all_stocks(self):
        """加载所有个股数据（只从SQLite加载）"""
        self._sqlite_load_all_stocks()
        if len(self.all_stocks) == 0:
            print("⚠️ SQLite中无个股数据")

    def _try_load_full_a_index(self):
        """
        尝试加载中证全A指数数据
        优先级：本地文件 > baostock下载 > None（用等权构造）
        """
        # 1. 尝试本地文件
        local_file = os.path.join(self.index_history_dir, 'all_a_kline.csv')
        if os.path.exists(local_file):
            print("✅ 加载本地中证全A指数数据")
            return self._load_index('all_a_kline.csv')

        # 2. 检查baostock失败标记（避免重复尝试）
        fail_flag = os.path.join(self.index_history_dir, '.all_a_fetch_failed')
        if os.path.exists(fail_flag):
            # 24小时内不再重试
            if _time.time() - os.path.getmtime(fail_flag) < 86400:
                return None

        # 3. 尝试从baostock下载 (sh.000985)
        try:
            import baostock as bs
            bs.login()
            rs = bs.query_history_k_data_plus('sh.000985',
                'date,close,volume,amount,high,low',
                start_date='2020-01-01', end_date='2026-12-31',
                frequency='d', adjustflag='3')
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            bs.logout()

            if len(rows) > 100:  # 有足够数据
                df = pd.DataFrame(rows,
                    columns=['date', 'close', 'volume', 'amount', 'high', 'low'])
                df['date'] = pd.to_datetime(df['date'])
                for col in ['close', 'volume', 'amount', 'high', 'low']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                df = df.sort_values('date').reset_index(drop=True)
                # 缓存到本地
                df.to_csv(local_file, index=False)
                print(f"✅ 从baostock下载中证全A指数 ({len(df)} 条)")
                return df
            else:
                # 写入失败标记
                with open(fail_flag, 'w') as f:
                    f.write(str(_time.time()))
        except Exception as e:
            print(f"⚠️ baostock下载中证全A失败: {e}")
            try:
                with open(fail_flag, 'w') as f:
                    f.write(str(_time.time()))
            except:
                pass

        print("ℹ️ 无中证全A数据，将用等权构造或沪深300近似")
        return None

    def _build_equal_weight_index(self):
        """
        用1469只个股等权平均收盘价构造"全A等权指数"
        用于因子1的均线偏离度计算
        向量化实现，避免逐日期逐股票的双重循环
        """
        print("🔨 构造全A等权指数（向量化）...")
        t0 = _time.time()

        # 将所有股票数据合并为一个大表
        frames = []
        for code, df in self.all_stocks.items():
            name = self.stock_names.get(code, '')
            if 'ST' in name or '退' in name:
                continue
            sub = df[['date', 'close', 'high', 'low']].copy()
            sub = sub.dropna(subset=['close'])
            frames.append(sub)

        if not frames:
            print("⚠️ 无个股数据，跳过等权构造")
            return

        merged = pd.concat(frames, ignore_index=True)

        # 按日期分组计算等权均值
        grouped = merged.groupby('date').agg(
            close=('close', 'mean'),
            high=('high', 'mean'),
            low=('low', 'mean'),
            count=('close', 'count')
        ).reset_index()

        # 只保留有足够样本的日期（至少100只）
        grouped = grouped[grouped['count'] >= 100].reset_index(drop=True)
        grouped = grouped[['date', 'close', 'high', 'low']]
        grouped = grouped.sort_values('date').reset_index(drop=True)

        self.equal_weight_index = grouped
        print(f"✅ 全A等权指数构造完成 ({len(grouped)} 条, 耗时{_time.time()-t0:.1f}s)")

    def _build_daily_stock_agg(self):
        """
        预计算每日股票聚合指标，用于加速历史计算
        输出: DataFrame, index=date, columns=[adv, dec, flat, limit_up, limit_down,
               traded, high_turnover_count]
        """
        print("🔨 预计算每日股票聚合数据...")
        t0 = _time.time()

        # 合并所有非ST/非退市股票数据
        frames = []
        for code, df in self.all_stocks.items():
            name = self.stock_names.get(code, '')
            if 'ST' in name or '退' in name:
                continue

            # 判断涨跌停阈值
            if code.startswith('3') or code.startswith('68'):
                up_th, down_th = 19.9, -19.9
            else:
                up_th, down_th = 9.9, -9.9

            sub = df[['date', 'change_pct', 'turnover']].copy()
            sub = sub.dropna(subset=['change_pct'])

            # 预计算标签
            sub['is_up'] = (sub['change_pct'] > 0).astype(int)
            sub['is_down'] = (sub['change_pct'] < 0).astype(int)
            sub['is_flat'] = (sub['change_pct'] == 0).astype(int)
            sub['is_limit_up'] = (sub['change_pct'] >= up_th).astype(int)
            sub['is_limit_down'] = (sub['change_pct'] <= down_th).astype(int)
            sub['is_high_turnover'] = (sub['turnover'] > 5.0).fillna(False).astype(int)
            sub['has_data'] = 1

            frames.append(sub[['date', 'is_up', 'is_down', 'is_flat',
                               'is_limit_up', 'is_limit_down',
                               'is_high_turnover', 'has_data']])

        merged = pd.concat(frames, ignore_index=True)

        # 按日期聚合
        agg = merged.groupby('date').sum().reset_index()
        agg = agg.sort_values('date').reset_index(drop=True)

        self._daily_stock_agg = agg
        print(f"✅ 每日聚合数据完成 ({len(agg)} 条, 耗时{_time.time()-t0:.1f}s)")

    def _get_daily_agg(self, date_str):
        """获取指定日期的股票聚合数据"""
        if self._daily_stock_agg is None:
            self._build_daily_stock_agg()

        dt = pd.Timestamp(date_str)
        row = self._daily_stock_agg[self._daily_stock_agg['date'] == dt]
        if row.empty:
            return None
        return row.iloc[0]

    def _get_date_idx(self, df, date_str):
        """获取DataFrame中指定日期的索引"""
        dt = pd.Timestamp(date_str)
        idx = df[df['date'] == dt].index
        return idx[0] if len(idx) > 0 else None

    # ==================== 8个因子计算 ====================

    def factor_ma_deviation(self, date_str):
        """
        因子1: 指数均线偏离度（修正：优先使用全A指数）
        (收盘价 - MA60) / MA60 * 100
        偏离越大 → 越贪婪
        得分映射: 偏离 -10%~+10% → 0~100

        优先级：中证全A > 等权构造的全A指数 > 沪深300近似
        """
        # 选择数据源
        if self.all_a_index is not None:
            df = self.all_a_index
            source = "中证全A"
        elif self.equal_weight_index is not None:
            df = self.equal_weight_index
            source = "全A等权"
        else:
            df = self.hs300
            source = "沪深300"

        idx = self._get_date_idx(df, date_str)
        if idx is None or idx < 59:
            return {'score': 50.0, 'raw': 0.0, 'source': source}

        ma60 = df.loc[idx - 59:idx, 'close'].mean()
        close = df.loc[idx, 'close']
        raw = (close - ma60) / ma60 * 100

        # 线性映射: -10%→0, +10%→100
        score = np.clip((raw + 10) / 20 * 100, 0, 100)
        return {'score': round(float(score), 1), 'raw': round(float(raw), 2),
                'source': source}

    def factor_volume_ratio(self, date_str):
        """
        因子2: 市场量能比
        当日成交量 / 20日均量
        放量 = 贪婪
        得分映射: 量比 0.3~2.5 → 0~100
        """
        df = self.hs300
        idx = self._get_date_idx(df, date_str)
        if idx is None or idx < 19:
            return {'score': 50.0, 'raw': 1.0}

        vol = df.loc[idx, 'volume']
        avg_vol = df.loc[idx - 19:idx, 'volume'].mean()
        raw = vol / avg_vol if avg_vol > 0 else 1.0

        # 线性映射: 0.3→0, 2.5→100
        score = np.clip((raw - 0.3) / (2.5 - 0.3) * 100, 0, 100)
        return {'score': round(float(score), 1), 'raw': round(float(raw), 2)}

    def factor_advance_decline(self, date_str):
        """
        因子3: 涨跌家数比（排除ST和退市股）
        上涨家数 / (上涨+下跌+平盘) × 100
        上涨占比高 = 贪婪
        得分映射: 上涨占比 10%~90% → 0~100
        """
        agg = self._get_daily_agg(date_str)
        if agg is not None:
            adv = int(agg['is_up'])
            dec = int(agg['is_down'])
            flat = int(agg['is_flat'])
        else:
            # 回退到逐股票计算
            dt = pd.Timestamp(date_str)
            adv, dec, flat = 0, 0, 0
            for code, df in self.all_stocks.items():
                name = self.stock_names.get(code, '')
                if 'ST' in name or '退' in name:
                    continue
                row = df[df['date'] == dt]
                if row.empty or pd.isna(row.iloc[0]['change_pct']):
                    continue
                pct = row.iloc[0]['change_pct']
                if pct > 0:
                    adv += 1
                elif pct < 0:
                    dec += 1
                else:
                    flat += 1

        total = adv + dec + flat
        if total == 0:
            return {'score': 50.0, 'raw': 50.0}

        raw = (adv / total) * 100
        score = np.clip((raw - 10) / 80 * 100, 0, 100)
        return {'score': round(float(score), 1), 'raw': round(float(raw), 1)}

    def factor_limit_ratio(self, date_str):
        """
        因子4: 涨跌停比（排除ST和退市股）
        涨停家数 / 跌停家数
        涨停多 = 贪婪
        得分映射:
        - 无涨停无跌停 → 50
        - 有涨停无跌停 → 90
        - ratio 0~5 → 10~80
        """
        agg = self._get_daily_agg(date_str)
        if agg is not None:
            limit_up = int(agg['is_limit_up'])
            limit_down = int(agg['is_limit_down'])
        else:
            # 回退到逐股票计算
            dt = pd.Timestamp(date_str)
            limit_up, limit_down = 0, 0
            for code, df in self.all_stocks.items():
                name = self.stock_names.get(code, '')
                if 'ST' in name or '退' in name:
                    continue
                row = df[df['date'] == dt]
                if row.empty or pd.isna(row.iloc[0]['change_pct']):
                    continue
                pct = row.iloc[0]['change_pct']
                if code.startswith('3') or code.startswith('68'):
                    up_th, down_th = 19.9, -19.9
                else:
                    up_th, down_th = 9.9, -9.9
                if pct >= up_th:
                    limit_up += 1
                elif pct <= down_th:
                    limit_down += 1

        if limit_down == 0:
            if limit_up == 0:
                raw, score = 0.0, 50.0
            else:
                raw, score = 10.0, 90.0  # 有涨停无跌停 → 极度贪婪
        else:
            raw = limit_up / limit_down
            # 映射: ratio 0→10, 5→80
            score = np.clip(10 + (raw / 5) * 70, 10, 90)

        return {'score': round(float(score), 1), 'raw': round(float(raw), 1),
                'detail': f'{limit_up}/{limit_down}'}

    def factor_amplitude(self, date_str):
        """
        因子5: 市场振幅（反向指标）
        振幅 = (最高-最低)/收盘 × 100，取5日均值
        振幅大 = 恐慌 → 反向
        得分映射: 振幅 0.5%~3.0% → 100~0 (反向)
        """
        df = self.hs300
        idx = self._get_date_idx(df, date_str)
        if idx is None or idx < 4:
            return {'score': 50.0, 'raw': 1.5}

        # 计算每日振幅
        start = idx - 4
        sub = df.loc[start:idx].copy()
        sub['amp'] = (sub['high'] - sub['low']) / sub['close'] * 100
        raw = float(sub['amp'].mean())

        # 反向线性映射: 0.5%→100, 3.0%→0
        score = np.clip((3.0 - raw) / (3.0 - 0.5) * 100, 0, 100)
        return {'score': round(float(score), 1), 'raw': round(float(raw), 2)}

    def factor_rsi(self, date_str):
        """
        因子6: RSI(14)
        标准RSI公式: RSI = 100 - 100/(1 + RS), RS = avg_gain/avg_loss
        RSI高 = 贪婪
        得分映射: RSI直接线性 (RSI本身0~100)
        """
        df = self.hs300
        idx = self._get_date_idx(df, date_str)
        if idx is None or idx < 14:
            return {'score': 50.0, 'raw': 50.0}

        # 取14+1天数据计算涨跌
        closes = df.loc[idx - 14:idx, 'close'].values
        deltas = np.diff(closes)

        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)

        avg_gain = np.mean(gains)
        avg_loss = np.mean(losses)

        if avg_loss == 0:
            raw = 100.0
        elif avg_gain == 0:
            raw = 0.0
        else:
            rs = avg_gain / avg_loss
            raw = 100 - 100 / (1 + rs)

        # RSI直接作为得分（RSI高=贪婪）
        score = np.clip(raw, 0, 100)
        return {'score': round(float(score), 1), 'raw': round(float(raw), 1)}

    def factor_high_turnover(self, date_str):
        """
        因子7: 高换手占比
        换手率 > 5% 的股票占当日交易股票的比例
        高换手多 = 贪婪（活跃度高）
        得分映射: 占比 2%~35% → 0~100
        """
        agg = self._get_daily_agg(date_str)
        if agg is not None:
            total = int(agg['has_data'])
            high = int(agg['is_high_turnover'])
        else:
            # 回退到逐股票计算
            dt = pd.Timestamp(date_str)
            total, high = 0, 0
            for code, df in self.all_stocks.items():
                name = self.stock_names.get(code, '')
                if 'ST' in name or '退' in name:
                    continue
                row = df[df['date'] == dt]
                if row.empty or pd.isna(row.iloc[0]['turnover']):
                    continue
                total += 1
                if row.iloc[0]['turnover'] > 5.0:
                    high += 1

        if total == 0:
            return {'score': 50.0, 'raw': 0.0}

        raw = (high / total) * 100
        # 线性映射: 2%→0, 35%→100
        score = np.clip((raw - 2) / (35 - 2) * 100, 0, 100)
        return {'score': round(float(score), 1), 'raw': round(float(raw), 1)}

    def factor_offense_defense(self, date_str, lookback=5):
        """
        因子8: 进攻/防守风格

        优先方案（领先指标法）：
        - 白线 = 加权指数（大盘股）
        - 黄线 = 领先指标/不加权指数（小盘股）
        - 黄线涨幅 > 白线涨幅 → 小盘股领涨 → 进攻风格 → 偏贪婪
        - 白线涨幅 > 黄线涨幅 → 大盘股领涨 → 防守风格 → 偏恐慌
        - 对比指数: 上证(sh.000001)、深证成指(sz.399001)、创业板指(sz.399006)

        备选方案（指数对比法）：
        - 进攻型: 中证1000（成长型）
        - 防守型: 沪深300（价值型）
        - 差值 = 进攻N日涨幅 - 防守N日涨幅

        得分映射: 差值 -8%~+8% → 0~100 (tanh函数)
        """
        # 尝试领先指标方案
        lead_result = self._offense_defense_leading(date_str, lookback)
        if lead_result is not None:
            return lead_result

        # 备选方案：中证1000 vs 沪深300
        return self._offense_defense_index_compare(date_str, lookback)

    def _offense_defense_leading(self, date_str, lookback):
        """
        领先指标方案：对比三个指数的黄线(领先指标)和白线(加权指数)涨跌幅
        黄线 > 白线 → 进攻 → 贪婪

        需要领先指标历史数据，当前无数据源支持，返回None触发备选方案。
        预留接口：如有数据源可在此接入。
        """
        # 检查是否有领先指标数据缓存
        if not hasattr(self, '_leading_indicator_data') or self._leading_indicator_data is None:
            return None

        # 以下是领先指标方案的完整实现（数据就绪后启用）
        data = self._leading_indicator_data
        dt = pd.Timestamp(date_str)

        diffs = []
        for idx_name in ['sh000001', 'sz399001', 'sz399006']:
            if idx_name not in data:
                return None
            df = data[idx_name]
            idx = self._get_date_idx(df, dt)
            if idx is None or idx < lookback:
                return None

            # 白线N日涨幅
            white_ret = (df.loc[idx, 'white_close'] - df.loc[idx - lookback, 'white_close']) / df.loc[idx - lookback, 'white_close'] * 100
            # 黄线N日涨幅
            yellow_ret = (df.loc[idx, 'yellow_close'] - df.loc[idx - lookback, 'yellow_close']) / df.loc[idx - lookback, 'yellow_close'] * 100
            diffs.append(yellow_ret - white_ret)

        raw = np.mean(diffs)
        score = 50 + 50 * np.tanh(raw / 4)
        score = np.clip(score, 0, 100)
        return {'score': round(float(score), 1), 'raw': round(float(raw), 2),
                'method': 'leading_indicator'}

    def load_leading_indicator_data(self, data_dict):
        """
        加载领先指标数据（外部调用）

        参数:
            data_dict: {
                'sh000001': DataFrame(date, white_close, yellow_close),
                'sz399001': DataFrame(date, white_close, yellow_close),
                'sz399006': DataFrame(date, white_close, yellow_close)
            }
        """
        self._leading_indicator_data = data_dict
        print("✅ 领先指标数据已加载，将优先使用领先指标方案")

    def _offense_defense_index_compare(self, date_str, lookback):
        """
        备选方案：中证1000 vs 沪深300涨跌幅对比
        进攻型(中证1000)涨幅 - 防守型(沪深300)涨幅
        差值越大 → 进攻风格 → 越贪婪
        """
        offense_ret = self._n_day_return(self.zz1000, date_str, lookback)
        defense_ret = self._n_day_return(self.hs300, date_str, lookback)

        if offense_ret is None or defense_ret is None:
            return {'score': 50.0, 'raw': 0.0, 'method': 'index_compare'}

        raw = offense_ret - defense_ret
        # tanh映射: 平滑S型曲线
        score = 50 + 50 * np.tanh(raw / 4)
        score = np.clip(score, 0, 100)
        return {'score': round(float(score), 1), 'raw': round(float(raw), 2),
                'method': 'index_compare'}

    def _n_day_return(self, df, date_str, n):
        """计算指数近N日涨跌幅（%）"""
        idx = self._get_date_idx(df, date_str)
        if idx is None or idx < n:
            return None
        curr = df.loc[idx, 'close']
        prev = df.loc[idx - n, 'close']
        if prev == 0:
            return None
        return (curr - prev) / prev * 100

    # ==================== 综合计算 ====================

    def calculate(self, date_str=None):
        """
        计算指定日期的恐贪指数

        参数:
            date_str: 'YYYY-MM-DD'，默认为最新交易日

        返回:
            dict: 恐贪指数详细结果
        """
        if date_str is None:
            date_str = str(self.hs300['date'].max().date())

        # 计算8个因子
        factors = {
            'ma_deviation': self.factor_ma_deviation(date_str),
            'volume_ratio': self.factor_volume_ratio(date_str),
            'advance_decline': self.factor_advance_decline(date_str),
            'limit_ratio': self.factor_limit_ratio(date_str),
            'amplitude': self.factor_amplitude(date_str),
            'rsi': self.factor_rsi(date_str),
            'high_turnover': self.factor_high_turnover(date_str),
            'offense_defense': self.factor_offense_defense(date_str),
        }

        # 等权平均
        scores = [f['score'] for f in factors.values()]
        fg_score = round(sum(scores) / len(scores), 1)
        level = self.get_sentiment_level(fg_score)

        # 历史均值
        history_avg = self._calc_history_averages(date_str)

        return {
            'date': date_str,
            'fear_greed_score': fg_score,
            'level': level,
            'factors': factors,
            'history_avg': history_avg,
        }

    def calculate_history(self, days=250):
        """
        计算过去N天的恐贪指数历史

        返回:
            list[dict]: 每天的恐贪指数结果
        """
        # 获取所有交易日
        trade_dates = sorted(self.hs300['date'].unique())
        # 只取最近N个交易日
        recent_dates = trade_dates[-days:]

        results = []
        total = len(recent_dates)

        for i, dt in enumerate(recent_dates):
            date_str = str(dt.date())
            try:
                result = self.calculate(date_str)
                results.append(result)
            except Exception as e:
                # 跳过计算失败的日期（数据不足等）
                pass

            if (i + 1) % 50 == 0:
                print(f"  历史计算进度: {i + 1}/{total}")

        self._history_cache = results
        return results

    def save_history(self, filepath):
        """保存历史数据到JSON"""
        if self._history_cache is None:
            print("⚠️ 请先调用 calculate_history() 计算历史数据")
            return False

        # 确保目录存在
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self._history_cache, f, ensure_ascii=False, indent=2)

        print(f"✅ 历史数据已保存到 {filepath}（{len(self._history_cache)} 条记录）")
        return True

    # ==================== 辅助方法 ====================

    def _calc_history_averages(self, date_str):
        """从指数数据直接计算简易历史均值（基于RSI粗估）"""
        df = self.hs300
        idx = self._get_date_idx(df, date_str)
        if idx is None:
            return {'avg_1w': None, 'avg_1m': None, 'avg_1y': None}

        # 如果有缓存的历史数据，从缓存计算
        if self._history_cache is not None:
            return self._avg_from_cache(date_str)

        # 否则返回None（需要先计算历史）
        return {'avg_1w': None, 'avg_1m': None, 'avg_1y': None}

    def _avg_from_cache(self, date_str):
        """从历史缓存中计算各周期均值"""
        if not self._history_cache:
            return {'avg_1w': None, 'avg_1m': None, 'avg_1y': None}

        # 构建日期到分数的映射
        score_map = {r['date']: r['fear_greed_score'] for r in self._history_cache}
        dates_sorted = sorted(score_map.keys())

        if date_str not in score_map:
            return {'avg_1w': None, 'avg_1m': None, 'avg_1y': None}

        idx = dates_sorted.index(date_str)

        # 近一周(5个交易日)
        w1_scores = [score_map[dates_sorted[i]] for i in range(max(0, idx - 4), idx + 1)]
        # 近一月(20个交易日)
        m1_scores = [score_map[dates_sorted[i]] for i in range(max(0, idx - 19), idx + 1)]
        # 近一年(250个交易日)
        y1_scores = [score_map[dates_sorted[i]] for i in range(max(0, idx - 249), idx + 1)]

        return {
            'avg_1w': round(sum(w1_scores) / len(w1_scores), 1) if w1_scores else None,
            'avg_1m': round(sum(m1_scores) / len(m1_scores), 1) if m1_scores else None,
            'avg_1y': round(sum(y1_scores) / len(y1_scores), 1) if y1_scores else None,
        }

    @staticmethod
    def get_sentiment_level(score):
        """情绪等级判断"""
        if score <= 15:
            return "极度恐慌 🔴🔴🔴"
        elif score <= 30:
            return "恐慌 🔴🔴"
        elif score <= 45:
            return "偏恐慌 🟠"
        elif score <= 55:
            return "中性 🟡"
        elif score <= 70:
            return "偏贪婪 🟢"
        elif score <= 85:
            return "贪婪 🟢🟢"
        else:
            return "极度贪婪 🟢🟢🟢"

    @staticmethod
    def format_report(result):
        """格式化输出恐贪指数报告"""
        lines = []
        lines.append(f"📊 A股恐贪指数 — {result['date']}")
        lines.append("=" * 40)
        lines.append(f"🎯 综合恐贪指数: {result['fear_greed_score']}")
        lines.append(f"📌 情绪等级: {result['level']}")
        lines.append("")

        lines.append("📈 各因子得分:")
        f = result['factors']

        # 均线偏离度（显示数据源）
        ma_src = f['ma_deviation'].get('source', '')
        src_tag = f" [{ma_src}]" if ma_src else ""
        lines.append(f"  均线偏离度:  {f['ma_deviation']['score']:>5.1f}  (MA60偏离: {f['ma_deviation']['raw']:+.2f}%){src_tag}")
        lines.append(f"  市场量能比:  {f['volume_ratio']['score']:>5.1f}  (量比: {f['volume_ratio']['raw']:.2f})")
        lines.append(f"  涨跌家数比:  {f['advance_decline']['score']:>5.1f}  (上涨占比: {f['advance_decline']['raw']:.1f}%)")

        limit_detail = f['limit_ratio'].get('detail', '')
        if limit_detail:
            lines.append(f"  涨跌停比:    {f['limit_ratio']['score']:>5.1f}  (涨停{limit_detail.split('/')[0]}/跌停{limit_detail.split('/')[1]})")
        else:
            lines.append(f"  涨跌停比:    {f['limit_ratio']['score']:>5.1f}  (比值: {f['limit_ratio']['raw']:.1f})")

        lines.append(f"  市场振幅:    {f['amplitude']['score']:>5.1f}  (5日均振幅: {f['amplitude']['raw']:.2f}%)")
        lines.append(f"  RSI(14):     {f['rsi']['score']:>5.1f}  (RSI: {f['rsi']['raw']:.1f})")
        lines.append(f"  高换手占比:  {f['high_turnover']['score']:>5.1f}  (高换手股: {f['high_turnover']['raw']:.1f}%)")

        # 进攻防守比（显示计算方法）
        od_method = f['offense_defense'].get('method', '')
        od_tag = " [领先指标]" if od_method == 'leading_indicator' else " [中证1000vs沪深300]"
        lines.append(f"  进攻防守比:  {f['offense_defense']['score']:>5.1f}  (进攻-防守差: {f['offense_defense']['raw']:+.2f}%){od_tag}")

        lines.append("")
        avg = result.get('history_avg', {})
        if avg.get('avg_1w') is not None:
            lines.append("📊 历史均值:")
            lines.append(f"  近一周: {avg['avg_1w']}")
            lines.append(f"  近一月: {avg['avg_1m']}")
            lines.append(f"  近一年: {avg['avg_1y']}")
        else:
            lines.append("📊 历史均值: 需先计算历史数据")

        lines.append("=" * 40)
        return "\n".join(lines)


# ==================== 测试入口 ====================

if __name__ == '__main__':
    from paths import DAILY_DATA_DIR, INDEX_HISTORY_DIR, PROJECT_ROOT
    DATA_DIR = DAILY_DATA_DIR
    INDEX_DIR = INDEX_HISTORY_DIR
    OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'tail_trading', 'output')

    print("🚀 初始化恐贪指数引擎...")
    fgi = FearGreedIndex(DATA_DIR, INDEX_DIR)

    # 1. 计算最新交易日恐贪指数
    print("\n" + "=" * 50)
    print("📋 计算最新交易日恐贪指数")
    print("=" * 50)
    result = fgi.calculate()
    print(FearGreedIndex.format_report(result))

    # 2. 计算过去250个交易日历史
    print("\n" + "=" * 50)
    print("📋 计算历史恐贪指数（250个交易日）")
    print("=" * 50)
    history = fgi.calculate_history(days=250)
    print(f"✅ 已计算 {len(history)} 个交易日的恐贪指数")

    # 3. 保存历史数据
    output_file = os.path.join(OUTPUT_DIR, 'fear_greed_history.json')
    fgi.save_history(output_file)

    # 4. 重新计算带历史均值的结果
    print("\n" + "=" * 50)
    print("📋 最新交易日恐贪指数（含历史均值）")
    print("=" * 50)
    result_with_avg = fgi.calculate()
    print(FearGreedIndex.format_report(result_with_avg))
