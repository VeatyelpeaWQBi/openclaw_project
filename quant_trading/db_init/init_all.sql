-- ============================================
-- quant_trading 数据库初始化
-- 最后更新: 2026-04-09
-- ============================================

-- ============================================
-- 1. 日K线数据
-- ============================================
CREATE TABLE IF NOT EXISTS daily_kline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,                  -- 股票代码（如 000001）
    name TEXT,                           -- 股票名称
    date TEXT NOT NULL,                  -- 交易日期（YYYY-MM-DD）
    open REAL,                           -- 开盘价
    high REAL,                           -- 最高价
    low REAL,                            -- 最低价
    close REAL,                          -- 收盘价
    volume INTEGER,                      -- 成交量（股）
    amount REAL,                         -- 成交额（元）
    turnover REAL,                       -- 换手率（%）
    pe_ratio REAL,                       -- 市盈率（PE TTM）
    pb_ratio REAL,                       -- 市净率（PB）
    ps_ratio REAL,                       -- 市销率（PS）
    pcf_ratio REAL,                      -- 市现率（PCF）
    volume_ratio REAL,                   -- 量比（当日成交量/前5日均量）
    change_pct REAL,                     -- 涨跌幅（%）
    mktcap REAL,                         -- 总市值（元）
    nmc REAL,                            -- 流通市值（元）
    outstanding_share REAL,              -- 流通股本（股）
    UNIQUE(code, date)
);

CREATE INDEX IF NOT EXISTS idx_daily_kline_code ON daily_kline(code);
CREATE INDEX IF NOT EXISTS idx_daily_kline_date ON daily_kline(date);

-- ============================================
-- 2. 分钟K线数据
-- ============================================
CREATE TABLE IF NOT EXISTS minute_kline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,                  -- 股票代码
    name TEXT,                           -- 股票名称
    date TEXT,                           -- 交易日期（YYYY-MM-DD）
    datetime TEXT NOT NULL,              -- 分钟时间戳（YYYY-MM-DD HH:MM）
    open REAL,                           -- 分钟开盘价
    high REAL,                           -- 分钟最高价
    low REAL,                            -- 分钟最低价
    close REAL,                          -- 分钟收盘价
    volume INTEGER,                      -- 分钟成交量
    amount REAL,                         -- 分钟成交额
    UNIQUE(code, datetime)
);

CREATE INDEX IF NOT EXISTS idx_minute_kline_code ON minute_kline(code);
CREATE INDEX IF NOT EXISTS idx_minute_kline_date ON minute_kline(date);

-- ============================================
-- 3. 指数日K线数据
-- ============================================
CREATE TABLE IF NOT EXISTS index_daily_kline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    index_code TEXT NOT NULL,            -- 指数代码（如 000300）
    index_name TEXT,                     -- 指数名称
    date TEXT NOT NULL,                  -- 交易日期
    open REAL,                           -- 开盘点位
    high REAL,                           -- 最高点位
    low REAL,                            -- 最低点位
    close REAL,                          -- 收盘点位
    volume INTEGER,                      -- 成交量
    amount REAL,                         -- 成交额
    change REAL,                         -- 涨跌点数
    change_pct REAL,                     -- 涨跌幅（%）
    constituent_count INTEGER,           -- 成分股数量
    pe_ttm REAL,                         -- 滚动市盈率
    UNIQUE(index_code, date)
);

CREATE INDEX IF NOT EXISTS idx_index_daily_kline_code ON index_daily_kline(index_code);

-- ============================================
-- 4. RS Score 评分历史
-- ============================================
CREATE TABLE IF NOT EXISTS rs_score (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,                  -- 个股代码
    benchmark_code TEXT NOT NULL,        -- 基准指数代码
    calc_date TEXT NOT NULL,             -- 计算日期
    rs_ratio REAL,                       -- RS比率
    rs_score REAL,                       -- RS评分（0-100）
    rs_rank INTEGER,                     -- RS排名
    stock_return REAL,                   -- 个股涨跌幅
    benchmark_return REAL,               -- 基准涨跌幅
    lookback_days INTEGER DEFAULT 250,   -- 回溯天数
    write_at TEXT,                       -- 写入时间
    UNIQUE(code, benchmark_code, calc_date)
);

CREATE INDEX IF NOT EXISTS idx_rs_code ON rs_score(code);
CREATE INDEX IF NOT EXISTS idx_rs_date ON rs_score(calc_date);
CREATE INDEX IF NOT EXISTS idx_rs_composite ON rs_score(code, benchmark_code, calc_date);

-- ============================================
-- 5. VCP 波动收缩评分
-- ============================================
CREATE TABLE IF NOT EXISTS vcp_score (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,                      -- 股票代码
    calc_date TEXT NOT NULL,                  -- 计算日期（YYYY-MM-DD）
    score REAL NOT NULL,                      -- VCP 总分（-40 ~ +95）
    score_compression REAL,                   -- ① 弹簧压缩度 (+25 ~ -10)
    score_contraction REAL,                   -- ② 收缩质量 (+20 ~ -10)
    score_credibility REAL,                   -- ③ 形态可信度 (+15 ~ -5)
    score_swing_count REAL,                   -- ④ 波段数量 (+10 ~ -5)
    score_volume REAL,                        -- ⑤ 量能枯竭 (+15 ~ 0)
    score_triangle_type REAL,                 -- ⑥ 三角类型 (+10 ~ -10)
    data_start TEXT,                          -- 分析窗口起始日期
    data_end TEXT,                            -- 分析窗口结束日期
    write_at TEXT,                            -- 写入时间
    UNIQUE(code, calc_date)
);

CREATE INDEX IF NOT EXISTS idx_vcp_score_code ON vcp_score(code);
CREATE INDEX IF NOT EXISTS idx_vcp_score_date ON vcp_score(calc_date);
CREATE INDEX IF NOT EXISTS idx_vcp_score_score ON vcp_score(score);

-- ============================================
-- 5b. ADX 评分
-- ============================================
CREATE TABLE IF NOT EXISTS adx_score (
    code          TEXT NOT NULL,           -- 股票代码
    calc_date     TEXT NOT NULL,           -- 计算日期 YYYY-MM-DD
    period        INTEGER NOT NULL DEFAULT 14,  -- 计算周期
    adx           REAL,                    -- ADX原始值
    plus_di       REAL,                    -- +DI值
    minus_di      REAL,                    -- -DI值
    dx            REAL,                    -- DX值
    adx_score_val REAL,                    -- 0-100评分
    write_at      TEXT,                    -- 写入时间
    PRIMARY KEY (code, calc_date, period)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_adx_date ON adx_score(calc_date);
CREATE INDEX IF NOT EXISTS idx_adx_score ON adx_score(adx_score_val);
CREATE INDEX IF NOT EXISTS idx_adx_code_date ON adx_score(code, calc_date);

-- ============================================
-- 6. 指数元数据
-- ============================================
CREATE TABLE IF NOT EXISTS index_info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,           -- 指数代码
    name TEXT,                           -- 指数全称
    type TEXT,                           -- 类型
    constituent_count INTEGER,           -- 成分股数量
    publish_date TEXT,                   -- 发布日期
    daily_kline_done INTEGER DEFAULT 0,  -- 日K是否已下载
    created_at TEXT,                     -- 入库时间
    last_update_at TEXT,                 -- 最后更新
    median_daily_volume REAL,            -- 成分股日均成交量中位数
    median_daily_amount REAL,            -- 成分股日均成交额中位数
    base_date TEXT,                      -- 基日
    short_name TEXT                      -- 简称
);

-- ============================================
-- 6. 指数成分股
-- ============================================
CREATE TABLE IF NOT EXISTS index_members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    index_code TEXT NOT NULL,            -- 指数代码
    stock_code TEXT NOT NULL,            -- 成分股代码
    stock_name TEXT,                     -- 成分股名称
    weight REAL,                         -- 权重（%）
    snapshot_date TEXT NOT NULL,         -- 快照日期
    created_at TEXT                      -- 入库时间
);

CREATE INDEX IF NOT EXISTS idx_index_members_code ON index_members(index_code);

-- ============================================
-- 7. 交易日历
-- ============================================
CREATE TABLE IF NOT EXISTS trade_calendar (
    trade_date TEXT NOT NULL PRIMARY KEY, -- 日期（YYYY-MM-DD）
    trade_status INTEGER NOT NULL,       -- 1=交易日, 0=非交易日
    day_week INTEGER NOT NULL            -- 星期几（1-7）
);

-- ============================================
-- 8. 账户表
-- ============================================
CREATE TABLE IF NOT EXISTS account (
    id INTEGER PRIMARY KEY,
    total_capital REAL NOT NULL,         -- 总资金
    available_capital REAL NOT NULL,     -- 可用资金
    realized_profit REAL DEFAULT 0,      -- 已实现盈亏
    active INTEGER DEFAULT 1,            -- 是否启用
    updated_at TEXT,                     -- 更新时间
    note TEXT,                           -- 备注
    bind_id TEXT,                        -- 绑定ID
    nickname TEXT,                       -- 账户昵称
    simulator INTEGER DEFAULT 0,        -- 0=机器, 1=手工
    turtle_s1_filter_active INTEGER DEFAULT 0, -- S1过滤器
    unit_pct REAL DEFAULT 5.0,          -- 单位仓位比例（%）
    max_holdings INTEGER DEFAULT 5,      -- 最大持仓数
    max_daily_open INTEGER DEFAULT 2     -- 每日最大开仓数
);

-- ============================================
-- 9. 资金流水
-- ============================================
CREATE TABLE IF NOT EXISTS account_flow (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,         -- 账户ID
    type TEXT NOT NULL,                  -- 类型（入金/出金/盈利/亏损）
    amount REAL NOT NULL,                -- 金额
    balance_after REAL,                  -- 操作后余额
    created_at TEXT                      -- 操作时间
);

-- ============================================
-- 10. 持仓表
-- ============================================
CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,         -- 账户ID
    code TEXT NOT NULL,                  -- 股票代码
    name TEXT,                           -- 股票名称
    status TEXT NOT NULL DEFAULT 'HOLDING', -- 状态
    turtle_units INTEGER DEFAULT 1,      -- 当前单位数
    total_shares INTEGER DEFAULT 0,      -- 总持股数
    avg_cost REAL DEFAULT 0,             -- 平均成本
    entry_price REAL,                    -- 入场价
    last_add_price REAL,                 -- 上次加仓价
    current_stop REAL,                   -- 当前止损价
    next_add_price REAL,                 -- 下次加仓价
    exit_price REAL,                     -- 退出价
    turtle_atr_value REAL,               -- ATR值
    cooldown_until TEXT,                 -- 冷却截止日
    opened_at TEXT,                      -- 开仓时间
    closed_at TEXT,                      -- 平仓时间
    updated_at TEXT,                     -- 更新时间
    turtle_entry_system TEXT,            -- 入场系统（S1/S2）
    has_reduced INTEGER DEFAULT 0,       -- 是否已减仓
    last_buy_date TEXT,                  -- 最后买入日
    last_buy_shares INTEGER DEFAULT 0,   -- 最后买入股数
    shares_per_unit INTEGER DEFAULT 0    -- 每单位股数
);

-- ============================================
-- 11. 持仓流水
-- ============================================
CREATE TABLE IF NOT EXISTS position_flow (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,         -- 账户ID
    code TEXT NOT NULL,                  -- 股票代码
    name TEXT,                           -- 股票名称
    action TEXT NOT NULL,                -- 操作（开仓/加仓/减仓/清仓）
    shares INTEGER DEFAULT 0,            -- 股数
    price REAL DEFAULT 0,                -- 价格
    amount REAL DEFAULT 0,               -- 金额
    profit REAL DEFAULT 0,               -- 盈亏
    fees REAL DEFAULT 0,                 -- 手续费
    units_before INTEGER DEFAULT 0,      -- 操作前单位数
    units_after INTEGER DEFAULT 0,       -- 操作后单位数
    stop_price REAL DEFAULT 0,           -- 止损价
    reason TEXT,                         -- 操作原因
    created_at TEXT                      -- 操作时间
);

-- ============================================
-- 12. 候选池（关注列表）
-- ============================================
CREATE TABLE IF NOT EXISTS watchlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT,                           -- 股票/ETF代码
    name TEXT,                           -- 名称
    keyword TEXT,                        -- 来源标识（如 中证A500/自选）
    type TEXT NOT NULL,                  -- 标的类型：stock/etf
    note TEXT,                           -- 备注（行业|主营）
    added_at TEXT,                       -- 入池时间
    active INTEGER DEFAULT 1,            -- 是否启用
    pool_type TEXT DEFAULT 'manual',     -- 池类型：candidate/manual/hotspot
    account_id INTEGER DEFAULT NULL,     -- 账户ID（多账户隔离）
    index_code TEXT DEFAULT NULL         -- 基准指数代码（RS评分用）
);

-- ============================================
-- 13. 股票基础信息
-- ============================================
CREATE TABLE IF NOT EXISTS stock_info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,           -- 股票代码
    name TEXT,                           -- 股票名称
    daily_kline_done INTEGER DEFAULT 0,  -- 日K是否已下载
    industry TEXT,                       -- 行业
    concept TEXT,                        -- 概念
    created_at TEXT,                     -- 入库时间
    last_update_at TEXT                  -- 最后更新
);

-- ============================================
-- 14. 恐贪指数历史
-- ============================================
CREATE TABLE IF NOT EXISTS fear_greed_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,                  -- 日期
    score REAL NOT NULL,                 -- 综合得分（0-100）
    level TEXT NOT NULL,                 -- 等级（极度恐惧/恐惧/中性/贪婪/极度贪婪）
    f_ma_deviation REAL,                 -- 均线偏离因子
    f_volume_ratio REAL,                 -- 量比因子
    f_advance_decline REAL,              -- 涨跌家数比因子
    f_limit_ratio REAL,                  -- 涨停跌停比因子
    f_amplitude REAL,                    -- 振幅因子
    f_rsi REAL,                          -- RSI因子
    f_high_turnover REAL,                -- 高换手因子
    f_offense_defense REAL               -- 攻防转换因子
);

CREATE INDEX IF NOT EXISTS idx_fear_greed_date ON fear_greed_history(date);
