-- ============================================
-- quant_trading 数据库初始化脚本
-- 数据库: SQLite (stock_data.db)
-- 用法: sqlite3 stock_data.db < init_all.sql
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
    mktcap REAL,                         -- 总市值（元）
    nmc REAL,                            -- 流通市值（元）
    outstanding_share REAL,              -- 流通股本（股）
    UNIQUE(code, date)
);

CREATE INDEX IF NOT EXISTS idx_daily_kline_code ON daily_kline(code);
CREATE INDEX IF NOT EXISTS idx_daily_kline_date ON daily_kline(date);

-- ============================================
-- 2. 分钟线数据
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
    index_code TEXT NOT NULL,
    index_name TEXT,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    amount REAL,
    change REAL,                   -- 涨跌点数
    change_pct REAL,               -- 涨跌幅(%)
    constituent_count INTEGER,     -- 成分股数量
    pe_ttm REAL,                   -- 滚动市盈率
    UNIQUE(index_code, date)
);

CREATE INDEX IF NOT EXISTS idx_index_daily_kline_code ON index_daily_kline(index_code);

-- ============================================
-- 3c. RS Score历史表
-- ============================================
CREATE TABLE IF NOT EXISTS rs_score (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,                 -- 个股代码
    benchmark_code TEXT NOT NULL,       -- 基准指数代码
    calc_date TEXT NOT NULL,            -- 计算日期
    rs_ratio REAL,                      -- RS比率: (1+个股涨跌幅)/(1+基准涨跌幅)
    rs_score REAL,                      -- RS Score（转换后评分）
    rs_rank INTEGER,                    -- RS排名百分位（1-99）
    stock_return REAL,                  -- 个股N日涨跌幅
    benchmark_return REAL,              -- 基准N日涨跌幅
    lookback_days INTEGER DEFAULT 250,  -- 回看天数
    write_at TEXT,                       -- 写入时间
    UNIQUE(code, benchmark_code, calc_date)
);

CREATE INDEX IF NOT EXISTS idx_rs_score_code ON rs_score(code);
CREATE INDEX IF NOT EXISTS idx_rs_score_date ON rs_score(calc_date);
CREATE INDEX IF NOT EXISTS idx_rs_score_composite ON rs_score(code, benchmark_code, calc_date);

-- ============================================
-- 3b. 指数基础信息表
-- ============================================
CREATE TABLE IF NOT EXISTS index_info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,                  -- 指数代码（如 000001, 399001）
    name TEXT,                           -- 指数名称
    type TEXT,                           -- 指数类型: exchange/csindex/cnindex/sw/concept/custom
    constituent_count INTEGER,           -- 成分股数量
    publish_date TEXT,                   -- 发布日期
    daily_kline_done INTEGER DEFAULT 0,  -- 是否已下载日K记录
    median_daily_volume REAL,            -- 成分股日均成交量中位数（股）
    median_daily_amount REAL,            -- 成分股日均成交额中位数（元）
    created_at TEXT,                     -- 记录创建时间
    last_update_at TEXT,                 -- 最后更新时间
    UNIQUE(code)
);

CREATE INDEX IF NOT EXISTS idx_index_info_code ON index_info(code);

-- ============================================
-- 3c. 指数成分股关系表
-- ============================================
CREATE TABLE IF NOT EXISTS index_members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    index_code TEXT NOT NULL,            -- 指数代码（关联 index_info.code）
    stock_code TEXT NOT NULL,            -- 成分股代码（关联 stock_info.code）
    stock_name TEXT,                     -- 成分股名称（冗余，方便查询）
    weight REAL,                         -- 权重（百分比，部分指数提供）
    snapshot_date TEXT NOT NULL,         -- 快照日期（成分股定期调整，记录是哪天的名单）
    created_at TEXT,                     -- 记录创建时间
    UNIQUE(index_code, stock_code, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_index_members_index ON index_members(index_code);
CREATE INDEX IF NOT EXISTS idx_index_members_stock ON index_members(stock_code);

-- ============================================
-- 4. 交易日历
-- ============================================
CREATE TABLE IF NOT EXISTS trade_calendar (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL UNIQUE,
    trade_status INTEGER DEFAULT 1  -- 1=交易日, 0=休市
);

CREATE INDEX IF NOT EXISTS idx_trade_calendar_date ON trade_calendar(trade_date);

-- ============================================
-- 5. 海龟交易法 — 账户表（多账户）
-- ============================================
CREATE TABLE IF NOT EXISTS account (
    id INTEGER PRIMARY KEY,
    total_capital REAL NOT NULL,
    available_capital REAL NOT NULL,
    realized_profit REAL DEFAULT 0,
    active INTEGER DEFAULT 1,
    bind_id TEXT,
    nickname TEXT,
    simulator INTEGER DEFAULT 1,        -- 0=机器模拟, 1=手工账户
    turtle_s1_filter_active INTEGER DEFAULT 1, -- 1=不过滤, 0=S1过滤激活
    unit_pct REAL DEFAULT 5.0,          -- 单标的1单位仓位占总资金百分比
    max_holdings INTEGER DEFAULT 5,   -- 账户最大持仓标的数
    max_daily_open INTEGER DEFAULT 2,  -- 单日最大开仓标的数
    updated_at TEXT,
    note TEXT
);

CREATE INDEX IF NOT EXISTS idx_account_bind ON account(bind_id);

-- ============================================
-- 6. 海龟交易法 — 资金流水表
-- ============================================
CREATE TABLE IF NOT EXISTS account_flow (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    type TEXT NOT NULL,             -- 入金/出金/买入/卖出
    amount REAL NOT NULL,
    balance_after REAL,
    created_at TEXT,
    FOREIGN KEY (account_id) REFERENCES account(id)
);

CREATE INDEX IF NOT EXISTS idx_account_flow_account ON account_flow(account_id);

-- ============================================
-- 7. 海龟交易法 — 持仓表（多账户）
-- ============================================
CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    status TEXT NOT NULL DEFAULT 'HOLDING',  -- HOLDING/COOLING/CLOSED
    turtle_units INTEGER DEFAULT 1,
    total_shares INTEGER DEFAULT 0,
    avg_cost REAL DEFAULT 0,
    entry_price REAL,
    last_add_price REAL,
    current_stop REAL,
    next_add_price REAL,
    exit_price REAL,
    turtle_atr_value REAL,
    shares_per_unit INTEGER DEFAULT 0,  -- 开仓时固定的一单位手数
    has_reduced INTEGER DEFAULT 0,  -- 0=未减仓, 1=已减仓
    turtle_entry_system TEXT,               -- 'S1' 或 'S2'
    last_buy_date TEXT,
    last_buy_shares INTEGER DEFAULT 0,
    cooldown_until TEXT,
    opened_at TEXT,
    closed_at TEXT,
    updated_at TEXT,
    FOREIGN KEY (account_id) REFERENCES account(id)
);

CREATE INDEX IF NOT EXISTS idx_positions_account ON positions(account_id);
CREATE INDEX IF NOT EXISTS idx_positions_code ON positions(code);
CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);

-- ============================================
-- 8. 海龟交易法 — 持仓流水表
-- ============================================
CREATE TABLE IF NOT EXISTS position_flow (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    action TEXT NOT NULL,           -- 开仓/加仓/减仓/清仓止损/清仓止盈/部分平仓
    shares INTEGER DEFAULT 0,
    price REAL DEFAULT 0,
    amount REAL DEFAULT 0,
    profit REAL DEFAULT 0,
    fees REAL DEFAULT 0,
    units_before INTEGER DEFAULT 0,
    units_after INTEGER DEFAULT 0,
    stop_price REAL DEFAULT 0,
    reason TEXT,
    created_at TEXT,
    FOREIGN KEY (account_id) REFERENCES account(id)
);

CREATE INDEX IF NOT EXISTS idx_position_flow_account ON position_flow(account_id);
CREATE INDEX IF NOT EXISTS idx_position_flow_code ON position_flow(code);

-- ============================================
-- 9. 海龟交易法 — 自选池表
-- ============================================
CREATE TABLE IF NOT EXISTS watchlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT,                          -- 股票/ETF代码（如 000001）
    name TEXT,                          -- 名称（如 平安银行）
    keyword TEXT,                       -- 来源标识（如 中证A500/华为概念/自选）
    type TEXT NOT NULL,                 -- 标的类型：stock=个股, etf=ETF
    note TEXT,                          -- 备注信息
    added_at TEXT,                      -- 入池时间（YYYY-MM-DD HH:MM:SS）
    active INTEGER DEFAULT 1,           -- 是否启用：1=启用, 0=停用
    pool_type TEXT DEFAULT 'manual',    -- 池类型：candidate=候选池, manual=自选池, hotspot=热点池
    account_id INTEGER DEFAULT NULL     -- 账户ID（多账户隔离，NULL=全局）
);

-- ============================================
-- 9. 股票基础信息表
-- ============================================
CREATE TABLE IF NOT EXISTS stock_info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,                  -- 股票代码
    name TEXT,                           -- 股票名称
    daily_kline_done INTEGER DEFAULT 0,  -- 是否已下载日K记录 (0=未完成, 1=已完成)
    industry TEXT,                       -- 所属行业（三级分类，逗号分隔）
    concept TEXT,                        -- 所属概念题材（全部概念，逗号分隔）
    created_at TEXT,                     -- 记录创建时间
    last_update_at TEXT,                 -- 最后更新时间
    UNIQUE(code)
);

CREATE INDEX IF NOT EXISTS idx_stock_info_code ON stock_info(code);
