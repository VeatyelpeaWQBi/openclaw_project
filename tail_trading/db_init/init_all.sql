-- ============================================
-- tail_trading 数据库初始化脚本
-- 数据库: SQLite (stock_data.db)
-- 用法: sqlite3 stock_data.db < init_all.sql
-- ============================================

-- ============================================
-- 1. 日K线数据
-- ============================================
CREATE TABLE IF NOT EXISTS daily_kline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    name TEXT,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    amount REAL,
    turnover REAL,
    pe_ratio REAL,
    pb_ratio REAL,
    ps_ratio REAL,
    pcf_ratio REAL,
    volume_ratio REAL,
    UNIQUE(code, date)
);

CREATE INDEX IF NOT EXISTS idx_daily_kline_code ON daily_kline(code);
CREATE INDEX IF NOT EXISTS idx_daily_kline_date ON daily_kline(date);

-- ============================================
-- 2. 分钟线数据
-- ============================================
CREATE TABLE IF NOT EXISTS minute_kline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    name TEXT,
    date TEXT,
    datetime TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    amount REAL,
    UNIQUE(code, datetime)
);

CREATE INDEX IF NOT EXISTS idx_minute_kline_code ON minute_kline(code);
CREATE INDEX IF NOT EXISTS idx_minute_kline_date ON minute_kline(date);

-- ============================================
-- 3. 指数K线数据
-- ============================================
CREATE TABLE IF NOT EXISTS index_kline (
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
    UNIQUE(index_code, date)
);

CREATE INDEX IF NOT EXISTS idx_index_kline_code ON index_kline(index_code);

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
CREATE TABLE IF NOT EXISTS turtle_account (
    id INTEGER PRIMARY KEY,
    total_capital REAL NOT NULL,
    available_capital REAL NOT NULL,
    realized_profit REAL DEFAULT 0,
    active INTEGER DEFAULT 1,
    bind_id TEXT,
    nickname TEXT,
    simulator INTEGER DEFAULT 1,        -- 0=机器模拟, 1=手工账户
    s1_filter_active INTEGER DEFAULT 1, -- 1=不过滤, 0=S1过滤激活
    unit_pct REAL DEFAULT 5.0,          -- 单标的1单位仓位占总资金百分比
    max_holdings INTEGER DEFAULT 5,   -- 账户最大持仓标的数
    max_daily_open INTEGER DEFAULT 2,  -- 单日最大开仓标的数
    updated_at TEXT,
    note TEXT
);

CREATE INDEX IF NOT EXISTS idx_turtle_account_bind ON turtle_account(bind_id);

-- ============================================
-- 6. 海龟交易法 — 资金流水表
-- ============================================
CREATE TABLE IF NOT EXISTS turtle_account_flow (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    type TEXT NOT NULL,             -- 入金/出金/买入/卖出
    amount REAL NOT NULL,
    balance_after REAL,
    created_at TEXT,
    FOREIGN KEY (account_id) REFERENCES turtle_account(id)
);

CREATE INDEX IF NOT EXISTS idx_account_flow_account ON turtle_account_flow(account_id);

-- ============================================
-- 7. 海龟交易法 — 持仓表（多账户）
-- ============================================
CREATE TABLE IF NOT EXISTS turtle_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    status TEXT NOT NULL DEFAULT 'HOLDING',  -- HOLDING/COOLING/CLOSED
    units INTEGER DEFAULT 1,
    total_shares INTEGER DEFAULT 0,
    avg_cost REAL DEFAULT 0,
    entry_price REAL,
    last_add_price REAL,
    current_stop REAL,
    next_add_price REAL,
    exit_price REAL,
    atr_value REAL,
    shares_per_unit INTEGER DEFAULT 0,  -- 开仓时固定的一单位手数
    has_reduced INTEGER DEFAULT 0,  -- 0=未减仓, 1=已减仓
    system_type TEXT,               -- 'S1' 或 'S2'
    last_buy_date TEXT,
    last_buy_shares INTEGER DEFAULT 0,
    cooldown_until TEXT,
    opened_at TEXT,
    closed_at TEXT,
    updated_at TEXT,
    FOREIGN KEY (account_id) REFERENCES turtle_account(id)
);

CREATE INDEX IF NOT EXISTS idx_positions_account ON turtle_positions(account_id);
CREATE INDEX IF NOT EXISTS idx_positions_code ON turtle_positions(code);
CREATE INDEX IF NOT EXISTS idx_positions_status ON turtle_positions(status);

-- ============================================
-- 8. 海龟交易法 — 持仓流水表
-- ============================================
CREATE TABLE IF NOT EXISTS turtle_position_flow (
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
    FOREIGN KEY (account_id) REFERENCES turtle_account(id)
);

CREATE INDEX IF NOT EXISTS idx_position_flow_account ON turtle_position_flow(account_id);
CREATE INDEX IF NOT EXISTS idx_position_flow_code ON turtle_position_flow(code);

-- ============================================
-- 9. 海龟交易法 — 自选池表
-- ============================================
CREATE TABLE IF NOT EXISTS turtle_watchlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT,
    name TEXT,
    keyword TEXT,
    type TEXT NOT NULL,
    note TEXT,
    added_at TEXT,
    active INTEGER DEFAULT 1
);
