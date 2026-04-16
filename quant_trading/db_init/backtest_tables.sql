-- 回测月度收益汇总表
CREATE TABLE IF NOT EXISTS backtest_monthly (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    year_month TEXT NOT NULL,           -- '2026-04'
    start_date TEXT NOT NULL,           -- 该月首个交易日
    end_date TEXT NOT NULL,             -- 该月最后交易日
    trade_days INTEGER DEFAULT 0,
    start_capital REAL DEFAULT 0,       -- 月初总资产
    end_capital REAL DEFAULT 0,         -- 月末总资产
    profit REAL DEFAULT 0,              -- 盈亏 = end - start
    profit_pct REAL DEFAULT 0,          -- 收益率 = profit / start * 100
    open_count INTEGER DEFAULT 0,       -- 开仓次数
    add_count INTEGER DEFAULT 0,        -- 加仓次数
    reduce_count INTEGER DEFAULT 0,     -- 减仓次数
    close_count INTEGER DEFAULT 0,      -- 平仓次数
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(account_id, year_month)
);
