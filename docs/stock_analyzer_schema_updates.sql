-- Stock Analyzer Schema Updates
-- PFAS - Personal Financial Accounting System
-- Version: 1.0.0

-- ============================================================================
-- NEW TABLE: stock_holdings - Current portfolio snapshot per user/broker
-- ============================================================================
CREATE TABLE IF NOT EXISTS stock_holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    broker_id INTEGER,
    symbol TEXT NOT NULL,
    isin TEXT,
    company_name TEXT,
    sector TEXT,

    -- Quantities
    quantity_held INTEGER NOT NULL DEFAULT 0,
    quantity_lt INTEGER DEFAULT 0,  -- Long-term quantity (>365 days)
    quantity_pledged INTEGER DEFAULT 0,
    quantity_blocked INTEGER DEFAULT 0,
    quantity_discrepant INTEGER DEFAULT 0,

    -- Cost Basis
    average_buy_price DECIMAL(15,4) NOT NULL DEFAULT 0,
    total_cost_basis DECIMAL(15,2) NOT NULL DEFAULT 0,

    -- Current Valuation
    current_price DECIMAL(15,4) NOT NULL DEFAULT 0,
    market_value DECIMAL(15,2) NOT NULL DEFAULT 0,

    -- P&L
    unrealized_pnl DECIMAL(15,2) DEFAULT 0,
    unrealized_pnl_pct DECIMAL(10,4) DEFAULT 0,
    price_change_pct DECIMAL(10,4) DEFAULT 0,

    -- Metadata
    as_of_date DATE NOT NULL,
    source_file TEXT,
    demat_account TEXT,  -- ICICI, Zerodha, Groww, etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    UNIQUE(user_id, broker_id, isin, as_of_date),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (broker_id) REFERENCES stock_brokers(id) ON DELETE RESTRICT
);

-- Indexes for stock_holdings
CREATE INDEX IF NOT EXISTS idx_stock_holdings_user ON stock_holdings(user_id);
CREATE INDEX IF NOT EXISTS idx_stock_holdings_broker ON stock_holdings(broker_id);
CREATE INDEX IF NOT EXISTS idx_stock_holdings_symbol ON stock_holdings(symbol);
CREATE INDEX IF NOT EXISTS idx_stock_holdings_isin ON stock_holdings(isin);
CREATE INDEX IF NOT EXISTS idx_stock_holdings_date ON stock_holdings(as_of_date);
CREATE INDEX IF NOT EXISTS idx_stock_holdings_sector ON stock_holdings(sector);

-- ============================================================================
-- NEW TABLE: stock_capital_gains_detail - Per-transaction CG detail for ITR
-- ============================================================================
CREATE TABLE IF NOT EXISTS stock_capital_gains_detail (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    broker_id INTEGER,
    financial_year TEXT NOT NULL,
    quarter TEXT,  -- Q1, Q2, Q3, Q4, Q5

    -- Security Details
    symbol TEXT NOT NULL,
    isin TEXT,
    company_name TEXT,

    -- Transaction Details
    quantity INTEGER NOT NULL,
    buy_date DATE NOT NULL,
    sell_date DATE NOT NULL,
    holding_period_days INTEGER NOT NULL,

    -- Values
    buy_price DECIMAL(15,4) NOT NULL,
    sell_price DECIMAL(15,4) NOT NULL,
    buy_value DECIMAL(15,2) NOT NULL,
    sell_value DECIMAL(15,2) NOT NULL,
    buy_expenses DECIMAL(15,2) DEFAULT 0,
    sell_expenses DECIMAL(15,2) DEFAULT 0,

    -- Grandfathering (for LTCG on equity)
    fmv_31jan2018 DECIMAL(15,4),
    grandfathered_price DECIMAL(15,4),
    is_grandfathered BOOLEAN DEFAULT FALSE,

    -- Capital Gains
    gross_profit_loss DECIMAL(15,2) NOT NULL,
    cost_of_acquisition DECIMAL(15,2) NOT NULL,
    taxable_profit DECIMAL(15,2) NOT NULL,

    -- Classification
    is_long_term BOOLEAN NOT NULL,
    gain_type TEXT NOT NULL CHECK(gain_type IN ('STCG', 'LTCG')),

    -- STT
    stt_paid DECIMAL(15,2) DEFAULT 0,

    -- Turnover (for audit purposes)
    turnover DECIMAL(15,2) DEFAULT 0,

    -- Source
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(user_id, symbol, buy_date, sell_date, quantity),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (broker_id) REFERENCES stock_brokers(id) ON DELETE RESTRICT
);

-- Indexes for capital gains detail
CREATE INDEX IF NOT EXISTS idx_stock_cg_detail_user_fy ON stock_capital_gains_detail(user_id, financial_year);
CREATE INDEX IF NOT EXISTS idx_stock_cg_detail_quarter ON stock_capital_gains_detail(user_id, financial_year, quarter);
CREATE INDEX IF NOT EXISTS idx_stock_cg_detail_symbol ON stock_capital_gains_detail(symbol);
CREATE INDEX IF NOT EXISTS idx_stock_cg_detail_type ON stock_capital_gains_detail(gain_type);
CREATE INDEX IF NOT EXISTS idx_stock_cg_detail_sell_date ON stock_capital_gains_detail(sell_date);

-- ============================================================================
-- NEW TABLE: stock_xirr_performance - XIRR calculations per stock
-- ============================================================================
CREATE TABLE IF NOT EXISTS stock_xirr_performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    calculation_date DATE NOT NULL,

    -- Scope
    scope_type TEXT NOT NULL CHECK(scope_type IN ('OVERALL', 'BROKER', 'STOCK', 'SECTOR')),
    scope_value TEXT,  -- broker_code, symbol, sector name

    -- XIRR Result
    xirr_annual DECIMAL(10,6),  -- Annual return as decimal (0.15 = 15%)
    xirr_status TEXT CHECK(xirr_status IN ('SUCCESS', 'NO_DATA', 'ERROR', 'INSUFFICIENT')),

    -- CAGR (for realized positions)
    cagr_annual DECIMAL(10,6),

    -- Supporting Data
    total_invested DECIMAL(15,2),
    total_current_value DECIMAL(15,2),
    total_realized_gains DECIMAL(15,2),
    total_dividends DECIMAL(15,2),

    -- Period
    first_transaction_date DATE,
    holding_period_days INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(user_id, calculation_date, scope_type, scope_value),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_stock_xirr_user ON stock_xirr_performance(user_id);
CREATE INDEX IF NOT EXISTS idx_stock_xirr_date ON stock_xirr_performance(calculation_date);
CREATE INDEX IF NOT EXISTS idx_stock_xirr_scope ON stock_xirr_performance(scope_type, scope_value);

-- ============================================================================
-- ALTER: Add columns to stock_trades if missing
-- ============================================================================
-- Note: SQLite doesn't support IF NOT EXISTS for ALTER TABLE
-- These should be run only if columns don't exist

-- ALTER TABLE stock_trades ADD COLUMN fmv_31jan2018 DECIMAL(15,4);
-- ALTER TABLE stock_trades ADD COLUMN grandfathered_price DECIMAL(15,4);
-- ALTER TABLE stock_trades ADD COLUMN is_grandfathered BOOLEAN DEFAULT FALSE;
-- ALTER TABLE stock_trades ADD COLUMN quarter TEXT;
-- ALTER TABLE stock_trades ADD COLUMN company_name TEXT;
-- ALTER TABLE stock_trades ADD COLUMN turnover DECIMAL(15,2);

-- ============================================================================
-- Insert default brokers
-- ============================================================================
INSERT OR IGNORE INTO stock_brokers (name, broker_code) VALUES
    ('ICICI Direct', 'ICICI'),
    ('Zerodha', 'ZERODHA'),
    ('Groww', 'GROWW'),
    ('HDFC Securities', 'HDFC'),
    ('Kotak Securities', 'KOTAK'),
    ('Angel One', 'ANGEL'),
    ('Upstox', 'UPSTOX'),
    ('5Paisa', '5PAISA'),
    ('Motilal Oswal', 'MOTILAL'),
    ('Sharekhan', 'SHAREKHAN');

-- ============================================================================
-- VIEW: Current holdings with broker info
-- ============================================================================
CREATE VIEW IF NOT EXISTS v_stock_holdings_summary AS
SELECT
    h.user_id,
    u.name as user_name,
    b.name as broker_name,
    b.broker_code,
    h.symbol,
    h.isin,
    h.company_name,
    h.sector,
    h.quantity_held,
    h.quantity_lt,
    h.quantity_pledged,
    h.average_buy_price,
    h.total_cost_basis,
    h.current_price,
    h.market_value,
    h.unrealized_pnl,
    h.unrealized_pnl_pct,
    h.as_of_date
FROM stock_holdings h
JOIN users u ON h.user_id = u.id
LEFT JOIN stock_brokers b ON h.broker_id = b.id
ORDER BY h.market_value DESC;

-- ============================================================================
-- VIEW: Capital gains summary by FY and quarter
-- ============================================================================
CREATE VIEW IF NOT EXISTS v_stock_capital_gains_quarterly AS
SELECT
    user_id,
    financial_year,
    quarter,
    gain_type,
    COUNT(*) as transaction_count,
    SUM(quantity) as total_quantity,
    SUM(sell_value) as total_sell_value,
    SUM(buy_value) as total_buy_value,
    SUM(taxable_profit) as total_taxable_profit,
    SUM(stt_paid) as total_stt,
    SUM(turnover) as total_turnover
FROM stock_capital_gains_detail
GROUP BY user_id, financial_year, quarter, gain_type
ORDER BY user_id, financial_year, quarter, gain_type;

-- ============================================================================
-- VIEW: FY capital gains summary for ITR
-- ============================================================================
CREATE VIEW IF NOT EXISTS v_stock_capital_gains_fy_itr AS
SELECT
    user_id,
    financial_year,
    SUM(CASE WHEN gain_type = 'STCG' THEN taxable_profit ELSE 0 END) as total_stcg,
    SUM(CASE WHEN gain_type = 'LTCG' THEN taxable_profit ELSE 0 END) as total_ltcg,
    SUM(CASE WHEN gain_type = 'LTCG' AND taxable_profit > 0 THEN
        CASE WHEN taxable_profit <= 125000 THEN taxable_profit ELSE 125000 END
        ELSE 0 END) as ltcg_exemption_used,
    SUM(CASE WHEN gain_type = 'LTCG' AND taxable_profit > 125000 THEN
        taxable_profit - 125000 ELSE 0 END) as ltcg_above_exemption,
    SUM(stt_paid) as total_stt_paid,
    SUM(turnover) as total_turnover,
    COUNT(DISTINCT symbol) as stocks_traded
FROM stock_capital_gains_detail
GROUP BY user_id, financial_year;
