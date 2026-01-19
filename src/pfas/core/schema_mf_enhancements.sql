-- MF Schema Enhancements for PFAS v2.0
-- Adds: Capital Gains Reconciliation, FY Transaction Summary, Holdings Growth Tracking

-- ============================================================================
-- 1. Capital Gains Reconciliation
-- ============================================================================
-- Tracks reconciliation between calculated capital gains and RTA-reported gains
CREATE TABLE IF NOT EXISTS mf_cg_reconciliation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    financial_year TEXT NOT NULL,
    rta TEXT NOT NULL CHECK(rta IN ('CAMS', 'KFINTECH', 'COMBINED')),
    asset_class TEXT NOT NULL CHECK(asset_class IN ('EQUITY', 'DEBT', 'HYBRID', 'OTHER', 'ALL')),

    -- Calculated values (from our system)
    calc_stcg DECIMAL(15,2) DEFAULT 0,
    calc_ltcg DECIMAL(15,2) DEFAULT 0,
    calc_total_gain DECIMAL(15,2) DEFAULT 0,

    -- Reported values (from RTA statements)
    reported_stcg DECIMAL(15,2) DEFAULT 0,
    reported_ltcg DECIMAL(15,2) DEFAULT 0,
    reported_total_gain DECIMAL(15,2) DEFAULT 0,

    -- Differences
    stcg_difference DECIMAL(15,2) DEFAULT 0,
    ltcg_difference DECIMAL(15,2) DEFAULT 0,
    total_difference DECIMAL(15,2) DEFAULT 0,

    -- Status
    is_reconciled BOOLEAN DEFAULT FALSE,
    tolerance_used DECIMAL(10,4),
    reconciliation_notes TEXT,

    -- Audit fields
    source_file TEXT,
    reconciled_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(user_id, financial_year, rta, asset_class),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- Detailed reconciliation items for audit trail
CREATE TABLE IF NOT EXISTS mf_cg_reconciliation_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reconciliation_id INTEGER NOT NULL,
    scheme_name TEXT NOT NULL,
    folio_number TEXT NOT NULL,

    -- Calculated values
    calc_stcg DECIMAL(15,2) DEFAULT 0,
    calc_ltcg DECIMAL(15,2) DEFAULT 0,

    -- Reported values
    reported_stcg DECIMAL(15,2) DEFAULT 0,
    reported_ltcg DECIMAL(15,2) DEFAULT 0,

    -- Difference
    difference DECIMAL(15,2) DEFAULT 0,
    match_status TEXT CHECK(match_status IN ('MATCH', 'MISMATCH', 'MISSING_CALC', 'MISSING_REPORTED')),
    notes TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (reconciliation_id) REFERENCES mf_cg_reconciliation(id) ON DELETE CASCADE
);

-- ============================================================================
-- 2. Financial Year Transaction Summary
-- ============================================================================
-- Aggregated transaction summary by FY, scheme type, AMC, and RTA
CREATE TABLE IF NOT EXISTS mf_fy_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    financial_year TEXT NOT NULL,

    -- Grouping dimensions
    scheme_type TEXT NOT NULL CHECK(scheme_type IN ('EQUITY', 'DEBT', 'HYBRID', 'OTHER', 'ALL')),
    amc_name TEXT,  -- NULL means "ALL AMCs"
    rta TEXT,       -- NULL means "ALL RTAs"

    -- Opening balance (start of FY)
    opening_units DECIMAL(15,4) DEFAULT 0,
    opening_value DECIMAL(15,2) DEFAULT 0,
    opening_cost DECIMAL(15,2) DEFAULT 0,

    -- Purchases during FY
    purchase_units DECIMAL(15,4) DEFAULT 0,
    purchase_amount DECIMAL(15,2) DEFAULT 0,
    purchase_count INTEGER DEFAULT 0,

    -- Redemptions during FY
    redemption_units DECIMAL(15,4) DEFAULT 0,
    redemption_amount DECIMAL(15,2) DEFAULT 0,
    redemption_count INTEGER DEFAULT 0,

    -- Switch In/Out during FY
    switch_in_units DECIMAL(15,4) DEFAULT 0,
    switch_in_amount DECIMAL(15,2) DEFAULT 0,
    switch_out_units DECIMAL(15,4) DEFAULT 0,
    switch_out_amount DECIMAL(15,2) DEFAULT 0,

    -- Dividends
    dividend_payout DECIMAL(15,2) DEFAULT 0,
    dividend_reinvest DECIMAL(15,2) DEFAULT 0,

    -- Capital Gains
    stcg_realized DECIMAL(15,2) DEFAULT 0,
    ltcg_realized DECIMAL(15,2) DEFAULT 0,

    -- Closing balance (end of FY)
    closing_units DECIMAL(15,4) DEFAULT 0,
    closing_value DECIMAL(15,2) DEFAULT 0,
    closing_cost DECIMAL(15,2) DEFAULT 0,

    -- Performance
    absolute_return DECIMAL(15,2) DEFAULT 0,
    xirr DECIMAL(8,4),

    -- Metadata
    source_files TEXT,  -- JSON array of source files
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(user_id, financial_year, scheme_type, amc_name, rta),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- ============================================================================
-- 3. Holdings Growth Tracking (Year-over-Year)
-- ============================================================================
-- Point-in-time holdings snapshot for YoY growth comparison
CREATE TABLE IF NOT EXISTS mf_holdings_snapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    snapshot_date DATE NOT NULL,
    snapshot_type TEXT NOT NULL CHECK(snapshot_type IN ('FY_START', 'FY_END', 'QUARTERLY', 'MONTHLY', 'ADHOC')),
    financial_year TEXT,

    -- Individual scheme holdings (stored as JSON for flexibility)
    holdings_json TEXT NOT NULL,  -- JSON array of {scheme_name, folio, units, nav, value, cost, type}

    -- Aggregated totals
    total_schemes INTEGER DEFAULT 0,
    total_folios INTEGER DEFAULT 0,
    total_units DECIMAL(20,4) DEFAULT 0,
    total_value DECIMAL(15,2) NOT NULL,
    total_cost DECIMAL(15,2) DEFAULT 0,
    total_appreciation DECIMAL(15,2) DEFAULT 0,

    -- Category breakdown
    equity_value DECIMAL(15,2) DEFAULT 0,
    equity_schemes INTEGER DEFAULT 0,
    debt_value DECIMAL(15,2) DEFAULT 0,
    debt_schemes INTEGER DEFAULT 0,
    hybrid_value DECIMAL(15,2) DEFAULT 0,
    hybrid_schemes INTEGER DEFAULT 0,

    -- Performance metrics
    weighted_xirr DECIMAL(8,4),

    -- Metadata
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(user_id, snapshot_date, snapshot_type),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- YoY Growth comparison table (pre-computed for reporting)
CREATE TABLE IF NOT EXISTS mf_yoy_growth (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    base_year TEXT NOT NULL,      -- e.g., "2023-24"
    compare_year TEXT NOT NULL,   -- e.g., "2024-25"

    -- Value growth
    base_value DECIMAL(15,2) NOT NULL,
    compare_value DECIMAL(15,2) NOT NULL,
    value_change DECIMAL(15,2) DEFAULT 0,
    value_change_pct DECIMAL(8,4) DEFAULT 0,

    -- Cost growth (net investments)
    base_cost DECIMAL(15,2) DEFAULT 0,
    compare_cost DECIMAL(15,2) DEFAULT 0,
    net_investment DECIMAL(15,2) DEFAULT 0,

    -- Appreciation growth
    base_appreciation DECIMAL(15,2) DEFAULT 0,
    compare_appreciation DECIMAL(15,2) DEFAULT 0,
    appreciation_change DECIMAL(15,2) DEFAULT 0,

    -- Category-wise growth
    equity_growth_pct DECIMAL(8,4) DEFAULT 0,
    debt_growth_pct DECIMAL(8,4) DEFAULT 0,
    hybrid_growth_pct DECIMAL(8,4) DEFAULT 0,

    -- Scheme changes
    schemes_added INTEGER DEFAULT 0,
    schemes_removed INTEGER DEFAULT 0,
    schemes_unchanged INTEGER DEFAULT 0,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(user_id, base_year, compare_year),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- ============================================================================
-- 4. Scheme Master with Distribution Tracking
-- ============================================================================
-- Extended scheme details for portfolio analysis
CREATE TABLE IF NOT EXISTS mf_scheme_analysis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    analysis_date DATE NOT NULL,

    -- Scheme identification
    scheme_name TEXT NOT NULL,
    amc_name TEXT NOT NULL,
    folio_number TEXT NOT NULL,
    isin TEXT,
    scheme_type TEXT NOT NULL CHECK(scheme_type IN ('EQUITY', 'DEBT', 'HYBRID', 'OTHER')),
    rta TEXT NOT NULL CHECK(rta IN ('CAMS', 'KFINTECH')),

    -- Holdings
    units DECIMAL(15,4) NOT NULL,
    nav DECIMAL(15,4),
    current_value DECIMAL(15,2) NOT NULL,
    cost_value DECIMAL(15,2) DEFAULT 0,
    appreciation DECIMAL(15,2) DEFAULT 0,

    -- Portfolio distribution
    pct_of_total_portfolio DECIMAL(8,4) DEFAULT 0,
    pct_of_category DECIMAL(8,4) DEFAULT 0,
    pct_of_amc DECIMAL(8,4) DEFAULT 0,

    -- Performance
    annualized_return DECIMAL(8,4),
    holding_period_days INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(user_id, analysis_date, scheme_name, folio_number),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- ============================================================================
-- Indexes for Performance
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_mf_cg_recon_user_fy ON mf_cg_reconciliation(user_id, financial_year);
CREATE INDEX IF NOT EXISTS idx_mf_cg_recon_rta ON mf_cg_reconciliation(rta);
CREATE INDEX IF NOT EXISTS idx_mf_cg_recon_items_recon ON mf_cg_reconciliation_items(reconciliation_id);

CREATE INDEX IF NOT EXISTS idx_mf_fy_summary_user_fy ON mf_fy_summary(user_id, financial_year);
CREATE INDEX IF NOT EXISTS idx_mf_fy_summary_type ON mf_fy_summary(scheme_type);
CREATE INDEX IF NOT EXISTS idx_mf_fy_summary_amc ON mf_fy_summary(amc_name);

CREATE INDEX IF NOT EXISTS idx_mf_holdings_snapshot_user ON mf_holdings_snapshot(user_id);
CREATE INDEX IF NOT EXISTS idx_mf_holdings_snapshot_date ON mf_holdings_snapshot(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_mf_holdings_snapshot_fy ON mf_holdings_snapshot(financial_year);

CREATE INDEX IF NOT EXISTS idx_mf_yoy_growth_user ON mf_yoy_growth(user_id);
CREATE INDEX IF NOT EXISTS idx_mf_yoy_growth_years ON mf_yoy_growth(base_year, compare_year);

CREATE INDEX IF NOT EXISTS idx_mf_scheme_analysis_user ON mf_scheme_analysis(user_id);
CREATE INDEX IF NOT EXISTS idx_mf_scheme_analysis_date ON mf_scheme_analysis(analysis_date);
CREATE INDEX IF NOT EXISTS idx_mf_scheme_analysis_type ON mf_scheme_analysis(scheme_type);

-- ============================================================================
-- Triggers for auto-updating timestamps
-- ============================================================================
CREATE TRIGGER IF NOT EXISTS update_mf_cg_reconciliation_timestamp
AFTER UPDATE ON mf_cg_reconciliation
BEGIN
    UPDATE mf_cg_reconciliation SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS update_mf_fy_summary_timestamp
AFTER UPDATE ON mf_fy_summary
BEGIN
    UPDATE mf_fy_summary SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;
