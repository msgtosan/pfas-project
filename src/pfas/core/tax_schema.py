"""
Tax Rules Database Schema for PFAS Advance Tax System.

This module contains the SQL schema for data-driven tax rules storage.
All tax rates, slabs, exemptions are stored in database tables instead of hardcoded values.
"""

TAX_RULES_SCHEMA = """
-- ============================================================
-- TAX RULES TABLES
-- All tax rules stored in database for easy updates without code changes
-- ============================================================

-- Income Tax Slabs by FY and Regime
CREATE TABLE IF NOT EXISTS income_tax_slabs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    financial_year TEXT NOT NULL,
    tax_regime TEXT NOT NULL CHECK(tax_regime IN ('OLD', 'NEW')),
    slab_order INTEGER NOT NULL,
    lower_limit DECIMAL(15,2) NOT NULL,
    upper_limit DECIMAL(15,2),
    tax_rate DECIMAL(5,4) NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(financial_year, tax_regime, slab_order)
);

CREATE INDEX IF NOT EXISTS idx_tax_slabs_fy ON income_tax_slabs(financial_year, tax_regime);

-- Capital Gains Rates by Asset Type
CREATE TABLE IF NOT EXISTS capital_gains_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    financial_year TEXT NOT NULL,
    asset_type TEXT NOT NULL CHECK(asset_type IN (
        'EQUITY_LISTED', 'EQUITY_MF', 'DEBT_MF', 'DEBT_MF_POST_APR2023',
        'GOLD', 'REAL_ESTATE', 'FOREIGN_EQUITY', 'UNLISTED', 'INTERNATIONAL_FUNDS'
    )),
    gain_type TEXT NOT NULL CHECK(gain_type IN ('STCG', 'LTCG')),
    holding_period_months INTEGER NOT NULL,
    tax_rate DECIMAL(5,4) NOT NULL,
    rate_type TEXT NOT NULL CHECK(rate_type IN ('FLAT', 'SLAB', 'INDEXED')),
    exemption_limit DECIMAL(15,2) DEFAULT 0,
    stt_required BOOLEAN DEFAULT FALSE,
    effective_from DATE NOT NULL,
    effective_to DATE,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(financial_year, asset_type, gain_type)
);

CREATE INDEX IF NOT EXISTS idx_cg_rates_fy ON capital_gains_rates(financial_year);
CREATE INDEX IF NOT EXISTS idx_cg_rates_asset ON capital_gains_rates(asset_type);

-- Standard Deductions
CREATE TABLE IF NOT EXISTS standard_deductions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    financial_year TEXT NOT NULL,
    tax_regime TEXT NOT NULL CHECK(tax_regime IN ('OLD', 'NEW', 'BOTH')),
    deduction_type TEXT NOT NULL CHECK(deduction_type IN (
        'SALARY', 'PENSION', 'FAMILY_PENSION', 'HOUSE_PROPERTY'
    )),
    deduction_amount DECIMAL(15,2),
    deduction_percent DECIMAL(5,4),
    max_limit DECIMAL(15,2),
    effective_from DATE NOT NULL,
    effective_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(financial_year, tax_regime, deduction_type)
);

CREATE INDEX IF NOT EXISTS idx_std_ded_fy ON standard_deductions(financial_year, tax_regime);

-- Chapter VI-A Deduction Limits
CREATE TABLE IF NOT EXISTS chapter_via_limits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    financial_year TEXT NOT NULL,
    tax_regime TEXT NOT NULL CHECK(tax_regime IN ('OLD', 'NEW', 'BOTH')),
    section TEXT NOT NULL,
    max_limit DECIMAL(15,2) NOT NULL,
    combined_limit_section TEXT,
    combined_max DECIMAL(15,2),
    available_in_new_regime BOOLEAN DEFAULT FALSE,
    effective_from DATE NOT NULL,
    effective_to DATE,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(financial_year, tax_regime, section)
);

CREATE INDEX IF NOT EXISTS idx_via_limits_fy ON chapter_via_limits(financial_year);

-- Surcharge Rates
CREATE TABLE IF NOT EXISTS surcharge_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    financial_year TEXT NOT NULL,
    income_type TEXT NOT NULL CHECK(income_type IN ('NORMAL', 'EQUITY_CG')),
    lower_limit DECIMAL(15,2) NOT NULL,
    upper_limit DECIMAL(15,2),
    surcharge_rate DECIMAL(5,4) NOT NULL,
    marginal_relief_applicable BOOLEAN DEFAULT TRUE,
    effective_from DATE NOT NULL,
    effective_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(financial_year, income_type, lower_limit)
);

CREATE INDEX IF NOT EXISTS idx_surcharge_fy ON surcharge_rates(financial_year);

-- Cess Rates
CREATE TABLE IF NOT EXISTS cess_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    financial_year TEXT NOT NULL,
    cess_type TEXT NOT NULL CHECK(cess_type IN ('HEALTH_EDUCATION', 'OTHER')),
    rate DECIMAL(5,4) NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(financial_year, cess_type)
);

-- Rebate Limits (Section 87A)
CREATE TABLE IF NOT EXISTS rebate_limits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    financial_year TEXT NOT NULL,
    tax_regime TEXT NOT NULL CHECK(tax_regime IN ('OLD', 'NEW')),
    income_limit DECIMAL(15,2) NOT NULL,
    max_rebate DECIMAL(15,2) NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(financial_year, tax_regime)
);

-- ============================================================
-- INCOME AGGREGATION TABLES
-- Pre-computed income summaries for fast report generation
-- ============================================================

-- User Income Summary (aggregated from parsed data)
CREATE TABLE IF NOT EXISTS user_income_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    financial_year TEXT NOT NULL,
    income_type TEXT NOT NULL CHECK(income_type IN (
        'SALARY', 'CAPITAL_GAINS', 'OTHER_SOURCES', 'HOUSE_PROPERTY', 'BUSINESS'
    )),
    sub_classification TEXT,
    income_sub_grouping TEXT,
    gross_amount DECIMAL(15,2) NOT NULL DEFAULT 0,
    deductions DECIMAL(15,2) DEFAULT 0,
    taxable_amount DECIMAL(15,2) NOT NULL DEFAULT 0,
    tds_deducted DECIMAL(15,2) DEFAULT 0,
    applicable_tax_rate_type TEXT CHECK(applicable_tax_rate_type IN (
        'SLAB', 'FLAT_10', 'FLAT_12.5', 'FLAT_15', 'FLAT_20', 'INDEXED'
    )),
    source_table TEXT NOT NULL,
    source_ids TEXT,
    last_synced_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id),
    UNIQUE(user_id, financial_year, income_type, sub_classification, income_sub_grouping)
);

CREATE INDEX IF NOT EXISTS idx_income_summary_user_fy ON user_income_summary(user_id, financial_year);

-- Statement Processing Log (prevents re-parsing)
CREATE TABLE IF NOT EXISTS statement_processing_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    file_modified_at TIMESTAMP,
    statement_type TEXT NOT NULL CHECK(statement_type IN (
        'ZERODHA_TAX_PNL', 'CAMS_CAS', 'KARVY_CG', 'ICICI_DIRECT',
        'ETRADE_GL', 'ETRADE_RSU', 'ETRADE_ESPP',
        'SALARY_PAYSLIP', 'FORM16', 'FORM_26AS',
        'EPF_PASSBOOK', 'PPF_STATEMENT', 'NPS_STATEMENT',
        'BANK_STATEMENT', 'OTHER'
    )),
    financial_year TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    processing_status TEXT NOT NULL CHECK(processing_status IN (
        'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED'
    )),
    records_extracted INTEGER,
    error_message TEXT,
    first_processed_at TIMESTAMP,
    last_processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id),
    UNIQUE(user_id, file_hash)
);

CREATE INDEX IF NOT EXISTS idx_stmt_log_user ON statement_processing_log(user_id);
CREATE INDEX IF NOT EXISTS idx_stmt_log_status ON statement_processing_log(processing_status);
CREATE INDEX IF NOT EXISTS idx_stmt_log_fy ON statement_processing_log(financial_year);

-- Advance Tax Computation History
CREATE TABLE IF NOT EXISTS advance_tax_computation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    financial_year TEXT NOT NULL,
    tax_regime TEXT NOT NULL CHECK(tax_regime IN ('OLD', 'NEW')),
    computation_date TIMESTAMP NOT NULL,

    -- Income Summary
    total_salary_income DECIMAL(15,2) DEFAULT 0,
    total_stcg_equity DECIMAL(15,2) DEFAULT 0,
    total_ltcg_equity DECIMAL(15,2) DEFAULT 0,
    total_capital_gains_slab DECIMAL(15,2) DEFAULT 0,
    total_other_income DECIMAL(15,2) DEFAULT 0,
    total_house_property DECIMAL(15,2) DEFAULT 0,

    gross_total_income DECIMAL(15,2) NOT NULL,
    total_deductions DECIMAL(15,2) DEFAULT 0,
    taxable_income DECIMAL(15,2) NOT NULL,

    -- Tax Breakdown
    tax_on_slab_income DECIMAL(15,2) DEFAULT 0,
    tax_on_stcg_equity DECIMAL(15,2) DEFAULT 0,
    tax_on_ltcg_equity DECIMAL(15,2) DEFAULT 0,
    total_tax_before_cess DECIMAL(15,2) NOT NULL,

    surcharge_rate DECIMAL(5,4) DEFAULT 0,
    surcharge_amount DECIMAL(15,2) DEFAULT 0,
    cess_rate DECIMAL(5,4) DEFAULT 0.04,
    cess_amount DECIMAL(15,2) DEFAULT 0,

    total_tax_liability DECIMAL(15,2) NOT NULL,

    -- Credits/Payments
    tds_deducted DECIMAL(15,2) DEFAULT 0,
    advance_tax_paid DECIMAL(15,2) DEFAULT 0,
    self_assessment_tax DECIMAL(15,2) DEFAULT 0,
    balance_tax_payable DECIMAL(15,2) DEFAULT 0,

    -- Metadata
    computation_json TEXT,
    is_latest BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_adv_tax_user_fy ON advance_tax_computation(user_id, financial_year);
CREATE INDEX IF NOT EXISTS idx_adv_tax_latest ON advance_tax_computation(user_id, financial_year, is_latest);
"""

TAX_RULES_SEED_DATA = """
-- ============================================================
-- SEED DATA: FY 2024-25 Tax Rules (New Regime)
-- ============================================================

-- Income Tax Slabs - New Regime FY 2024-25
INSERT OR IGNORE INTO income_tax_slabs (financial_year, tax_regime, slab_order, lower_limit, upper_limit, tax_rate, effective_from) VALUES
('2024-25', 'NEW', 1, 0, 300000, 0.00, '2024-04-01'),
('2024-25', 'NEW', 2, 300000, 700000, 0.05, '2024-04-01'),
('2024-25', 'NEW', 3, 700000, 1000000, 0.10, '2024-04-01'),
('2024-25', 'NEW', 4, 1000000, 1200000, 0.15, '2024-04-01'),
('2024-25', 'NEW', 5, 1200000, 1500000, 0.20, '2024-04-01'),
('2024-25', 'NEW', 6, 1500000, NULL, 0.30, '2024-04-01');

-- Income Tax Slabs - Old Regime FY 2024-25
INSERT OR IGNORE INTO income_tax_slabs (financial_year, tax_regime, slab_order, lower_limit, upper_limit, tax_rate, effective_from) VALUES
('2024-25', 'OLD', 1, 0, 250000, 0.00, '2024-04-01'),
('2024-25', 'OLD', 2, 250000, 500000, 0.05, '2024-04-01'),
('2024-25', 'OLD', 3, 500000, 1000000, 0.20, '2024-04-01'),
('2024-25', 'OLD', 4, 1000000, NULL, 0.30, '2024-04-01');

-- Income Tax Slabs - New Regime FY 2025-26
INSERT OR IGNORE INTO income_tax_slabs (financial_year, tax_regime, slab_order, lower_limit, upper_limit, tax_rate, effective_from) VALUES
('2025-26', 'NEW', 1, 0, 400000, 0.00, '2025-04-01'),
('2025-26', 'NEW', 2, 400000, 800000, 0.05, '2025-04-01'),
('2025-26', 'NEW', 3, 800000, 1200000, 0.10, '2025-04-01'),
('2025-26', 'NEW', 4, 1200000, 1600000, 0.15, '2025-04-01'),
('2025-26', 'NEW', 5, 1600000, 2000000, 0.20, '2025-04-01'),
('2025-26', 'NEW', 6, 2000000, 2400000, 0.25, '2025-04-01'),
('2025-26', 'NEW', 7, 2400000, NULL, 0.30, '2025-04-01');

-- Income Tax Slabs - Old Regime FY 2025-26
INSERT OR IGNORE INTO income_tax_slabs (financial_year, tax_regime, slab_order, lower_limit, upper_limit, tax_rate, effective_from) VALUES
('2025-26', 'OLD', 1, 0, 250000, 0.00, '2025-04-01'),
('2025-26', 'OLD', 2, 250000, 500000, 0.05, '2025-04-01'),
('2025-26', 'OLD', 3, 500000, 1000000, 0.20, '2025-04-01'),
('2025-26', 'OLD', 4, 1000000, NULL, 0.30, '2025-04-01');

-- Capital Gains Rates - FY 2024-25
INSERT OR IGNORE INTO capital_gains_rates (financial_year, asset_type, gain_type, holding_period_months, tax_rate, rate_type, exemption_limit, stt_required, effective_from, notes) VALUES
-- Listed Equity
('2024-25', 'EQUITY_LISTED', 'STCG', 12, 0.15, 'FLAT', 0, TRUE, '2024-04-01', 'Listed equity with STT'),
('2024-25', 'EQUITY_LISTED', 'LTCG', 12, 0.10, 'FLAT', 100000, TRUE, '2024-04-01', 'Exempt up to 1L'),
-- Equity MF
('2024-25', 'EQUITY_MF', 'STCG', 12, 0.15, 'FLAT', 0, TRUE, '2024-04-01', 'Equity MF with STT'),
('2024-25', 'EQUITY_MF', 'LTCG', 12, 0.10, 'FLAT', 100000, TRUE, '2024-04-01', 'Exempt up to 1L'),
-- Debt MF (post Apr 2023)
('2024-25', 'DEBT_MF_POST_APR2023', 'STCG', 0, 0.00, 'SLAB', 0, FALSE, '2024-04-01', 'Taxed at slab rates'),
('2024-25', 'DEBT_MF_POST_APR2023', 'LTCG', 0, 0.00, 'SLAB', 0, FALSE, '2024-04-01', 'No LTCG benefit post Apr 2023'),
-- Foreign Equity
('2024-25', 'FOREIGN_EQUITY', 'STCG', 24, 0.00, 'SLAB', 0, FALSE, '2024-04-01', 'USA stocks - slab rates'),
('2024-25', 'FOREIGN_EQUITY', 'LTCG', 24, 0.20, 'INDEXED', 0, FALSE, '2024-04-01', '20% with indexation'),
-- Gold
('2024-25', 'GOLD', 'STCG', 24, 0.00, 'SLAB', 0, FALSE, '2024-04-01', 'Physical gold - slab rates'),
('2024-25', 'GOLD', 'LTCG', 24, 0.20, 'INDEXED', 0, FALSE, '2024-04-01', '20% with indexation'),
-- Real Estate
('2024-25', 'REAL_ESTATE', 'STCG', 24, 0.00, 'SLAB', 0, FALSE, '2024-04-01', 'Slab rates'),
('2024-25', 'REAL_ESTATE', 'LTCG', 24, 0.20, 'INDEXED', 0, FALSE, '2024-04-01', '20% with indexation');

-- Capital Gains Rates - FY 2025-26 (Updated rates)
INSERT OR IGNORE INTO capital_gains_rates (financial_year, asset_type, gain_type, holding_period_months, tax_rate, rate_type, exemption_limit, stt_required, effective_from, notes) VALUES
-- Listed Equity (increased rates)
('2025-26', 'EQUITY_LISTED', 'STCG', 12, 0.20, 'FLAT', 0, TRUE, '2025-04-01', 'Increased to 20%'),
('2025-26', 'EQUITY_LISTED', 'LTCG', 12, 0.125, 'FLAT', 125000, TRUE, '2025-04-01', 'Increased to 12.5%, exempt 1.25L'),
-- Equity MF
('2025-26', 'EQUITY_MF', 'STCG', 12, 0.20, 'FLAT', 0, TRUE, '2025-04-01', 'Increased to 20%'),
('2025-26', 'EQUITY_MF', 'LTCG', 12, 0.125, 'FLAT', 125000, TRUE, '2025-04-01', 'Increased to 12.5%, exempt 1.25L'),
-- Debt MF (post Apr 2023)
('2025-26', 'DEBT_MF_POST_APR2023', 'STCG', 0, 0.00, 'SLAB', 0, FALSE, '2025-04-01', 'Taxed at slab rates'),
('2025-26', 'DEBT_MF_POST_APR2023', 'LTCG', 0, 0.00, 'SLAB', 0, FALSE, '2025-04-01', 'No LTCG benefit'),
-- Foreign Equity (flat rate now)
('2025-26', 'FOREIGN_EQUITY', 'STCG', 24, 0.00, 'SLAB', 0, FALSE, '2025-04-01', 'USA stocks - slab rates'),
('2025-26', 'FOREIGN_EQUITY', 'LTCG', 24, 0.125, 'FLAT', 0, FALSE, '2025-04-01', '12.5% flat (no indexation)'),
-- Gold
('2025-26', 'GOLD', 'STCG', 24, 0.00, 'SLAB', 0, FALSE, '2025-04-01', 'Slab rates'),
('2025-26', 'GOLD', 'LTCG', 24, 0.125, 'FLAT', 0, FALSE, '2025-04-01', '12.5% flat'),
-- Real Estate
('2025-26', 'REAL_ESTATE', 'STCG', 24, 0.00, 'SLAB', 0, FALSE, '2025-04-01', 'Slab rates'),
('2025-26', 'REAL_ESTATE', 'LTCG', 24, 0.125, 'FLAT', 0, FALSE, '2025-04-01', '12.5% flat or 20% indexed for pre-Jul 2024');

-- Standard Deductions
INSERT OR IGNORE INTO standard_deductions (financial_year, tax_regime, deduction_type, deduction_amount, effective_from) VALUES
('2024-25', 'NEW', 'SALARY', 50000, '2024-04-01'),
('2024-25', 'OLD', 'SALARY', 50000, '2024-04-01'),
('2025-26', 'NEW', 'SALARY', 75000, '2025-04-01'),
('2025-26', 'OLD', 'SALARY', 50000, '2025-04-01'),
('2024-25', 'NEW', 'PENSION', 50000, '2024-04-01'),
('2025-26', 'NEW', 'PENSION', 75000, '2025-04-01');

-- House Property Standard Deduction (30% of NAV)
INSERT OR IGNORE INTO standard_deductions (financial_year, tax_regime, deduction_type, deduction_percent, effective_from) VALUES
('2024-25', 'BOTH', 'HOUSE_PROPERTY', 0.30, '2024-04-01'),
('2025-26', 'BOTH', 'HOUSE_PROPERTY', 0.30, '2025-04-01');

-- Surcharge Rates
INSERT OR IGNORE INTO surcharge_rates (financial_year, income_type, lower_limit, upper_limit, surcharge_rate, marginal_relief_applicable, effective_from) VALUES
-- FY 2024-25
('2024-25', 'NORMAL', 0, 5000000, 0.00, FALSE, '2024-04-01'),
('2024-25', 'NORMAL', 5000000, 10000000, 0.10, TRUE, '2024-04-01'),
('2024-25', 'NORMAL', 10000000, 20000000, 0.15, TRUE, '2024-04-01'),
('2024-25', 'NORMAL', 20000000, 50000000, 0.25, TRUE, '2024-04-01'),
('2024-25', 'NORMAL', 50000000, NULL, 0.37, TRUE, '2024-04-01'),
('2024-25', 'EQUITY_CG', 0, NULL, 0.15, FALSE, '2024-04-01'),
-- FY 2025-26
('2025-26', 'NORMAL', 0, 5000000, 0.00, FALSE, '2025-04-01'),
('2025-26', 'NORMAL', 5000000, 10000000, 0.10, TRUE, '2025-04-01'),
('2025-26', 'NORMAL', 10000000, 20000000, 0.15, TRUE, '2025-04-01'),
('2025-26', 'NORMAL', 20000000, 50000000, 0.25, TRUE, '2025-04-01'),
('2025-26', 'NORMAL', 50000000, NULL, 0.37, TRUE, '2025-04-01'),
('2025-26', 'EQUITY_CG', 0, NULL, 0.15, FALSE, '2025-04-01');

-- Cess Rates
INSERT OR IGNORE INTO cess_rates (financial_year, cess_type, rate, effective_from) VALUES
('2024-25', 'HEALTH_EDUCATION', 0.04, '2024-04-01'),
('2025-26', 'HEALTH_EDUCATION', 0.04, '2025-04-01');

-- Rebate Limits (Section 87A)
INSERT OR IGNORE INTO rebate_limits (financial_year, tax_regime, income_limit, max_rebate, effective_from) VALUES
('2024-25', 'NEW', 700000, 25000, '2024-04-01'),
('2024-25', 'OLD', 500000, 12500, '2024-04-01'),
('2025-26', 'NEW', 800000, 25000, '2025-04-01'),
('2025-26', 'OLD', 500000, 12500, '2025-04-01');

-- Chapter VI-A Limits (Old Regime)
INSERT OR IGNORE INTO chapter_via_limits (financial_year, tax_regime, section, max_limit, combined_limit_section, combined_max, available_in_new_regime, effective_from, notes) VALUES
('2024-25', 'OLD', '80C', 150000, '80CCE', 150000, FALSE, '2024-04-01', 'PPF, ELSS, LIC, etc.'),
('2024-25', 'OLD', '80CCC', 150000, '80CCE', 150000, FALSE, '2024-04-01', 'Pension plans'),
('2024-25', 'OLD', '80CCD1', 150000, '80CCE', 150000, FALSE, '2024-04-01', 'NPS employee contribution'),
('2024-25', 'OLD', '80CCD1B', 50000, NULL, NULL, FALSE, '2024-04-01', 'Additional NPS (over 80CCE)'),
('2024-25', 'NEW', '80CCD2', 750000, NULL, NULL, TRUE, '2024-04-01', 'Employer NPS - available in new regime'),
('2024-25', 'OLD', '80D', 100000, NULL, NULL, FALSE, '2024-04-01', 'Health insurance (senior: 50K+50K)'),
('2024-25', 'OLD', '80TTA', 10000, NULL, NULL, FALSE, '2024-04-01', 'Savings interest'),
('2024-25', 'OLD', '80TTB', 50000, NULL, NULL, FALSE, '2024-04-01', 'Senior citizen interest'),
('2025-26', 'OLD', '80C', 150000, '80CCE', 150000, FALSE, '2025-04-01', 'PPF, ELSS, LIC, etc.'),
('2025-26', 'OLD', '80CCD1B', 50000, NULL, NULL, FALSE, '2025-04-01', 'Additional NPS'),
('2025-26', 'NEW', '80CCD2', 750000, NULL, NULL, TRUE, '2025-04-01', 'Employer NPS');
"""


def init_tax_schema(db_connection) -> None:
    """Initialize tax rules schema and seed data."""
    db_connection.executescript(TAX_RULES_SCHEMA)
    db_connection.executescript(TAX_RULES_SEED_DATA)
    db_connection.commit()
