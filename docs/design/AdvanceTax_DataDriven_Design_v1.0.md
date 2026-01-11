# Advance Tax System - Data-Driven Design Proposal

**Version:** 1.0
**Date:** January 2026
**Author:** PFAS Development Team

---

## 1. Executive Summary

This document proposes design and implementation changes to make the Advance Tax Report generation system fully **data-driven** and **scalable**. The key objectives are:

1. **Eliminate re-parsing** of already processed statements
2. **Store tax rules in database** for easy updates without code changes
3. **Consolidate income data** from existing parsed records
4. **Support multiple users** with same codebase
5. **Handle evolving tax regulations** for future financial years

---

## 2. Current State Analysis

### 2.1 What Exists Today

| Component | Current State | Limitation |
|-----------|--------------|------------|
| **Parsers** | Parse statements directly from files | Re-parses every time |
| **Database** | Comprehensive income tables exist | Not used for tax computation |
| **Tax Rates** | Hardcoded in Python (`TaxSlabs` class) | Code changes needed for updates |
| **Report Generator** | Reads from files via loaders | Duplicates parsing logic |
| **User Data** | Folder-based (`Data/Users/<name>`) | No centralized income summary |

### 2.2 Database Tables Available (Already Parsed Data)

```
Income Source          | Table Name              | Key Fields
-----------------------|-------------------------|----------------------------------
Salary                 | salary_records          | gross_salary, net_pay, tax_deducted
RSU Perquisites        | perquisites            | perquisite_type, taxable_value
Form 16                | form16_records         | taxable_income, tax_payable
Mutual Funds           | mf_capital_gains       | stcg, ltcg by asset_class
Indian Stocks          | stock_capital_gains    | stcg, ltcg by trade_category
Stock Dividends        | stock_dividend_summary | total_dividend, tds_deducted
Bank Interest          | bank_interest_summary  | total_interest, tds_deducted
EPF                    | epf_interest           | interest_amount, tds_deducted
PPF                    | ppf_transactions       | interest credited
NPS                    | nps_transactions       | contributions
Foreign Stocks (RSU)   | rsu_sales              | capital_gain, is_ltcg
Foreign Dividends      | foreign_dividends      | gross_amount, withholding_tax
```

---

## 3. Proposed Architecture

### 3.1 High-Level Design

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ADVANCE TAX SYSTEM v2.0                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────┐  │
│  │   Parsers    │───▶│   Database   │───▶│  Income Aggregation      │  │
│  │  (One-time)  │    │   (Source)   │    │  Views/Materialized      │  │
│  └──────────────┘    └──────────────┘    └──────────────────────────┘  │
│                                                    │                    │
│                                                    ▼                    │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                    TAX RULES DATABASE                             │  │
│  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────────┐  │  │
│  │  │ income_tax_    │  │ capital_gains_ │  │ surcharge_         │  │  │
│  │  │ slabs          │  │ rates          │  │ rates              │  │  │
│  │  └────────────────┘  └────────────────┘  └────────────────────┘  │  │
│  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────────┐  │  │
│  │  │ deduction_     │  │ exemption_     │  │ cess_rates         │  │  │
│  │  │ limits         │  │ limits         │  │                    │  │  │
│  │  └────────────────┘  └────────────────┘  └────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                │                                        │
│                                ▼                                        │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                    ADVANCE TAX CALCULATOR                         │  │
│  │  • Reads income from aggregation views                           │  │
│  │  • Applies tax rules from database                               │  │
│  │  • No hardcoded rates                                            │  │
│  │  • Generates Excel/PDF reports                                   │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Key Design Principles

1. **Parse Once, Use Many Times**: Statements are parsed once and stored in database
2. **Tax Rules as Data**: All tax rates, slabs, exemptions stored in database tables
3. **Fiscal Year Versioning**: Each rule has effective dates for historical accuracy
4. **User Isolation**: All queries scoped by `user_id`
5. **Incremental Updates**: Only new/modified statements need parsing

---

## 4. Database Schema Changes

### 4.1 New Tables for Tax Rules

```sql
-- ============================================================
-- TABLE: income_tax_slabs
-- Purpose: Store slab-wise tax rates for each regime and FY
-- ============================================================
CREATE TABLE income_tax_slabs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    financial_year TEXT NOT NULL,           -- '2024-25', '2025-26'
    tax_regime TEXT NOT NULL,               -- 'OLD', 'NEW'
    slab_order INTEGER NOT NULL,            -- 1, 2, 3... for ordering
    lower_limit DECIMAL(15,2) NOT NULL,     -- 0, 300000, 700000...
    upper_limit DECIMAL(15,2),              -- NULL for highest slab
    tax_rate DECIMAL(5,4) NOT NULL,         -- 0.05, 0.10, 0.15...
    effective_from DATE NOT NULL,
    effective_to DATE,                      -- NULL if current
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(financial_year, tax_regime, slab_order)
);

-- Index for fast FY lookup
CREATE INDEX idx_tax_slabs_fy ON income_tax_slabs(financial_year, tax_regime);

-- ============================================================
-- TABLE: capital_gains_rates
-- Purpose: Store CG rates by asset type, holding period, FY
-- ============================================================
CREATE TABLE capital_gains_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    financial_year TEXT NOT NULL,
    asset_type TEXT NOT NULL,               -- 'EQUITY_LISTED', 'EQUITY_MF', 'DEBT_MF',
                                            -- 'GOLD', 'REAL_ESTATE', 'FOREIGN_EQUITY', 'UNLISTED'
    gain_type TEXT NOT NULL,                -- 'STCG', 'LTCG'
    holding_period_months INTEGER NOT NULL, -- 12 for equity, 24 for others
    tax_rate DECIMAL(5,4) NOT NULL,         -- 0.15, 0.10, 0.125...
    rate_type TEXT NOT NULL,                -- 'FLAT', 'SLAB', 'INDEXED'
    exemption_limit DECIMAL(15,2),          -- 100000, 125000 for LTCG
    stt_required BOOLEAN DEFAULT FALSE,     -- STT paid required for special rate?
    effective_from DATE NOT NULL,
    effective_to DATE,
    notes TEXT,                             -- e.g., "With indexation option"
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(financial_year, asset_type, gain_type)
);

-- ============================================================
-- TABLE: standard_deductions
-- Purpose: Store standard deduction limits by category
-- ============================================================
CREATE TABLE standard_deductions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    financial_year TEXT NOT NULL,
    tax_regime TEXT NOT NULL,
    deduction_type TEXT NOT NULL,           -- 'SALARY', 'PENSION', 'FAMILY_PENSION',
                                            -- 'HOUSE_PROPERTY'
    deduction_amount DECIMAL(15,2),         -- Fixed amount (50000, 75000)
    deduction_percent DECIMAL(5,4),         -- Percentage (0.30 for house property)
    max_limit DECIMAL(15,2),                -- Cap if percentage-based
    effective_from DATE NOT NULL,
    effective_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(financial_year, tax_regime, deduction_type)
);

-- ============================================================
-- TABLE: chapter_via_limits
-- Purpose: Store Chapter VI-A deduction limits (80C, 80D, etc.)
-- ============================================================
CREATE TABLE chapter_via_limits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    financial_year TEXT NOT NULL,
    tax_regime TEXT NOT NULL,               -- Some available only in OLD regime
    section TEXT NOT NULL,                  -- '80C', '80CCC', '80CCD1', '80CCD1B',
                                            -- '80CCD2', '80D', '80E', '80G', '80TTA', '80TTB'
    max_limit DECIMAL(15,2) NOT NULL,       -- 150000 for 80C, 50000 for 80CCD1B
    combined_limit_section TEXT,            -- '80CCE' combines 80C+80CCC+80CCD1
    combined_max DECIMAL(15,2),             -- 150000 combined
    available_in_new_regime BOOLEAN DEFAULT FALSE,
    effective_from DATE NOT NULL,
    effective_to DATE,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(financial_year, tax_regime, section)
);

-- ============================================================
-- TABLE: surcharge_rates
-- Purpose: Store surcharge rates by income level
-- ============================================================
CREATE TABLE surcharge_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    financial_year TEXT NOT NULL,
    income_type TEXT NOT NULL,              -- 'NORMAL', 'EQUITY_CG' (capped at 15%)
    lower_limit DECIMAL(15,2) NOT NULL,
    upper_limit DECIMAL(15,2),
    surcharge_rate DECIMAL(5,4) NOT NULL,
    marginal_relief_applicable BOOLEAN DEFAULT TRUE,
    effective_from DATE NOT NULL,
    effective_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(financial_year, income_type, lower_limit)
);

-- ============================================================
-- TABLE: cess_rates
-- Purpose: Store Health & Education Cess rates
-- ============================================================
CREATE TABLE cess_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    financial_year TEXT NOT NULL,
    cess_type TEXT NOT NULL,                -- 'HEALTH_EDUCATION'
    rate DECIMAL(5,4) NOT NULL,             -- 0.04 (4%)
    effective_from DATE NOT NULL,
    effective_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(financial_year, cess_type)
);

-- ============================================================
-- TABLE: rebate_limits
-- Purpose: Store tax rebate limits (Section 87A)
-- ============================================================
CREATE TABLE rebate_limits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    financial_year TEXT NOT NULL,
    tax_regime TEXT NOT NULL,
    income_limit DECIMAL(15,2) NOT NULL,    -- 700000 (FY24-25), 800000 (FY25-26)
    max_rebate DECIMAL(15,2) NOT NULL,      -- 25000
    effective_from DATE NOT NULL,
    effective_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(financial_year, tax_regime)
);

-- ============================================================
-- TABLE: forex_rates
-- Purpose: Store SBI TT Buying rates for foreign income
-- ============================================================
CREATE TABLE forex_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rate_date DATE NOT NULL,
    currency_from TEXT NOT NULL,            -- 'USD', 'EUR', 'GBP'
    currency_to TEXT DEFAULT 'INR',
    tt_buying_rate DECIMAL(10,4) NOT NULL,
    source TEXT DEFAULT 'SBI',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(rate_date, currency_from, currency_to)
);
```

### 4.2 New Tables for Income Aggregation

```sql
-- ============================================================
-- TABLE: user_income_summary
-- Purpose: Aggregated income by type per user per FY
-- Populated automatically from parsed data
-- ============================================================
CREATE TABLE user_income_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    financial_year TEXT NOT NULL,

    -- Income Categories
    income_type TEXT NOT NULL,              -- 'SALARY', 'CAPITAL_GAINS', 'OTHER_SOURCES',
                                            -- 'HOUSE_PROPERTY', 'BUSINESS'
    sub_classification TEXT,                -- 'STCG', 'LTCG', 'DIVIDENDS', 'INTEREST', 'RENTAL'
    income_sub_grouping TEXT,               -- 'EQUITY_LISTED', 'EQUITY_MF', 'USA_RSU'

    -- Amounts
    gross_amount DECIMAL(15,2) NOT NULL,
    deductions DECIMAL(15,2) DEFAULT 0,
    taxable_amount DECIMAL(15,2) NOT NULL,
    tds_deducted DECIMAL(15,2) DEFAULT 0,

    -- Tax Treatment
    applicable_tax_rate_type TEXT,          -- 'SLAB', 'FLAT_15', 'FLAT_10', 'FLAT_12.5', 'INDEXED'

    -- Source Tracking
    source_table TEXT NOT NULL,             -- 'salary_records', 'stock_capital_gains', etc.
    source_ids TEXT,                        -- JSON array of source record IDs
    last_synced_at TIMESTAMP,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (user_id) REFERENCES users(id),
    UNIQUE(user_id, financial_year, income_type, sub_classification, income_sub_grouping)
);

-- Index for fast user+FY lookup
CREATE INDEX idx_income_summary_user_fy ON user_income_summary(user_id, financial_year);

-- ============================================================
-- TABLE: statement_processing_log
-- Purpose: Track which statements have been processed
-- Prevents re-parsing of already processed files
-- ============================================================
CREATE TABLE statement_processing_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    file_hash TEXT NOT NULL,                -- SHA256 hash of file content
    file_size INTEGER NOT NULL,
    file_modified_at TIMESTAMP,

    -- Processing Details
    statement_type TEXT NOT NULL,           -- 'ZERODHA_TAX_PNL', 'CAMS_CAS', 'KARVY_CG',
                                            -- 'ETRADE_GL', 'SALARY_PAYSLIP', 'FORM16'
    financial_year TEXT NOT NULL,
    parser_version TEXT NOT NULL,           -- Track parser version for re-parse if needed

    -- Status
    processing_status TEXT NOT NULL,        -- 'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED'
    records_extracted INTEGER,
    error_message TEXT,

    -- Timestamps
    first_processed_at TIMESTAMP,
    last_processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (user_id) REFERENCES users(id),
    UNIQUE(user_id, file_hash)
);

-- ============================================================
-- TABLE: advance_tax_computation
-- Purpose: Store computed advance tax for each user/FY
-- ============================================================
CREATE TABLE advance_tax_computation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    financial_year TEXT NOT NULL,
    tax_regime TEXT NOT NULL,               -- 'OLD', 'NEW'
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

    -- TDS/Advance Tax Already Paid
    tds_deducted DECIMAL(15,2) DEFAULT 0,
    advance_tax_paid DECIMAL(15,2) DEFAULT 0,
    self_assessment_tax DECIMAL(15,2) DEFAULT 0,
    balance_tax_payable DECIMAL(15,2) DEFAULT 0,

    -- Metadata
    computation_json TEXT,                  -- Full breakdown as JSON
    is_latest BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Index for latest computation lookup
CREATE INDEX idx_advance_tax_latest ON advance_tax_computation(user_id, financial_year, is_latest);
```

### 4.3 Database Views for Income Aggregation

```sql
-- ============================================================
-- VIEW: v_user_salary_summary
-- Aggregates salary data from salary_records and form16_records
-- ============================================================
CREATE VIEW v_user_salary_summary AS
SELECT
    s.user_id,
    CASE
        WHEN s.pay_date BETWEEN '2024-04-01' AND '2025-03-31' THEN '2024-25'
        WHEN s.pay_date BETWEEN '2025-04-01' AND '2026-03-31' THEN '2025-26'
        ELSE 'OTHER'
    END AS financial_year,
    'SALARY' AS income_type,
    'N/A' AS sub_classification,
    'Employer Salary' AS income_sub_grouping,
    SUM(s.gross_salary) AS gross_amount,
    SUM(s.professional_tax) AS deductions,
    SUM(s.gross_salary - s.professional_tax) AS taxable_amount,
    SUM(s.income_tax_deducted) AS tds_deducted,
    'salary_records' AS source_table
FROM salary_records s
GROUP BY s.user_id, financial_year;

-- ============================================================
-- VIEW: v_user_capital_gains_summary
-- Aggregates CG from multiple sources
-- ============================================================
CREATE VIEW v_user_capital_gains_summary AS
-- Mutual Fund Capital Gains
SELECT
    cg.user_id,
    cg.financial_year,
    'CAPITAL_GAINS' AS income_type,
    'STCG' AS sub_classification,
    'Equity Mutual Funds' AS income_sub_grouping,
    cg.stcg AS gross_amount,
    0 AS deductions,
    cg.stcg AS taxable_amount,
    0 AS tds_deducted,
    'FLAT_15' AS applicable_tax_rate_type,
    'mf_capital_gains' AS source_table
FROM mf_capital_gains cg
WHERE cg.asset_class = 'EQUITY' AND cg.stcg > 0

UNION ALL

-- Mutual Fund LTCG
SELECT
    cg.user_id,
    cg.financial_year,
    'CAPITAL_GAINS' AS income_type,
    'LTCG' AS sub_classification,
    'Equity Mutual Funds' AS income_sub_grouping,
    cg.ltcg AS gross_amount,
    cg.ltcg_exemption AS deductions,
    cg.ltcg - cg.ltcg_exemption AS taxable_amount,
    0 AS tds_deducted,
    'FLAT_10' AS applicable_tax_rate_type,
    'mf_capital_gains' AS source_table
FROM mf_capital_gains cg
WHERE cg.asset_class = 'EQUITY' AND cg.ltcg > 0

UNION ALL

-- Stock Capital Gains (Delivery)
SELECT
    cg.user_id,
    cg.financial_year,
    'CAPITAL_GAINS' AS income_type,
    CASE WHEN cg.trade_category = 'DELIVERY' AND cg.is_long_term = 1 THEN 'LTCG' ELSE 'STCG' END,
    'Indian Listed Equity' AS income_sub_grouping,
    cg.total_gain AS gross_amount,
    CASE WHEN cg.is_long_term = 1 THEN 100000 ELSE 0 END AS deductions,
    CASE WHEN cg.is_long_term = 1 THEN cg.total_gain - 100000 ELSE cg.total_gain END AS taxable_amount,
    0 AS tds_deducted,
    CASE WHEN cg.is_long_term = 1 THEN 'FLAT_10' ELSE 'FLAT_15' END,
    'stock_capital_gains' AS source_table
FROM stock_capital_gains cg
WHERE cg.trade_category = 'DELIVERY'

UNION ALL

-- Stock Capital Gains (Intraday - Speculative)
SELECT
    cg.user_id,
    cg.financial_year,
    'CAPITAL_GAINS' AS income_type,
    'SPECULATIVE' AS sub_classification,
    'Intraday Trading' AS income_sub_grouping,
    cg.total_gain AS gross_amount,
    0 AS deductions,
    cg.total_gain AS taxable_amount,
    0 AS tds_deducted,
    'SLAB' AS applicable_tax_rate_type,
    'stock_capital_gains' AS source_table
FROM stock_capital_gains cg
WHERE cg.trade_category = 'INTRADAY'

UNION ALL

-- Foreign Stock (RSU) Sales
SELECT
    rs.user_id,
    CASE
        WHEN rs.sale_date BETWEEN '2024-04-01' AND '2025-03-31' THEN '2024-25'
        WHEN rs.sale_date BETWEEN '2025-04-01' AND '2026-03-31' THEN '2025-26'
    END AS financial_year,
    'CAPITAL_GAINS' AS income_type,
    CASE WHEN rs.is_ltcg = 1 THEN 'LTCG' ELSE 'STCG' END AS sub_classification,
    'USA Stocks (RSU/ESPP)' AS income_sub_grouping,
    rs.capital_gain_inr AS gross_amount,
    0 AS deductions,
    rs.capital_gain_inr AS taxable_amount,
    0 AS tds_deducted,
    'SLAB' AS applicable_tax_rate_type,  -- Foreign CG at slab rates
    'rsu_sales' AS source_table
FROM rsu_sales rs;

-- ============================================================
-- VIEW: v_user_other_income_summary
-- Aggregates dividends, interest, rental income
-- ============================================================
CREATE VIEW v_user_other_income_summary AS
-- Indian Dividends
SELECT
    ds.user_id,
    ds.financial_year,
    'OTHER_SOURCES' AS income_type,
    'DIVIDENDS' AS sub_classification,
    'Indian Equity Dividends' AS income_sub_grouping,
    ds.total_dividend AS gross_amount,
    0 AS deductions,
    ds.total_dividend AS taxable_amount,
    ds.tds_deducted AS tds_deducted,
    'SLAB' AS applicable_tax_rate_type,
    'stock_dividend_summary' AS source_table
FROM stock_dividend_summary ds

UNION ALL

-- Foreign Dividends
SELECT
    fd.user_id,
    CASE
        WHEN fd.dividend_date BETWEEN '2024-04-01' AND '2025-03-31' THEN '2024-25'
        WHEN fd.dividend_date BETWEEN '2025-04-01' AND '2026-03-31' THEN '2025-26'
    END AS financial_year,
    'OTHER_SOURCES' AS income_type,
    'DIVIDENDS' AS sub_classification,
    'Foreign Dividends (USA)' AS income_sub_grouping,
    SUM(fd.gross_amount_inr) AS gross_amount,
    0 AS deductions,
    SUM(fd.gross_amount_inr) AS taxable_amount,
    SUM(fd.withholding_tax_inr) AS tds_deducted,  -- FTC eligible
    'SLAB' AS applicable_tax_rate_type,
    'foreign_dividends' AS source_table
FROM foreign_dividends fd
GROUP BY fd.user_id, financial_year

UNION ALL

-- Bank Interest
SELECT
    bi.user_id,
    bi.financial_year,
    'OTHER_SOURCES' AS income_type,
    'INTEREST' AS sub_classification,
    'Savings Bank Interest' AS income_sub_grouping,
    bi.total_interest AS gross_amount,
    LEAST(bi.interest_80tta_eligible, 10000) AS deductions,  -- 80TTA limit
    bi.total_interest - LEAST(bi.interest_80tta_eligible, 10000) AS taxable_amount,
    bi.tds_deducted AS tds_deducted,
    'SLAB' AS applicable_tax_rate_type,
    'bank_interest_summary' AS source_table
FROM bank_interest_summary bi;
```

---

## 5. Implementation Changes

### 5.1 New Module: Tax Rules Service

```python
# src/pfas/services/tax_rules_service.py

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
from pfas.core.database import DatabaseManager


@dataclass
class TaxSlab:
    """Tax slab configuration loaded from database."""
    lower_limit: Decimal
    upper_limit: Optional[Decimal]
    tax_rate: Decimal


@dataclass
class CapitalGainsRate:
    """Capital gains rate configuration."""
    asset_type: str
    gain_type: str  # STCG or LTCG
    holding_period_months: int
    tax_rate: Decimal
    rate_type: str  # FLAT, SLAB, INDEXED
    exemption_limit: Decimal


class TaxRulesService:
    """
    Service to fetch tax rules from database.
    Eliminates hardcoded tax rates in application code.
    """

    def __init__(self, db: DatabaseManager):
        self.db = db
        self._cache = {}  # Simple in-memory cache

    def get_tax_slabs(
        self,
        financial_year: str,
        tax_regime: str = 'NEW'
    ) -> list[TaxSlab]:
        """Fetch tax slabs for given FY and regime."""
        cache_key = f"slabs_{financial_year}_{tax_regime}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        query = """
            SELECT lower_limit, upper_limit, tax_rate
            FROM income_tax_slabs
            WHERE financial_year = ?
              AND tax_regime = ?
              AND (effective_to IS NULL OR effective_to >= date('now'))
            ORDER BY slab_order
        """
        rows = self.db.execute(query, (financial_year, tax_regime))

        slabs = [
            TaxSlab(
                lower_limit=Decimal(str(row['lower_limit'])),
                upper_limit=Decimal(str(row['upper_limit'])) if row['upper_limit'] else None,
                tax_rate=Decimal(str(row['tax_rate']))
            )
            for row in rows
        ]

        self._cache[cache_key] = slabs
        return slabs

    def get_capital_gains_rate(
        self,
        financial_year: str,
        asset_type: str,
        gain_type: str  # 'STCG' or 'LTCG'
    ) -> CapitalGainsRate:
        """Fetch CG rate for specific asset type."""
        query = """
            SELECT asset_type, gain_type, holding_period_months,
                   tax_rate, rate_type, exemption_limit
            FROM capital_gains_rates
            WHERE financial_year = ?
              AND asset_type = ?
              AND gain_type = ?
              AND (effective_to IS NULL OR effective_to >= date('now'))
        """
        row = self.db.execute_one(query, (financial_year, asset_type, gain_type))

        if not row:
            raise ValueError(f"No CG rate found for {asset_type}/{gain_type} in {financial_year}")

        return CapitalGainsRate(
            asset_type=row['asset_type'],
            gain_type=row['gain_type'],
            holding_period_months=row['holding_period_months'],
            tax_rate=Decimal(str(row['tax_rate'])),
            rate_type=row['rate_type'],
            exemption_limit=Decimal(str(row['exemption_limit'] or 0))
        )

    def get_standard_deduction(
        self,
        financial_year: str,
        tax_regime: str,
        deduction_type: str = 'SALARY'
    ) -> Decimal:
        """Fetch standard deduction amount."""
        query = """
            SELECT deduction_amount
            FROM standard_deductions
            WHERE financial_year = ?
              AND tax_regime = ?
              AND deduction_type = ?
        """
        row = self.db.execute_one(query, (financial_year, tax_regime, deduction_type))
        return Decimal(str(row['deduction_amount'])) if row else Decimal('0')

    def get_surcharge_rate(
        self,
        financial_year: str,
        total_income: Decimal,
        income_type: str = 'NORMAL'
    ) -> Decimal:
        """Fetch applicable surcharge rate based on income."""
        query = """
            SELECT surcharge_rate
            FROM surcharge_rates
            WHERE financial_year = ?
              AND income_type = ?
              AND lower_limit <= ?
              AND (upper_limit IS NULL OR upper_limit >= ?)
            ORDER BY lower_limit DESC
            LIMIT 1
        """
        row = self.db.execute_one(
            query,
            (financial_year, income_type, float(total_income), float(total_income))
        )
        return Decimal(str(row['surcharge_rate'])) if row else Decimal('0')

    def get_cess_rate(self, financial_year: str) -> Decimal:
        """Fetch Health & Education Cess rate."""
        query = """
            SELECT rate
            FROM cess_rates
            WHERE financial_year = ?
              AND cess_type = 'HEALTH_EDUCATION'
        """
        row = self.db.execute_one(query, (financial_year,))
        return Decimal(str(row['rate'])) if row else Decimal('0.04')

    def get_rebate_limit(
        self,
        financial_year: str,
        tax_regime: str
    ) -> tuple[Decimal, Decimal]:
        """Fetch Section 87A rebate limits (income_limit, max_rebate)."""
        query = """
            SELECT income_limit, max_rebate
            FROM rebate_limits
            WHERE financial_year = ?
              AND tax_regime = ?
        """
        row = self.db.execute_one(query, (financial_year, tax_regime))
        if row:
            return (
                Decimal(str(row['income_limit'])),
                Decimal(str(row['max_rebate']))
            )
        return (Decimal('0'), Decimal('0'))
```

### 5.2 New Module: Income Aggregation Service

```python
# src/pfas/services/income_aggregation_service.py

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
from pfas.core.database import DatabaseManager


@dataclass
class IncomeRecord:
    """Aggregated income record from database."""
    income_type: str
    sub_classification: str
    income_sub_grouping: str
    gross_amount: Decimal
    deductions: Decimal
    taxable_amount: Decimal
    tds_deducted: Decimal
    applicable_tax_rate_type: str
    source_table: str


class IncomeAggregationService:
    """
    Service to aggregate income from parsed database records.
    Eliminates need to re-parse statement files.
    """

    def __init__(self, db: DatabaseManager):
        self.db = db

    def get_user_income_summary(
        self,
        user_id: int,
        financial_year: str
    ) -> list[IncomeRecord]:
        """
        Fetch all income records for a user from the database.
        Uses pre-aggregated data - no file parsing needed.
        """
        # First check if we have pre-computed summary
        records = self._get_from_summary_table(user_id, financial_year)

        if not records:
            # Fall back to real-time aggregation from views
            records = self._aggregate_from_views(user_id, financial_year)

        return records

    def _get_from_summary_table(
        self,
        user_id: int,
        financial_year: str
    ) -> list[IncomeRecord]:
        """Fetch from pre-computed summary table."""
        query = """
            SELECT income_type, sub_classification, income_sub_grouping,
                   gross_amount, deductions, taxable_amount, tds_deducted,
                   applicable_tax_rate_type, source_table
            FROM user_income_summary
            WHERE user_id = ? AND financial_year = ?
            ORDER BY income_type, sub_classification
        """
        rows = self.db.execute(query, (user_id, financial_year))
        return [self._row_to_record(row) for row in rows]

    def _aggregate_from_views(
        self,
        user_id: int,
        financial_year: str
    ) -> list[IncomeRecord]:
        """Real-time aggregation from database views."""
        records = []

        # Aggregate from each view
        views = [
            'v_user_salary_summary',
            'v_user_capital_gains_summary',
            'v_user_other_income_summary'
        ]

        for view in views:
            query = f"""
                SELECT income_type, sub_classification, income_sub_grouping,
                       gross_amount, deductions, taxable_amount, tds_deducted,
                       COALESCE(applicable_tax_rate_type, 'SLAB') as applicable_tax_rate_type,
                       source_table
                FROM {view}
                WHERE user_id = ? AND financial_year = ?
            """
            rows = self.db.execute(query, (user_id, financial_year))
            records.extend([self._row_to_record(row) for row in rows])

        return records

    def refresh_summary(self, user_id: int, financial_year: str) -> int:
        """
        Refresh the user_income_summary table from source tables.
        Called after new statements are parsed.
        Returns number of records updated.
        """
        # Delete existing summary records
        self.db.execute(
            "DELETE FROM user_income_summary WHERE user_id = ? AND financial_year = ?",
            (user_id, financial_year)
        )

        # Re-aggregate from views and insert
        records = self._aggregate_from_views(user_id, financial_year)

        insert_query = """
            INSERT INTO user_income_summary (
                user_id, financial_year, income_type, sub_classification,
                income_sub_grouping, gross_amount, deductions, taxable_amount,
                tds_deducted, applicable_tax_rate_type, source_table, last_synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """

        for r in records:
            self.db.execute(insert_query, (
                user_id, financial_year, r.income_type, r.sub_classification,
                r.income_sub_grouping, float(r.gross_amount), float(r.deductions),
                float(r.taxable_amount), float(r.tds_deducted),
                r.applicable_tax_rate_type, r.source_table
            ))

        return len(records)

    def _row_to_record(self, row: dict) -> IncomeRecord:
        """Convert database row to IncomeRecord."""
        return IncomeRecord(
            income_type=row['income_type'],
            sub_classification=row['sub_classification'] or 'N/A',
            income_sub_grouping=row['income_sub_grouping'] or 'N/A',
            gross_amount=Decimal(str(row['gross_amount'] or 0)),
            deductions=Decimal(str(row['deductions'] or 0)),
            taxable_amount=Decimal(str(row['taxable_amount'] or 0)),
            tds_deducted=Decimal(str(row['tds_deducted'] or 0)),
            applicable_tax_rate_type=row['applicable_tax_rate_type'] or 'SLAB',
            source_table=row['source_table']
        )
```

### 5.3 Updated Advance Tax Calculator (Database-Driven)

```python
# src/pfas/services/advance_tax_calculator.py

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional
from pfas.core.database import DatabaseManager
from pfas.services.tax_rules_service import TaxRulesService
from pfas.services.income_aggregation_service import IncomeAggregationService, IncomeRecord


@dataclass
class AdvanceTaxResult:
    """Complete advance tax computation result."""
    user_id: int
    financial_year: str
    tax_regime: str

    # Income breakdown
    income_items: list[IncomeRecord] = field(default_factory=list)

    # Totals by category
    total_salary_income: Decimal = Decimal('0')
    total_stcg_equity: Decimal = Decimal('0')
    total_ltcg_equity: Decimal = Decimal('0')
    total_capital_gains_slab: Decimal = Decimal('0')
    total_other_income: Decimal = Decimal('0')
    total_house_property: Decimal = Decimal('0')

    # Aggregates
    gross_total_income: Decimal = Decimal('0')
    total_deductions: Decimal = Decimal('0')
    taxable_income: Decimal = Decimal('0')

    # Tax computation
    tax_on_slab_income: Decimal = Decimal('0')
    tax_on_stcg_equity: Decimal = Decimal('0')
    tax_on_ltcg_equity: Decimal = Decimal('0')
    total_tax_before_cess: Decimal = Decimal('0')

    surcharge_rate: Decimal = Decimal('0')
    surcharge_amount: Decimal = Decimal('0')
    cess_rate: Decimal = Decimal('0.04')
    cess_amount: Decimal = Decimal('0')

    total_tax_liability: Decimal = Decimal('0')

    # Credits
    tds_deducted: Decimal = Decimal('0')
    advance_tax_paid: Decimal = Decimal('0')
    balance_payable: Decimal = Decimal('0')


class AdvanceTaxCalculator:
    """
    Data-driven advance tax calculator.
    All tax rules fetched from database - no hardcoded rates.
    """

    def __init__(self, db: DatabaseManager):
        self.db = db
        self.tax_rules = TaxRulesService(db)
        self.income_service = IncomeAggregationService(db)

    def calculate(
        self,
        user_id: int,
        financial_year: str,
        tax_regime: str = 'NEW'
    ) -> AdvanceTaxResult:
        """
        Calculate advance tax for a user.

        Flow:
        1. Fetch aggregated income from database (no file parsing)
        2. Fetch tax rules from database (no hardcoded rates)
        3. Apply tax calculations
        4. Return result
        """
        result = AdvanceTaxResult(
            user_id=user_id,
            financial_year=financial_year,
            tax_regime=tax_regime
        )

        # Step 1: Get income from database
        income_records = self.income_service.get_user_income_summary(
            user_id, financial_year
        )
        result.income_items = income_records

        # Step 2: Categorize income
        self._categorize_income(result, income_records)

        # Step 3: Calculate deductions
        self._apply_deductions(result, financial_year, tax_regime)

        # Step 4: Calculate tax
        self._calculate_tax(result, financial_year, tax_regime)

        # Step 5: Store computation in database
        self._store_computation(result)

        return result

    def _categorize_income(
        self,
        result: AdvanceTaxResult,
        records: list[IncomeRecord]
    ):
        """Categorize income records into tax buckets."""
        for r in records:
            if r.income_type == 'SALARY':
                result.total_salary_income += r.taxable_amount

            elif r.income_type == 'CAPITAL_GAINS':
                if r.sub_classification == 'STCG':
                    if r.applicable_tax_rate_type in ('FLAT_15', 'FLAT_20'):
                        result.total_stcg_equity += r.taxable_amount
                    else:
                        result.total_capital_gains_slab += r.taxable_amount

                elif r.sub_classification == 'LTCG':
                    if r.applicable_tax_rate_type in ('FLAT_10', 'FLAT_12.5'):
                        result.total_ltcg_equity += r.taxable_amount
                    else:
                        result.total_capital_gains_slab += r.taxable_amount

                else:  # SPECULATIVE, etc.
                    result.total_capital_gains_slab += r.taxable_amount

            elif r.income_type == 'OTHER_SOURCES':
                result.total_other_income += r.taxable_amount

            elif r.income_type == 'HOUSE_PROPERTY':
                result.total_house_property += r.taxable_amount

            # Accumulate TDS
            result.tds_deducted += r.tds_deducted

        # Calculate gross total
        result.gross_total_income = (
            result.total_salary_income +
            result.total_stcg_equity +
            result.total_ltcg_equity +
            result.total_capital_gains_slab +
            result.total_other_income +
            result.total_house_property
        )

    def _apply_deductions(
        self,
        result: AdvanceTaxResult,
        financial_year: str,
        tax_regime: str
    ):
        """Apply standard deductions from database."""
        if result.total_salary_income > 0:
            std_deduction = self.tax_rules.get_standard_deduction(
                financial_year, tax_regime, 'SALARY'
            )
            result.total_deductions += std_deduction

        result.taxable_income = max(
            Decimal('0'),
            result.gross_total_income - result.total_deductions
        )

    def _calculate_tax(
        self,
        result: AdvanceTaxResult,
        financial_year: str,
        tax_regime: str
    ):
        """Calculate tax using database rules."""
        # Get tax slabs
        slabs = self.tax_rules.get_tax_slabs(financial_year, tax_regime)

        # Calculate slab income (excluding special rate CG)
        slab_income = (
            result.total_salary_income +
            result.total_capital_gains_slab +
            result.total_other_income +
            result.total_house_property -
            result.total_deductions
        )
        slab_income = max(Decimal('0'), slab_income)

        # Tax on slab income
        result.tax_on_slab_income = self._calculate_slab_tax(slab_income, slabs)

        # Tax on STCG equity
        if result.total_stcg_equity > 0:
            stcg_rate = self.tax_rules.get_capital_gains_rate(
                financial_year, 'EQUITY_LISTED', 'STCG'
            )
            result.tax_on_stcg_equity = (
                result.total_stcg_equity * stcg_rate.tax_rate
            ).quantize(Decimal('1'), rounding=ROUND_HALF_UP)

        # Tax on LTCG equity (after exemption)
        if result.total_ltcg_equity > 0:
            ltcg_rate = self.tax_rules.get_capital_gains_rate(
                financial_year, 'EQUITY_LISTED', 'LTCG'
            )
            ltcg_taxable = max(
                Decimal('0'),
                result.total_ltcg_equity - ltcg_rate.exemption_limit
            )
            result.tax_on_ltcg_equity = (
                ltcg_taxable * ltcg_rate.tax_rate
            ).quantize(Decimal('1'), rounding=ROUND_HALF_UP)

        # Total before cess
        result.total_tax_before_cess = (
            result.tax_on_slab_income +
            result.tax_on_stcg_equity +
            result.tax_on_ltcg_equity
        )

        # Apply rebate if eligible
        rebate_limit, max_rebate = self.tax_rules.get_rebate_limit(
            financial_year, tax_regime
        )
        if result.taxable_income <= rebate_limit:
            rebate = min(result.total_tax_before_cess, max_rebate)
            result.total_tax_before_cess -= rebate

        # Surcharge
        result.surcharge_rate = self.tax_rules.get_surcharge_rate(
            financial_year, result.gross_total_income
        )
        result.surcharge_amount = (
            result.total_tax_before_cess * result.surcharge_rate
        ).quantize(Decimal('1'), rounding=ROUND_HALF_UP)

        tax_with_surcharge = result.total_tax_before_cess + result.surcharge_amount

        # Cess
        result.cess_rate = self.tax_rules.get_cess_rate(financial_year)
        result.cess_amount = (
            tax_with_surcharge * result.cess_rate
        ).quantize(Decimal('1'), rounding=ROUND_HALF_UP)

        # Total tax liability
        result.total_tax_liability = tax_with_surcharge + result.cess_amount

        # Balance payable
        result.balance_payable = max(
            Decimal('0'),
            result.total_tax_liability - result.tds_deducted - result.advance_tax_paid
        )

    def _calculate_slab_tax(
        self,
        income: Decimal,
        slabs: list
    ) -> Decimal:
        """Calculate tax based on slabs."""
        tax = Decimal('0')
        remaining = income

        for slab in slabs:
            if remaining <= 0:
                break

            slab_size = (slab.upper_limit - slab.lower_limit) if slab.upper_limit else remaining
            taxable_in_slab = min(remaining, slab_size)

            if income > slab.lower_limit:
                applicable = min(taxable_in_slab, income - slab.lower_limit)
                if applicable > 0:
                    tax += applicable * slab.tax_rate
                remaining -= slab_size

        return tax.quantize(Decimal('1'), rounding=ROUND_HALF_UP)

    def _store_computation(self, result: AdvanceTaxResult):
        """Store computation result in database."""
        # Mark previous computations as not latest
        self.db.execute("""
            UPDATE advance_tax_computation
            SET is_latest = FALSE
            WHERE user_id = ? AND financial_year = ?
        """, (result.user_id, result.financial_year))

        # Insert new computation
        self.db.execute("""
            INSERT INTO advance_tax_computation (
                user_id, financial_year, tax_regime, computation_date,
                total_salary_income, total_stcg_equity, total_ltcg_equity,
                total_capital_gains_slab, total_other_income, total_house_property,
                gross_total_income, total_deductions, taxable_income,
                tax_on_slab_income, tax_on_stcg_equity, tax_on_ltcg_equity,
                total_tax_before_cess, surcharge_rate, surcharge_amount,
                cess_rate, cess_amount, total_tax_liability,
                tds_deducted, advance_tax_paid, balance_tax_payable, is_latest
            ) VALUES (?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
        """, (
            result.user_id, result.financial_year, result.tax_regime,
            float(result.total_salary_income), float(result.total_stcg_equity),
            float(result.total_ltcg_equity), float(result.total_capital_gains_slab),
            float(result.total_other_income), float(result.total_house_property),
            float(result.gross_total_income), float(result.total_deductions),
            float(result.taxable_income), float(result.tax_on_slab_income),
            float(result.tax_on_stcg_equity), float(result.tax_on_ltcg_equity),
            float(result.total_tax_before_cess), float(result.surcharge_rate),
            float(result.surcharge_amount), float(result.cess_rate),
            float(result.cess_amount), float(result.total_tax_liability),
            float(result.tds_deducted), float(result.advance_tax_paid),
            float(result.balance_payable)
        ))
```

### 5.4 Statement Processing Tracker (Avoid Re-parsing)

```python
# src/pfas/services/statement_tracker.py

import hashlib
from pathlib import Path
from datetime import datetime
from pfas.core.database import DatabaseManager


class StatementTracker:
    """
    Tracks which statements have been processed.
    Prevents re-parsing of already processed files.
    """

    def __init__(self, db: DatabaseManager):
        self.db = db

    def is_processed(self, user_id: int, file_path: Path) -> bool:
        """Check if a file has already been processed."""
        file_hash = self._calculate_hash(file_path)

        row = self.db.execute_one("""
            SELECT id, processing_status
            FROM statement_processing_log
            WHERE user_id = ? AND file_hash = ?
        """, (user_id, file_hash))

        return row is not None and row['processing_status'] == 'COMPLETED'

    def needs_reprocessing(
        self,
        user_id: int,
        file_path: Path,
        parser_version: str
    ) -> bool:
        """
        Check if file needs reprocessing due to:
        - File content changed
        - Parser version updated
        """
        file_hash = self._calculate_hash(file_path)

        row = self.db.execute_one("""
            SELECT file_hash, parser_version, processing_status
            FROM statement_processing_log
            WHERE user_id = ? AND file_path = ?
        """, (user_id, str(file_path)))

        if not row:
            return True  # Never processed

        if row['file_hash'] != file_hash:
            return True  # File changed

        if row['parser_version'] != parser_version:
            return True  # Parser updated

        if row['processing_status'] != 'COMPLETED':
            return True  # Previous processing failed

        return False

    def mark_processing(
        self,
        user_id: int,
        file_path: Path,
        statement_type: str,
        financial_year: str,
        parser_version: str
    ) -> int:
        """Mark file as being processed."""
        file_hash = self._calculate_hash(file_path)
        file_stat = file_path.stat()

        # Upsert
        self.db.execute("""
            INSERT INTO statement_processing_log (
                user_id, file_path, file_hash, file_size, file_modified_at,
                statement_type, financial_year, parser_version,
                processing_status, first_processed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PROCESSING', datetime('now'))
            ON CONFLICT(user_id, file_hash) DO UPDATE SET
                processing_status = 'PROCESSING',
                parser_version = excluded.parser_version,
                last_processed_at = datetime('now')
        """, (
            user_id, str(file_path), file_hash, file_stat.st_size,
            datetime.fromtimestamp(file_stat.st_mtime),
            statement_type, financial_year, parser_version
        ))

        row = self.db.execute_one(
            "SELECT id FROM statement_processing_log WHERE file_hash = ?",
            (file_hash,)
        )
        return row['id']

    def mark_completed(
        self,
        log_id: int,
        records_extracted: int
    ):
        """Mark processing as completed."""
        self.db.execute("""
            UPDATE statement_processing_log
            SET processing_status = 'COMPLETED',
                records_extracted = ?,
                last_processed_at = datetime('now')
            WHERE id = ?
        """, (records_extracted, log_id))

    def mark_failed(self, log_id: int, error_message: str):
        """Mark processing as failed."""
        self.db.execute("""
            UPDATE statement_processing_log
            SET processing_status = 'FAILED',
                error_message = ?,
                last_processed_at = datetime('now')
            WHERE id = ?
        """, (error_message, log_id))

    def _calculate_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file content."""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()
```

---

## 6. Data Migration & Seeding

### 6.1 Tax Rules Seed Data

```sql
-- ============================================================
-- SEED DATA: FY 2024-25 Tax Rules (New Regime)
-- ============================================================

-- Income Tax Slabs - New Regime FY 2024-25
INSERT INTO income_tax_slabs (financial_year, tax_regime, slab_order, lower_limit, upper_limit, tax_rate, effective_from) VALUES
('2024-25', 'NEW', 1, 0, 300000, 0.00, '2024-04-01'),
('2024-25', 'NEW', 2, 300000, 700000, 0.05, '2024-04-01'),
('2024-25', 'NEW', 3, 700000, 1000000, 0.10, '2024-04-01'),
('2024-25', 'NEW', 4, 1000000, 1200000, 0.15, '2024-04-01'),
('2024-25', 'NEW', 5, 1200000, 1500000, 0.20, '2024-04-01'),
('2024-25', 'NEW', 6, 1500000, NULL, 0.30, '2024-04-01');

-- Income Tax Slabs - New Regime FY 2025-26
INSERT INTO income_tax_slabs (financial_year, tax_regime, slab_order, lower_limit, upper_limit, tax_rate, effective_from) VALUES
('2025-26', 'NEW', 1, 0, 400000, 0.00, '2025-04-01'),
('2025-26', 'NEW', 2, 400000, 800000, 0.05, '2025-04-01'),
('2025-26', 'NEW', 3, 800000, 1200000, 0.10, '2025-04-01'),
('2025-26', 'NEW', 4, 1200000, 1600000, 0.15, '2025-04-01'),
('2025-26', 'NEW', 5, 1600000, 2000000, 0.20, '2025-04-01'),
('2025-26', 'NEW', 6, 2000000, 2400000, 0.25, '2025-04-01'),
('2025-26', 'NEW', 7, 2400000, NULL, 0.30, '2025-04-01');

-- Capital Gains Rates
INSERT INTO capital_gains_rates (financial_year, asset_type, gain_type, holding_period_months, tax_rate, rate_type, exemption_limit, stt_required, effective_from) VALUES
-- FY 2024-25
('2024-25', 'EQUITY_LISTED', 'STCG', 12, 0.15, 'FLAT', NULL, TRUE, '2024-04-01'),
('2024-25', 'EQUITY_LISTED', 'LTCG', 12, 0.10, 'FLAT', 100000, TRUE, '2024-04-01'),
('2024-25', 'EQUITY_MF', 'STCG', 12, 0.15, 'FLAT', NULL, TRUE, '2024-04-01'),
('2024-25', 'EQUITY_MF', 'LTCG', 12, 0.10, 'FLAT', 100000, TRUE, '2024-04-01'),
('2024-25', 'DEBT_MF', 'STCG', 36, 0.00, 'SLAB', NULL, FALSE, '2024-04-01'),
('2024-25', 'DEBT_MF', 'LTCG', 36, 0.20, 'INDEXED', NULL, FALSE, '2024-04-01'),
('2024-25', 'FOREIGN_EQUITY', 'STCG', 24, 0.00, 'SLAB', NULL, FALSE, '2024-04-01'),
('2024-25', 'FOREIGN_EQUITY', 'LTCG', 24, 0.20, 'INDEXED', NULL, FALSE, '2024-04-01'),
('2024-25', 'REAL_ESTATE', 'STCG', 24, 0.00, 'SLAB', NULL, FALSE, '2024-04-01'),
('2024-25', 'REAL_ESTATE', 'LTCG', 24, 0.20, 'INDEXED', NULL, FALSE, '2024-04-01'),
-- FY 2025-26 (Changed rates)
('2025-26', 'EQUITY_LISTED', 'STCG', 12, 0.20, 'FLAT', NULL, TRUE, '2025-04-01'),
('2025-26', 'EQUITY_LISTED', 'LTCG', 12, 0.125, 'FLAT', 125000, TRUE, '2025-04-01'),
('2025-26', 'EQUITY_MF', 'STCG', 12, 0.20, 'FLAT', NULL, TRUE, '2025-04-01'),
('2025-26', 'EQUITY_MF', 'LTCG', 12, 0.125, 'FLAT', 125000, TRUE, '2025-04-01'),
('2025-26', 'FOREIGN_EQUITY', 'STCG', 24, 0.00, 'SLAB', NULL, FALSE, '2025-04-01'),
('2025-26', 'FOREIGN_EQUITY', 'LTCG', 24, 0.125, 'FLAT', NULL, FALSE, '2025-04-01');

-- Standard Deductions
INSERT INTO standard_deductions (financial_year, tax_regime, deduction_type, deduction_amount, effective_from) VALUES
('2024-25', 'NEW', 'SALARY', 50000, '2024-04-01'),
('2024-25', 'OLD', 'SALARY', 50000, '2024-04-01'),
('2025-26', 'NEW', 'SALARY', 75000, '2025-04-01'),
('2025-26', 'OLD', 'SALARY', 50000, '2025-04-01');

INSERT INTO standard_deductions (financial_year, tax_regime, deduction_type, deduction_percent, max_limit, effective_from) VALUES
('2024-25', 'NEW', 'HOUSE_PROPERTY', 0.30, NULL, '2024-04-01'),
('2024-25', 'OLD', 'HOUSE_PROPERTY', 0.30, NULL, '2024-04-01'),
('2025-26', 'NEW', 'HOUSE_PROPERTY', 0.30, NULL, '2025-04-01');

-- Surcharge Rates
INSERT INTO surcharge_rates (financial_year, income_type, lower_limit, upper_limit, surcharge_rate, effective_from) VALUES
('2024-25', 'NORMAL', 0, 5000000, 0.00, '2024-04-01'),
('2024-25', 'NORMAL', 5000000, 10000000, 0.10, '2024-04-01'),
('2024-25', 'NORMAL', 10000000, 20000000, 0.15, '2024-04-01'),
('2024-25', 'NORMAL', 20000000, 50000000, 0.25, '2024-04-01'),
('2024-25', 'NORMAL', 50000000, NULL, 0.37, '2024-04-01'),
('2024-25', 'EQUITY_CG', 0, NULL, 0.15, '2024-04-01'),  -- Capped at 15%
('2025-26', 'NORMAL', 0, 5000000, 0.00, '2025-04-01'),
('2025-26', 'NORMAL', 5000000, 10000000, 0.10, '2025-04-01'),
('2025-26', 'NORMAL', 10000000, 20000000, 0.15, '2025-04-01'),
('2025-26', 'NORMAL', 20000000, 50000000, 0.25, '2025-04-01'),
('2025-26', 'NORMAL', 50000000, NULL, 0.37, '2025-04-01');

-- Cess Rates
INSERT INTO cess_rates (financial_year, cess_type, rate, effective_from) VALUES
('2024-25', 'HEALTH_EDUCATION', 0.04, '2024-04-01'),
('2025-26', 'HEALTH_EDUCATION', 0.04, '2025-04-01');

-- Rebate Limits (Section 87A)
INSERT INTO rebate_limits (financial_year, tax_regime, income_limit, max_rebate, effective_from) VALUES
('2024-25', 'NEW', 700000, 25000, '2024-04-01'),
('2024-25', 'OLD', 500000, 12500, '2024-04-01'),
('2025-26', 'NEW', 800000, 25000, '2025-04-01'),
('2025-26', 'OLD', 500000, 12500, '2025-04-01');
```

---

## 7. Usage Flow (After Implementation)

### 7.1 One-Time Setup (Per User)

```python
from pfas.core.database import DatabaseManager
from pfas.services.statement_tracker import StatementTracker

db = DatabaseManager()
tracker = StatementTracker(db)

# Process all statements once (stored in database)
for statement_file in user_statement_files:
    if not tracker.is_processed(user_id, statement_file):
        # Parse and store in database
        parser.parse_and_store(statement_file, user_id)
```

### 7.2 Advance Tax Report Generation (Subsequent)

```python
from pfas.services.advance_tax_calculator import AdvanceTaxCalculator
from pfas.reports.advance_tax_report import AdvanceTaxReportGenerator

db = DatabaseManager()
calculator = AdvanceTaxCalculator(db)

# No file parsing - reads from database
result = calculator.calculate(
    user_id=sanjay_user_id,
    financial_year='2024-25',
    tax_regime='NEW'
)

# Generate Excel report
generator = AdvanceTaxReportGenerator(db, output_path)
report_file = generator.generate_from_result(result)
```

### 7.3 When New Statement Arrives

```python
tracker = StatementTracker(db)
aggregator = IncomeAggregationService(db)

# Check if new file needs processing
if tracker.needs_reprocessing(user_id, new_file, PARSER_VERSION):
    # Parse new file only
    log_id = tracker.mark_processing(user_id, new_file, 'ZERODHA_TAX_PNL', '2024-25', PARSER_VERSION)
    try:
        records = zerodha_parser.parse(new_file)
        store_in_database(records)
        tracker.mark_completed(log_id, len(records))

        # Refresh income summary
        aggregator.refresh_summary(user_id, '2024-25')
    except Exception as e:
        tracker.mark_failed(log_id, str(e))
```

### 7.4 When Tax Rules Change (New FY)

```sql
-- DBA/Admin adds new FY rules to database
-- No code changes needed!

INSERT INTO income_tax_slabs (financial_year, tax_regime, slab_order, lower_limit, upper_limit, tax_rate, effective_from) VALUES
('2026-27', 'NEW', 1, 0, 500000, 0.00, '2026-04-01'),
('2026-27', 'NEW', 2, 500000, 900000, 0.05, '2026-04-01'),
-- ... more slabs

INSERT INTO capital_gains_rates (financial_year, asset_type, gain_type, holding_period_months, tax_rate, rate_type, exemption_limit, effective_from) VALUES
('2026-27', 'EQUITY_LISTED', 'STCG', 12, 0.20, 'FLAT', NULL, '2026-04-01'),
-- ... more rates
```

---

## 8. Benefits Summary

| Aspect | Before | After |
|--------|--------|-------|
| **File Parsing** | Every report generation | Once per file |
| **Tax Rate Updates** | Code change + deployment | Database INSERT |
| **New FY Support** | New Python code | SQL seed data |
| **Multi-User** | Folder-based | Database with user_id |
| **Income Tracking** | Scattered in files | Centralized views |
| **Historical Data** | Re-parse old files | Database lookup |
| **Scalability** | File system dependent | Database indexed |
| **Audit Trail** | None | Full computation history |

---

## 9. Implementation Phases

### Phase 1: Database Schema (Week 1)
- Create tax rules tables
- Create income summary tables
- Create statement tracking table
- Seed FY 2024-25 and 2025-26 rules

### Phase 2: Services Layer (Week 2)
- Implement TaxRulesService
- Implement IncomeAggregationService
- Implement StatementTracker
- Update AdvanceTaxCalculator

### Phase 3: Integration (Week 3)
- Update existing parsers to use StatementTracker
- Create database views for income aggregation
- Migrate existing parsed data to summary tables

### Phase 4: Testing & Documentation (Week 4)
- Unit tests for all services
- Integration tests for complete flow
- Update user documentation
- Admin guide for tax rule updates

---

## 10. Future Enhancements

1. **Tax Comparison Tool**: Compare OLD vs NEW regime from same database
2. **Tax Planning Advisor**: Suggest optimal investments based on tax rules
3. **Multi-Year Analysis**: Track tax trajectory across years
4. **Audit Support**: Generate ITR-2 ready data from database
5. **API Layer**: REST API for tax calculations
6. **Real-time Sync**: Auto-parse statements from email/cloud storage

---

## Appendix A: Asset Type Mapping

| Asset Category | asset_type Code | Holding Period | Rate Type |
|---------------|-----------------|----------------|-----------|
| Listed Indian Equity | EQUITY_LISTED | 12 months | FLAT |
| Equity Mutual Funds | EQUITY_MF | 12 months | FLAT |
| Debt Mutual Funds | DEBT_MF | 36 months | SLAB/INDEXED |
| Gold (Physical/ETF) | GOLD | 24 months | SLAB/INDEXED |
| Real Estate | REAL_ESTATE | 24 months | INDEXED |
| USA Stocks (RSU/ESPP) | FOREIGN_EQUITY | 24 months | SLAB/FLAT |
| Unlisted Shares | UNLISTED | 24 months | SLAB |

---

*End of Design Document*
