# PFAS Design Document - Current State Review & Scalability Assessment

**Version:** 7.0
**Date:** January 2026
**Status:** Comprehensive Review

---

## 1. Executive Summary

### Overall Maturity Assessment

| Dimension | Score (1-10) | Status |
|-----------|-------------|--------|
| Multi-user Data Isolation | 3 | Critical Gap |
| Data Normalization | 5 | Adequate |
| Asset Class Modeling | 6 | Adequate |
| Cash Flow Operations | 4 | Weak |
| Income Statement Support | 6 | Adequate |
| Cash Flow Statement Support | 1 | Critical Gap |
| Balance Sheet Support | 2 | Critical Gap |
| Tax Rules Extensibility | 8 | Strong |

**Overall Maturity: 4.4/10** - The system is well-architected for tax computation but lacks fundamental accounting capabilities required for a scalable personal finance system.

### Strongest Features

1. **Data-Driven Tax Rules** (`src/pfas/services/tax_rules_service.py`, `src/pfas/core/tax_schema.py`)
   - Tax slabs, CG rates, surcharge, cess all stored in database
   - No hardcoded tax rates - fully configurable by FY and regime
   - Clean separation of rules from computation logic

2. **Comprehensive Parser Coverage** (`src/pfas/parsers/`)
   - 9 parser modules covering 12+ source formats
   - CAMS, Karvy (MF), Zerodha, ICICI Direct (stocks), EPF, PPF, NPS, Bank statements
   - Foreign assets (RSU, ESPP, dividends) support

3. **Database Encryption & Security** (`src/pfas/core/database.py`)
   - SQLCipher AES-256 encryption
   - Field-level encryption for PAN, account numbers
   - Automatic audit logging via triggers

4. **Double-Entry Journal System** (`src/pfas/core/database.py:58-83`)
   - Chart of accounts with 18 asset classes
   - Balance validation with 0.01 tolerance
   - Foundation for proper accounting exists

### Most Critical Architectural Gaps

1. **No Financial Statement Generation Engine**
   - Balance Sheet: Zero capability
   - Cash Flow Statement: Not implemented
   - Income Statement: Partial (tax-focused only)

2. **Weak Multi-User Isolation**
   - `user_id` is optional (nullable) across all tables
   - No row-level security enforcement
   - Queries lack systematic user context filtering

3. **No Unified Transaction Model**
   - Each parser has independent models
   - No staging → normalized → user context pipeline
   - Heterogeneous source handling is ad-hoc

---

## 2. Current Architecture Overview

### High-Level Component Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        PFAS Architecture                             │
├─────────────────────────────────────────────────────────────────────┤
│  CLI Layer                                                           │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │  advance_tax_cli.py    run.py (bank_intelligence)               ││
│  └─────────────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────────────┤
│  Report Layer                                                        │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │  advance_tax_report.py   mf_capital_gains_report.py             ││
│  │  stock_holdings_report.py                                        ││
│  └─────────────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────────────┤
│  Service Layer                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │  tax_rules_service.py      income_aggregation_service.py        ││
│  │  advance_tax_calculator.py statement_tracker.py                 ││
│  │  bank_intelligence/        foreign/ (rsu, espp, dtaa)           ││
│  │  itr/ (schedule_fa, itr2_exporter)                              ││
│  └─────────────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────────────┤
│  Parser Layer                                                        │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │  bank/     mf/ (cams, karvy)   stock/ (zerodha, icici)         ││
│  │  epf/      ppf/                 nps/                             ││
│  │  salary/   foreign/             assets/ (sgb, reit, dividends)  ││
│  └─────────────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────────────┤
│  Core Layer                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │  database.py (SQLCipher)       accounts.py (Chart of Accounts)  ││
│  │  journal.py (Double-Entry)     encryption.py                    ││
│  │  currency.py                   audit.py    session.py           ││
│  │  tax_schema.py (seed data)     exceptions.py                    ││
│  └─────────────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────────────┤
│  Data Layer: SQLCipher Encrypted SQLite (WAL mode)                  │
│  40+ tables, user_id foreign keys, audit triggers                   │
└─────────────────────────────────────────────────────────────────────┘
```

### Current Database Schema Structure

**Key Tables by Category:**

| Category | Tables | user_id Present | Notes |
|----------|--------|-----------------|-------|
| Core | users, accounts, journals, journal_entries | Yes (FK) | Foundation exists |
| Bank | bank_accounts, bank_transactions, bank_interest_summary | Yes (nullable) | user_id optional |
| Mutual Funds | mf_amcs, mf_schemes, mf_folios, mf_transactions, mf_capital_gains | Yes (nullable) | Folio-based isolation |
| Stocks | stock_brokers, stock_trades, stock_dividends, stock_capital_gains, stock_stt_ledger | Yes (nullable) | Trade-level tracking |
| Retirement | epf_accounts, epf_transactions, ppf_accounts, ppf_transactions, nps_accounts, nps_transactions | Yes (nullable) | Account-based |
| Foreign | foreign_accounts, stock_plans, rsu_vests, rsu_sales, espp_purchases, espp_sales, foreign_dividends | Yes (nullable) | Comprehensive foreign asset support |
| Salary | employers, salary_records, rsu_tax_credits, form16_records, perquisites | Yes (FK) | RSU correlation innovative |
| Tax | income_tax_slabs, capital_gains_rates, surcharge_rates, cess_rates, rebate_limits | No | Global rules (correct) |
| Summary | user_income_summary, advance_tax_computation, deductions, capital_gains, schedule_fa | Yes (FK) | Pre-computed aggregates |

### Data Flow Patterns

```
User Files (Excel/PDF)
        │
        ▼
    ┌──────────┐
    │ Parsers  │  Each parser has its own model
    │          │  (MFTransaction, StockTrade, EPFTransaction...)
    └────┬─────┘
         │
         ▼
    ┌──────────────┐
    │  Database    │  Direct insert per parser
    │  (per table) │  No staging layer
    └────┬─────────┘
         │
         ▼
    ┌────────────────────┐
    │ Income Aggregation │  Real-time query OR pre-computed
    │ Service            │
    └────┬───────────────┘
         │
         ▼
    ┌──────────────────┐
    │ Tax Calculator   │  Applies DB-stored tax rules
    │                  │
    └────┬─────────────┘
         │
         ▼
    ┌──────────────┐
    │   Reports    │  Excel/PDF output
    └──────────────┘
```

**Critical Observation:** No unified ledger or transaction model - data remains siloed by source type.

---

## 3. Detailed Evaluation - Critical Dimensions

### 3a. Multi-user Data Isolation & Security

**Current Status: WEAK (Critical Gap)**

**Evidence from Code:**

1. **user_id is NULLABLE across all transaction tables**
   - `src/pfas/core/database.py:52`: `user_id INTEGER,` (no NOT NULL constraint)
   - Same pattern in lines 79, 92, 105, 148, 174, etc.

2. **Parsers don't enforce user_id**
   - `src/pfas/parsers/mf/cams.py:491`: `save_to_db(self, result: ParseResult, user_id: int = None)`
   - User ID is optional parameter defaulting to None
   - `src/pfas/parsers/mf/cams.py:530`: scheme_id created without user isolation

3. **No query-level filtering**
   - `src/pfas/services/income_aggregation_service.py:72-79`: Queries use `WHERE user_id = ?`
   - But what if user_id is NULL? Records are orphaned.

4. **Shared entities without user scope**
   - `mf_amcs` (AMC master) has no user_id - correct for shared reference data
   - `mf_schemes` has user_id but ISIN lookup ignores user (`WHERE isin = ?` only)

**Risk/Impact:**
- Data leakage between users if system evolves to multi-tenant
- Orphaned records with NULL user_id
- Query bugs when user_id not consistently applied

**Improvement Suggestions:**

*Short-term (P0):*
- Add NOT NULL constraint to user_id in all transaction tables
- Create `current_user` session context for all database operations
- Add default user_id enforcement in all parsers

*Medium-term (P1):*
- Implement row-level security views per user
- Add user context middleware to all service methods
- Create user-scoped connection wrapper

---

### 3b. Data Normalization & Extensibility for Heterogeneous Sources

**Current Status: ADEQUATE (but fragile)**

**Evidence from Code:**

1. **Each parser has independent models**
   - `src/pfas/parsers/mf/models.py`: `MFTransaction`, `MFScheme`
   - `src/pfas/parsers/stock/models.py`: `StockTrade`, `StockDividend`
   - `src/pfas/parsers/epf/epf.py`: `EPFTransaction` (inline dataclass)
   - No shared `Transaction` base class

2. **Column name handling per source**
   - `src/pfas/parsers/mf/cams.py:373-389`: `_get_column_value()` tries multiple column names
   - Fragile: New broker format = new code

3. **No staging layer**
   - Raw file → Direct to final tables
   - No intermediate normalization step
   - No source-agnostic transaction format

4. **Normalization strengths**
   - `src/pfas/parsers/mf/classifier.py`: Asset class classification logic exists
   - `src/pfas/services/bank_intelligence/category_rules.py`: Data-driven categorization

**Risk/Impact:**
- Adding new broker (e.g., Groww, Kuvera) requires new parser + possible schema changes
- No unified view across similar transactions from different sources
- Duplicate detection logic repeated per parser

**Improvement Suggestions:**

*Short-term (P1):*
- Create abstract `BaseTransaction` with common fields (date, amount, type, source)
- Implement `NormalizedTransaction` as intermediate format
- Add `source_type` and `source_raw_data` columns for traceability

*Medium-term (P1):*
- Build staging tables: `staging_raw` → `staging_normalized` → `final_tables`
- Implement parser plugin system with standard interface
- Add configuration-driven column mapping (JSON/YAML)

---

### 3c. Asset Class Modeling & Extensibility

**Current Status: ADEQUATE**

**Supported Asset Classes (Evidence from `database.py`):**

| Asset Class | Table(s) | Tax Treatment | Status |
|-------------|----------|---------------|--------|
| Bank Savings/Current | bank_accounts, bank_transactions | Interest at slab | Implemented |
| Bank FD | bank_accounts (type='FD') | Interest at slab | Partial |
| Mutual Funds (Equity) | mf_* tables | STCG 15%→20%, LTCG 10%→12.5% | Implemented |
| Mutual Funds (Debt) | mf_* tables | Slab rate | Implemented |
| Indian Stocks | stock_* tables | STCG 15%→20%, LTCG 10%→12.5% | Implemented |
| USA Stocks (RSU/ESPP) | rsu_*, espp_*, foreign_* | DTAA, Slab/12.5% | Implemented |
| EPF | epf_* tables | 80C, interest exempt | Implemented |
| PPF | ppf_* tables | EEE (exempt) | Implemented |
| NPS | nps_* tables | 80CCD, partial taxable | Implemented |
| SGB | assets/sgb.py | Interest at slab, CG exempt at maturity | Implemented |
| REIT/InvIT | assets/reit.py | Distribution breakdown | Implemented |
| Dividends | assets/dividends.py | At slab | Implemented |
| Rental Income | assets/rental.py | 30% standard deduction | Implemented |

**NOT Implemented:**
- RBI Floating Rate Bonds
- Company FDs
- Unlisted Shares (partial - schema exists)
- Real Estate (Property)
- Crypto
- Insurance (ULIP, Endowment)

**Extensibility Analysis:**

1. **Schema is asset-specific** - Adding RBI Bonds requires new tables
2. **Tax rules are configurable** - `capital_gains_rates` table can accommodate new asset types
3. **No generic "asset" abstraction** - Each class is independently modeled

**Improvement Suggestions:**

*Short-term (P2):*
- Add `asset_type` enum table for centralized asset class definition
- Create `asset_valuations` generic table for portfolio tracking

*Medium-term (P1):*
- Design generic `holdings` table with asset-specific attributes in JSON
- Implement asset class plugins with standard interface:
  - `parse()`, `get_holdings()`, `calculate_gains()`, `get_income()`

---

### 3d. Cash Flow & Income Operations Modeling

**Current Status: WEAK**

**Evidence from Code:**

1. **Income tracking is good**
   - `src/pfas/services/income_aggregation_service.py`: Aggregates from all sources
   - `IncomeRecord` dataclass with type, gross, deductions, taxable, TDS

2. **Cash flow tracking is absent**
   - No `cash_inflow`, `cash_outflow` classification
   - No operating/investing/financing activity categorization
   - Bank transactions have `debit`/`credit` but no activity type

3. **Income sources supported:**
   - Salary (from form16_records, salary_records)
   - Bank Interest (from bank_interest_summary)
   - Dividends (from stock_dividend_summary, foreign_dividends)
   - Capital Gains (from mf_capital_gains, stock_capital_gains)
   - Rental Income (from assets/rental.py)
   - SGB Interest (from assets/sgb.py)

4. **Missing income sources:**
   - PPF Interest (exists in ppf_transactions but not in IncomeAggregationService)
   - EPF Interest (exists in epf_interest but not aggregated)
   - FD Interest (no dedicated table)

**Risk/Impact:**
- Cannot generate Cash Flow Statement
- No visibility into cash position changes
- Investment tracking limited to gains, not flows

**Improvement Suggestions:**

*Short-term (P1):*
- Add `activity_type` enum: OPERATING, INVESTING, FINANCING
- Extend bank_transactions with activity classification
- Add PPF/EPF interest to IncomeAggregationService

*Medium-term (P0):*
- Design `cash_flows` table with:
  - `activity_type`, `flow_type` (IN/OUT), `amount`, `date`
  - Link to source transaction
- Implement CashFlowService for statement generation

---

### 3e. Support for Core Financial Statements

#### Income Statement Generation Capability

**Current Status: ADEQUATE (for tax purposes only)**

**Evidence:**
- `AdvanceTaxResult` in `advance_tax_calculator.py:17-58` provides comprehensive income breakdown
- Income by type: salary, STCG, LTCG, other, house property
- Deductions and taxable amounts computed
- But structured for ITR, not standard P&L format

**Gap:**
- No revenue vs expense classification
- No period-over-period comparison
- No gross margin, operating profit concepts (not applicable for personal finance)

**Verdict:** Sufficient for personal tax accounting. A "Personal Income Statement" can be derived from existing data.

---

#### Cash Flow Statement (Operating/Investing/Financing)

**Current Status: CRITICAL GAP (Not Implemented)**

**Evidence:**
- Zero code references to "cash flow statement"
- No activity classification in any transaction table
- Bank transactions exist but lack operating/investing/financing categorization

**What Would Be Needed:**

```python
# Not present in codebase - conceptual
@dataclass
class CashFlowStatement:
    period: str

    # Operating Activities
    salary_received: Decimal
    dividends_received: Decimal
    interest_received: Decimal
    rent_received: Decimal
    taxes_paid: Decimal
    net_operating: Decimal

    # Investing Activities
    mf_purchases: Decimal
    mf_redemptions: Decimal
    stock_buys: Decimal
    stock_sells: Decimal
    net_investing: Decimal

    # Financing Activities
    loan_proceeds: Decimal
    loan_repayments: Decimal
    net_financing: Decimal

    net_change_in_cash: Decimal
    opening_cash: Decimal
    closing_cash: Decimal
```

**Improvement Required (P0):**
- Design cash flow schema
- Classify bank transactions by activity
- Link investment transactions to cash movements
- Build CashFlowStatementGenerator service

---

#### Balance Sheet (Assets & Liabilities Snapshot)

**Current Status: CRITICAL GAP (Not Implemented)**

**Evidence:**
- Chart of accounts exists (`database.py:43-56`) with ASSET, LIABILITY, EQUITY types
- But no `get_balance_sheet()` function anywhere
- No period-end snapshot capability
- No asset valuation aggregation

**What Exists:**
- Individual holdings: `stock_holdings_report.py`, `foreign_holdings` table
- But no consolidated balance sheet view

**What Would Be Needed:**

```python
# Not present - conceptual
@dataclass
class BalanceSheet:
    as_of_date: date

    # Assets
    bank_balances: Decimal          # From bank_transactions
    mf_holdings: Decimal            # From mf_transactions (NAV * units)
    stock_holdings: Decimal         # From stock_trades (current price)
    foreign_holdings: Decimal       # From foreign_holdings
    epf_balance: Decimal            # From epf_transactions
    ppf_balance: Decimal            # From ppf_transactions
    nps_balance: Decimal            # From nps_transactions
    sgb_holdings: Decimal           # From sgb holdings
    real_estate: Decimal            # NOT IMPLEMENTED
    total_assets: Decimal

    # Liabilities
    tax_payable: Decimal            # From advance_tax_computation
    loans: Decimal                  # NOT IMPLEMENTED
    total_liabilities: Decimal

    # Net Worth
    net_worth: Decimal
```

**Improvement Required (P0):**
- Add `asset_valuations` table for point-in-time snapshots
- Implement BalanceSheetService
- Add current price/NAV lookup integration
- Design liabilities tracking (loans, credit cards)

---

### 3f. Per-Asset Reporting Design & Extensibility

**Current Status: WEAK**

**Evidence:**

1. **Existing reports are limited:**
   - `mf_capital_gains_report.py`: MF-specific gains report
   - `stock_holdings_report.py`: Stock portfolio view
   - `advance_tax_report.py`: Tax computation report
   - No unified asset report framework

2. **No standard report interface:**
   - Each report is independently coded
   - No `AssetReport` base class
   - No templating system

3. **Missing reports:**
   - Per-asset performance (ROI, XIRR)
   - Unrealized gains by asset class
   - Tax lot detail reports
   - Year-end portfolio statement

**Improvement Suggestions:**

*Short-term (P2):*
- Create `ReportGenerator` interface with `generate()`, `export_excel()`, `export_pdf()`
- Implement per-asset report templates

*Medium-term (P1):*
- Build unified portfolio report with drill-down by asset class
- Add XIRR calculator for investment performance
- Implement tax lot tracking and reports

---

### 3g. Tax & Business Rules Extensibility

**Current Status: STRONG**

**Evidence:**

1. **Tax rules in database** (`src/pfas/core/tax_schema.py`):
   - `income_tax_slabs`: Configurable by FY and regime
   - `capital_gains_rates`: By asset type (EQUITY_LISTED, EQUITY_MF, FOREIGN_EQUITY...)
   - `standard_deductions`: Section 16 deductions
   - `chapter_via_limits`: 80C, 80CCC, 80CCD limits
   - `surcharge_rates`: By income level
   - `cess_rates`: Health education cess

2. **Service layer fetches from DB** (`src/pfas/services/tax_rules_service.py`):
   ```python
   def get_tax_slabs(self, financial_year: str, regime: str) -> list[TaxSlab]
   def get_capital_gains_rate(self, fy: str, asset_type: str, gain_type: str)
   def get_surcharge_rate(self, fy: str, income: Decimal) -> Decimal
   ```

3. **New FY support = data update only**:
   - Add new rows to tax tables
   - No code changes required

**Minor Gap:**
- Holding period rules are partially hardcoded
- `stock/models.py:205`: `return self.holding_period_days > 365` (hardcoded 365 days)
- Should be configurable per asset type

**Improvement Suggestions:**

*Short-term (P2):*
- Move holding period thresholds to database
- Add `holding_period_rules` table

*Medium-term (P2):*
- Implement rule versioning for retroactive corrections
- Add business rule engine for complex conditions

---

### 3h. Idempotency, Duplicate Handling & Data Integrity

**Current Status: ADEQUATE**

**Evidence:**

1. **Duplicate detection via UNIQUE constraints:**
   - `mf_transactions`: `UNIQUE(folio_id, date, transaction_type, amount, purchase_date)`
   - `stock_trades`: No unique constraint (potential issue)
   - `bank_transactions`: `UNIQUE(bank_account_id, date, description, debit, credit)`

2. **File hash tracking** (`statement_tracker.py`):
   - SHA256 hash prevents re-parsing same file
   - `statement_processing_log` table

3. **Parser duplicate handling:**
   - `src/pfas/parsers/mf/cams.py:661-668`: Catches IntegrityError, logs duplicate
   - Returns `False` on duplicate, counted separately

4. **Transaction atomicity:**
   - `database.py:1504-1525`: `transaction()` context manager
   - Proper BEGIN/COMMIT/ROLLBACK

**Gap:**
- `stock_trades` lacks unique constraint - potential duplicates
- No upsert logic - re-import overwrites vs merges

**Improvement Suggestions:**

*Short-term (P1):*
- Add unique constraint to stock_trades
- Implement upsert pattern for all parsers

*Medium-term (P2):*
- Add `import_batch_id` for tracking import sessions
- Implement rollback-by-batch capability

---

## 4. Gap Analysis Summary Table

| Dimension | Current Status | Major Gaps Identified | Priority | Effort |
|-----------|---------------|----------------------|----------|--------|
| **Multi-user Isolation** | Weak | user_id nullable, no row-level security, no session context | P0 | Medium |
| **Data Normalization** | Adequate | No unified transaction model, no staging layer, per-parser models | P1 | High |
| **Asset Class Modeling** | Adequate | Missing RBI Bonds, Company FD, Crypto, Insurance, Real Estate | P2 | Medium |
| **Cash Flow & Income Operations** | Weak | No activity classification, missing interest sources | P1 | Medium |
| **Income Statement Support** | Adequate | Tax-focused only, no standard P&L format | P2 | Low |
| **Cash Flow Statement Support** | Critical Gap | Not implemented at all | P0 | High |
| **Balance Sheet Support** | Critical Gap | No asset valuation snapshot, no liabilities tracking | P0 | High |
| **Per-Asset Reporting** | Weak | No unified framework, missing performance metrics | P1 | Medium |
| **Tax/Business Rules Extensibility** | Strong | Minor: holding periods hardcoded | P2 | Low |
| **Idempotency/Duplicates** | Adequate | stock_trades lacks unique constraint | P1 | Low |

---

## 5. Recommended Target Architecture Patterns

### 5.1 Database Modeling for Assets/Income/Cash-Flow

**Proposed Schema Evolution:**

```sql
-- Core Extension Tables

-- Generic holdings snapshot for any asset
CREATE TABLE asset_holdings (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    asset_type TEXT NOT NULL,  -- 'MF', 'STOCK', 'SGB', 'EPF', etc.
    asset_id INTEGER NOT NULL,  -- FK to asset-specific table
    valuation_date DATE NOT NULL,
    quantity DECIMAL(15,4),
    unit_price DECIMAL(15,4),
    total_value DECIMAL(15,2),
    cost_basis DECIMAL(15,2),
    unrealized_gain DECIMAL(15,2),
    currency TEXT DEFAULT 'INR',
    UNIQUE(user_id, asset_type, asset_id, valuation_date)
);

-- Cash flow tracking
CREATE TABLE cash_flows (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    flow_date DATE NOT NULL,
    activity_type TEXT NOT NULL CHECK(activity_type IN ('OPERATING', 'INVESTING', 'FINANCING')),
    flow_direction TEXT NOT NULL CHECK(flow_direction IN ('INFLOW', 'OUTFLOW')),
    amount DECIMAL(15,2) NOT NULL,
    category TEXT,  -- 'SALARY', 'MF_PURCHASE', 'LOAN_EMI', etc.
    description TEXT,
    source_table TEXT,
    source_id INTEGER,
    bank_account_id INTEGER REFERENCES bank_accounts(id)
);

-- Balance sheet snapshots
CREATE TABLE balance_sheet_snapshots (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    snapshot_date DATE NOT NULL,
    total_assets DECIMAL(15,2),
    total_liabilities DECIMAL(15,2),
    net_worth DECIMAL(15,2),
    details_json TEXT,  -- Breakdown by asset class
    UNIQUE(user_id, snapshot_date)
);

-- Liabilities tracking (missing currently)
CREATE TABLE liabilities (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    liability_type TEXT NOT NULL,  -- 'HOME_LOAN', 'CAR_LOAN', 'CREDIT_CARD'
    principal_amount DECIMAL(15,2),
    outstanding_amount DECIMAL(15,2),
    interest_rate DECIMAL(5,2),
    start_date DATE,
    end_date DATE,
    emi_amount DECIMAL(15,2),
    lender_name TEXT
);
```

### 5.2 Normalization Pipeline

**Proposed Flow:**

```
Raw Files
    │
    ▼
┌────────────────┐
│ staging_raw    │  Store original data as-is
│ (JSON blob)    │  Source-specific schema preserved
└───────┬────────┘
        │
        ▼
┌────────────────────┐
│ Parser Plugins     │  Implement standard interface:
│                    │  parse(file) → NormalizedTransaction[]
└───────┬────────────┘
        │
        ▼
┌────────────────────┐
│ staging_normalized │  Unified transaction format
│                    │  Common fields + asset-specific JSON
└───────┬────────────┘
        │
        ▼
┌────────────────────┐
│ User Context       │  Apply user_id, validate, enrich
│ Service            │
└───────┬────────────┘
        │
        ▼
┌────────────────────┐
│ Final Tables       │  Per-asset tables (current schema)
│                    │  + unified views
└────────────────────┘
```

### 5.3 Financial Statement Generation Approach

**Recommended: Derived Query Approach (not double-entry for everything)**

```python
class FinancialStatementService:
    """Generates financial statements from transaction data."""

    def get_income_statement(self, user_id: int, period: str) -> IncomeStatement:
        """
        Derives P&L from existing tables:
        - Income: salary_records, bank_interest, dividends, capital_gains
        - Expenses: taxes paid, professional fees
        """
        pass

    def get_cash_flow_statement(self, user_id: int, period: str) -> CashFlowStatement:
        """
        Derives from cash_flows table:
        - Operating: salary, dividends, interest, taxes
        - Investing: MF/stock purchases, redemptions
        - Financing: loans, EMIs
        """
        pass

    def get_balance_sheet(self, user_id: int, as_of: date) -> BalanceSheet:
        """
        Derives from asset_holdings and liabilities:
        - Assets: Current holdings valuation
        - Liabilities: Outstanding loans
        - Equity: Net worth
        """
        pass
```

**Why not full double-entry for personal finance?**
- Personal finance doesn't require audit-grade accounting
- Simpler implementation with derived queries
- Journal system useful for specific use cases (tax adjustments)

### 5.4 Configuration Strategy

**Recommended: Hybrid JSON/DB Approach**

```
config/
├── tax_rules/           # Database-driven (existing)
│   └── (tables: income_tax_slabs, capital_gains_rates, etc.)
│
├── parser_configs/      # JSON configuration
│   ├── cams_columns.json
│   ├── zerodha_columns.json
│   └── icici_columns.json
│
├── category_rules/      # JSON/YAML (existing pattern in bank_intelligence)
│   └── user_overrides.json
│
└── report_templates/    # JSON templates
    ├── advance_tax.json
    └── portfolio_summary.json
```

### 5.5 Plugin System for New Parsers

**Proposed Interface:**

```python
from abc import ABC, abstractmethod
from typing import List
from dataclasses import dataclass

@dataclass
class NormalizedTransaction:
    """Universal transaction format."""
    date: date
    amount: Decimal
    transaction_type: str  # BUY, SELL, CREDIT, DEBIT, etc.
    asset_type: str
    asset_identifier: str  # ISIN, symbol, account number
    quantity: Optional[Decimal] = None
    price: Optional[Decimal] = None
    charges: Decimal = Decimal("0")
    source_type: str = ""
    raw_data: dict = field(default_factory=dict)

class BaseParser(ABC):
    """Standard interface for all parsers."""

    @abstractmethod
    def parse(self, file_path: Path) -> List[NormalizedTransaction]:
        """Parse file and return normalized transactions."""
        pass

    @abstractmethod
    def get_supported_formats(self) -> List[str]:
        """Return list of supported file extensions."""
        pass

    @abstractmethod
    def validate(self, file_path: Path) -> bool:
        """Check if file is valid for this parser."""
        pass

# Registration
class ParserRegistry:
    _parsers: dict[str, type[BaseParser]] = {}

    @classmethod
    def register(cls, name: str, parser_class: type[BaseParser]):
        cls._parsers[name] = parser_class

    @classmethod
    def get_parser(cls, name: str) -> BaseParser:
        return cls._parsers[name]()
```

---

## 6. Conclusion & Prioritized Roadmap

### Overall Readiness for 3-5 Year Evolution

**Current State:** The system is well-designed for tax computation but lacks fundamental accounting capabilities (Balance Sheet, Cash Flow Statement) and has weak multi-user isolation. It cannot serve as a comprehensive personal finance system without significant architectural enhancements.

**Estimated Effort to Address Critical Gaps:** 3-4 months of focused development

### Top 8 Must-Have Architectural Improvements

| Priority | Improvement | Impact | Effort | Target |
|----------|-------------|--------|--------|--------|
| **1** | **Enforce user_id NOT NULL** | Prevents data leakage, enables multi-user | Low | Immediate |
| **2** | **Implement Balance Sheet Service** | Enables net worth tracking, portfolio view | High | Phase 7.1 |
| **3** | **Implement Cash Flow Statement** | Enables complete financial picture | High | Phase 7.1 |
| **4** | **Add cash_flows tracking table** | Foundation for cash flow statement | Medium | Phase 7.1 |
| **5** | **Create unified NormalizedTransaction** | Simplifies parser development, enables cross-source analysis | Medium | Phase 7.2 |
| **6** | **Add liabilities tracking** | Complete balance sheet, loan management | Medium | Phase 7.2 |
| **7** | **Build portfolio valuation service** | Real-time holdings valuation, unrealized gains | Medium | Phase 7.2 |
| **8** | **Implement report templating system** | Consistent, extensible reporting | Low | Phase 7.3 |

### Suggested Next Milestone

**Phase 7.1: Financial Statement Foundation**

*Deliverables:*
1. Enforce user_id across all tables (migration script)
2. `cash_flows` table and CashFlowService
3. `asset_holdings` table and BalanceSheetService
4. Basic Balance Sheet report (Assets only, no liabilities yet)
5. Basic Cash Flow Statement (from bank transactions)

*Success Criteria:*
- `get_balance_sheet(user_id, date)` returns asset snapshot
- `get_cash_flow_statement(user_id, period)` returns categorized flows
- All new records have user_id NOT NULL
- Unit tests for new services (90% coverage target)

---

**Document Prepared By:** Claude (Automated Analysis)
**Based On:** PFAS codebase as of January 2026
**Files Analyzed:** 100+ Python files, 40+ database tables
