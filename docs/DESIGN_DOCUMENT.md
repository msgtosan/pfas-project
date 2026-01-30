# PFAS Design Document - Current State Review & Scalability Assessment

**Version:** 1.0
**Date:** January 2026
**Status:** Current State Analysis

---

## 1. Executive Summary

### 1.1 System Purpose
PFAS (Personal Financial Accounting System) is designed for Indian tax residents managing 18 asset classes across domestic and foreign holdings. The system ingests financial documents (PDFs, Excel, CSVs), maintains a double-entry accounting journal, and generates tax-compliant reports including ITR-2 schedules.

### 1.2 Architecture Maturity Rating

| Dimension | Rating | Notes |
|-----------|--------|-------|
| **Multi-User Isolation** | 3/5 | Per-user directories and databases exist; user_id columns present but inconsistently enforced |
| **Data Normalization** | 4/5 | Clean 3NF schema for most tables; some denormalization in transaction tables for performance |
| **Asset Class Modeling** | 4/5 | 18 asset classes with hierarchical Chart of Accounts (1xxx-5xxx); extensible structure |
| **Cash Flow Operations** | 3/5 | CashFlowService exists with 20+ categories; lacks automated bank reconciliation |
| **Financial Statements** | 4/5 | BalanceSheetService, CashFlowService implemented; Statement of Changes missing |
| **Per-Asset Reporting** | 4/5 | NetworthReport, MFCapitalGainsReport, StockHoldingsReport, AdvanceTaxReport implemented |
| **Tax Rules Extensibility** | 4/5 | TaxRulesService with JSON-configurable slabs; DTAA calculator present |
| **Idempotency** | 3/5 | MD5 file hashing for deduplication; record-level dedup varies by parser |

**Overall Maturity: 3.6/5** - Functional prototype with solid foundations, requiring hardening for production multi-user deployment.

### 1.3 Key Strengths
- **Config-Driven Architecture**: PathResolver, JSON configs for tax rules, report settings
- **Comprehensive Asset Coverage**: All 18 Indian tax-relevant asset classes modeled
- **Modern Python Practices**: Type hints, dataclasses, pytest fixtures, 90%+ test coverage target
- **Encryption Support**: SQLCipher integration for sensitive financial data

### 1.4 Critical Gaps
1. No unified transaction ledger (journal_entries table exists but not consistently used)
2. Batch ingestion lacks atomic rollback on partial failures
3. NAV/price history tables empty - relies on transaction-time values
4. No audit trail for data modifications

---

## 2. Current Architecture Overview

### 2.1 Module Structure

```
src/pfas/
├── core/               # Foundation layer
│   ├── accounts.py     # Chart of Accounts (18 asset classes)
│   ├── journal.py      # Double-entry journal (underutilized)
│   ├── models.py       # Core dataclasses
│   ├── paths.py        # PathResolver (config-driven paths)
│   ├── tax_schema.py   # Tax slab definitions
│   └── encryption.py   # SQLCipher wrapper
├── parsers/            # Document ingestion
│   ├── bank/           # HDFC, ICICI, SBI statement parsers
│   ├── mf/             # CAMS, Karvy CAS parsers
│   ├── stock/          # Zerodha, ICICI Direct parsers
│   ├── salary/         # Form16, payslip, RSU correlation
│   ├── foreign/        # Morgan Stanley RSU/ESPP
│   ├── assets/         # SGB, REIT, rental, dividends
│   ├── epf/            # EPF passbook parser
│   ├── ppf/            # PPF statement parser
│   └── nps/            # NPS statement parser
├── services/           # Business logic
│   ├── balance_sheet_service.py
│   ├── cash_flow_service.py
│   ├── tax_rules_service.py
│   ├── income_aggregation_service.py
│   ├── advance_tax_calculator.py
│   ├── bank_intelligence/      # Transaction categorization
│   ├── currency/               # Forex rate provider
│   ├── foreign/                # RSU/ESPP/DTAA processors
│   └── itr/                    # ITR-2 export, Schedule FA
├── reports/            # Report generators
│   ├── networth_report.py      # Multi-asset networth with XIRR
│   ├── mf_capital_gains_report.py
│   ├── stock_holdings_report.py
│   ├── advance_tax_report.py
│   └── template_engine.py      # Excel generation
├── cli/                # Command-line interfaces
│   ├── networth_cli.py
│   └── reports_cli.py
└── audit/              # Reconciliation tools
    ├── mf_audit_parser.py
    └── reconciler.py
```

### 2.2 Data Layer

**Database**: SQLCipher (AES-256 encrypted SQLite)
**Per-User Isolation**: `Data/{UserName}/db/finance.db`

**Key Tables** (50+ total):

| Category | Tables |
|----------|--------|
| **Core** | `users`, `accounts`, `journal_entries`, `journal_lines` |
| **Bank** | `bank_accounts`, `bank_transactions`, `bank_statement_files` |
| **MF** | `mf_schemes`, `mf_folios`, `mf_transactions`, `mf_nav_history` |
| **Stocks** | `stock_trades`, `stock_capital_gains`, `stock_dividends` |
| **Retirement** | `epf_transactions`, `ppf_transactions`, `nps_transactions` |
| **Foreign** | `rsu_vests`, `espp_purchases`, `foreign_holdings` |
| **Tax** | `salary_records`, `form16_data`, `tds_entries` |
| **Assets** | `sgb_holdings`, `rbi_bonds`, `rental_income`, `reits` |

### 2.3 Configuration Layer

```
config/
├── paths.json              # Directory structure config
├── networth_config.json    # Asset categories, report settings
├── tax_rules/
│   ├── fy_2024_25.json     # Tax slabs, exemptions
│   └── dtaa_usa.json       # US treaty rates
└── parsers/
    └── bank_rules.json     # Transaction categorization
```

**PathResolver Pattern** (`src/pfas/core/paths.py`):
```python
class PathResolver:
    def __init__(self, root_path, user_name):
        self.user_dir = root / users_base / user_name

    def db_path(self) -> Path
    def inbox(self) -> Path
    def reports() -> Path
    def report_file(asset_type, report_type, ...) -> Path
```

---

## 3. Detailed Evaluation - Critical Dimensions

### 3.1 Multi-User Isolation

**Current Implementation:**
- Physical separation: `Data/{UserName}/` directory structure
- Logical separation: `user_id` foreign key in most tables
- PathResolver enforces user context for file operations

**Gaps:**
```python
# accounts.py - user_id optional, not consistently filtered
def setup_chart_of_accounts(conn, user_id: Optional[int] = None)

# Some services don't pass user_id through call chain
class BalanceSheetService:
    def _populate_mutual_funds(self, snapshot, user_id, as_of):
        # user_id used, but no validation of data ownership
```

**Evidence:**
- `accounts` table has `user_id` column but CoA is shared
- No row-level security or views filtering by user
- Direct SQL queries possible without user context

**Recommendation:** Add user_id validation middleware; create per-user views for all asset tables.

---

### 3.2 Data Normalization

**Current Implementation (3NF+):**
```sql
-- Proper normalization for MF
mf_schemes (id, name, isin, asset_class, amc_id)
mf_folios (id, scheme_id, folio_number, user_id)
mf_transactions (id, folio_id, date, units, nav, amount)

-- Denormalized for performance
stock_trades (symbol, exchange, price, quantity, trade_date,
              security_name)  -- denormalized from securities table
```

**Strengths:**
- AMC normalized in separate table
- Scheme-folio-transaction hierarchy properly linked
- Foreign keys enforced

**Gaps:**
- `stock_trades.security_name` duplicates `securities.name`
- `mf_transactions` stores `nav` instead of referencing `mf_nav_history`
- No versioning for NAV corrections

---

### 3.3 Asset Class Modeling

**Chart of Accounts Structure:**
```python
CHART_OF_ACCOUNTS = {
    # Assets (1xxx) - 5 levels
    "1000": {"name": "Assets", "type": "ASSET"},
    "1100": {"name": "Current Assets", "parent": "1000"},
    "1101": {"name": "Bank - Savings", "parent": "1100"},

    "1200": {"name": "Investments", "parent": "1000"},
    "1201": {"name": "Mutual Funds - Equity", "parent": "1200"},
    "1203": {"name": "Indian Stocks", "parent": "1200"},

    "1300": {"name": "Retirement Funds"},
    "1301": {"name": "EPF - Employee"},
    "1303": {"name": "PPF"},
    "1304": {"name": "NPS - Tier I"},

    "1400": {"name": "Foreign Assets", "currency": "USD"},
    "1401": {"name": "US Stocks - RSU"},
    "1402": {"name": "US Stocks - ESPP"},

    # Income (4xxx)
    "4301": {"name": "STCG - Equity 20%"},
    "4302": {"name": "LTCG - Equity 12.5%"},
}
```

**Strengths:**
- Hierarchical structure supports rollup reporting
- Currency attribute on foreign accounts
- Direct mapping to ITR schedules (4301→Schedule CG)

**Gaps:**
- No `asset_type` enum enforced at database level
- Missing: Unlisted shares tracking for 2-year LTCG
- No sub-classification for gold (physical vs ETF vs SGB)

---

### 3.4 Cash Flow Operations

**CashFlowService Categories:**
```python
class CashFlowCategory(Enum):
    # Operating
    SALARY_INCOME = "salary_income"
    BANK_INTEREST = "bank_interest"
    DIVIDEND_INCOME = "dividend_income"

    # Investing
    MF_PURCHASE = "mf_purchase"
    MF_REDEMPTION = "mf_redemption"
    STOCK_PURCHASE = "stock_purchase"
    STOCK_SALE = "stock_sale"

    # Financing
    TAX_PAYMENT = "tax_payment"
    LOAN_REPAYMENT = "loan_repayment"
```

**Implementation:**
```python
class CashFlowService:
    def get_cash_flow(self, user_id, from_date, to_date):
        activities = defaultdict(list)
        self._add_salary_income(activities, ...)
        self._add_bank_interest(activities, ...)
        self._add_mf_transactions(activities, ...)
        # ... 15+ more categories
```

**Gaps:**
- No automated bank transaction categorization (bank_intelligence exists but manual trigger)
- Inter-account transfers not reconciled
- Missing: Recurring expense tracking

---

### 3.5 Financial Statements Support

| Statement | Implementation | Completeness |
|-----------|---------------|--------------|
| **Balance Sheet** | BalanceSheetService | 85% - Missing forex mark-to-market |
| **Cash Flow** | CashFlowService | 75% - No indirect method, no bank recon |
| **Income Statement** | IncomeAggregationService | 70% - Salary + investment income only |
| **Statement of Changes** | Not implemented | 0% |

**BalanceSheetSnapshot Structure:**
```python
@dataclass
class BalanceSheetSnapshot:
    snapshot_date: date
    bank_savings: Decimal = Decimal("0")
    bank_fd: Decimal = Decimal("0")
    mutual_funds_equity: Decimal = Decimal("0")
    mutual_funds_debt: Decimal = Decimal("0")
    indian_stocks: Decimal = Decimal("0")
    epf_balance: Decimal = Decimal("0")
    ppf_balance: Decimal = Decimal("0")
    nps_balance: Decimal = Decimal("0")
    # ... more fields

    @property
    def total_assets(self) -> Decimal

    @property
    def net_worth(self) -> Decimal
```

---

### 3.6 Per-Asset Reporting

**Implemented Reports:**

| Report | Generator Class | Output |
|--------|-----------------|--------|
| Networth Summary | NetworthReportGenerator | Excel with charts |
| MF Capital Gains | MFCapitalGainsReport | Excel + PDF |
| Stock Holdings | StockHoldingsReport | Excel |
| Advance Tax | AdvanceTaxReportGenerator | Excel |

**NetworthReport Features:**
- Multi-asset consolidation (MF, Stocks, EPF, PPF, NPS, SGB)
- Holdings derivation from trades (FIFO cost basis)
- XIRR calculation using Newton-Raphson
- FY/Quarterly/Monthly granularity
- Excel output with pie charts, growth trends

**Example Configuration:**
```json
{
  "asset_categories": {
    "mutual_funds": {
      "enabled": true,
      "sub_categories": {
        "MF-Equity": {"filter": "asset_class = 'EQUITY'"},
        "MF-Debt": {"filter": "asset_class = 'DEBT'"}
      }
    }
  }
}
```

---

### 3.7 Tax Rules Extensibility

**TaxRulesService:**
```python
class TaxRulesService:
    def __init__(self, fy: str):
        self.rules = self._load_rules(fy)

    def get_slab_rates(self, regime: str) -> List[TaxSlab]
    def calculate_tax(self, income: Decimal, regime: str) -> Decimal
    def get_exemption_limit(self, category: str) -> Decimal
```

**Configurable via JSON:**
```json
{
  "fy": "2024-25",
  "slabs": {
    "old": [
      {"min": 0, "max": 250000, "rate": 0},
      {"min": 250001, "max": 500000, "rate": 5},
      {"min": 500001, "max": 1000000, "rate": 20}
    ],
    "new": [...]
  },
  "capital_gains": {
    "stcg_equity": 20,
    "ltcg_equity": 12.5,
    "ltcg_exemption": 125000
  }
}
```

**DTAA Support:**
```python
class DTAACalculator:
    def calculate_us_tax_credit(self, foreign_income, us_tax_paid):
        # Treaty rate application
        # Average rate calculation
        # Section 90/90A compliance
```

**Gaps:**
- No automatic FY rule file selection
- Missing: Section 54/54F reinvestment exemptions
- No carry-forward loss tracking

---

### 3.8 Idempotency & Deduplication

**File-Level Deduplication:**
```python
class ICICIDirectProcessor:
    def _calculate_file_hash(self, file_path: Path) -> str:
        return hashlib.md5(file_path.read_bytes()).hexdigest()

    def _is_file_processed(self, file_hash: str) -> bool:
        cursor = self.conn.execute(
            "SELECT 1 FROM processed_files WHERE file_hash = ?",
            (file_hash,)
        )
        return cursor.fetchone() is not None
```

**Record-Level Deduplication (varies by parser):**
```python
# MF Parser - uses composite key
existing = self._find_duplicate(folio, date, amount, units)

# Stock Parser - trade_id based
if not self._trade_exists(trade_id):
    self._insert_trade(trade)
```

**Gaps:**
- No unified deduplication interface across parsers
- Bank transactions use date+amount (collision-prone)
- No idempotency key for manual entries

---

## 4. Gap Analysis Summary Table

| Area | Current State | Gap | Priority | Effort |
|------|---------------|-----|----------|--------|
| **Multi-User Security** | Per-user DB files, user_id columns | No RLS, optional user_id in CoA | HIGH | Medium |
| **Journal Integrity** | journal_entries table exists | Underutilized; transactions bypass journal | HIGH | High |
| **NAV/Price History** | Stored at transaction time | No historical lookup table | MEDIUM | Medium |
| **Audit Trail** | None | No modification tracking | MEDIUM | Medium |
| **Bank Reconciliation** | Manual via bank_intelligence | No automated matching | LOW | High |
| **Atomic Ingestion** | Per-file commits | No batch rollback | MEDIUM | Medium |
| **Statement of Changes** | Not implemented | Required for full GAAP | LOW | Low |
| **Loss Carry-Forward** | Not tracked | Required for accurate CG tax | MEDIUM | Medium |

---

## 5. Recommended Target Architecture Patterns

### 5.1 Unified Transaction Layer
```python
class TransactionService:
    def record_transaction(
        self,
        user_id: int,
        entries: List[JournalEntry],
        source: TransactionSource,
        idempotency_key: str
    ) -> TransactionResult:
        """
        All asset changes go through here.
        Enforces double-entry, user isolation, audit trail.
        """
        with self.conn.transaction():
            if self._exists(idempotency_key):
                return TransactionResult.DUPLICATE

            self._validate_user_owns_accounts(user_id, entries)
            self._record_journal_entries(entries)
            self._update_balances(entries)
            self._log_audit(user_id, entries, source)

        return TransactionResult.SUCCESS
```

### 5.2 Row-Level Security Views
```sql
CREATE VIEW user_mf_transactions AS
SELECT * FROM mf_transactions
WHERE folio_id IN (
    SELECT id FROM mf_folios WHERE user_id = current_user_id()
);
```

### 5.3 Event Sourcing for Valuations
```python
@dataclass
class ValuationEvent:
    event_type: str  # NAV_UPDATE, PRICE_UPDATE, FOREX_RATE
    asset_id: str
    value: Decimal
    timestamp: datetime
    source: str

class ValuationStore:
    def get_value_at(self, asset_id: str, as_of: date) -> Decimal:
        """Point-in-time valuation from event log."""
```

---

## 6. Conclusion & Prioritized Roadmap

### Phase 1: Foundation Hardening (Immediate)
1. **Enforce user_id on all queries** - Add middleware/decorator
2. **Populate NAV history tables** - Enable historical valuation
3. **Add audit_log table** - Track all data modifications

### Phase 2: Transaction Integrity (Short-term)
4. **Unified TransactionService** - All changes via journal
5. **Idempotency keys** - Standardize across all parsers
6. **Atomic batch ingestion** - Rollback on partial failure

### Phase 3: Reporting Completeness (Medium-term)
7. **Loss carry-forward tracking** - 8-year CG loss carry
8. **Statement of Changes** - Equity movement report
9. **Section 54/54F tracking** - Reinvestment exemptions

### Phase 4: Production Readiness (Long-term)
10. **Multi-tenant mode** - Shared DB with RLS
11. **API layer** - REST/GraphQL for web UI
12. **Real-time sync** - Bank API integration (Account Aggregator)

---

## Appendix A: Table Inventory

| Category | Tables | Row Count (Sample User) |
|----------|--------|------------------------|
| Core | users, accounts, journal_entries, journal_lines | 1, 55, 0, 0 |
| Bank | bank_accounts, bank_transactions | 4, 2,500+ |
| MF | mf_schemes, mf_folios, mf_transactions | 85, 12, 450 |
| Stock | stock_trades, stock_capital_gains | 631, 256 |
| Salary | salary_records, form16_data | 36, 3 |

## Appendix B: Key File References

| File | Purpose |
|------|---------|
| `src/pfas/core/accounts.py` | Chart of Accounts definition |
| `src/pfas/core/paths.py` | PathResolver for config-driven paths |
| `src/pfas/services/balance_sheet_service.py` | Balance sheet generation |
| `src/pfas/services/cash_flow_service.py` | Cash flow statement |
| `src/pfas/reports/networth_report.py` | Multi-asset networth report |
| `config/networth_config.json` | Asset categories, report settings |

---

*Document generated: January 2026*
*Analysis based on: pfas-project codebase (commit 8a2b983)*
