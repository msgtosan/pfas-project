# PFAS Project Context

**Personal Financial Accounting System for Indian Tax Residents**

This document defines mandatory architectural requirements, conventions, and guidelines for the PFAS project. It serves as the authoritative reference for maintaining consistency, preventing regressions, and ensuring scalability as the system grows to support multiple users, asset classes, and tax jurisdictions.

---

## Table of Contents

1. [Project Vision & Objectives](#1-project-vision--objectives)
2. [System Architecture Overview](#2-system-architecture-overview)
3. [Directory Structure & File Organization](#3-directory-structure--file-organization)
4. [Data Flow & Processing Pipeline](#4-data-flow--processing-pipeline)
5. [Configuration Management](#5-configuration-management)
6. [Database Architecture](#6-database-architecture)
7. [Double-Entry Accounting Model](#7-double-entry-accounting-model)
8. [Parser Development Guidelines](#8-parser-development-guidelines)
9. [Service Layer Architecture](#9-service-layer-architecture)
10. [Testing Strategy](#10-testing-strategy)
11. [Usage Guidelines](#11-usage-guidelines)
12. [Scalability Patterns](#12-scalability-patterns)
13. [Security & Compliance](#13-security--compliance)
14. [Pre-Implementation Checklist](#14-pre-implementation-checklist)

---

## 1. Project Vision & Objectives

### Vision
Build a comprehensive, scalable personal finance accounting system that enables Indian tax residents to:
- Consolidate financial data from 18+ asset classes
- Maintain accurate double-entry accounting records
- Generate ITR-2 compliant tax computations
- Track capital gains with FIFO methodology
- Support multi-user households with data isolation

### Design Principles
| Principle | Implementation |
|-----------|----------------|
| **User Isolation** | Each user has separate data directory and encrypted database |
| **Source of Truth** | Original documents preserved in archive; database is derived |
| **Auditability** | Every transaction links to source file and creates journal entries |
| **Extensibility** | New asset classes/banks/brokers added without core changes |
| **Tax Compliance** | Indian FY (Apr-Mar), LTCG/STCG rules, Section 80C/80D tracking |

### Project Phases
| Phase | Scope | Key Deliverables |
|-------|-------|------------------|
| **Phase 1** | Indian Assets | Bank, MF, Stocks, EPF, NPS, PPF, SGB, Salary/Form16 |
| **Phase 2** | Foreign Assets | USA Stocks (RSU/ESPP), DTAA, Foreign Bank, ITR-2 Export |
| **Phase 3** | Advanced | Tax optimization, What-if scenarios, Multi-year trends |

### Key Technical Decisions
| Decision | Rule | Rationale |
|----------|------|-----------|
| RSU Tax Credit | NEGATIVE deduction = credit at vesting | Matches Form 16 representation |
| Currency Conversion | SBI TT Buying Rate | RBI recognized rate for tax |
| LTCG (Indian Equity) | 12 months holding | As per Income Tax Act |
| LTCG (Foreign/Unlisted) | 24 months holding | As per Income Tax Act |
| Indian Fiscal Year | April 1 to March 31 | Standard Indian FY |
| Grandfathering Date | Jan 31, 2018 | For LTCG on equity |
| Database Encryption | SQLCipher AES-256 | PAN and financial data protection |

---

## 2. System Architecture Overview

### High-Level Architecture
```
┌─────────────────────────────────────────────────────────────────────┐
│                        PFAS System Architecture                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │   INPUTS     │    │  PROCESSING  │    │      OUTPUTS         │  │
│  │              │    │              │    │                      │  │
│  │ Bank Stmt    │───▶│   Parsers    │───▶│ Journal Entries      │  │
│  │ MF CAS       │    │      ▼       │    │ Balance Sheet        │  │
│  │ Stock P&L    │    │   Services   │    │ Cash Flow Statement  │  │
│  │ EPF Passbook │    │      ▼       │    │ Capital Gains Report │  │
│  │ Form 16      │    │  Analyzers   │    │ Tax Computation      │  │
│  │ NPS/PPF      │    │              │    │ ITR-2 JSON           │  │
│  └──────────────┘    └──────────────┘    └──────────────────────┘  │
│         │                   │                      │                │
│         ▼                   ▼                      ▼                │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                    DATA LAYER                                 │  │
│  │  ┌─────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────┐ │  │
│  │  │ inbox/  │  │  archive/   │  │  config/    │  │   db/   │ │  │
│  │  │ (new)   │──▶│ (processed) │  │ (settings)  │  │(SQLite) │ │  │
│  │  └─────────┘  └─────────────┘  └─────────────┘  └─────────┘ │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities
| Component | Responsibility | Location |
|-----------|----------------|----------|
| **Core** | Database, Paths, Accounts, Encryption | `src/pfas/core/` |
| **Parsers** | Extract data from source files | `src/pfas/parsers/{asset}/` |
| **Services** | Business logic, calculations | `src/pfas/services/` |
| **Analyzers** | Data analysis, reconciliation | `src/pfas/analyzers/` |
| **Reports** | Generate output reports | `src/pfas/reports/` |

---

## 3. Directory Structure & File Organization

### Project Structure
```
pfas-project/
├── src/pfas/                      # Source code (importable as pfas.*)
│   ├── __init__.py
│   ├── core/                      # Core infrastructure
│   │   ├── __init__.py
│   │   ├── database.py            # DatabaseManager, schema
│   │   ├── paths.py               # PathResolver
│   │   ├── accounts.py            # Chart of accounts setup
│   │   ├── encryption.py          # Field encryption
│   │   └── schema_*.sql           # Schema enhancement files
│   ├── parsers/                   # Asset-specific parsers
│   │   ├── bank/                  # Bank statement parsers
│   │   │   ├── __init__.py
│   │   │   ├── base.py            # BankStatementParser base
│   │   │   ├── models.py          # BankTransaction, etc.
│   │   │   ├── icici.py           # ICICI PDF parser
│   │   │   ├── icici_excel.py     # ICICI Excel parser
│   │   │   ├── sbi.py             # SBI parser
│   │   │   └── hdfc.py            # HDFC parser
│   │   ├── mf/                    # Mutual Fund parsers
│   │   │   ├── cams.py            # CAMS CAS parser
│   │   │   ├── karvy.py           # KFintech parser
│   │   │   ├── cas_pdf_parser.py  # Consolidated CAS PDF
│   │   │   ├── capital_gains.py   # MF capital gains calculator
│   │   │   └── fifo_tracker.py    # FIFO unit tracking
│   │   ├── stock/                 # Stock/equity parsers
│   │   │   ├── zerodha.py         # Zerodha Tax P&L
│   │   │   └── icici_direct.py    # ICICI Direct
│   │   ├── epf/                   # EPF parsers
│   │   ├── nps/                   # NPS parsers
│   │   ├── ppf/                   # PPF parsers
│   │   ├── salary/                # Salary/Form16 parsers
│   │   └── foreign/               # Phase 2: Foreign assets
│   ├── services/                  # Business logic services
│   │   ├── balance_sheet.py
│   │   ├── cash_flow.py
│   │   ├── portfolio_valuation.py
│   │   ├── tax_computation.py
│   │   └── bank_intelligence/     # Bank analysis suite
│   ├── analyzers/                 # Data analyzers
│   │   ├── mf_reconciler.py
│   │   └── mf_fy_analyzer.py
│   └── reports/                   # Report generators
│       ├── excel_reports.py
│       └── itr_export.py
│
├── tests/                         # All tests
│   ├── unit/                      # Unit tests (mirror src/ structure)
│   │   ├── test_core/
│   │   ├── test_parsers/
│   │   └── test_services/
│   ├── integration/               # Integration tests with real data
│   │   ├── conftest.py            # Shared fixtures
│   │   ├── test_sanjay_full_fy.py
│   │   ├── test_bank_intelligence/
│   │   └── golden_masters/        # Expected outputs for comparison
│   ├── regression/                # Regression test suite
│   ├── manual/                    # Manual test scripts
│   └── fixtures/                  # Synthetic test data
│       ├── bank/
│       ├── mf/
│       └── stock/
│
├── config/                        # Project-level configuration
│   ├── paths.json                 # Path structure configuration
│   ├── test_config.json           # Test behavior configuration
│   ├── mf_analyzer_config.json    # MF-specific settings
│   └── chart_of_accounts.json     # Account hierarchy definition
│
├── scripts/                       # Utility scripts
│   ├── setup_test_env.sh          # Test environment setup
│   ├── run_fy_reports.py          # Generate FY reports
│   └── migrate_db.py              # Database migrations
│
├── docs/                          # Documentation
│   ├── api/                       # API documentation
│   ├── user_guide/                # User documentation
│   └── tax_rules/                 # Tax rule documentation
│
├── Data/                          # → Symlink to actual data location
│
├── CLAUDE.md                      # AI assistant instructions
├── PFAS_PROJECT_CONTEXT.md        # This file
├── pytest.ini                     # Pytest configuration
├── pyproject.toml                 # Project metadata
└── requirements.txt               # Dependencies
```

### User Data Structure (CRITICAL)
```
Data/
├── Users/                         # One directory per user
│   ├── {Username}/                # e.g., Sanjay, Priya, Joint
│   │   │
│   │   ├── inbox/                 # NEW files to be processed
│   │   │   ├── Bank/
│   │   │   │   ├── ICICI/         # Bank-specific folders
│   │   │   │   ├── SBI/
│   │   │   │   └── HDFC/
│   │   │   ├── Mutual-Fund/
│   │   │   │   ├── CAMS/          # RTA-specific folders
│   │   │   │   │   ├── transactions/  # Optional: organize by type
│   │   │   │   │   └── holdings/
│   │   │   │   └── KARVY/
│   │   │   ├── Indian-Stocks/
│   │   │   │   ├── Zerodha/
│   │   │   │   ├── ICICIDirect/
│   │   │   │   └── Unlisted/
│   │   │   ├── EPF/
│   │   │   ├── NPS/
│   │   │   ├── PPF/
│   │   │   ├── SGB/
│   │   │   ├── FD-Bonds/
│   │   │   ├── Salary/
│   │   │   ├── USA-Stocks/        # Phase 2
│   │   │   └── Other-Income/
│   │   │
│   │   ├── archive/               # PROCESSED files (same structure as inbox)
│   │   │   └── {same structure}/  # Files moved here after processing
│   │   │
│   │   ├── config/                # User-specific configuration
│   │   │   ├── preferences.json   # Display, report preferences
│   │   │   ├── passwords.json     # Encrypted file passwords
│   │   │   ├── user_bank_config.json  # Bank parsing overrides
│   │   │   └── category_mappings.json # Custom categorizations
│   │   │
│   │   ├── reports/               # Generated reports
│   │   │   ├── FY2024-25/         # Organized by fiscal year
│   │   │   │   ├── Balance_Sheet_2024-25.xlsx
│   │   │   │   ├── Capital_Gains_2024-25.xlsx
│   │   │   │   └── Tax_Computation_2024-25.xlsx
│   │   │   └── adhoc/             # Ad-hoc reports
│   │   │
│   │   └── db/
│   │       ├── finance.db         # Main encrypted database
│   │       └── finance.db.bak     # Auto-backup
│   │
│   └── {AnotherUser}/             # Additional users
│
├── config/                        # Global configuration
│   ├── defaults.json              # Default preferences
│   └── paths.json                 # Path structure (copy of project config)
│
└── shared/                        # Shared reference data
    └── masters/
        ├── mf_schemes.json        # MF scheme master
        ├── stock_symbols.json     # Stock symbol master
        └── bank_ifsc.json         # Bank IFSC codes
```

### File Naming Conventions
```
# Processed files (in archive) - Prefixed with date and user
{YYYY-MM-DD}_{Username}_{Source}_{Description}_{Period}.{ext}

Examples:
2026-01-17_Sanjay_ICICI_SanjaySB_FY24-25.xls
2026-01-17_Sanjay_CAMS_CG_FY2024-25_v2.xlsx
2026-01-17_Sanjay_Zerodha_taxpnl_2024_2025.xlsx
2026-01-17_Sanjay_EPF_Passbook_2025.pdf

# Generated reports
{ReportType}_{FY}_{Username}_{Date}.xlsx

Examples:
Balance_Sheet_FY2024-25_Sanjay_2026-01-17.xlsx
Capital_Gains_FY2024-25_Sanjay_2026-01-17.xlsx
```

### File Flow Lifecycle
```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  DOWNLOAD   │────▶│   INBOX     │────▶│   PARSE     │────▶│   ARCHIVE   │
│  from bank/ │     │  (staging)  │     │  & ingest   │     │ (permanent) │
│  broker     │     │             │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                          │                    │                    │
                          │                    ▼                    │
                          │            ┌─────────────┐              │
                          │            │  DATABASE   │              │
                          │            │  (derived)  │              │
                          │            └─────────────┘              │
                          │                    │                    │
                          └────────────────────┴────────────────────┘
                                    Source of Truth:
                              Archive files + Config files
                              (Database can be regenerated)
```

---

## 4. Data Flow & Processing Pipeline

### Standard Processing Pipeline
```python
# 1. Discover files
resolver = PathResolver(root_path=PFAS_ROOT, user_name="Sanjay")
inbox_files = list(resolver.inbox().glob("Bank/ICICI/*.xls"))

# 2. Parse files
parser = ICICIExcelParser(conn)
for file_path in inbox_files:
    result = parser.parse(file_path)

    if result.success:
        # 3. Save to database (creates journal entries)
        parser.save_to_db(result, user_id=user_id)

        # 4. Move to archive
        archive_path = resolver.archive() / file_path.relative_to(resolver.inbox())
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(file_path, archive_path)
    else:
        # Move to failed folder
        failed_path = file_path.parent / "failed" / file_path.name
        shutil.move(file_path, failed_path)
```

### Ingestion Result Pattern
```python
@dataclass
class IngestionResult:
    """Standard result for all ingestion operations."""
    success: bool
    transactions_processed: int = 0
    transactions_inserted: int = 0
    transactions_skipped: int = 0  # Duplicates
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    source_files: List[str] = field(default_factory=list)

    def add_error(self, msg: str):
        self.errors.append(msg)
        self.success = False

    def add_warning(self, msg: str):
        self.warnings.append(msg)
```

---

## 5. Configuration Management

### Configuration Hierarchy (Precedence: High → Low)
```
1. Environment Variables     (PFAS_TEST_USER, PFAS_ROOT, etc.)
       ↓
2. User Config              (Data/Users/{user}/config/*.json)
       ↓
3. Global Defaults          (Data/config/defaults.json)
       ↓
4. Project Config           (config/*.json)
       ↓
5. Code Defaults            (hardcoded in classes)
```

### Configuration Files Reference

#### `config/paths.json` - Path Structure
```json
{
  "_comment": "All paths relative to PFAS_ROOT",
  "users_base": "Data/Users",
  "global": {
    "config_dir": "Data/config",
    "shared_masters": "Data/shared/masters"
  },
  "per_user": {
    "db_file": "db/finance.db",
    "db_backup_suffix": ".bak",
    "user_config_dir": "config",
    "inbox": "inbox",
    "archive": "archive",
    "reports": "reports"
  }
}
```

#### `config/test_config.json` - Test Behavior
```json
{
  "file_sources": {
    "primary": "inbox",
    "fallback_to_archive": true
  },
  "test_data": {
    "prefer_latest_files": true,
    "exclude_failed_folder": true
  },
  "users": {
    "default": "Sanjay"
  }
}
```

#### User Config: `preferences.json`
```json
{
  "reports": {
    "default_format": "xlsx",
    "include_timestamp": true,
    "auto_open": false
  },
  "financial_year": {
    "current": "2024-25",
    "start_month": 4
  },
  "display": {
    "currency_symbol": "₹",
    "decimal_places": 2,
    "date_format": "DD-MMM-YYYY",
    "negative_in_brackets": true
  },
  "capital_gains": {
    "grandfathering_date": "2018-01-31",
    "equity_ltcg_exemption": 125000
  }
}
```

#### User Config: `passwords.json`
```json
{
  "_comment": "Passwords for encrypted PDF files",
  "patterns": {
    "CAMS*.pdf": "password123",
    "EPF*.pdf": "epfpassword"
  },
  "files": {
    "specific_file.pdf": "specificpassword"
  }
}
```

#### User Config: `user_bank_config.json`
```json
{
  "user_name": "Sanjay",
  "bank_name": "ICICI",
  "account_type": "SAVINGS",
  "header_search_keywords": ["Value Date", "Transaction Remarks", "Balance"],
  "date_format": "%d/%m/%Y",
  "category_overrides": {
    "QUALCOMM": "SALARY",
    "RAMAKRISHNAN": "RENT_INCOME",
    "INT.PD": "SAVINGS_INTEREST",
    "ZERODHA": "STOCK_INVESTMENT"
  }
}
```

### Configuration Loading Pattern
```python
from pfas.core.paths import PathResolver

class ConfigurableService:
    def __init__(self, resolver: PathResolver):
        self.resolver = resolver
        self.config = self._load_config()

    def _load_config(self) -> dict:
        """Load config with fallback chain."""
        config = {}

        # Load defaults first
        defaults_path = self.resolver.global_config() / "defaults.json"
        if defaults_path.exists():
            config.update(json.loads(defaults_path.read_text()))

        # Override with user config
        user_config_path = self.resolver.user_config_dir() / "preferences.json"
        if user_config_path.exists():
            config.update(json.loads(user_config_path.read_text()))

        return config
```

---

## 6. Database Architecture

### Database Technology Stack
- **Engine**: SQLCipher 4.x (encrypted SQLite)
- **Encryption**: AES-256-CBC
- **Python Driver**: `pysqlcipher3` or `sqlcipher3`
- **Migrations**: Manual SQL scripts in `src/pfas/core/schema_*.sql`

### DatabaseManager Singleton Pattern
```python
from pfas.core.database import DatabaseManager

# Initialize (creates tables if not exist)
db = DatabaseManager()
conn = db.init(db_path="Data/Users/Sanjay/db/finance.db", password="secure_password")

# Use connection
cursor = conn.execute("SELECT * FROM accounts")

# Close when done
db.close()

# For tests - reset singleton
DatabaseManager.reset_instance()
```

### Core Schema

#### Accounting Foundation
```sql
-- Users table
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pan_encrypted BLOB NOT NULL,
    pan_salt BLOB NOT NULL,
    name TEXT NOT NULL,
    email TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Chart of Accounts (hierarchical)
CREATE TABLE accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT UNIQUE NOT NULL,        -- e.g., "1000", "1100", "1110"
    name TEXT NOT NULL,                -- e.g., "Assets", "Current Assets", "Bank"
    account_type TEXT NOT NULL,        -- ASSET, LIABILITY, EQUITY, INCOME, EXPENSE
    parent_id INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    display_order INTEGER,
    FOREIGN KEY (parent_id) REFERENCES accounts(id)
);

-- Journal Headers
CREATE TABLE journals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    description TEXT NOT NULL,
    reference TEXT,                    -- External reference number
    source_type TEXT,                  -- BANK, MF, STOCK, SALARY, MANUAL
    source_id INTEGER,                 -- Link to source transaction
    user_id INTEGER NOT NULL,
    is_reversed BOOLEAN DEFAULT FALSE,
    reversal_of INTEGER,               -- Link to reversed journal
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (reversal_of) REFERENCES journals(id)
);

-- Journal Entries (double-entry)
CREATE TABLE journal_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    journal_id INTEGER NOT NULL,
    account_id INTEGER NOT NULL,
    debit DECIMAL(15,2) DEFAULT 0,
    credit DECIMAL(15,2) DEFAULT 0,
    narration TEXT,
    FOREIGN KEY (journal_id) REFERENCES journals(id) ON DELETE CASCADE,
    FOREIGN KEY (account_id) REFERENCES accounts(id),
    CHECK (debit >= 0 AND credit >= 0),
    CHECK (NOT (debit > 0 AND credit > 0))  -- Either debit or credit, not both
);

-- Ingestion Log (track processed files)
CREATE TABLE ingestion_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    source_file TEXT NOT NULL,
    file_hash TEXT NOT NULL,           -- SHA256 for deduplication
    asset_type TEXT NOT NULL,
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_skipped INTEGER DEFAULT 0,
    status TEXT DEFAULT 'SUCCESS',     -- SUCCESS, PARTIAL, FAILED
    error_message TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, file_hash)
);
```

#### Asset-Specific Tables (Examples)
```sql
-- Mutual Fund Transactions
CREATE TABLE mf_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    folio_id INTEGER NOT NULL,
    date DATE NOT NULL,
    transaction_type TEXT NOT NULL,    -- PURCHASE, REDEMPTION, SWITCH_IN, SWITCH_OUT, DIVIDEND
    units DECIMAL(15,4) NOT NULL,
    nav DECIMAL(15,4) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    stamp_duty DECIMAL(10,2) DEFAULT 0,
    stt DECIMAL(10,2) DEFAULT 0,
    source_file TEXT,
    journal_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, folio_id, date, transaction_type, units, amount),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (folio_id) REFERENCES mf_folios(id),
    FOREIGN KEY (journal_id) REFERENCES journals(id)
);

-- Bank Transactions
CREATE TABLE bank_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    account_id INTEGER NOT NULL,       -- FK to bank_accounts
    txn_date DATE NOT NULL,
    value_date DATE,
    description TEXT NOT NULL,
    reference TEXT,
    debit DECIMAL(15,2) DEFAULT 0,
    credit DECIMAL(15,2) DEFAULT 0,
    balance DECIMAL(15,2),
    category TEXT,
    sub_category TEXT,
    is_income BOOLEAN DEFAULT FALSE,
    source_file TEXT,
    txn_hash TEXT UNIQUE,              -- For deduplication
    journal_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (journal_id) REFERENCES journals(id)
);
```

### Deduplication Strategy
```python
def generate_transaction_hash(txn: Transaction) -> str:
    """Generate unique hash for deduplication."""
    components = [
        txn.date.isoformat(),
        txn.description[:50],
        str(txn.amount),
        txn.reference or ""
    ]
    return hashlib.sha256("|".join(components).encode()).hexdigest()

def insert_if_not_duplicate(conn, txn: Transaction) -> bool:
    """Insert transaction if not already exists."""
    txn_hash = generate_transaction_hash(txn)

    try:
        conn.execute("""
            INSERT INTO bank_transactions (txn_hash, ...)
            VALUES (?, ...)
        """, (txn_hash, ...))
        return True
    except sqlite3.IntegrityError:
        # Duplicate - txn_hash already exists
        return False
```

---

## 7. Double-Entry Accounting Model

### Fundamental Principle
```
For every transaction: Sum(Debits) = Sum(Credits)
```

### Account Types and Normal Balances
| Type | Normal Balance | Debit Effect | Credit Effect | Examples |
|------|----------------|--------------|---------------|----------|
| **ASSET** | Debit | Increase | Decrease | Bank, MF, Stocks, EPF |
| **LIABILITY** | Credit | Decrease | Increase | Loans, Credit Card |
| **EQUITY** | Credit | Decrease | Increase | Capital, Retained Earnings |
| **INCOME** | Credit | Decrease | Increase | Salary, Interest, Dividends |
| **EXPENSE** | Debit | Increase | Decrease | Taxes, Fees, Charges |

### Chart of Accounts Structure
```
1000 - Assets
├── 1100 - Current Assets
│   ├── 1110 - Bank Accounts
│   │   ├── 1111 - ICICI Savings
│   │   └── 1112 - SBI Savings
│   └── 1120 - Investments
│       ├── 1121 - Mutual Funds - Equity
│       ├── 1122 - Mutual Funds - Debt
│       ├── 1123 - Indian Stocks
│       └── 1124 - Foreign Stocks
├── 1200 - Retirement Assets
│   ├── 1210 - EPF
│   ├── 1220 - PPF
│   └── 1230 - NPS
└── 1300 - Other Assets
    └── 1310 - SGB

2000 - Liabilities
├── 2100 - Current Liabilities
│   └── 2110 - Credit Card Payable
└── 2200 - Long-term Liabilities
    └── 2210 - Home Loan

3000 - Equity
└── 3100 - Owner's Capital

4000 - Income
├── 4100 - Earned Income
│   ├── 4110 - Salary
│   └── 4120 - Bonus
├── 4200 - Investment Income
│   ├── 4210 - Interest - Savings
│   ├── 4220 - Interest - FD
│   ├── 4230 - Dividends
│   └── 4240 - Rental Income
└── 4300 - Capital Gains
    ├── 4310 - STCG - Equity
    ├── 4320 - LTCG - Equity
    ├── 4330 - STCG - Debt
    └── 4340 - LTCG - Debt

5000 - Expenses
├── 5100 - Investment Expenses
│   ├── 5110 - Brokerage
│   ├── 5120 - STT
│   └── 5130 - Stamp Duty
└── 5200 - Tax Expenses
    ├── 5210 - TDS
    └── 5220 - Advance Tax
```

### Journal Entry Examples

#### Example 1: Salary Credit
```
Date: 2024-06-30
Description: Salary - June 2024

┌─────────────────────────┬────────────┬────────────┐
│ Account                 │ Debit (₹)  │ Credit (₹) │
├─────────────────────────┼────────────┼────────────┤
│ 1111 ICICI Savings      │ 1,50,000   │            │
│ 4110 Salary Income      │            │ 1,50,000   │
├─────────────────────────┼────────────┼────────────┤
│ TOTAL                   │ 1,50,000   │ 1,50,000   │
└─────────────────────────┴────────────┴────────────┘
```

#### Example 2: Mutual Fund Purchase
```
Date: 2024-06-15
Description: MF Purchase - HDFC Equity Fund

┌─────────────────────────┬────────────┬────────────┐
│ Account                 │ Debit (₹)  │ Credit (₹) │
├─────────────────────────┼────────────┼────────────┤
│ 1121 MF - Equity        │ 50,000     │            │
│ 5130 Stamp Duty         │ 2.50       │            │
│ 1111 ICICI Savings      │            │ 50,002.50  │
├─────────────────────────┼────────────┼────────────┤
│ TOTAL                   │ 50,002.50  │ 50,002.50  │
└─────────────────────────┴────────────┴────────────┘
```

#### Example 3: Stock Sale with LTCG
```
Date: 2024-06-20
Description: Sold 100 RELIANCE @ ₹2,500 (Cost: ₹2,000)

┌─────────────────────────┬────────────┬────────────┐
│ Account                 │ Debit (₹)  │ Credit (₹) │
├─────────────────────────┼────────────┼────────────┤
│ 1111 ICICI Savings      │ 2,49,750   │            │ (Net proceeds)
│ 5110 Brokerage          │ 50         │            │
│ 5120 STT                │ 200        │            │
│ 1123 Indian Stocks      │            │ 2,00,000   │ (Cost basis)
│ 4320 LTCG - Equity      │            │ 50,000     │ (Gain)
├─────────────────────────┼────────────┼────────────┤
│ TOTAL                   │ 2,50,000   │ 2,50,000   │
└─────────────────────────┴────────────┴────────────┘
```

### Journal Entry Creation Code
```python
def create_balanced_journal(
    conn,
    date: date,
    description: str,
    entries: List[Tuple[str, Decimal, Decimal, str]],  # (account_code, debit, credit, narration)
    user_id: int,
    source_type: str = None,
    source_id: int = None
) -> int:
    """Create a balanced journal entry."""

    # Validate balance
    total_debit = sum(e[1] for e in entries)
    total_credit = sum(e[2] for e in entries)

    if abs(total_debit - total_credit) > Decimal("0.01"):
        raise ValueError(
            f"Journal must balance. Debit: {total_debit}, Credit: {total_credit}"
        )

    # Create journal header
    cursor = conn.execute("""
        INSERT INTO journals (date, description, source_type, source_id, user_id)
        VALUES (?, ?, ?, ?, ?)
    """, (date.isoformat(), description, source_type, source_id, user_id))

    journal_id = cursor.lastrowid

    # Create entries
    for account_code, debit, credit, narration in entries:
        account_id = get_account_id_by_code(conn, account_code)
        conn.execute("""
            INSERT INTO journal_entries (journal_id, account_id, debit, credit, narration)
            VALUES (?, ?, ?, ?, ?)
        """, (journal_id, account_id, str(debit), str(credit), narration))

    conn.commit()
    return journal_id
```

---

## 8. Parser Development Guidelines

### Parser Architecture
```
                    ┌─────────────────────────────────────┐
                    │           BaseParser                │
                    │  - conn: Connection                 │
                    │  - user_id: int                     │
                    │  + parse(file_path) → ParseResult   │
                    │  + save_to_db(result) → int         │
                    │  + create_journal_entries()         │
                    └─────────────────────────────────────┘
                                     △
                                     │
          ┌──────────────────────────┼──────────────────────────┐
          │                          │                          │
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│   BankParser    │      │    MFParser     │      │  StockParser    │
│  - icici.py     │      │  - cams.py      │      │  - zerodha.py   │
│  - sbi.py       │      │  - karvy.py     │      │  - icici_d.py   │
│  - hdfc.py      │      │  - cas_pdf.py   │      │                 │
└─────────────────┘      └─────────────────┘      └─────────────────┘
```

### Standard ParseResult
```python
@dataclass
class ParseResult:
    """Standard result returned by all parsers."""
    success: bool
    transactions: List[Any] = field(default_factory=list)
    account: Optional[Any] = None  # Account info if applicable
    summary: Optional[Any] = None  # Summary data if applicable
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    source_file: str = ""

    def add_error(self, msg: str):
        self.errors.append(msg)
        self.success = False

    def add_warning(self, msg: str):
        self.warnings.append(msg)
```

### Parser Implementation Template
```python
"""
Parser for {Asset Type} - {Source Name}

Supports:
- File formats: PDF, Excel, CSV
- Date range: Full history
- Features: ...

Usage:
    parser = MyAssetParser(conn)
    result = parser.parse(file_path)
    if result.success:
        parser.save_to_db(result, user_id=1)
"""

from dataclasses import dataclass
from decimal import Decimal
from datetime import date
from pathlib import Path
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class MyTransaction:
    """Transaction model for this asset type."""
    date: date
    description: str
    amount: Decimal
    # ... other fields


class MyAssetParser:
    """Parser for My Asset statements."""

    def __init__(self, conn, user_id: int = 1):
        self.conn = conn
        self.user_id = user_id

    def parse(self, file_path: Path, password: Optional[str] = None) -> ParseResult:
        """
        Parse statement file.

        Args:
            file_path: Path to statement file
            password: Password for encrypted files

        Returns:
            ParseResult with transactions and any errors
        """
        result = ParseResult(success=True, source_file=str(file_path))

        try:
            # Detect file type and delegate
            suffix = file_path.suffix.lower()
            if suffix == '.pdf':
                transactions = self._parse_pdf(file_path, password)
            elif suffix in ('.xls', '.xlsx'):
                transactions = self._parse_excel(file_path)
            elif suffix == '.csv':
                transactions = self._parse_csv(file_path)
            else:
                result.add_error(f"Unsupported file format: {suffix}")
                return result

            result.transactions = transactions
            logger.info(f"Parsed {len(transactions)} transactions from {file_path.name}")

        except Exception as e:
            result.add_error(f"Parse error: {str(e)}")
            logger.exception(f"Failed to parse {file_path}")

        return result

    def save_to_db(self, result: ParseResult, user_id: int) -> int:
        """Save parsed data to database with journal entries."""
        if not result.success:
            return 0

        inserted = 0
        for txn in result.transactions:
            if self._insert_transaction(txn, user_id, result.source_file):
                inserted += 1

        self.conn.commit()
        return inserted

    def _insert_transaction(self, txn: MyTransaction, user_id: int, source_file: str) -> bool:
        """Insert single transaction with deduplication."""
        # Generate hash for deduplication
        txn_hash = self._generate_hash(txn)

        try:
            # Insert transaction
            cursor = self.conn.execute("""
                INSERT INTO my_transactions (user_id, date, amount, ..., txn_hash, source_file)
                VALUES (?, ?, ?, ..., ?, ?)
            """, (user_id, txn.date.isoformat(), str(txn.amount), ..., txn_hash, source_file))

            txn_id = cursor.lastrowid

            # Create journal entry
            self._create_journal_entry(txn, user_id, txn_id)

            return True

        except sqlite3.IntegrityError:
            # Duplicate transaction
            return False

    def _create_journal_entry(self, txn: MyTransaction, user_id: int, source_id: int):
        """Create balanced journal entry for transaction."""
        # Implement double-entry logic here
        pass
```

### Parser Checklist
- [ ] Inherits from base or follows standard pattern
- [ ] Returns `ParseResult` with success/errors/warnings
- [ ] Supports all relevant file formats (PDF, Excel, CSV)
- [ ] Handles encrypted files via password parameter
- [ ] Implements deduplication with transaction hash
- [ ] Creates balanced journal entries
- [ ] Logs appropriately (info, warning, error)
- [ ] Has comprehensive unit tests
- [ ] Has integration tests with real files

---

## 9. Service Layer Architecture

### Service Design Principles
1. **Stateless**: All state in database, services are pure functions
2. **Single Responsibility**: One service per domain
3. **Dependency Injection**: Receive connection, don't create
4. **Return Dataclasses**: Not dicts, for type safety

### Service Template
```python
from dataclasses import dataclass
from decimal import Decimal
from datetime import date
from typing import List, Optional

@dataclass
class ServiceResult:
    """Result from service operation."""
    # Define result fields
    pass

class MyService:
    """Service for {domain} operations."""

    def __init__(self, conn):
        self.conn = conn

    def get_summary(self, user_id: int, as_of: date = None) -> ServiceResult:
        """Get summary for user."""
        pass

    def calculate_something(self, user_id: int, fy: str) -> Decimal:
        """Calculate something for fiscal year."""
        pass
```

### Key Services Reference
| Service | Purpose | Key Methods |
|---------|---------|-------------|
| `BalanceSheetService` | Generate balance sheet | `get_balance_sheet(user_id, as_of)` |
| `CashFlowStatementService` | Cash flow statement | `get_cash_flow_statement(user_id, fy)` |
| `PortfolioValuationService` | Portfolio metrics | `get_portfolio_summary()`, `calculate_xirr()` |
| `CapitalGainsCalculator` | FIFO capital gains | `calculate_gains(user_id, fy)` |
| `TaxComputationService` | Tax computation | `compute_tax(user_id, fy)` |
| `BankIntelligenceAnalyzer` | Bank statement analysis | `scan_and_ingest_all()` |

---

## 10. Testing Strategy

### Test Pyramid
```
                    ┌───────────┐
                    │   E2E     │  ← Few, slow, comprehensive
                    │   Tests   │
                    ├───────────┤
                    │Integration│  ← Medium count, real data
                    │   Tests   │
                    ├───────────┤
                    │   Unit    │  ← Many, fast, isolated
                    │   Tests   │
                    └───────────┘
```

### Test Directory Structure
```
tests/
├── unit/                          # Fast, isolated tests
│   ├── test_core/
│   │   ├── test_database.py
│   │   ├── test_paths.py
│   │   └── test_accounts.py
│   ├── test_parsers/
│   │   ├── test_bank/
│   │   │   ├── test_icici.py
│   │   │   └── test_sbi.py
│   │   ├── test_mf/
│   │   │   ├── test_cams.py
│   │   │   └── test_fifo.py
│   │   └── test_stock/
│   └── test_services/
│       ├── test_balance_sheet.py
│       └── test_capital_gains.py
│
├── integration/                   # Tests with real user data
│   ├── conftest.py                # Shared fixtures (PathResolver, etc.)
│   ├── test_sanjay_full_fy.py     # Full FY test for user
│   ├── test_bank_intelligence/
│   │   └── test_sanjay_integration.py
│   ├── test_mf_integration.py
│   ├── test_stock_integration.py
│   └── golden_masters/            # Expected outputs
│       ├── balance_sheet_fy2024.json
│       └── capital_gains_fy2024.json
│
├── regression/                    # Regression tests
│   └── test_issue_fixes.py
│
├── fixtures/                      # Synthetic test data
│   ├── bank/
│   │   ├── sample_icici.xls
│   │   └── sample_sbi.csv
│   ├── mf/
│   │   └── sample_cams.xlsx
│   └── stock/
│       └── sample_zerodha.xlsx
│
└── conftest.py                    # Root-level fixtures
```

### Test Fixtures Pattern
```python
# tests/integration/conftest.py

import os
import pytest
from pathlib import Path
from pfas.core.paths import PathResolver
from pfas.core.database import DatabaseManager

# Configuration from environment
DEFAULT_USER = "Sanjay"
TEST_USER = os.getenv("PFAS_TEST_USER", DEFAULT_USER)
TEST_BANK = os.getenv("PFAS_TEST_BANK", "ICICI")
PFAS_ROOT = os.getenv("PFAS_ROOT", str(Path.cwd()))
USE_ARCHIVE = os.getenv("PFAS_TEST_USE_ARCHIVE", "true").lower() == "true"


@pytest.fixture(scope="session")
def test_root() -> Path:
    """Project root directory."""
    return Path(PFAS_ROOT)


@pytest.fixture(scope="session", params=[TEST_USER])
def path_resolver(request, test_root) -> PathResolver:
    """PathResolver for test user."""
    user = request.param
    resolver = PathResolver(root_path=test_root, user_name=user)

    if not resolver.user_dir.exists():
        pytest.skip(f"User directory not found: {resolver.user_dir}")

    return resolver


@pytest.fixture(scope="session")
def test_db():
    """In-memory database for tests."""
    DatabaseManager.reset_instance()
    db = DatabaseManager()
    conn = db.init(":memory:", "test_password")
    setup_chart_of_accounts(conn)

    # Create test user
    conn.execute("""
        INSERT INTO users (id, pan_encrypted, pan_salt, name, email)
        VALUES (1, X'00', X'00', 'Test User', 'test@example.com')
    """)
    conn.commit()

    yield conn

    db.close()
    DatabaseManager.reset_instance()


def find_files_in_path(
    path_resolver: PathResolver,
    asset_subpath: str,
    extensions: List[str],
    pattern: str = '*',
    exclude_patterns: List[str] = None
) -> List[Path]:
    """Find files in inbox with archive fallback."""
    inbox_path = path_resolver.inbox() / asset_subpath
    archive_path = path_resolver.archive() / asset_subpath

    files = _search_directory(inbox_path, extensions, pattern, exclude_patterns)

    if not files and USE_ARCHIVE:
        files = _search_directory(archive_path, extensions, pattern, exclude_patterns)

    return files
```

### Unit Test Template
```python
"""Unit tests for {Module}."""

import pytest
from decimal import Decimal
from datetime import date
from pfas.parsers.bank.icici import ICICIParser


class TestICICIParser:
    """Tests for ICICI bank statement parser."""

    @pytest.fixture
    def parser(self, test_db):
        """Create parser instance."""
        return ICICIParser(test_db)

    @pytest.fixture
    def sample_file(self):
        """Path to sample test file."""
        return Path(__file__).parent / "fixtures" / "sample_icici.xls"

    def test_parse_valid_file(self, parser, sample_file):
        """Test parsing valid ICICI statement."""
        result = parser.parse(sample_file)

        assert result.success
        assert len(result.transactions) > 0
        assert result.errors == []

    def test_parse_detects_header(self, parser, sample_file):
        """Test header detection with fuzzy matching."""
        result = parser.parse(sample_file)

        # Verify transactions have required fields
        for txn in result.transactions:
            assert txn.date is not None
            assert txn.amount != Decimal("0")

    def test_handles_invalid_file(self, parser, tmp_path):
        """Test graceful handling of invalid file."""
        invalid_file = tmp_path / "invalid.xls"
        invalid_file.write_text("not a valid excel file")

        result = parser.parse(invalid_file)

        assert not result.success
        assert len(result.errors) > 0

    def test_deduplication(self, parser, sample_file, test_db):
        """Test that duplicate transactions are skipped."""
        # First ingestion
        result1 = parser.parse(sample_file)
        inserted1 = parser.save_to_db(result1, user_id=1)

        # Second ingestion (same file)
        result2 = parser.parse(sample_file)
        inserted2 = parser.save_to_db(result2, user_id=1)

        assert inserted1 > 0
        assert inserted2 == 0  # All duplicates
```

### Integration Test Template
```python
"""Integration tests for {Asset} with real data."""

import pytest
from pathlib import Path
from tests.integration.conftest import find_files_in_path


class TestAssetIntegration:
    """Integration tests using real user data."""

    def test_parse_real_files(self, path_resolver, test_db):
        """Test parsing actual user files."""
        # Find files (inbox with archive fallback)
        files = find_files_in_path(
            path_resolver,
            "Asset-Type/Subtype",
            ['.xlsx', '.pdf']
        )

        if not files:
            pytest.skip("No files found in inbox or archive")

        parser = AssetParser(test_db)

        for file_path in files:
            result = parser.parse(file_path)

            assert result.success, f"Failed to parse {file_path.name}: {result.errors}"
            assert len(result.transactions) > 0

    def test_journal_entries_balanced(self, path_resolver, test_db):
        """Verify all journal entries are balanced."""
        # ... parse files first

        # Check balance
        cursor = test_db.execute("""
            SELECT j.id, j.description,
                   SUM(je.debit) as total_debit,
                   SUM(je.credit) as total_credit
            FROM journals j
            JOIN journal_entries je ON j.id = je.journal_id
            GROUP BY j.id
            HAVING ABS(total_debit - total_credit) > 0.01
        """)

        unbalanced = cursor.fetchall()
        assert len(unbalanced) == 0, f"Found {len(unbalanced)} unbalanced journals"
```

### Running Tests
```bash
# Setup environment
source scripts/setup_test_env.sh

# Run all tests
pytest tests/ -v

# Run unit tests only (fast)
pytest tests/unit/ -v

# Run integration tests
pytest tests/integration/ -v

# Run for specific user
PFAS_TEST_USER=Priya pytest tests/integration/ -v

# Run specific test file
pytest tests/integration/test_sanjay_full_fy.py -v

# Run with coverage
pytest tests/ --cov=src/pfas --cov-report=html

# Run specific test class
pytest tests/unit/test_parsers/test_bank/test_icici.py::TestICICIParser -v

# Skip slow tests
pytest tests/ -v -m "not slow"
```

### Test Coverage Requirements
| Category | Minimum Coverage | Notes |
|----------|------------------|-------|
| Core modules | 95% | database, paths, accounts |
| Parsers | 90% | All file formats tested |
| Services | 85% | Key calculations tested |
| Overall | 90% | Target for CI/CD gate |

---

## 11. Usage Guidelines

### Adding a New User

1. **Create user directory structure**:
```bash
mkdir -p Data/Users/{Username}/{inbox,archive,config,reports,db}
mkdir -p Data/Users/{Username}/inbox/{Bank,Mutual-Fund,Indian-Stocks,EPF,NPS,PPF,Salary}
```

2. **Create user configuration**:
```bash
# Copy default preferences
cp Data/config/defaults.json Data/Users/{Username}/config/preferences.json

# Create passwords file
echo '{"patterns": {}, "files": {}}' > Data/Users/{Username}/config/passwords.json
```

3. **Initialize database**:
```python
from pfas.core.database import DatabaseManager
from pfas.core.accounts import setup_chart_of_accounts

db = DatabaseManager()
conn = db.init("Data/Users/{Username}/db/finance.db", "user_password")
setup_chart_of_accounts(conn)
db.close()
```

### Adding Support for a New Bank

1. **Create parser file**: `src/pfas/parsers/bank/{bank_name}.py`

2. **Follow parser template** (see Section 8)

3. **Register in `__init__.py`**:
```python
# src/pfas/parsers/bank/__init__.py
from .new_bank import NewBankParser

__all__ = [..., "NewBankParser"]
```

4. **Add unit tests**: `tests/unit/test_parsers/test_bank/test_new_bank.py`

5. **Add sample test data**: `tests/fixtures/bank/sample_new_bank.xls`

6. **Update documentation**

### Adding a New Asset Class

1. **Design data model** (transactions, holdings, etc.)

2. **Add database schema** in `src/pfas/core/database.py`

3. **Create parser module**: `src/pfas/parsers/{asset_type}/`

4. **Create service** (if needed): `src/pfas/services/{asset_type}_service.py`

5. **Update chart of accounts** (add account codes)

6. **Add comprehensive tests**

7. **Update user directory structure** in documentation

### Daily Workflow

```bash
# 1. Download statements from bank/broker websites
# 2. Place in appropriate inbox folder:
#    Data/Users/Sanjay/inbox/Bank/ICICI/statement.xls

# 3. Run ingestion
python -m pfas.services.bank_intelligence.run

# 4. Generate reports
python scripts/run_fy_reports.py --user Sanjay --fy 2024-25

# 5. Review reports in:
#    Data/Users/Sanjay/reports/FY2024-25/
```

### Generating Tax Reports

```python
from pfas.core.paths import PathResolver
from pfas.core.database import DatabaseManager
from pfas.services import TaxComputationService

resolver = PathResolver(root_path=".", user_name="Sanjay")
db = DatabaseManager()
conn = db.init(str(resolver.db_path()), "password")

service = TaxComputationService(conn)
tax_report = service.compute_tax(user_id=1, fy="2024-25")

print(f"Total Income: {tax_report.total_income}")
print(f"Taxable Income: {tax_report.taxable_income}")
print(f"Tax Payable: {tax_report.tax_payable}")
```

---

## 12. Scalability Patterns

### Multi-User Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                     PFAS Multi-User Design                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   User A (Sanjay)         User B (Priya)         User C (Joint) │
│   ┌─────────────┐         ┌─────────────┐        ┌─────────────┐│
│   │ inbox/      │         │ inbox/      │        │ inbox/      ││
│   │ archive/    │         │ archive/    │        │ archive/    ││
│   │ config/     │         │ config/     │        │ config/     ││
│   │ db/finance  │         │ db/finance  │        │ db/finance  ││
│   │   .db       │         │   .db       │        │   .db       ││
│   └─────────────┘         └─────────────┘        └─────────────┘│
│          │                       │                      │        │
│          └───────────────────────┴──────────────────────┘        │
│                                  │                               │
│                    ┌─────────────────────────┐                   │
│                    │   Shared Resources      │                   │
│                    │   - MF Scheme Master    │                   │
│                    │   - Stock Symbol Master │                   │
│                    │   - IFSC Codes          │                   │
│                    └─────────────────────────┘                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Extension Points

#### Adding New File Formats
```python
class MyParser:
    def parse(self, file_path: Path) -> ParseResult:
        suffix = file_path.suffix.lower()

        # Delegate to format-specific method
        format_handlers = {
            '.pdf': self._parse_pdf,
            '.xlsx': self._parse_excel,
            '.xls': self._parse_excel,
            '.csv': self._parse_csv,
            '.json': self._parse_json,  # Add new format
        }

        handler = format_handlers.get(suffix)
        if not handler:
            return ParseResult(success=False, errors=[f"Unsupported format: {suffix}"])

        return handler(file_path)
```

#### Adding New Tax Rules
```python
# src/pfas/core/tax_rules.py

class TaxRuleEngine:
    """Extensible tax rule engine."""

    def __init__(self, assessment_year: str):
        self.ay = assessment_year
        self.rules = self._load_rules(assessment_year)

    def _load_rules(self, ay: str) -> dict:
        """Load tax rules for assessment year."""
        rules_file = Path(f"config/tax_rules/ay_{ay}.json")
        if rules_file.exists():
            return json.loads(rules_file.read_text())
        return self._default_rules()

    def get_ltcg_threshold(self, asset_type: str) -> int:
        """Get LTCG holding period threshold in months."""
        return self.rules["ltcg_thresholds"].get(asset_type, 24)

    def get_exemption_limit(self, exemption_type: str) -> Decimal:
        """Get exemption limit amount."""
        return Decimal(self.rules["exemptions"].get(exemption_type, "0"))
```

#### Plugin Architecture for Parsers
```python
# src/pfas/parsers/registry.py

class ParserRegistry:
    """Registry for dynamically loading parsers."""

    _parsers: Dict[str, Type[BaseParser]] = {}

    @classmethod
    def register(cls, asset_type: str, file_pattern: str):
        """Decorator to register parser."""
        def decorator(parser_class):
            key = f"{asset_type}:{file_pattern}"
            cls._parsers[key] = parser_class
            return parser_class
        return decorator

    @classmethod
    def get_parser(cls, asset_type: str, file_path: Path, conn) -> BaseParser:
        """Get appropriate parser for file."""
        for key, parser_class in cls._parsers.items():
            at, pattern = key.split(":")
            if at == asset_type and fnmatch(file_path.name, pattern):
                return parser_class(conn)
        raise ValueError(f"No parser found for {asset_type}: {file_path}")

# Usage
@ParserRegistry.register("bank", "ICICI*.xls")
class ICICIExcelParser(BaseParser):
    pass
```

---

## 13. Security & Compliance

### Data Protection

#### Sensitive Data Classification
| Data Type | Classification | Protection |
|-----------|----------------|------------|
| PAN Number | HIGH | Encrypted in DB (AES-256) |
| Bank Account Numbers | HIGH | Encrypted in DB |
| Transaction Data | MEDIUM | Database encryption |
| File Passwords | HIGH | Stored in user config (restricted access) |

#### Encryption Pattern
```python
from pfas.core.encryption import encrypt_field, decrypt_field

# Encrypt sensitive field
pan_encrypted, pan_salt = encrypt_field(pan_number, master_key)

# Store encrypted
conn.execute("""
    INSERT INTO users (pan_encrypted, pan_salt, name)
    VALUES (?, ?, ?)
""", (pan_encrypted, pan_salt, name))

# Decrypt when needed
pan_number = decrypt_field(pan_encrypted, pan_salt, master_key)
```

#### Password Management
```python
# GOOD: Use PathResolver
password = path_resolver.get_file_password(file_path)

# BAD: Never hardcode
password = "secret123"  # NEVER DO THIS
```

### Audit Trail
- All database changes tracked via `created_at` timestamps
- Journal entries link to source transactions
- Ingestion log tracks all processed files
- File hashes prevent re-processing

---

## 14. Pre-Implementation Checklist

Before implementing any new feature, verify compliance with:

### Architecture Alignment
- [ ] Uses `PathResolver` for all file paths (no hardcoding)
- [ ] Supports inbox/archive directory structure
- [ ] Works for any user (not hardcoded to specific user)
- [ ] Configuration is externalized (user/global config files)
- [ ] Follows existing module structure

### Database
- [ ] Schema changes use `CREATE TABLE IF NOT EXISTS`
- [ ] Creates balanced journal entries (debits = credits)
- [ ] Implements deduplication (transaction hash)
- [ ] Uses parameterized queries (no SQL injection)
- [ ] Foreign keys reference existing tables

### Parser (if applicable)
- [ ] Returns standard `ParseResult`
- [ ] Supports multiple file formats (PDF, Excel, CSV)
- [ ] Handles encrypted files via password
- [ ] Creates journal entries for transactions
- [ ] Implements deduplication
- [ ] Logs appropriately

### Testing
- [ ] Unit tests written (90% coverage target)
- [ ] Integration tests with real data
- [ ] Tests use shared fixtures from conftest.py
- [ ] Tests verify journal balance
- [ ] Tests work with different users (parameterized)

### Code Quality
- [ ] Type hints on all functions
- [ ] Docstrings on public functions
- [ ] Follows PEP 8 style
- [ ] No hardcoded paths or credentials
- [ ] Appropriate logging

### Documentation
- [ ] Code is self-documenting with clear names
- [ ] Complex logic has comments
- [ ] Configuration options documented
- [ ] Usage examples provided

---

## Quick Reference

### Environment Setup
```bash
# One-time setup
source scripts/setup_test_env.sh

# Verify
echo "PFAS_ROOT: $PFAS_ROOT"
echo "PFAS_TEST_USER: $PFAS_TEST_USER"
```

### Common Imports
```python
from pfas.core.paths import PathResolver
from pfas.core.database import DatabaseManager
from pfas.core.accounts import setup_chart_of_accounts
```

### Running Tests
```bash
pytest tests/ -v                    # All tests
pytest tests/unit/ -v               # Unit only
pytest tests/integration/ -v        # Integration only
pytest tests/ --cov=src/pfas        # With coverage
```

### Key Files
| File | Purpose |
|------|---------|
| `config/paths.json` | Directory structure configuration |
| `config/test_config.json` | Test behavior configuration |
| `tests/integration/conftest.py` | Shared test fixtures |
| `scripts/setup_test_env.sh` | Environment setup script |

---

*Document Version: 2.0*
*Last Updated: January 2026*
*Maintainer: PFAS Development Team*
