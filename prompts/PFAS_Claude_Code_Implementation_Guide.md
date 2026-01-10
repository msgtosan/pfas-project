# PFAS Implementation Guide with Claude Code
## Using Sub-Agents for Modular Development & Testing

---

## Table of Contents
1. [Project Setup](#1-project-setup)
2. [Directory Structure](#2-directory-structure)
3. [Sub-Agent Architecture](#3-sub-agent-architecture)
4. [Implementation Workflow](#4-implementation-workflow)
5. [Module-by-Module Implementation](#5-module-by-module-implementation)
6. [Testing Strategy](#6-testing-strategy)
7. [Integration Verification](#7-integration-verification)
8. [Sample Commands](#8-sample-commands)

---

## 1. Project Setup

### Prerequisites
```bash
# Install Claude Code CLI (if not installed)
npm install -g @anthropic-ai/claude-code

# Verify installation
claude --version

# WSL/Ubuntu Setup
sudo apt update
sudo apt install python3.11 python3.11-venv python3-pip
sudo apt install default-jdk  # For tabula-py
```

### Initialize Project
```bash
# Create project directory
mkdir -p ~/pfas-project
cd ~/pfas-project

# Initialize git
git init
git branch -M main

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Create requirements.txt
cat > requirements.txt << 'EOF'
# PDF Processing
PyPDF2>=3.0.0
pdfplumber>=0.9.0
tabula-py>=2.7.0

# Excel/Data Processing
openpyxl>=3.1.0
pandas>=2.0.0
xlrd>=2.0.1

# Database
sqlcipher3>=0.5.0

# Security
cryptography>=41.0.0
keyring>=24.0.0

# CLI
click>=8.1.0
rich>=13.0.0

# Testing
pytest>=7.4.0
pytest-cov>=4.1.0
pytest-asyncio>=0.21.0

# Validation
jsonschema>=4.19.0
EOF

pip install -r requirements.txt
```

### Create CLAUDE.md (Project Instructions File)
```bash
cat > CLAUDE.md << 'EOF'
# PFAS Project - Claude Code Instructions

## Project Overview
Personal Financial Accounting System for Indian Tax Residents
- 18 Asset Classes
- Phase 1: Indian Assets + Salary/Form16
- Phase 2: Foreign Assets + DTAA + ITR-2 Export

## Key Technical Decisions
1. RSU Tax Credit: NEGATIVE deduction in payslip = credit when shares vest
2. Currency: SBI TT Buying Rate for USD→INR
3. LTCG: 12 months (Indian equity), 24 months (foreign/unlisted)
4. Database: SQLCipher (AES-256 encrypted SQLite)

## Testing Requirements
- Every module must have unit tests
- Test coverage target: 80%
- Integration tests after each module
- Use pytest with fixtures

## Code Standards
- Type hints required
- Docstrings for all public functions
- Use dataclasses for models
- Follow PEP 8

## File Locations
- Source: src/pfas/
- Tests: tests/
- Test Data: tests/fixtures/
- Documentation: docs/
EOF
```

---

## 2. Directory Structure

```
pfas-project/
├── CLAUDE.md                    # Claude Code instructions
├── requirements.txt
├── pyproject.toml
├── src/
│   └── pfas/
│       ├── __init__.py
│       ├── core/                # REQ-CORE-001 to 007
│       │   ├── __init__.py
│       │   ├── database.py      # SQLCipher setup
│       │   ├── encryption.py    # AES-256 field encryption
│       │   ├── journal.py       # Double-entry journal engine
│       │   ├── accounts.py      # Chart of accounts
│       │   ├── currency.py      # Multi-currency, SBI TT rates
│       │   └── audit.py         # Audit logging
│       ├── parsers/             # All parser modules
│       │   ├── __init__.py
│       │   ├── bank/            # REQ-BANK-001 to 004
│       │   ├── mf/              # REQ-MF-001 to 009
│       │   ├── stock/           # REQ-STK-001 to 007
│       │   ├── epf/             # REQ-EPF-001 to 006
│       │   ├── ppf/             # REQ-PPF-001 to 004
│       │   ├── nps/             # REQ-NPS-001 to 005
│       │   ├── salary/          # REQ-SAL-001 to 012
│       │   ├── tax/             # REQ-TAX-001 to 009
│       │   ├── rental/          # REQ-RNT-001 to 006
│       │   ├── sgb/             # REQ-SGB-001 to 005
│       │   ├── reit/            # REQ-REIT-001 to 005
│       │   ├── rsu/             # REQ-RSU-001 to 006 (Phase 2)
│       │   ├── espp/            # REQ-ESPP-001 to 005 (Phase 2)
│       │   └── unlisted/        # REQ-UNL-001 to 004 (Phase 2)
│       ├── models/              # Data models
│       │   ├── __init__.py
│       │   └── *.py             # Dataclass models
│       ├── services/            # Business logic
│       │   ├── __init__.py
│       │   ├── capital_gains.py
│       │   ├── tds_reconciliation.py
│       │   ├── dtaa.py          # Phase 2
│       │   └── itr_export.py    # Phase 2
│       ├── reports/             # REQ-RPT-001 to 008
│       │   ├── __init__.py
│       │   ├── net_worth.py
│       │   ├── tax_computation.py
│       │   └── gnucash_export.py
│       └── cli/
│           ├── __init__.py
│           └── main.py          # Click CLI
├── tests/
│   ├── __init__.py
│   ├── conftest.py              # Shared fixtures
│   ├── fixtures/                # Test data files
│   │   ├── bank/
│   │   ├── mf/
│   │   ├── salary/
│   │   └── ...
│   ├── unit/                    # Unit tests by module
│   │   ├── test_core/
│   │   ├── test_parsers/
│   │   └── test_services/
│   └── integration/             # Integration tests
│       ├── test_parser_to_db.py
│       ├── test_journal_flow.py
│       └── test_full_fy.py
└── docs/
    ├── requirements/
    ├── design/
    └── test_reports/
```

---

## 3. Sub-Agent Architecture

### Concept: Task-Specific Claude Code Sessions

The key insight is to use Claude Code in **focused sessions**, each handling a specific module with clear inputs/outputs. This mimics sub-agents.

```
┌─────────────────────────────────────────────────────────────┐
│                    ORCHESTRATOR SESSION                      │
│              (You driving claude code CLI)                   │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│ SESSION 1:    │    │ SESSION 2:    │    │ SESSION 3:    │
│ Core Module   │    │ Bank Parser   │    │ MF Parser     │
│               │    │               │    │               │
│ Input: Design │    │ Input: Sample │    │ Input: CAMS   │
│ Output: Code  │    │   PDF stmt    │    │   CAS sample  │
│ + Unit Tests  │    │ Output: Code  │    │ Output: Code  │
│               │    │ + Unit Tests  │    │ + Unit Tests  │
└───────────────┘    └───────────────┘    └───────────────┘
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              ▼
                    ┌───────────────┐
                    │ INTEGRATION   │
                    │ SESSION       │
                    │               │
                    │ Verify all    │
                    │ modules work  │
                    │ together      │
                    └───────────────┘
```

### Sub-Agent Prompt Templates

Create these as `.md` files in your project for reuse:

```bash
mkdir -p ~/pfas-project/prompts
```

#### prompts/module_implementation.md
```markdown
# Module Implementation Task

## Context
You are implementing module: {MODULE_NAME}
Requirements: {REQ_IDS}
Phase: {PHASE}

## Inputs Provided
1. Design document section (see PFAS_Design_v6.0.docx)
2. Sample data files in tests/fixtures/{module}/
3. Test cases from PFAS_TestCases_v6.0.docx

## Deliverables
1. Source code in src/pfas/{path}/
2. Unit tests in tests/unit/test_{module}/
3. All tests must pass with >80% coverage

## Constraints
- Follow existing code patterns in src/pfas/core/
- Use type hints
- Handle edge cases from test data
- Log all parsing errors, don't silently fail

## Verification
After implementation, run:
```bash
pytest tests/unit/test_{module}/ -v --cov=src/pfas/{path}
```
```

#### prompts/integration_test.md
```markdown
# Integration Test Task

## Context
Testing integration between: {MODULE_A} → {MODULE_B}
Requirements being verified: {REQ_IDS}

## Test Scenario
1. Parse input file using {MODULE_A}
2. Store results in database
3. Retrieve and process with {MODULE_B}
4. Verify journal entries are balanced
5. Verify reports generate correctly

## Success Criteria
- No data loss between modules
- Journal entries balance (Dr = Cr)
- Audit trail captures all operations
- Error handling works correctly
```

---

## 4. Implementation Workflow

### Step-by-Step Process

```
┌──────────────────────────────────────────────────────────────┐
│ STEP 1: Implement Core Foundation First                      │
│ - Database, Encryption, Journal Engine                       │
│ - This is the foundation everything else depends on          │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│ STEP 2: Implement Parsers in Dependency Order                │
│ - Bank → MF → Stock → EPF → PPF → NPS                        │
│ - Each parser: Code → Unit Test → Integration Test           │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│ STEP 3: Implement Complex Modules                            │
│ - Salary/Form16 (depends on nothing but core)                │
│ - TDS Reconciliation (depends on Tax + Salary)               │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│ STEP 4: Implement Reports                                    │
│ - Net Worth, Tax Computation, GNUCash Export                 │
│ - Depends on all parsers being complete                      │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│ STEP 5: Full Integration Test                                │
│ - Load all FY24-25 data                                      │
│ - Generate all reports                                       │
│ - Verify against manual calculations                         │
└──────────────────────────────────────────────────────────────┘
```

---

## 5. Module-by-Module Implementation

### Session 1: Core Foundation (Sprint 1)

```bash
# Start Claude Code session
cd ~/pfas-project
claude

# In the Claude Code session, paste this prompt:
```

**Prompt for Core Implementation:**
```
I need you to implement the PFAS Core Foundation module.

## Requirements to implement:
- REQ-CORE-001: SQLCipher encrypted database initialization
- REQ-CORE-002: Chart of accounts setup (18 asset classes)
- REQ-CORE-003: Double-entry journal engine with balance validation
- REQ-CORE-004: Multi-currency support with SBI TT rate lookup
- REQ-CORE-005: AES-256-GCM field-level encryption for PAN/Aadhaar
- REQ-CORE-006: Audit logging for all data changes
- REQ-CORE-007: Session management with 15-min timeout

## Database Schema (from Design doc):
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    pan_encrypted BLOB NOT NULL,
    pan_salt BLOB NOT NULL,
    name TEXT NOT NULL,
    email TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    account_type TEXT NOT NULL CHECK(account_type IN ('ASSET','LIABILITY','INCOME','EXPENSE','EQUITY')),
    parent_id INTEGER REFERENCES accounts(id),
    currency TEXT DEFAULT 'INR',
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE journals (
    id INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    description TEXT NOT NULL,
    reference_type TEXT,
    reference_id INTEGER,
    created_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE journal_entries (
    id INTEGER PRIMARY KEY,
    journal_id INTEGER REFERENCES journals(id) NOT NULL,
    account_id INTEGER REFERENCES accounts(id) NOT NULL,
    debit DECIMAL(15,2) DEFAULT 0,
    credit DECIMAL(15,2) DEFAULT 0,
    currency TEXT DEFAULT 'INR',
    exchange_rate DECIMAL(10,6) DEFAULT 1.0
);

CREATE TABLE audit_log (
    id INTEGER PRIMARY KEY,
    table_name TEXT NOT NULL,
    record_id INTEGER NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('INSERT','UPDATE','DELETE')),
    old_values TEXT,
    new_values TEXT,
    user_id INTEGER,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

## Files to create:
1. src/pfas/core/database.py - SQLCipher initialization
2. src/pfas/core/encryption.py - AES-256-GCM encryption
3. src/pfas/core/accounts.py - Chart of accounts management
4. src/pfas/core/journal.py - Journal engine with balance validation
5. src/pfas/core/currency.py - Multi-currency with SBI TT rates
6. src/pfas/core/audit.py - Audit logging
7. src/pfas/core/session.py - Session management
8. tests/unit/test_core/test_database.py
9. tests/unit/test_core/test_encryption.py
10. tests/unit/test_core/test_journal.py

## Test Cases to implement:
- TC-CORE-001: DB encryption init
- TC-CORE-002: Chart of accounts setup
- TC-CORE-003: Journal balance validation (reject unbalanced)
- TC-CORE-004: USD to INR conversion
- TC-CORE-005: PAN encryption round-trip
- TC-CORE-006: Audit log entry creation
- TC-CORE-007: Session timeout

Please implement all files with proper type hints and docstrings.
After implementation, show me how to run the tests.
```

**After Core Implementation - Verify:**
```bash
# Exit Claude Code session, then run:
source venv/bin/activate
pytest tests/unit/test_core/ -v --cov=src/pfas/core --cov-report=term-missing

# Expected: All tests pass, >80% coverage
```

### Session 2: Bank Parser (Sprint 1)

```bash
# Start new Claude Code session
claude

# Paste this prompt:
```

**Prompt for Bank Parser:**
```
I need you to implement the Bank Statement Parser module.

## Requirements:
- REQ-BANK-001: Parse bank statements (PDF/Excel) - ICICI, SBI, HDFC formats
- REQ-BANK-002: Calculate interest and apply 80TTA deduction
- REQ-BANK-003: Handle password-protected PDFs using keyring
- REQ-BANK-004: Multi-account consolidation sorted by date

## Sample Data Location:
tests/fixtures/bank/

## Database Tables (already created by core module):
CREATE TABLE bank_accounts (
    id INTEGER PRIMARY KEY,
    account_number_encrypted BLOB NOT NULL,
    account_number_salt BLOB NOT NULL,
    bank_name TEXT NOT NULL,
    branch TEXT,
    ifsc_code TEXT,
    account_type TEXT DEFAULT 'SAVINGS'
);

CREATE TABLE bank_transactions (
    id INTEGER PRIMARY KEY,
    bank_account_id INTEGER REFERENCES bank_accounts(id),
    date DATE NOT NULL,
    description TEXT NOT NULL,
    debit DECIMAL(15,2) DEFAULT 0,
    credit DECIMAL(15,2) DEFAULT 0,
    balance DECIMAL(15,2),
    category TEXT,
    is_interest BOOLEAN DEFAULT FALSE
);

## Files to create:
1. src/pfas/parsers/bank/__init__.py
2. src/pfas/parsers/bank/base.py - Base parser class
3. src/pfas/parsers/bank/icici.py - ICICI format
4. src/pfas/parsers/bank/sbi.py - SBI format
5. src/pfas/parsers/bank/hdfc.py - HDFC format
6. tests/unit/test_parsers/test_bank.py

## Test Cases:
- TC-BANK-001: ICICI statement parse
- TC-BANK-002: Interest 80TTA calculation
- TC-BANK-003: Password PDF decryption
- TC-BANK-004: Multi-bank merge

## Integration Test:
After unit tests pass, create tests/integration/test_bank_to_journal.py
that verifies:
1. Parse bank statement
2. Store transactions in DB
3. Create journal entries (Dr Bank, Cr Income for interest)
4. Verify audit log entries
```

### Session 3: MF Parser (Sprint 2)

**Prompt for MF Parser:**
```
I need you to implement the Mutual Fund Parser module.

## Requirements:
- REQ-MF-001: Parse CAMS CAS statements (password-protected PDF)
- REQ-MF-002: Parse KARVY/KFINTECH statements (Excel)
- REQ-MF-003: Classify funds as EQUITY or DEBT based on scheme name/ISIN
- REQ-MF-004: Track both classification types for tax purposes
- REQ-MF-005: Calculate STCG for equity (<12 months) at 20%
- REQ-MF-006: Calculate LTCG for equity (>12 months) at 12.5%
- REQ-MF-007: Apply slab rate for debt fund gains
- REQ-MF-008: Implement grandfathering for pre-31-Jan-2018 purchases
- REQ-MF-009: Generate capital gains statement

## Reference: Indian-MF-Stock-CG_sheet_details.md
The CAMS CAS file has these sheets:
- INVESTOR_DETAILS: PAN, Name, Address
- TRXN_DETAILS: All transactions with CG calculation
- SCHEMEWISE_EQUITY: Equity scheme summary
- SCHEMEWISE_DEBT: Debt scheme summary

## Sample Data:
tests/fixtures/mf/cams_cas_sample.pdf
tests/fixtures/mf/karvy_sample.xlsx

## Key Logic:
1. For redemptions, calculate holding period from purchase date
2. If EQUITY and >12 months: LTCG at 12.5%
3. If EQUITY and <12 months: STCG at 20%
4. If DEBT: Always slab rate (no special LTCG)
5. For pre-31-Jan-2018 purchases: Use higher of (actual cost, FMV on 31-Jan-2018)

## Files to create:
1. src/pfas/parsers/mf/__init__.py
2. src/pfas/parsers/mf/cams.py
3. src/pfas/parsers/mf/karvy.py
4. src/pfas/parsers/mf/capital_gains.py
5. src/pfas/models/mf.py (dataclasses)
6. tests/unit/test_parsers/test_mf.py

## Test Cases:
- TC-MF-001 through TC-MF-009 (see test cases document)
```

---

## 6. Testing Strategy

### Test Pyramid

```
                    ┌─────────────────┐
                    │   E2E Tests     │  ← Full FY data load
                    │   (Few, Slow)   │
                    └─────────────────┘
                   /                   \
          ┌───────────────────────────────┐
          │      Integration Tests        │  ← Module interactions
          │      (Moderate, Medium)       │
          └───────────────────────────────┘
         /                                 \
┌─────────────────────────────────────────────────┐
│              Unit Tests                          │  ← Individual functions
│              (Many, Fast)                        │
└─────────────────────────────────────────────────┘
```

### Verification Commands

```bash
# Run unit tests for specific module
pytest tests/unit/test_parsers/test_bank.py -v

# Run with coverage
pytest tests/unit/test_core/ --cov=src/pfas/core --cov-report=html

# Run integration tests
pytest tests/integration/ -v

# Run all tests
pytest --cov=src/pfas --cov-report=term-missing

# Run specific test case by name
pytest -k "TC_CORE_003" -v
```

### Continuous Verification Script

Create `scripts/verify_module.sh`:
```bash
#!/bin/bash
MODULE=$1

echo "=== Verifying Module: $MODULE ==="

# Step 1: Lint
echo ">>> Running linter..."
ruff check src/pfas/$MODULE/

# Step 2: Type check
echo ">>> Running type checker..."
mypy src/pfas/$MODULE/

# Step 3: Unit tests
echo ">>> Running unit tests..."
pytest tests/unit/test_$MODULE/ -v --cov=src/pfas/$MODULE --cov-fail-under=80

# Step 4: Integration tests (if exist)
if [ -f "tests/integration/test_${MODULE}_integration.py" ]; then
    echo ">>> Running integration tests..."
    pytest tests/integration/test_${MODULE}_integration.py -v
fi

echo "=== Module $MODULE Verified ==="
```

Usage:
```bash
chmod +x scripts/verify_module.sh
./scripts/verify_module.sh core
./scripts/verify_module.sh parsers/bank
./scripts/verify_module.sh parsers/mf
```

---

## 7. Integration Verification

### After Each Module - Integration Check

Create `tests/integration/conftest.py`:
```python
import pytest
from pfas.core.database import init_database
from pfas.core.accounts import setup_chart_of_accounts

@pytest.fixture(scope="session")
def test_db():
    """Create a test database for integration tests."""
    db_path = ":memory:"  # In-memory for speed
    conn = init_database(db_path, password="test_password")
    setup_chart_of_accounts(conn)
    yield conn
    conn.close()

@pytest.fixture
def clean_db(test_db):
    """Reset database state between tests."""
    # Clear all transaction tables, keep accounts
    test_db.execute("DELETE FROM journal_entries")
    test_db.execute("DELETE FROM journals")
    test_db.execute("DELETE FROM bank_transactions")
    test_db.execute("DELETE FROM mf_transactions")
    # ... other tables
    test_db.commit()
    yield test_db
```

### Full FY Integration Test

Create `tests/integration/test_full_fy.py`:
```python
"""
Full FY24-25 Integration Test

This test loads all actual data files and verifies:
1. All parsers work correctly
2. All journal entries balance
3. Tax computation matches manual calculation
4. ITR-2 JSON validates against schema
"""

import pytest
from pathlib import Path

class TestFullFY2425:
    """Full financial year integration test."""
    
    @pytest.fixture(scope="class")
    def fy_data_path(self):
        return Path("tests/fixtures/fy2425/")
    
    def test_01_load_bank_statements(self, clean_db, fy_data_path):
        """Load all bank statements."""
        from pfas.parsers.bank import parse_all_statements
        
        statements = list(fy_data_path.glob("bank/*.pdf"))
        for stmt in statements:
            result = parse_all_statements(stmt, clean_db)
            assert result.success
            assert result.transactions_count > 0
    
    def test_02_load_mf_statements(self, clean_db, fy_data_path):
        """Load CAMS and KARVY statements."""
        from pfas.parsers.mf import parse_cams, parse_karvy
        
        cams_file = fy_data_path / "mf/cams_cas.pdf"
        if cams_file.exists():
            result = parse_cams(cams_file, clean_db)
            assert result.success
    
    def test_03_load_salary_form16(self, clean_db, fy_data_path):
        """Load salary data from Form 16."""
        from pfas.parsers.salary import parse_form16
        
        form16_zip = fy_data_path / "salary/form16.zip"
        if form16_zip.exists():
            result = parse_form16(form16_zip, clean_db)
            assert result.success
    
    def test_04_verify_journal_balance(self, clean_db):
        """Verify all journal entries are balanced."""
        cursor = clean_db.execute("""
            SELECT j.id, j.description,
                   SUM(je.debit) as total_debit,
                   SUM(je.credit) as total_credit
            FROM journals j
            JOIN journal_entries je ON j.id = je.journal_id
            GROUP BY j.id
            HAVING ABS(total_debit - total_credit) > 0.01
        """)
        unbalanced = cursor.fetchall()
        assert len(unbalanced) == 0, f"Unbalanced journals: {unbalanced}"
    
    def test_05_tds_reconciliation(self, clean_db):
        """Verify TDS matches between Form 16 and Form 26AS."""
        from pfas.services.tds_reconciliation import reconcile
        
        result = reconcile(clean_db)
        assert result.variance < 100  # Allow ₹100 variance
    
    def test_06_generate_tax_computation(self, clean_db):
        """Generate tax computation for both regimes."""
        from pfas.reports.tax_computation import compute_tax
        
        old_regime = compute_tax(clean_db, regime="OLD")
        new_regime = compute_tax(clean_db, regime="NEW")
        
        assert old_regime.total_income > 0
        assert new_regime.total_income > 0
    
    def test_07_itr2_json_export(self, clean_db):
        """Export ITR-2 JSON and validate schema."""
        from pfas.services.itr_export import export_itr2
        import jsonschema
        import json
        
        itr_json = export_itr2(clean_db, assessment_year="2025-26")
        
        # Load CBDT schema
        with open("tests/fixtures/ITR-2_2025_Main_V1.2.json") as f:
            schema = json.load(f)
        
        # Validate
        jsonschema.validate(itr_json, schema)
```

---

## 8. Sample Commands

### Daily Development Workflow

```bash
# Morning: Start work on a module
cd ~/pfas-project
source venv/bin/activate
git checkout -b feature/mf-parser

# Start Claude Code session for the module
claude

# ... implement the module with Claude Code ...

# After implementation, verify
./scripts/verify_module.sh parsers/mf

# If tests pass, commit
git add .
git commit -m "feat: Implement MF parser (REQ-MF-001 to 009)"

# Run full test suite before merge
pytest --cov=src/pfas

# Merge to main
git checkout main
git merge feature/mf-parser
```

### Claude Code Session Commands

Inside a Claude Code session:
```
# Show current project structure
/tree

# Run tests
/run pytest tests/unit/test_core/ -v

# Check coverage
/run pytest --cov=src/pfas/core --cov-report=term-missing

# Install a new package
/run pip install some-package

# Git operations
/run git status
/run git diff

# Read a file for context
/read src/pfas/core/database.py

# Edit a file
/edit src/pfas/parsers/bank/icici.py
```

### Module Implementation Checklist

Use this checklist for each module:

```markdown
## Module: {MODULE_NAME}
Requirements: {REQ_IDS}

### Implementation
- [ ] Create source files in src/pfas/{path}/
- [ ] Add type hints to all functions
- [ ] Add docstrings with examples
- [ ] Handle edge cases

### Testing
- [ ] Create unit tests in tests/unit/test_{module}/
- [ ] All test cases from TestCases doc implemented
- [ ] Coverage > 80%
- [ ] Integration test created

### Verification
- [ ] `pytest tests/unit/test_{module}/ -v` - PASS
- [ ] `pytest tests/integration/test_{module}_integration.py -v` - PASS
- [ ] `mypy src/pfas/{path}/` - No errors
- [ ] `ruff check src/pfas/{path}/` - No errors

### Documentation
- [ ] README.md updated
- [ ] Example usage added
- [ ] API docs generated
```

---

## Quick Reference Card

| Phase | Sprint | Module | Claude Code Prompt File |
|-------|--------|--------|------------------------|
| 1 | S1 | Core | prompts/core.md |
| 1 | S1 | Bank | prompts/bank_parser.md |
| 1 | S2 | MF-CAMS | prompts/mf_cams.md |
| 1 | S2 | MF-KARVY | prompts/mf_karvy.md |
| 1 | S3 | Stock-ICICI | prompts/stock_icici.md |
| 1 | S3 | Stock-Zerodha | prompts/stock_zerodha.md |
| 1 | S4 | EPF | prompts/epf.md |
| 1 | S4 | PPF | prompts/ppf.md |
| 1 | S4 | NPS | prompts/nps.md |
| 1 | S5 | Salary | prompts/salary.md |
| 1 | S5 | Form16 | prompts/form16.md |
| 1 | S6 | Form26AS | prompts/form26as.md |
| 1 | S6 | TDS Recon | prompts/tds_recon.md |
| 1 | S7 | Rental | prompts/rental.md |
| 1 | S7 | SGB | prompts/sgb.md |
| 1 | S7 | REIT | prompts/reit.md |
| 1 | S8 | Reports | prompts/reports.md |
| 2 | S9 | Currency | prompts/currency.md |
| 2 | S9 | E*TRADE | prompts/etrade.md |
| 2 | S10 | RSU | prompts/rsu.md |
| 2 | S11 | ESPP | prompts/espp.md |
| 2 | S12 | DTAA | prompts/dtaa.md |
| 2 | S13 | Schedule FA | prompts/schedule_fa.md |
| 2 | S14 | ITR-2 Export | prompts/itr2.md |

---

## Summary: The Sub-Agent Approach

1. **One Module = One Focused Session**: Each Claude Code session handles one module with clear requirements and test cases

2. **Verify Before Proceeding**: After each module, run tests and verify integration before starting the next

3. **Build on Solid Foundation**: Core module must be complete and tested before starting parsers

4. **Integration Tests Are Checkpoints**: After every 2-3 modules, run integration tests to catch issues early

5. **Full FY Test Is Final Verification**: Load actual FY24-25 data and verify everything works together

This approach ensures each "sub-agent" (Claude Code session) has a clear, bounded task with measurable success criteria, while the overall system is verified through integration testing.
