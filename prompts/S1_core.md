# Sprint 1: Core Foundation Module

## Module Overview
**Sprint:** S1 (Week 1-2)
**Phase:** 1
**Requirements:** REQ-CORE-001 to REQ-CORE-007
**Priority:** P1 (Must complete before any other module)

---

## Requirements to Implement

### REQ-CORE-001: Database Engine
- **Input:** Database path, encryption password
- **Processing:** Initialize SQLCipher encrypted SQLite database
- **Output:** Encrypted database file with all schema tables created

### REQ-CORE-002: Chart of Accounts
- **Input:** Configuration file with account definitions
- **Processing:** Create hierarchical account structure for 18 asset classes
- **Output:** Populated accounts table with proper parent-child relationships

### REQ-CORE-003: Journal Engine
- **Input:** Journal entry with multiple line items
- **Processing:** Validate double-entry (Debit = Credit), create entries
- **Output:** Balanced journal entries with audit trail
- **Error Handling:** Reject unbalanced entries with clear error message

### REQ-CORE-004: Multi-Currency Support
- **Input:** Amount in foreign currency, transaction date
- **Processing:** Look up SBI TT Buying Rate for date, convert to INR
- **Output:** INR amount with exchange rate recorded
- **Note:** For Phase 1, implement structure; SBI rate lookup in Phase 2

### REQ-CORE-005: Field-Level Encryption
- **Input:** Sensitive data (PAN, Aadhaar, Bank Account)
- **Processing:** Encrypt using AES-256-GCM with unique salt per field
- **Output:** Encrypted blob stored in database

### REQ-CORE-006: Audit Logging
- **Input:** Any data change operation
- **Processing:** Capture table, record_id, action, old/new values, timestamp
- **Output:** Audit log entry for compliance

### REQ-CORE-007: Session Management
- **Input:** User PIN/password
- **Processing:** Authenticate, track session, timeout after 15 minutes idle
- **Output:** Session token, automatic re-authentication prompt on timeout

---

## Database Schema

```sql
-- Core Tables
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pan_encrypted BLOB NOT NULL,
    pan_salt BLOB NOT NULL,
    name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    account_type TEXT NOT NULL CHECK(account_type IN ('ASSET','LIABILITY','INCOME','EXPENSE','EQUITY')),
    parent_id INTEGER REFERENCES accounts(id),
    currency TEXT DEFAULT 'INR',
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS journals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    description TEXT NOT NULL,
    reference_type TEXT,  -- 'SALARY', 'MF_REDEMPTION', 'BANK_INTEREST', etc.
    reference_id INTEGER, -- FK to source table
    created_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_reversed BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS journal_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    journal_id INTEGER REFERENCES journals(id) NOT NULL,
    account_id INTEGER REFERENCES accounts(id) NOT NULL,
    debit DECIMAL(15,2) DEFAULT 0,
    credit DECIMAL(15,2) DEFAULT 0,
    currency TEXT DEFAULT 'INR',
    exchange_rate DECIMAL(10,6) DEFAULT 1.0,
    narration TEXT
);

CREATE TABLE IF NOT EXISTS exchange_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    from_currency TEXT NOT NULL,
    to_currency TEXT NOT NULL DEFAULT 'INR',
    rate DECIMAL(10,6) NOT NULL,
    source TEXT DEFAULT 'SBI_TT_BUYING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, from_currency, to_currency)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    record_id INTEGER NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('INSERT','UPDATE','DELETE')),
    old_values TEXT,  -- JSON
    new_values TEXT,  -- JSON
    user_id INTEGER REFERENCES users(id),
    ip_address TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id) NOT NULL,
    token TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_journal_entries_journal ON journal_entries(journal_id);
CREATE INDEX IF NOT EXISTS idx_journal_entries_account ON journal_entries(account_id);
CREATE INDEX IF NOT EXISTS idx_journals_date ON journals(date);
CREATE INDEX IF NOT EXISTS idx_audit_log_table ON audit_log(table_name, record_id);
CREATE INDEX IF NOT EXISTS idx_exchange_rates_date ON exchange_rates(date, from_currency);
```

---

## Chart of Accounts Structure

```python
CHART_OF_ACCOUNTS = {
    # Assets (1xxx)
    "1000": {"name": "Assets", "type": "ASSET", "parent": None},
    "1100": {"name": "Current Assets", "type": "ASSET", "parent": "1000"},
    "1101": {"name": "Bank - Savings", "type": "ASSET", "parent": "1100"},
    "1102": {"name": "Bank - Current", "type": "ASSET", "parent": "1100"},
    "1103": {"name": "Bank - FD", "type": "ASSET", "parent": "1100"},
    "1104": {"name": "Cash in Hand", "type": "ASSET", "parent": "1100"},
    
    "1200": {"name": "Investments", "type": "ASSET", "parent": "1000"},
    "1201": {"name": "Mutual Funds - Equity", "type": "ASSET", "parent": "1200"},
    "1202": {"name": "Mutual Funds - Debt", "type": "ASSET", "parent": "1200"},
    "1203": {"name": "Indian Stocks", "type": "ASSET", "parent": "1200"},
    "1204": {"name": "SGB", "type": "ASSET", "parent": "1200"},
    "1205": {"name": "RBI Bonds", "type": "ASSET", "parent": "1200"},
    "1206": {"name": "REIT/InvIT", "type": "ASSET", "parent": "1200"},
    
    "1300": {"name": "Retirement Funds", "type": "ASSET", "parent": "1000"},
    "1301": {"name": "EPF - Employee", "type": "ASSET", "parent": "1300"},
    "1302": {"name": "EPF - Employer", "type": "ASSET", "parent": "1300"},
    "1303": {"name": "PPF", "type": "ASSET", "parent": "1300"},
    "1304": {"name": "NPS - Tier I", "type": "ASSET", "parent": "1300"},
    "1305": {"name": "NPS - Tier II", "type": "ASSET", "parent": "1300"},
    
    "1400": {"name": "Foreign Assets", "type": "ASSET", "parent": "1000", "currency": "USD"},
    "1401": {"name": "US Stocks - RSU", "type": "ASSET", "parent": "1400", "currency": "USD"},
    "1402": {"name": "US Stocks - ESPP", "type": "ASSET", "parent": "1400", "currency": "USD"},
    "1403": {"name": "US Stocks - DRIP", "type": "ASSET", "parent": "1400", "currency": "USD"},
    "1404": {"name": "US Brokerage Cash", "type": "ASSET", "parent": "1400", "currency": "USD"},
    
    "1500": {"name": "Other Assets", "type": "ASSET", "parent": "1000"},
    "1501": {"name": "Unlisted Shares", "type": "ASSET", "parent": "1500"},
    "1502": {"name": "Real Estate", "type": "ASSET", "parent": "1500"},
    
    "1600": {"name": "Tax Assets", "type": "ASSET", "parent": "1000"},
    "1601": {"name": "TDS Receivable", "type": "ASSET", "parent": "1600"},
    "1602": {"name": "TCS Receivable", "type": "ASSET", "parent": "1600"},
    "1603": {"name": "Advance Tax Paid", "type": "ASSET", "parent": "1600"},
    "1604": {"name": "Foreign Tax Credit", "type": "ASSET", "parent": "1600"},
    
    # Liabilities (2xxx)
    "2000": {"name": "Liabilities", "type": "LIABILITY", "parent": None},
    "2100": {"name": "Tax Payable", "type": "LIABILITY", "parent": "2000"},
    "2101": {"name": "Income Tax Payable", "type": "LIABILITY", "parent": "2100"},
    "2102": {"name": "Professional Tax Payable", "type": "LIABILITY", "parent": "2100"},
    
    # Income (4xxx)
    "4000": {"name": "Income", "type": "INCOME", "parent": None},
    "4100": {"name": "Salary Income", "type": "INCOME", "parent": "4000"},
    "4101": {"name": "Basic Salary", "type": "INCOME", "parent": "4100"},
    "4102": {"name": "HRA", "type": "INCOME", "parent": "4100"},
    "4103": {"name": "Special Allowance", "type": "INCOME", "parent": "4100"},
    "4104": {"name": "RSU Perquisite", "type": "INCOME", "parent": "4100"},
    "4105": {"name": "ESPP Perquisite", "type": "INCOME", "parent": "4100"},
    "4106": {"name": "Other Perquisites", "type": "INCOME", "parent": "4100"},
    
    "4200": {"name": "Investment Income", "type": "INCOME", "parent": "4000"},
    "4201": {"name": "Bank Interest", "type": "INCOME", "parent": "4200"},
    "4202": {"name": "FD Interest", "type": "INCOME", "parent": "4200"},
    "4203": {"name": "Dividend - Indian", "type": "INCOME", "parent": "4200"},
    "4204": {"name": "Dividend - Foreign", "type": "INCOME", "parent": "4200"},
    "4205": {"name": "EPF Interest", "type": "INCOME", "parent": "4200"},
    "4206": {"name": "PPF Interest", "type": "INCOME", "parent": "4200"},
    "4207": {"name": "SGB Interest", "type": "INCOME", "parent": "4200"},
    
    "4300": {"name": "Capital Gains", "type": "INCOME", "parent": "4000"},
    "4301": {"name": "STCG - Equity 20%", "type": "INCOME", "parent": "4300"},
    "4302": {"name": "LTCG - Equity 12.5%", "type": "INCOME", "parent": "4300"},
    "4303": {"name": "CG - Debt (Slab)", "type": "INCOME", "parent": "4300"},
    "4304": {"name": "STCG - Foreign", "type": "INCOME", "parent": "4300"},
    "4305": {"name": "LTCG - Foreign 12.5%", "type": "INCOME", "parent": "4300"},
    
    "4400": {"name": "Rental Income", "type": "INCOME", "parent": "4000"},
    "4401": {"name": "Gross Rental Income", "type": "INCOME", "parent": "4400"},
    
    # Expenses (5xxx)
    "5000": {"name": "Expenses", "type": "EXPENSE", "parent": None},
    "5100": {"name": "Tax Deductions", "type": "EXPENSE", "parent": "5000"},
    "5101": {"name": "Section 80C", "type": "EXPENSE", "parent": "5100"},
    "5102": {"name": "Section 80D", "type": "EXPENSE", "parent": "5100"},
    "5103": {"name": "Section 80CCD(1B)", "type": "EXPENSE", "parent": "5100"},
    "5104": {"name": "Section 80TTA/80TTB", "type": "EXPENSE", "parent": "5100"},
    "5105": {"name": "Section 24 - HP Interest", "type": "EXPENSE", "parent": "5100"},
    "5106": {"name": "Standard Deduction 16(ia)", "type": "EXPENSE", "parent": "5100"},
    
    "5200": {"name": "Investment Expenses", "type": "EXPENSE", "parent": "5000"},
    "5201": {"name": "STT Paid", "type": "EXPENSE", "parent": "5200"},
    "5202": {"name": "Brokerage", "type": "EXPENSE", "parent": "5200"},
    "5203": {"name": "Rental Standard Deduction", "type": "EXPENSE", "parent": "5200"},
    "5204": {"name": "Municipal Tax", "type": "EXPENSE", "parent": "5200"},
    
    # Equity (3xxx)
    "3000": {"name": "Equity", "type": "EQUITY", "parent": None},
    "3100": {"name": "Opening Balance", "type": "EQUITY", "parent": "3000"},
    "3200": {"name": "Retained Earnings", "type": "EQUITY", "parent": "3000"},
}
```

---

## Files to Create

```
src/pfas/
├── __init__.py
├── core/
│   ├── __init__.py
│   ├── database.py      # SQLCipher initialization, connection management
│   ├── encryption.py    # AES-256-GCM field encryption/decryption
│   ├── accounts.py      # Chart of accounts management
│   ├── journal.py       # Double-entry journal engine
│   ├── currency.py      # Multi-currency support, exchange rates
│   ├── audit.py         # Audit logging
│   ├── session.py       # Session management
│   └── exceptions.py    # Custom exceptions
tests/
├── __init__.py
├── conftest.py          # Shared fixtures
└── unit/
    └── test_core/
        ├── __init__.py
        ├── test_database.py
        ├── test_encryption.py
        ├── test_accounts.py
        ├── test_journal.py
        ├── test_currency.py
        ├── test_audit.py
        └── test_session.py
```

---

## Implementation Guidelines

### database.py
```python
"""
SQLCipher database initialization and connection management.

Key functions:
- init_database(db_path: str, password: str) -> Connection
- get_connection() -> Connection (singleton pattern)
- execute_schema(conn: Connection) -> None
- close_connection() -> None
"""

import sqlcipher3
from pathlib import Path
from typing import Optional

class DatabaseManager:
    _instance: Optional['DatabaseManager'] = None
    _connection: Optional[sqlcipher3.Connection] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def init(self, db_path: str, password: str) -> sqlcipher3.Connection:
        """Initialize encrypted database."""
        self._connection = sqlcipher3.connect(db_path)
        self._connection.execute(f"PRAGMA key = '{password}'")
        self._connection.execute("PRAGMA cipher_compatibility = 4")
        self._execute_schema()
        return self._connection
    
    def _execute_schema(self) -> None:
        """Create all tables if not exist."""
        # Execute all CREATE TABLE statements
        pass
```

### encryption.py
```python
"""
AES-256-GCM field-level encryption for sensitive data.

Key functions:
- encrypt_field(plaintext: str, master_key: bytes) -> tuple[bytes, bytes]
- decrypt_field(ciphertext: bytes, salt: bytes, master_key: bytes) -> str
- derive_key(password: str, salt: bytes) -> bytes
"""

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import os

def encrypt_field(plaintext: str, master_key: bytes) -> tuple[bytes, bytes]:
    """Encrypt sensitive field with unique salt."""
    salt = os.urandom(16)
    nonce = os.urandom(12)
    key = derive_key(master_key, salt)
    aesgcm = AESGCM(key)
    ciphertext = aesgcm.encrypt(nonce, plaintext.encode(), None)
    return nonce + ciphertext, salt

def decrypt_field(ciphertext: bytes, salt: bytes, master_key: bytes) -> str:
    """Decrypt sensitive field."""
    key = derive_key(master_key, salt)
    nonce = ciphertext[:12]
    ct = ciphertext[12:]
    aesgcm = AESGCM(key)
    return aesgcm.decrypt(nonce, ct, None).decode()
```

### journal.py
```python
"""
Double-entry journal engine with balance validation.

Key functions:
- create_journal(date, description, entries: list[JournalEntry]) -> int
- validate_balance(entries: list[JournalEntry]) -> bool
- reverse_journal(journal_id: int) -> int
- get_account_balance(account_id: int, as_of_date: date) -> Decimal
"""

from dataclasses import dataclass
from decimal import Decimal
from datetime import date
from typing import List

@dataclass
class JournalEntry:
    account_id: int
    debit: Decimal = Decimal("0")
    credit: Decimal = Decimal("0")
    currency: str = "INR"
    exchange_rate: Decimal = Decimal("1")
    narration: str = ""

class JournalEngine:
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def create_journal(self, txn_date: date, description: str, 
                       entries: List[JournalEntry], 
                       reference_type: str = None,
                       reference_id: int = None) -> int:
        """Create a balanced journal entry."""
        if not self._validate_balance(entries):
            raise UnbalancedJournalError("Debit does not equal Credit")
        # Insert journal and entries
        pass
    
    def _validate_balance(self, entries: List[JournalEntry]) -> bool:
        """Validate sum of debits equals sum of credits."""
        total_debit = sum(e.debit * e.exchange_rate for e in entries)
        total_credit = sum(e.credit * e.exchange_rate for e in entries)
        return abs(total_debit - total_credit) < Decimal("0.01")
```

---

## Test Cases to Implement

### TC-CORE-001: Database Encryption Initialization
```python
def test_database_encryption_init():
    """Test SQLCipher encrypted database creation."""
    db_path = ":memory:"
    password = "test_password_123"
    
    db = DatabaseManager()
    conn = db.init(db_path, password)
    
    # Verify tables exist
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )
    tables = [row[0] for row in cursor.fetchall()]
    
    assert "users" in tables
    assert "accounts" in tables
    assert "journals" in tables
    assert "journal_entries" in tables
    assert "audit_log" in tables
```

### TC-CORE-002: Chart of Accounts Setup
```python
def test_chart_of_accounts_setup():
    """Test 18 asset class account creation."""
    from pfas.core.accounts import setup_chart_of_accounts, get_account_by_code
    
    conn = get_test_connection()
    setup_chart_of_accounts(conn)
    
    # Verify key accounts exist
    assert get_account_by_code(conn, "1101") is not None  # Bank Savings
    assert get_account_by_code(conn, "1201") is not None  # MF Equity
    assert get_account_by_code(conn, "1401") is not None  # US Stocks RSU
    assert get_account_by_code(conn, "4101") is not None  # Basic Salary
    
    # Verify hierarchy
    mf_equity = get_account_by_code(conn, "1201")
    assert mf_equity.parent_id is not None
```

### TC-CORE-003: Journal Balance Validation
```python
def test_journal_balance_validation():
    """Test rejection of unbalanced journal entries."""
    from pfas.core.journal import JournalEngine, JournalEntry, UnbalancedJournalError
    
    engine = JournalEngine(get_test_connection())
    
    # Unbalanced entry should raise error
    unbalanced_entries = [
        JournalEntry(account_id=1, debit=Decimal("1000")),
        JournalEntry(account_id=2, credit=Decimal("999")),  # Intentionally wrong
    ]
    
    with pytest.raises(UnbalancedJournalError):
        engine.create_journal(
            txn_date=date.today(),
            description="Test unbalanced",
            entries=unbalanced_entries
        )
    
    # Balanced entry should succeed
    balanced_entries = [
        JournalEntry(account_id=1, debit=Decimal("1000")),
        JournalEntry(account_id=2, credit=Decimal("1000")),
    ]
    
    journal_id = engine.create_journal(
        txn_date=date.today(),
        description="Test balanced",
        entries=balanced_entries
    )
    assert journal_id > 0
```

### TC-CORE-004: Multi-Currency Conversion
```python
def test_usd_to_inr_conversion():
    """Test USD to INR conversion using exchange rate."""
    from pfas.core.currency import CurrencyConverter
    from datetime import date
    
    converter = CurrencyConverter(get_test_connection())
    
    # Add test exchange rate
    converter.add_rate(
        date=date(2024, 6, 15),
        from_currency="USD",
        rate=Decimal("83.50")
    )
    
    # Convert
    inr_amount = converter.convert(
        amount=Decimal("100"),
        from_currency="USD",
        as_of_date=date(2024, 6, 15)
    )
    
    assert inr_amount == Decimal("8350.00")
```

### TC-CORE-005: PAN Encryption
```python
def test_pan_encryption_roundtrip():
    """Test PAN encryption and decryption."""
    from pfas.core.encryption import encrypt_field, decrypt_field
    
    master_key = b"test_master_key_32_bytes_long!!"
    pan = "AAPPS0793R"
    
    # Encrypt
    ciphertext, salt = encrypt_field(pan, master_key)
    
    # Verify encrypted
    assert ciphertext != pan.encode()
    assert len(salt) == 16
    
    # Decrypt
    decrypted = decrypt_field(ciphertext, salt, master_key)
    assert decrypted == pan
```

### TC-CORE-006: Audit Log Entry
```python
def test_audit_log_creation():
    """Test audit log entry for data changes."""
    from pfas.core.audit import AuditLogger
    
    logger = AuditLogger(get_test_connection())
    
    # Log an insert
    logger.log_change(
        table_name="users",
        record_id=1,
        action="INSERT",
        old_values=None,
        new_values={"name": "Test User", "pan": "****"}
    )
    
    # Verify log entry
    cursor = get_test_connection().execute(
        "SELECT * FROM audit_log WHERE table_name = 'users' AND record_id = 1"
    )
    log_entry = cursor.fetchone()
    
    assert log_entry is not None
    assert log_entry["action"] == "INSERT"
```

### TC-CORE-007: Session Timeout
```python
def test_session_timeout():
    """Test session expiration after 15 minutes."""
    from pfas.core.session import SessionManager
    from datetime import datetime, timedelta
    
    manager = SessionManager(get_test_connection())
    
    # Create session
    token = manager.create_session(user_id=1)
    assert manager.is_valid(token)
    
    # Simulate 16 minutes passing
    manager._update_last_activity(token, datetime.now() - timedelta(minutes=16))
    
    # Session should be expired
    assert not manager.is_valid(token)
```

---

## Verification Commands

```bash
# Run all core tests
pytest tests/unit/test_core/ -v

# Run with coverage
pytest tests/unit/test_core/ --cov=src/pfas/core --cov-report=term-missing

# Run specific test
pytest tests/unit/test_core/test_journal.py::test_journal_balance_validation -v

# Expected output:
# - All 7 test cases pass
# - Coverage > 80%
```

---

## Success Criteria

- [ ] All database tables created successfully
- [ ] Chart of accounts with 18 asset classes populated
- [ ] Journal entries validated for balance (Dr = Cr)
- [ ] PAN/Aadhaar encryption working with round-trip
- [ ] Audit log captures all changes
- [ ] Session timeout after 15 minutes
- [ ] All unit tests passing
- [ ] Code coverage > 80%

---

## Dependencies for Next Modules

This Core module provides:
1. `DatabaseManager` - Used by all parsers to store data
2. `JournalEngine` - Used to create accounting entries
3. `encrypt_field/decrypt_field` - Used to protect sensitive data
4. `AuditLogger` - Used for compliance logging
5. `CHART_OF_ACCOUNTS` - Referenced for account lookups

**Do not proceed to Bank/MF parsers until Core is complete and tested.**
