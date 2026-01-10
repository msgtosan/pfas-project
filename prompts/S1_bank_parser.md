# Sprint 1: Bank Statement Parser Module

## Module Overview
**Sprint:** S1 (Week 1-2)
**Phase:** 1
**Requirements:** REQ-BANK-001 to REQ-BANK-004
**Dependencies:** Core module must be complete

---

## Requirements to Implement

### REQ-BANK-001: Bank Statement Parser
- **Input:** Bank statement PDF/Excel (ICICI, SBI, HDFC formats)
- **Processing:** Extract transactions with date, description, debit, credit, balance
- **Output:** Parsed transactions stored in database

### REQ-BANK-002: Interest Calculation & 80TTA
- **Input:** Bank transactions for FY
- **Processing:** Identify interest credits, calculate 80TTA deduction (max ₹10,000)
- **Output:** Interest income total, 80TTA eligible amount

### REQ-BANK-003: Password-Protected PDF Handling
- **Input:** Encrypted PDF, password from keyring
- **Processing:** Decrypt PDF using stored/prompted password
- **Output:** Decrypted content for parsing

### REQ-BANK-004: Multi-Account Consolidation
- **Input:** Statements from multiple bank accounts
- **Processing:** Merge transactions, sort by date, eliminate duplicates
- **Output:** Consolidated transaction list

---

## Database Schema

```sql
CREATE TABLE IF NOT EXISTS bank_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_number_encrypted BLOB NOT NULL,
    account_number_salt BLOB NOT NULL,
    account_number_last4 TEXT NOT NULL,  -- For display: ****1234
    bank_name TEXT NOT NULL,
    branch TEXT,
    ifsc_code TEXT,
    account_type TEXT DEFAULT 'SAVINGS' CHECK(account_type IN ('SAVINGS', 'CURRENT', 'FD', 'RD')),
    opening_date DATE,
    user_id INTEGER REFERENCES users(id),
    account_id INTEGER REFERENCES accounts(id),  -- Link to COA
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bank_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_account_id INTEGER REFERENCES bank_accounts(id) NOT NULL,
    date DATE NOT NULL,
    value_date DATE,
    description TEXT NOT NULL,
    reference_number TEXT,
    debit DECIMAL(15,2) DEFAULT 0,
    credit DECIMAL(15,2) DEFAULT 0,
    balance DECIMAL(15,2),
    category TEXT,  -- AUTO_CATEGORIZED, SALARY, INTEREST, TRANSFER, etc.
    is_interest BOOLEAN DEFAULT FALSE,
    is_reconciled BOOLEAN DEFAULT FALSE,
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(bank_account_id, date, description, debit, credit)  -- Prevent duplicates
);

CREATE TABLE IF NOT EXISTS bank_interest_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_account_id INTEGER REFERENCES bank_accounts(id) NOT NULL,
    financial_year TEXT NOT NULL,  -- '2024-25'
    total_interest DECIMAL(15,2) NOT NULL,
    tds_deducted DECIMAL(15,2) DEFAULT 0,
    section_80tta_eligible DECIMAL(15,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(bank_account_id, financial_year)
);

CREATE INDEX IF NOT EXISTS idx_bank_txn_date ON bank_transactions(date);
CREATE INDEX IF NOT EXISTS idx_bank_txn_account ON bank_transactions(bank_account_id);
CREATE INDEX IF NOT EXISTS idx_bank_txn_interest ON bank_transactions(is_interest);
```

---

## Bank Statement Formats

### ICICI Bank Statement (PDF)
```
Statement of Account
Account Number: 003101008527
Statement Period: 01-Apr-2024 to 31-Mar-2025

Date        Value Date   Description                          Debit       Credit      Balance
01-04-2024  01-04-2024  Opening Balance                                              1,234,567.89
05-04-2024  05-04-2024  NEFT-QUALCOMM INDIA PVT LTD                     630,338.38  1,864,906.27
10-04-2024  10-04-2024  UPI/123456789/XYZ                    5,000.00                1,859,906.27
30-06-2024  30-06-2024  INT PD 01-04-24 TO 30-06-24                      12,345.00  1,872,251.27
```

### SBI Bank Statement (Excel)
```
Columns: Txn Date | Value Date | Description | Ref No./Cheque No. | Debit | Credit | Balance
```

### HDFC Bank Statement (PDF)
```
Columns: Date | Narration | Chq./Ref.No. | Value Dt | Withdrawal Amt. | Deposit Amt. | Closing Balance
```

---

## Files to Create

```
src/pfas/parsers/
├── __init__.py
├── base.py              # Base parser class
└── bank/
    ├── __init__.py
    ├── base.py          # BankStatementParser base class
    ├── icici.py         # ICICI PDF parser
    ├── sbi.py           # SBI Excel parser
    ├── hdfc.py          # HDFC PDF parser
    ├── models.py        # BankTransaction, BankAccount dataclasses
    ├── categorizer.py   # Auto-categorize transactions
    └── interest.py      # Interest calculation, 80TTA

tests/unit/test_parsers/
├── __init__.py
└── test_bank/
    ├── __init__.py
    ├── test_icici.py
    ├── test_sbi.py
    ├── test_hdfc.py
    ├── test_categorizer.py
    └── test_interest.py

tests/fixtures/bank/
├── icici_sample.pdf
├── sbi_sample.xlsx
├── hdfc_sample.pdf
└── icici_password_protected.pdf
```

---

## Implementation Guidelines

### models.py
```python
"""Bank transaction and account data models."""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional
from enum import Enum

class TransactionCategory(Enum):
    SALARY = "SALARY"
    INTEREST = "INTEREST"
    TRANSFER = "TRANSFER"
    UPI = "UPI"
    NEFT = "NEFT"
    RTGS = "RTGS"
    ATM = "ATM"
    CARD = "CARD"
    CHEQUE = "CHEQUE"
    OTHER = "OTHER"

@dataclass
class BankTransaction:
    date: date
    description: str
    debit: Decimal = Decimal("0")
    credit: Decimal = Decimal("0")
    balance: Optional[Decimal] = None
    value_date: Optional[date] = None
    reference_number: Optional[str] = None
    category: TransactionCategory = TransactionCategory.OTHER
    is_interest: bool = False
    
    def __post_init__(self):
        """Auto-detect interest transactions."""
        desc_upper = self.description.upper()
        if any(kw in desc_upper for kw in ['INT PD', 'INTEREST', 'INT.PD', 'INT PAID']):
            self.is_interest = True
            self.category = TransactionCategory.INTEREST

@dataclass
class BankAccount:
    account_number: str  # Full number (will be encrypted)
    bank_name: str
    account_type: str = "SAVINGS"
    branch: Optional[str] = None
    ifsc_code: Optional[str] = None
    
    @property
    def masked_number(self) -> str:
        """Return masked account number: ****1234"""
        return f"****{self.account_number[-4:]}"

@dataclass
class ParseResult:
    success: bool
    transactions: list[BankTransaction] = field(default_factory=list)
    account: Optional[BankAccount] = None
    errors: list[str] = field(default_factory=list)
    source_file: str = ""
```

### base.py (Bank Parser)
```python
"""Base class for bank statement parsers."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional
import keyring
import PyPDF2
import pdfplumber

from .models import ParseResult, BankTransaction, BankAccount

class BankStatementParser(ABC):
    """Abstract base class for bank statement parsers."""
    
    BANK_NAME: str = ""  # Override in subclass
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def parse(self, file_path: Path, password: Optional[str] = None) -> ParseResult:
        """Parse bank statement file."""
        if not file_path.exists():
            return ParseResult(success=False, errors=[f"File not found: {file_path}"])
        
        # Handle password-protected PDFs
        if file_path.suffix.lower() == '.pdf':
            password = password or self._get_stored_password(file_path.name)
            content = self._read_pdf(file_path, password)
        elif file_path.suffix.lower() in ['.xlsx', '.xls']:
            content = self._read_excel(file_path)
        else:
            return ParseResult(success=False, errors=[f"Unsupported format: {file_path.suffix}"])
        
        return self._parse_content(content, str(file_path))
    
    def _get_stored_password(self, filename: str) -> Optional[str]:
        """Retrieve password from system keyring."""
        return keyring.get_password("pfas_bank", filename)
    
    def _store_password(self, filename: str, password: str) -> None:
        """Store password in system keyring."""
        keyring.set_password("pfas_bank", filename, password)
    
    def _read_pdf(self, file_path: Path, password: Optional[str]) -> str:
        """Read PDF content, handling encryption."""
        with pdfplumber.open(file_path, password=password) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() or ""
                # Also try to extract tables
                tables = page.extract_tables()
                for table in tables:
                    text += "\n" + self._table_to_text(table)
            return text
    
    @abstractmethod
    def _parse_content(self, content: str, source_file: str) -> ParseResult:
        """Parse extracted content. Override in subclass."""
        pass
    
    def save_to_db(self, result: ParseResult) -> int:
        """Save parsed transactions to database."""
        if not result.success or not result.account:
            return 0
        
        # Create or get bank account
        account_id = self._get_or_create_account(result.account)
        
        # Insert transactions (with duplicate prevention)
        count = 0
        for txn in result.transactions:
            if self._insert_transaction(account_id, txn, result.source_file):
                count += 1
        
        return count
```

### icici.py
```python
"""ICICI Bank statement parser."""

import re
from datetime import datetime
from decimal import Decimal
from .base import BankStatementParser
from .models import ParseResult, BankTransaction, BankAccount

class ICICIParser(BankStatementParser):
    """Parser for ICICI Bank PDF statements."""
    
    BANK_NAME = "ICICI Bank"
    
    # Regex patterns for ICICI format
    ACCOUNT_PATTERN = r"Account Number[:\s]+(\d+)"
    DATE_PATTERN = r"(\d{2}-\d{2}-\d{4})"
    TRANSACTION_PATTERN = r"(\d{2}-\d{2}-\d{4})\s+(\d{2}-\d{2}-\d{4})\s+(.+?)\s+([\d,]+\.\d{2})?\s*([\d,]+\.\d{2})?\s+([\d,]+\.\d{2})"
    
    def _parse_content(self, content: str, source_file: str) -> ParseResult:
        """Parse ICICI bank statement content."""
        errors = []
        transactions = []
        
        # Extract account number
        account_match = re.search(self.ACCOUNT_PATTERN, content)
        if not account_match:
            return ParseResult(success=False, errors=["Could not find account number"])
        
        account = BankAccount(
            account_number=account_match.group(1),
            bank_name=self.BANK_NAME
        )
        
        # Parse transactions
        for match in re.finditer(self.TRANSACTION_PATTERN, content):
            try:
                txn_date = datetime.strptime(match.group(1), "%d-%m-%Y").date()
                value_date = datetime.strptime(match.group(2), "%d-%m-%Y").date()
                description = match.group(3).strip()
                debit = self._parse_amount(match.group(4))
                credit = self._parse_amount(match.group(5))
                balance = self._parse_amount(match.group(6))
                
                txn = BankTransaction(
                    date=txn_date,
                    value_date=value_date,
                    description=description,
                    debit=debit,
                    credit=credit,
                    balance=balance
                )
                transactions.append(txn)
            except Exception as e:
                errors.append(f"Error parsing line: {str(e)}")
        
        return ParseResult(
            success=len(transactions) > 0,
            transactions=transactions,
            account=account,
            errors=errors,
            source_file=source_file
        )
    
    def _parse_amount(self, amount_str: str) -> Decimal:
        """Parse amount string to Decimal."""
        if not amount_str:
            return Decimal("0")
        return Decimal(amount_str.replace(",", ""))
```

### interest.py
```python
"""Bank interest calculation and 80TTA deduction."""

from decimal import Decimal
from datetime import date
from typing import List, Tuple

class InterestCalculator:
    """Calculate bank interest and 80TTA deduction."""
    
    MAX_80TTA = Decimal("10000")  # For non-senior citizens
    MAX_80TTB = Decimal("50000")  # For senior citizens
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def calculate_for_fy(self, bank_account_id: int, fy: str, 
                         is_senior_citizen: bool = False) -> Tuple[Decimal, Decimal]:
        """
        Calculate total interest and 80TTA/80TTB eligible amount for FY.
        
        Args:
            bank_account_id: Bank account ID
            fy: Financial year (e.g., '2024-25')
            is_senior_citizen: True for 80TTB (higher limit)
        
        Returns:
            Tuple of (total_interest, eligible_deduction)
        """
        # Get FY date range
        start_year = int(fy.split('-')[0])
        start_date = date(start_year, 4, 1)
        end_date = date(start_year + 1, 3, 31)
        
        # Query interest transactions
        cursor = self.conn.execute("""
            SELECT SUM(credit) as total_interest
            FROM bank_transactions
            WHERE bank_account_id = ?
              AND date BETWEEN ? AND ?
              AND is_interest = TRUE
        """, (bank_account_id, start_date, end_date))
        
        row = cursor.fetchone()
        total_interest = Decimal(str(row['total_interest'] or 0))
        
        # Calculate eligible deduction
        max_deduction = self.MAX_80TTB if is_senior_citizen else self.MAX_80TTA
        eligible = min(total_interest, max_deduction)
        
        # Save summary
        self._save_summary(bank_account_id, fy, total_interest, eligible)
        
        return total_interest, eligible
    
    def calculate_all_accounts(self, user_id: int, fy: str,
                               is_senior_citizen: bool = False) -> Tuple[Decimal, Decimal]:
        """Calculate combined interest from all bank accounts."""
        cursor = self.conn.execute("""
            SELECT id FROM bank_accounts WHERE user_id = ?
        """, (user_id,))
        
        total_interest = Decimal("0")
        for row in cursor.fetchall():
            interest, _ = self.calculate_for_fy(row['id'], fy, is_senior_citizen)
            total_interest += interest
        
        # 80TTA/80TTB applies to combined interest
        max_deduction = self.MAX_80TTB if is_senior_citizen else self.MAX_80TTA
        eligible = min(total_interest, max_deduction)
        
        return total_interest, eligible
```

---

## Test Cases

### TC-BANK-001: ICICI Statement Parse
```python
def test_icici_statement_parse(test_db, fixtures_path):
    """Test ICICI PDF statement parsing."""
    parser = ICICIParser(test_db)
    result = parser.parse(fixtures_path / "bank/icici_sample.pdf")
    
    assert result.success
    assert result.account is not None
    assert result.account.bank_name == "ICICI Bank"
    assert len(result.transactions) > 0
    
    # Verify transaction structure
    txn = result.transactions[0]
    assert txn.date is not None
    assert txn.description != ""
    assert txn.debit >= 0 or txn.credit >= 0
```

### TC-BANK-002: Interest 80TTA Calculation
```python
def test_interest_80tta_calculation(test_db):
    """Test 80TTA deduction calculation."""
    # Setup: Insert test interest transactions
    _insert_test_interest_transactions(test_db, total=15000)
    
    calculator = InterestCalculator(test_db)
    total, eligible = calculator.calculate_for_fy(
        bank_account_id=1,
        fy="2024-25",
        is_senior_citizen=False
    )
    
    assert total == Decimal("15000")
    assert eligible == Decimal("10000")  # Capped at 80TTA limit
```

### TC-BANK-003: Password-Protected PDF
```python
def test_password_protected_pdf(test_db, fixtures_path):
    """Test parsing password-protected bank statement."""
    import keyring
    
    # Store password in keyring
    keyring.set_password("pfas_bank", "icici_password_protected.pdf", "test123")
    
    parser = ICICIParser(test_db)
    result = parser.parse(fixtures_path / "bank/icici_password_protected.pdf")
    
    assert result.success
    assert len(result.transactions) > 0
```

### TC-BANK-004: Multi-Bank Consolidation
```python
def test_multi_bank_consolidation(test_db, fixtures_path):
    """Test merging transactions from multiple banks."""
    from pfas.parsers.bank import consolidate_bank_statements
    
    # Parse multiple statements
    icici_result = ICICIParser(test_db).parse(fixtures_path / "bank/icici_sample.pdf")
    sbi_result = SBIParser(test_db).parse(fixtures_path / "bank/sbi_sample.xlsx")
    
    # Consolidate
    consolidated = consolidate_bank_statements([icici_result, sbi_result])
    
    # Verify sorted by date
    dates = [t.date for t in consolidated]
    assert dates == sorted(dates)
    
    # Verify no duplicates
    unique_keys = set((t.date, t.description, t.debit, t.credit) for t in consolidated)
    assert len(unique_keys) == len(consolidated)
```

---

## Integration with Core

### Creating Journal Entries
```python
def create_bank_journal_entries(db_connection, bank_account_id: int, fy: str):
    """Create journal entries for bank interest."""
    from pfas.core.journal import JournalEngine, JournalEntry
    from pfas.core.accounts import get_account_id_by_code
    
    engine = JournalEngine(db_connection)
    
    # Get interest transactions
    cursor = db_connection.execute("""
        SELECT date, credit as amount, description
        FROM bank_transactions
        WHERE bank_account_id = ? AND is_interest = TRUE
        AND date BETWEEN ? AND ?
    """, (bank_account_id, fy_start, fy_end))
    
    for row in cursor.fetchall():
        entries = [
            JournalEntry(
                account_id=get_account_id_by_code(db_connection, "1101"),  # Bank
                debit=row['amount']
            ),
            JournalEntry(
                account_id=get_account_id_by_code(db_connection, "4201"),  # Interest Income
                credit=row['amount']
            )
        ]
        
        engine.create_journal(
            txn_date=row['date'],
            description=f"Bank Interest: {row['description']}",
            entries=entries,
            reference_type="BANK_INTEREST",
            reference_id=row['id']
        )
```

---

## Verification Commands

```bash
# Run bank parser tests
pytest tests/unit/test_parsers/test_bank/ -v

# Run with coverage
pytest tests/unit/test_parsers/test_bank/ --cov=src/pfas/parsers/bank --cov-report=term-missing

# Test specific parser
pytest tests/unit/test_parsers/test_bank/test_icici.py -v

# Expected: All 4 test cases pass, coverage > 80%
```

---

## Success Criteria

- [ ] ICICI PDF statements parsed correctly
- [ ] SBI Excel statements parsed correctly
- [ ] HDFC PDF statements parsed correctly
- [ ] Password-protected PDFs handled via keyring
- [ ] Interest transactions auto-detected
- [ ] 80TTA deduction calculated (max ₹10,000)
- [ ] Multiple statements consolidated without duplicates
- [ ] Journal entries created for interest income
- [ ] All unit tests passing
- [ ] Code coverage > 80%
