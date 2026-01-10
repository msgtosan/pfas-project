# Sprint 3: Zerodha Tax P&L Parser

## Module Overview
**Sprint:** S3 (Week 5-6)
**Phase:** 1
**Requirements:** REQ-STK-002
**Dependencies:** Core module, Stock base models

---

## Requirements

### REQ-STK-002: Zerodha Tax P&L Parser
- **Input:** Zerodha Tax P&L Excel (multi-sheet)
- **Processing:** Extract trades from Equity Intraday, Equity Delivery, F&O sheets
- **Output:** Parsed trades with CG pre-calculated

---

## Zerodha Tax P&L Excel Structure

Based on project file `taxpnlQY63472024_2025Q1Q4.xlsx`:

### Sheets:
1. **TRADEWISE** - Individual trade details
2. **SCRIPWISE** - Stock-wise summary
3. **SPECULATIVE** - Intraday trades
4. **SUMMARY** - Overall P&L

### TRADEWISE Columns:
```
Symbol | ISIN | Trade Type | Quantity | Buy Date | Buy Price | Buy Value | 
Sell Date | Sell Price | Sell Value | Profit/Loss | STT
```

---

## Implementation

### zerodha.py
```python
"""Zerodha Tax P&L parser."""

import pandas as pd
from pathlib import Path
from decimal import Decimal
from typing import List

from .models import StockTrade, TradeType

class ZerodhaParser:
    """Parser for Zerodha Tax P&L Excel."""
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def parse(self, file_path: Path) -> List[StockTrade]:
        """Parse Zerodha Tax P&L Excel."""
        trades = []
        excel = pd.ExcelFile(file_path)
        
        # Parse TRADEWISE sheet (main trade data)
        if 'TRADEWISE' in excel.sheet_names:
            df = pd.read_excel(excel, sheet_name='TRADEWISE')
            trades.extend(self._parse_tradewise(df))
        
        return trades
    
    def _parse_tradewise(self, df: pd.DataFrame) -> List[StockTrade]:
        """Parse TRADEWISE sheet."""
        trades = []
        
        for _, row in df.iterrows():
            # Create BUY trade
            buy = StockTrade(
                symbol=str(row['Symbol']),
                isin=str(row.get('ISIN', '')),
                trade_date=pd.to_datetime(row['Buy Date']).date(),
                trade_type=TradeType.BUY,
                quantity=int(row['Quantity']),
                price=Decimal(str(row['Buy Price'])),
                amount=Decimal(str(row['Buy Value'])),
                net_amount=Decimal(str(row['Buy Value']))
            )
            trades.append(buy)
            
            # Create SELL trade (pre-matched)
            sell = StockTrade(
                symbol=str(row['Symbol']),
                isin=str(row.get('ISIN', '')),
                trade_date=pd.to_datetime(row['Sell Date']).date(),
                trade_type=TradeType.SELL,
                quantity=int(row['Quantity']),
                price=Decimal(str(row['Sell Price'])),
                amount=Decimal(str(row['Sell Value'])),
                stt=Decimal(str(row.get('STT', 0))),
                net_amount=Decimal(str(row['Sell Value'])),
                # Pre-matched
                buy_date=pd.to_datetime(row['Buy Date']).date(),
                buy_price=Decimal(str(row['Buy Price'])),
                cost_of_acquisition=Decimal(str(row['Buy Value']))
            )
            trades.append(sell)
        
        return trades
```

---

# Sprint 4: EPF Passbook Parser

## Module Overview
**Sprint:** S4 (Week 7-8)
**Phase:** 1
**Requirements:** REQ-EPF-001 to REQ-EPF-006
**Dependencies:** Core module

---

## Requirements

### REQ-EPF-001: EPF Passbook Parser
- **Input:** EPFO PDF passbook (bilingual Hindi/English)
- **Processing:** Extract contributions, interest, TDS
- **Output:** EPF transactions in database

### REQ-EPF-002: Employee Contribution Tracking
- **Input:** Monthly contribution data
- **Processing:** Track 12% of Basic toward PF
- **Output:** EE PF contribution ledger

### REQ-EPF-003: Employer Contribution
- **Input:** Monthly contribution data
- **Processing:** Track ER PF (3.67%) and EPS (8.33%)
- **Output:** ER contribution and EPS ledger

### REQ-EPF-004: VPF Separation
- **Input:** Contributions exceeding statutory 12%
- **Processing:** Separate VPF from regular PF
- **Output:** VPF tracked separately

### REQ-EPF-005: Interest with TDS
- **Input:** Annual interest credit
- **Processing:** Calculate interest, apply TDS on excess (>₹2.5L)
- **Output:** Interest income, TDS if applicable

### REQ-EPF-006: 80C Eligible Amount
- **Input:** EE contribution + VPF
- **Processing:** Calculate 80C eligible (max ₹1.5L combined)
- **Output:** 80C deduction amount

---

## EPF Passbook Format

Based on project file `EPF_Interest_APHYD00476720000003193_2024.pdf`:

```
Establishment ID/Name: APHYD0047672000 / QUAL COMM INDIA PVT.LTD.
Member ID/Name: APHYD00476720000003193 / SANJAY SHANKAR
UAN: 100379251525

EPF Passbook [Financial Year - 2024-2025]

Wage Month | Date    | Type | EPF    | EPS    | Employee | Employer | Pension
Mar-2024   |09-04-24 | CR   |5,60,456|15,000  |1,23,301  |66,005    |1,250
Apr-2024   |10-05-24 | CR   |5,60,456|15,000  |1,23,301  |66,005    |1,250

Total Contributions: EE=9,96,888 | ER=8,13,750 | Pension=15,000
Interest: EE=16,28,748 | ER=8,23,494
TDS on interest (EE >2.5L): -34,159
```

### Key Fields:
- **Employee Balance**: Cumulative EE contributions
- **Employer Balance**: Cumulative ER contributions (PF portion only)
- **Pension Balance**: EPS (not withdrawable until retirement)
- **TDS on taxable interest**: For contributions >₹2.5L/year

---

## Database Schema

```sql
CREATE TABLE IF NOT EXISTS epf_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    uan TEXT UNIQUE NOT NULL,
    establishment_id TEXT NOT NULL,
    establishment_name TEXT,
    member_id TEXT NOT NULL,
    date_of_joining DATE,
    account_id INTEGER REFERENCES accounts(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS epf_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    epf_account_id INTEGER REFERENCES epf_accounts(id),
    wage_month TEXT NOT NULL,  -- 'Apr-2024'
    transaction_date DATE NOT NULL,
    transaction_type TEXT NOT NULL,  -- 'CR', 'DR', 'INT'
    wages DECIMAL(15,2),
    eps_wages DECIMAL(15,2),
    employee_contribution DECIMAL(15,2) DEFAULT 0,
    employer_contribution DECIMAL(15,2) DEFAULT 0,
    pension_contribution DECIMAL(15,2) DEFAULT 0,
    vpf_contribution DECIMAL(15,2) DEFAULT 0,  -- Amount exceeding 12%
    employee_balance DECIMAL(15,2),
    employer_balance DECIMAL(15,2),
    pension_balance DECIMAL(15,2),
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS epf_interest (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    epf_account_id INTEGER REFERENCES epf_accounts(id),
    financial_year TEXT NOT NULL,
    employee_interest DECIMAL(15,2) DEFAULT 0,
    employer_interest DECIMAL(15,2) DEFAULT 0,
    taxable_interest DECIMAL(15,2) DEFAULT 0,  -- Interest on contribution >2.5L
    tds_deducted DECIMAL(15,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(epf_account_id, financial_year)
);
```

---

## Implementation

### epf.py
```python
"""EPF Passbook PDF parser."""

import re
import pdfplumber
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class EPFAccount:
    uan: str
    establishment_id: str
    establishment_name: str
    member_id: str
    member_name: str

@dataclass
class EPFTransaction:
    wage_month: str
    transaction_date: date
    transaction_type: str
    wages: Decimal
    eps_wages: Decimal
    employee_contribution: Decimal
    employer_contribution: Decimal
    pension_contribution: Decimal
    employee_balance: Decimal = Decimal("0")
    employer_balance: Decimal = Decimal("0")
    pension_balance: Decimal = Decimal("0")

@dataclass
class EPFInterest:
    financial_year: str
    employee_interest: Decimal
    employer_interest: Decimal
    taxable_interest: Decimal = Decimal("0")
    tds_deducted: Decimal = Decimal("0")

class EPFParser:
    """Parser for EPFO Member Passbook PDF."""
    
    # Regex patterns for bilingual content
    ACCOUNT_PATTERNS = {
        'uan': r'UAN\s*[:\|]\s*(\d+)',
        'establishment_id': r'Establishment ID[/Name]*\s*[:\|]\s*(\w+)',
        'establishment_name': r'Establishment ID/Name\s*[:\|]\s*\w+\s*/\s*(.+?)(?:\n|,)',
        'member_id': r'Member ID[/Name]*\s*[:\|]\s*(\w+)',
        'member_name': r'Member ID/Name\s*[:\|]\s*\w+\s*/\s*(.+?)(?:\n|,)',
    }
    
    # Transaction line pattern: Wage Month | Date | Type | EPF | EPS | EE | ER | Pension
    TXN_PATTERN = r'(\w{3}-\d{4})\s+(\d{2}-\d{2}-\d{4})\s+(\w+)\s+.*?(\d[\d,]+\.\d{2})\s+(\d[\d,]+\.\d{2})\s+(\d[\d,]+\.\d{2})\s+(\d[\d,]+\.\d{2})\s+(\d[\d,]+\.\d{2})'
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def parse(self, file_path: Path) -> tuple[EPFAccount, List[EPFTransaction], Optional[EPFInterest]]:
        """Parse EPF passbook PDF."""
        with pdfplumber.open(file_path) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() or ""
        
        account = self._parse_account_info(text)
        transactions = self._parse_transactions(text)
        interest = self._parse_interest(text)
        
        return account, transactions, interest
    
    def _parse_account_info(self, text: str) -> EPFAccount:
        """Extract account header information."""
        info = {}
        for field, pattern in self.ACCOUNT_PATTERNS.items():
            match = re.search(pattern, text, re.IGNORECASE)
            info[field] = match.group(1).strip() if match else ""
        
        return EPFAccount(**info)
    
    def _parse_transactions(self, text: str) -> List[EPFTransaction]:
        """Extract monthly transactions."""
        transactions = []
        
        for match in re.finditer(self.TXN_PATTERN, text):
            txn = EPFTransaction(
                wage_month=match.group(1),
                transaction_date=datetime.strptime(match.group(2), "%d-%m-%Y").date(),
                transaction_type=match.group(3),
                wages=self._to_decimal(match.group(4)),
                eps_wages=self._to_decimal(match.group(5)),
                employee_contribution=self._to_decimal(match.group(6)),
                employer_contribution=self._to_decimal(match.group(7)),
                pension_contribution=self._to_decimal(match.group(8))
            )
            transactions.append(txn)
        
        return transactions
    
    def _parse_interest(self, text: str) -> Optional[EPFInterest]:
        """Extract interest and TDS information."""
        # Look for interest line
        int_match = re.search(r'Int\.\s*Updated.*?(\d[\d,]+\.\d{2})\s+(\d[\d,]+\.\d{2})', text)
        tds_match = re.search(r'TDS.*?(-?\d[\d,]+\.\d{2})', text)
        
        if int_match:
            return EPFInterest(
                financial_year="",  # Will be set from context
                employee_interest=self._to_decimal(int_match.group(1)),
                employer_interest=self._to_decimal(int_match.group(2)),
                tds_deducted=abs(self._to_decimal(tds_match.group(1))) if tds_match else Decimal("0")
            )
        return None
    
    def _to_decimal(self, value: str) -> Decimal:
        if not value:
            return Decimal("0")
        return Decimal(value.replace(",", ""))
    
    def calculate_80c_eligible(self, transactions: List[EPFTransaction]) -> Decimal:
        """Calculate 80C eligible amount (EE contribution + VPF)."""
        total_ee = sum(t.employee_contribution for t in transactions)
        # 80C limit is combined with other 80C investments
        return total_ee
```

---

# Sprint 4: PPF Statement Parser

## Requirements

### REQ-PPF-001 to REQ-PPF-004
- Parse bank PPF statement (Excel)
- Calculate interest at prevailing rate
- Track 80C deduction (max ₹1.5L)
- Calculate maturity date (15 years)

---

## Implementation

### ppf.py
```python
"""PPF Statement parser."""

import pandas as pd
from pathlib import Path
from datetime import date
from decimal import Decimal
from dataclasses import dataclass
from typing import List

@dataclass
class PPFTransaction:
    date: date
    transaction_type: str  # DEPOSIT, INTEREST, WITHDRAWAL
    amount: Decimal
    balance: Decimal

class PPFParser:
    """Parser for PPF bank statements."""
    
    PPF_INTEREST_RATE = Decimal("7.1")  # Current rate (subject to change)
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def parse(self, file_path: Path) -> List[PPFTransaction]:
        """Parse PPF statement Excel."""
        df = pd.read_excel(file_path)
        transactions = []
        
        for _, row in df.iterrows():
            txn = PPFTransaction(
                date=pd.to_datetime(row['Date']).date(),
                transaction_type=self._determine_type(row),
                amount=Decimal(str(row.get('Credit', 0) or row.get('Debit', 0))),
                balance=Decimal(str(row['Balance']))
            )
            transactions.append(txn)
        
        return transactions
    
    def calculate_80c_eligible(self, transactions: List[PPFTransaction], fy: str) -> Decimal:
        """Calculate 80C eligible deposits for FY."""
        start_year = int(fy.split('-')[0])
        fy_start = date(start_year, 4, 1)
        fy_end = date(start_year + 1, 3, 31)
        
        deposits = sum(
            t.amount for t in transactions 
            if t.transaction_type == 'DEPOSIT' and fy_start <= t.date <= fy_end
        )
        
        return min(deposits, Decimal("150000"))  # 80C limit
```

---

# Sprint 4: NPS Statement Parser

## Requirements

### REQ-NPS-001 to REQ-NPS-005
- Parse NPS statement CSV
- Track EE and ER contributions
- Calculate 80CCD(2) deduction (10% of Basic)
- Track additional 80CCD(1B) (₹50,000)
- Store NAV history

---

## NPS CSV Format

Based on project file `110091211424_NPS.csv`:

```csv
PRAN,Transaction Date,Transaction Type,Tier,Amount,Units,NAV,Scheme
110091211424,15-Apr-2024,Contribution,I,28023.00,1234.56,22.70,Scheme E - Tier I
```

---

## Implementation

### nps.py
```python
"""NPS Statement parser."""

import pandas as pd
from pathlib import Path
from datetime import date
from decimal import Decimal
from dataclasses import dataclass
from typing import List

@dataclass
class NPSTransaction:
    pran: str
    date: date
    transaction_type: str  # Contribution, Redemption
    tier: str  # I, II
    amount: Decimal
    units: Decimal
    nav: Decimal
    scheme: str

class NPSParser:
    """Parser for NPS statements."""
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def parse(self, file_path: Path) -> List[NPSTransaction]:
        """Parse NPS CSV statement."""
        df = pd.read_csv(file_path)
        transactions = []
        
        for _, row in df.iterrows():
            txn = NPSTransaction(
                pran=str(row['PRAN']),
                date=pd.to_datetime(row['Transaction Date'], dayfirst=True).date(),
                transaction_type=str(row['Transaction Type']),
                tier=str(row['Tier']),
                amount=Decimal(str(row['Amount'])),
                units=Decimal(str(row['Units'])),
                nav=Decimal(str(row['NAV'])),
                scheme=str(row['Scheme'])
            )
            transactions.append(txn)
        
        return transactions
    
    def calculate_deductions(self, transactions: List[NPSTransaction], 
                            basic_salary: Decimal, fy: str) -> dict:
        """
        Calculate NPS deductions.
        
        Returns:
            Dict with 80CCD(1), 80CCD(1B), 80CCD(2) amounts
        """
        start_year = int(fy.split('-')[0])
        fy_start = date(start_year, 4, 1)
        fy_end = date(start_year + 1, 3, 31)
        
        # Filter Tier I contributions in FY
        tier1_contributions = sum(
            t.amount for t in transactions
            if t.tier == 'I' and t.transaction_type == 'Contribution'
            and fy_start <= t.date <= fy_end
        )
        
        # 80CCD(2) - Employer contribution (max 10% of Basic)
        # This comes from payslip, not NPS statement
        max_80ccd2 = basic_salary * Decimal("0.10")
        
        # 80CCD(1B) - Additional ₹50,000
        additional_80ccd1b = min(tier1_contributions, Decimal("50000"))
        
        return {
            '80CCD_1': tier1_contributions,  # Part of 80C limit
            '80CCD_1B': additional_80ccd1b,  # Additional ₹50K
            '80CCD_2_limit': max_80ccd2  # ER contribution limit
        }
```

---

## Verification Commands

```bash
# Sprint 3 - Zerodha
pytest tests/unit/test_parsers/test_stock/test_zerodha.py -v

# Sprint 4 - EPF/PPF/NPS
pytest tests/unit/test_parsers/test_epf/ -v
pytest tests/unit/test_parsers/test_ppf/ -v
pytest tests/unit/test_parsers/test_nps/ -v

# All Sprint 4
pytest tests/unit/test_parsers/test_epf/ tests/unit/test_parsers/test_ppf/ tests/unit/test_parsers/test_nps/ -v --cov
```

---

## Success Criteria

### Sprint 3 - Zerodha
- [ ] Tax P&L Excel parsed (all sheets)
- [ ] Trades pre-matched correctly
- [ ] Speculative trades separated

### Sprint 4 - EPF
- [ ] Passbook PDF parsed (bilingual)
- [ ] EE/ER contributions tracked
- [ ] VPF separated if >12%
- [ ] Interest with TDS calculated
- [ ] 80C eligible amount computed

### Sprint 4 - PPF
- [ ] Statement Excel parsed
- [ ] Interest calculated
- [ ] 80C deduction tracked
- [ ] Maturity date calculated

### Sprint 4 - NPS
- [ ] CSV statement parsed
- [ ] Tier I/II separated
- [ ] 80CCD(1B) additional ₹50K tracked
- [ ] NAV history stored
