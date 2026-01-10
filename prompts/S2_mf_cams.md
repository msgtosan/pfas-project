# Sprint 2: Mutual Fund CAMS CAS Parser

## Module Overview
**Sprint:** S2 (Week 3-4)
**Phase:** 1
**Requirements:** REQ-MF-001, REQ-MF-003, REQ-MF-004, REQ-MF-005, REQ-MF-006, REQ-MF-008, REQ-MF-009
**Dependencies:** Core module complete

---

## Requirements to Implement

### REQ-MF-001: CAMS CAS Parser
- **Input:** CAMS Consolidated Account Statement (password-protected PDF/Excel)
- **Processing:** Extract investor details, transactions, capital gains
- **Output:** Parsed MF holdings and transactions in database

### REQ-MF-003: Equity Fund Classification
- **Input:** Scheme name, ISIN
- **Processing:** Classify as EQUITY if >65% equity holdings
- **Output:** asset_class = 'EQUITY' for tax treatment

### REQ-MF-004: Debt Fund Classification
- **Input:** Scheme name, ISIN
- **Processing:** Classify as DEBT if <65% equity holdings
- **Output:** asset_class = 'DEBT' for tax treatment

### REQ-MF-005: STCG Calculation (Equity)
- **Input:** Equity MF redemption <12 months holding
- **Processing:** Calculate gain, apply 20% tax rate
- **Output:** STCG amount with tax liability

### REQ-MF-006: LTCG Calculation (Equity)
- **Input:** Equity MF redemption >12 months holding
- **Processing:** Calculate gain, apply 12.5% tax rate (₹1.25L exemption)
- **Output:** LTCG amount with tax liability

### REQ-MF-008: Grandfathering (Pre-31-Jan-2018)
- **Input:** Purchase before 31-Jan-2018
- **Processing:** Use higher of (actual cost, FMV on 31-Jan-2018) as cost basis
- **Output:** Adjusted cost basis for CG calculation

### REQ-MF-009: Capital Gains Statement
- **Input:** All MF transactions for FY
- **Processing:** Generate quarterly CG statement
- **Output:** Excel report with STCG/LTCG breakdown

---

## CAMS CAS File Structure

Based on project file `Indian-MF-Stock-CG_sheet_details.md`:

### Sheet 1: INVESTOR_DETAILS
```
EMAIL | INV_NAME | ADDRESS1 | ADDRESS2 | ADDRESS3 | CITY | PINCODE | STATE | COUNTRY | MOBILE_NO
```

### Sheet 2: TRXN_DETAILS (Main Transaction Data)
```
AMC Name | Folio No | ASSET CLASS | NAME | STATUS | PAN | GUARDIAN_PAN | Scheme Name | 
Desc | Date | Units | Amount | Price | STT | Desc_1 | Date_1 | PurhUnit | RedUnits | 
Unit Cost | Indexed Cost | Units As On 31/01/2018 (Grandfathered Units) | 
NAV As On 31/01/2018 (Grandfathered NAV) | Market Value As On 31/01/2018 (Grandfathered Value) | 
Short Term | Long Term With Index | Long Term Without Index | Tax Perc | Tax Deduct | Tax Surcharge
```

### Sample Rows:
```
# DEBT Fund Example:
Kotak Mutual Fund | 1669887 / 01 | DEBT | Sanjay Shankar | Individual | AAPPS0793R | | 
Kotak Corporate Bond Fund Direct Growth, ISIN : INF178L01BY0 | Redemption | 11-Jul-2024 | 
672.634 | 2429280.930 | 3611.594 | 0.000 | Purchase (Continuous Offer) | 13-Oct-2020 | 
127.942 | 127.942 | 2931.018 | 3534.749 | 0.000 | 0.000 | 0.000 | 9831.660 | 0.000 | 0.000 | 0.000 | 0.000

# EQUITY Fund Example:
SBI Mutual Fund | 6021208 | EQUITY | Sanjay Shankar | Individual | AAPPS0793R | | 
SBI Consumption Opportunities Fund Direct Growth | Redemption | 11-Jul-2024 | 
192.345 | 69034.590 | 358.914 | 0.690 | Purchase - Systematic | 15-Jan-2024 | 
32.807 | 32.807 | 304.815 | 0.000 | 0.000 | 0.000 | 1774.834 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000
```

### Sheet 3: SCHEMEWISE_EQUITY
Equity scheme summary

### Sheet 4: SCHEMEWISE_DEBT
Debt scheme summary

---

## Database Schema

```sql
CREATE TABLE IF NOT EXISTS mf_amcs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    short_name TEXT,
    website TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mf_schemes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    amc_id INTEGER REFERENCES mf_amcs(id),
    name TEXT NOT NULL,
    isin TEXT UNIQUE,
    asset_class TEXT NOT NULL CHECK(asset_class IN ('EQUITY', 'DEBT', 'HYBRID', 'OTHER')),
    scheme_type TEXT,  -- GROWTH, DIVIDEND, IDCW
    nav_31jan2018 DECIMAL(15,4),  -- For grandfathering
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mf_folios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    scheme_id INTEGER REFERENCES mf_schemes(id),
    folio_number TEXT NOT NULL,
    status TEXT DEFAULT 'ACTIVE',
    opening_date DATE,
    account_id INTEGER REFERENCES accounts(id),  -- Link to COA
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, scheme_id, folio_number)
);

CREATE TABLE IF NOT EXISTS mf_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    folio_id INTEGER REFERENCES mf_folios(id) NOT NULL,
    transaction_type TEXT NOT NULL CHECK(transaction_type IN 
        ('PURCHASE', 'REDEMPTION', 'SWITCH_IN', 'SWITCH_OUT', 'DIVIDEND', 'DIVIDEND_REINVEST')),
    date DATE NOT NULL,
    units DECIMAL(15,4) NOT NULL,
    nav DECIMAL(15,4) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    stt DECIMAL(15,2) DEFAULT 0,
    stamp_duty DECIMAL(15,2) DEFAULT 0,
    -- For redemptions - link to original purchase
    purchase_date DATE,
    purchase_units DECIMAL(15,4),
    purchase_nav DECIMAL(15,4),
    purchase_amount DECIMAL(15,2),
    -- Grandfathering
    grandfathered_units DECIMAL(15,4),
    grandfathered_nav DECIMAL(15,4),
    grandfathered_value DECIMAL(15,2),
    -- Capital gains (for redemptions)
    holding_period_days INTEGER,
    is_long_term BOOLEAN,
    cost_of_acquisition DECIMAL(15,2),
    indexed_cost DECIMAL(15,2),
    short_term_gain DECIMAL(15,2),
    long_term_gain DECIMAL(15,2),
    tax_percentage DECIMAL(5,2),
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mf_capital_gains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    financial_year TEXT NOT NULL,
    asset_class TEXT NOT NULL,  -- EQUITY or DEBT
    stcg_amount DECIMAL(15,2) DEFAULT 0,
    ltcg_amount DECIMAL(15,2) DEFAULT 0,
    ltcg_exemption DECIMAL(15,2) DEFAULT 0,  -- ₹1.25L for equity
    taxable_stcg DECIMAL(15,2) DEFAULT 0,
    taxable_ltcg DECIMAL(15,2) DEFAULT 0,
    stcg_tax_rate DECIMAL(5,2),
    ltcg_tax_rate DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, financial_year, asset_class)
);

CREATE INDEX IF NOT EXISTS idx_mf_txn_folio ON mf_transactions(folio_id);
CREATE INDEX IF NOT EXISTS idx_mf_txn_date ON mf_transactions(date);
CREATE INDEX IF NOT EXISTS idx_mf_txn_type ON mf_transactions(transaction_type);
```

---

## Files to Create

```
src/pfas/parsers/mf/
├── __init__.py
├── base.py              # Base MF parser class
├── cams.py              # CAMS CAS parser
├── models.py            # MFTransaction, MFScheme dataclasses
├── classifier.py        # Equity/Debt classification
├── capital_gains.py     # CG calculation engine
└── grandfathering.py    # Pre-31-Jan-2018 logic

tests/unit/test_parsers/test_mf/
├── __init__.py
├── test_cams.py
├── test_classifier.py
├── test_capital_gains.py
└── test_grandfathering.py

tests/fixtures/mf/
├── cams_cas_sample.xlsx
├── cams_cas_equity.xlsx
└── cams_cas_debt.xlsx
```

---

## Implementation Guidelines

### models.py
```python
"""Mutual Fund data models."""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional
from enum import Enum

class AssetClass(Enum):
    EQUITY = "EQUITY"
    DEBT = "DEBT"
    HYBRID = "HYBRID"
    OTHER = "OTHER"

class TransactionType(Enum):
    PURCHASE = "PURCHASE"
    REDEMPTION = "REDEMPTION"
    SWITCH_IN = "SWITCH_IN"
    SWITCH_OUT = "SWITCH_OUT"
    DIVIDEND = "DIVIDEND"
    DIVIDEND_REINVEST = "DIVIDEND_REINVEST"

@dataclass
class MFScheme:
    name: str
    amc_name: str
    isin: Optional[str] = None
    asset_class: AssetClass = AssetClass.OTHER
    nav_31jan2018: Optional[Decimal] = None
    
    def __post_init__(self):
        """Auto-classify based on scheme name if not set."""
        if self.asset_class == AssetClass.OTHER:
            self.asset_class = classify_scheme(self.name)

@dataclass
class MFTransaction:
    folio_number: str
    scheme: MFScheme
    transaction_type: TransactionType
    date: date
    units: Decimal
    nav: Decimal
    amount: Decimal
    stt: Decimal = Decimal("0")
    
    # Purchase details (for redemptions)
    purchase_date: Optional[date] = None
    purchase_units: Optional[Decimal] = None
    purchase_nav: Optional[Decimal] = None
    
    # Grandfathering
    grandfathered_units: Optional[Decimal] = None
    grandfathered_nav: Optional[Decimal] = None
    grandfathered_value: Optional[Decimal] = None
    
    # Computed capital gains
    short_term_gain: Decimal = Decimal("0")
    long_term_gain: Decimal = Decimal("0")
    
    @property
    def holding_period_days(self) -> Optional[int]:
        """Calculate holding period in days."""
        if self.purchase_date and self.transaction_type == TransactionType.REDEMPTION:
            return (self.date - self.purchase_date).days
        return None
    
    @property
    def is_long_term(self) -> bool:
        """Check if qualifies for LTCG (>12 months for equity)."""
        if self.holding_period_days is None:
            return False
        threshold = 365 if self.scheme.asset_class == AssetClass.EQUITY else 730  # 24 months for debt (old rule)
        return self.holding_period_days > threshold
```

### cams.py
```python
"""CAMS Consolidated Account Statement parser."""

import pandas as pd
from pathlib import Path
from datetime import datetime
from decimal import Decimal
from typing import List, Optional
import pdfplumber

from .models import MFTransaction, MFScheme, AssetClass, TransactionType
from .classifier import classify_scheme

class CAMSParser:
    """Parser for CAMS CAS statements (PDF and Excel)."""
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def parse(self, file_path: Path, password: Optional[str] = None) -> List[MFTransaction]:
        """Parse CAMS CAS file."""
        if file_path.suffix.lower() in ['.xlsx', '.xls']:
            return self._parse_excel(file_path)
        elif file_path.suffix.lower() == '.pdf':
            return self._parse_pdf(file_path, password)
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")
    
    def _parse_excel(self, file_path: Path) -> List[MFTransaction]:
        """Parse Excel CAS with capital gains data."""
        transactions = []
        
        # Read transaction sheet
        df = pd.read_excel(file_path, sheet_name='TRXN_DETAILS')
        
        for _, row in df.iterrows():
            # Extract scheme info
            scheme_name = str(row.get('Scheme Name', ''))
            isin = self._extract_isin(scheme_name)
            asset_class_str = str(row.get('ASSET CLASS', '')).upper()
            
            scheme = MFScheme(
                name=scheme_name,
                amc_name=str(row.get('AMC Name', '')),
                isin=isin,
                asset_class=AssetClass[asset_class_str] if asset_class_str in ['EQUITY', 'DEBT'] else AssetClass.OTHER,
                nav_31jan2018=self._to_decimal(row.get('NAV As On 31/01/2018 (Grandfathered NAV)'))
            )
            
            # Determine transaction type
            desc = str(row.get('Desc', '')).upper()
            txn_type = self._determine_transaction_type(desc)
            
            # Create transaction
            txn = MFTransaction(
                folio_number=str(row.get('Folio No', '')),
                scheme=scheme,
                transaction_type=txn_type,
                date=pd.to_datetime(row.get('Date')).date(),
                units=self._to_decimal(row.get('Units')),
                nav=self._to_decimal(row.get('Price')),
                amount=self._to_decimal(row.get('Amount')),
                stt=self._to_decimal(row.get('STT')),
                # Purchase info (for redemptions)
                purchase_date=pd.to_datetime(row.get('Date_1')).date() if pd.notna(row.get('Date_1')) else None,
                purchase_units=self._to_decimal(row.get('PurhUnit')),
                purchase_nav=self._to_decimal(row.get('Unit Cost')),
                # Grandfathering
                grandfathered_units=self._to_decimal(row.get('Units As On 31/01/2018 (Grandfathered Units)')),
                grandfathered_nav=self._to_decimal(row.get('NAV As On 31/01/2018 (Grandfathered NAV)')),
                grandfathered_value=self._to_decimal(row.get('Market Value As On 31/01/2018 (Grandfathered Value)')),
                # Capital gains from CAMS
                short_term_gain=self._to_decimal(row.get('Short Term')),
                long_term_gain=self._to_decimal(row.get('Long Term Without Index'))
            )
            
            transactions.append(txn)
        
        return transactions
    
    def _extract_isin(self, scheme_name: str) -> Optional[str]:
        """Extract ISIN from scheme name."""
        import re
        match = re.search(r'ISIN\s*:\s*([A-Z0-9]{12})', scheme_name)
        return match.group(1) if match else None
    
    def _determine_transaction_type(self, description: str) -> TransactionType:
        """Determine transaction type from description."""
        desc = description.upper()
        if 'REDEMPTION' in desc:
            return TransactionType.REDEMPTION
        elif 'SWITCH OUT' in desc or 'SWITCH-OUT' in desc:
            return TransactionType.SWITCH_OUT
        elif 'SWITCH IN' in desc or 'SWITCH-IN' in desc:
            return TransactionType.SWITCH_IN
        elif 'DIVIDEND' in desc and 'REINVEST' in desc:
            return TransactionType.DIVIDEND_REINVEST
        elif 'DIVIDEND' in desc:
            return TransactionType.DIVIDEND
        else:
            return TransactionType.PURCHASE
    
    def _to_decimal(self, value) -> Decimal:
        """Convert value to Decimal safely."""
        if pd.isna(value) or value == '' or value is None:
            return Decimal("0")
        return Decimal(str(value))
```

### capital_gains.py
```python
"""Mutual Fund Capital Gains Calculation Engine."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import List, Tuple

from .models import MFTransaction, AssetClass, TransactionType

@dataclass
class CapitalGainsSummary:
    """Summary of capital gains for a period."""
    financial_year: str
    asset_class: AssetClass
    
    # Gross amounts
    stcg_amount: Decimal = Decimal("0")
    ltcg_amount: Decimal = Decimal("0")
    
    # Exemptions (₹1.25L for equity LTCG from FY 2024-25)
    ltcg_exemption: Decimal = Decimal("0")
    
    # Taxable amounts
    taxable_stcg: Decimal = Decimal("0")
    taxable_ltcg: Decimal = Decimal("0")
    
    # Tax rates
    stcg_tax_rate: Decimal = Decimal("0")
    ltcg_tax_rate: Decimal = Decimal("0")

class CapitalGainsCalculator:
    """Calculate capital gains for mutual fund transactions."""
    
    # Tax rates (Budget 2024)
    EQUITY_STCG_RATE = Decimal("20")  # 20% for equity STCG
    EQUITY_LTCG_RATE = Decimal("12.5")  # 12.5% for equity LTCG
    EQUITY_LTCG_EXEMPTION = Decimal("125000")  # ₹1.25 lakh exemption
    
    # Debt funds - taxed at slab rate (no special rate)
    DEBT_STCG_RATE = Decimal("0")  # Slab rate
    DEBT_LTCG_RATE = Decimal("0")  # Slab rate (no indexation from April 2023)
    
    GRANDFATHERING_DATE = date(2018, 1, 31)
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def calculate_for_transaction(self, txn: MFTransaction) -> Tuple[Decimal, Decimal]:
        """
        Calculate capital gain for a single redemption transaction.
        
        Returns:
            Tuple of (short_term_gain, long_term_gain)
        """
        if txn.transaction_type != TransactionType.REDEMPTION:
            return Decimal("0"), Decimal("0")
        
        # Use grandfathered cost if applicable
        cost_of_acquisition = self._get_cost_of_acquisition(txn)
        
        # Sale value
        sale_value = txn.amount
        
        # Capital gain
        gain = sale_value - cost_of_acquisition - txn.stt
        
        if txn.is_long_term:
            return Decimal("0"), gain
        else:
            return gain, Decimal("0")
    
    def _get_cost_of_acquisition(self, txn: MFTransaction) -> Decimal:
        """
        Get cost of acquisition, applying grandfathering if eligible.
        
        For pre-31-Jan-2018 purchases:
        - Cost = Higher of (actual cost, FMV on 31-Jan-2018)
        - But FMV is capped at sale price (to avoid artificial loss)
        """
        actual_cost = (txn.purchase_nav or Decimal("0")) * (txn.purchase_units or Decimal("0"))
        
        # Check if grandfathering applies
        if txn.purchase_date and txn.purchase_date <= self.GRANDFATHERING_DATE:
            if txn.grandfathered_value and txn.grandfathered_value > 0:
                # FMV on 31-Jan-2018
                fmv = txn.grandfathered_value
                
                # Cap FMV at sale price (to prevent artificial loss)
                sale_price = txn.amount
                fmv_capped = min(fmv, sale_price)
                
                # Use higher of actual cost or capped FMV
                return max(actual_cost, fmv_capped)
        
        return actual_cost
    
    def calculate_summary(self, user_id: int, fy: str) -> List[CapitalGainsSummary]:
        """
        Calculate capital gains summary for a financial year.
        
        Args:
            user_id: User ID
            fy: Financial year (e.g., '2024-25')
        
        Returns:
            List of CapitalGainsSummary for EQUITY and DEBT
        """
        summaries = []
        
        for asset_class in [AssetClass.EQUITY, AssetClass.DEBT]:
            summary = self._calculate_for_asset_class(user_id, fy, asset_class)
            summaries.append(summary)
        
        return summaries
    
    def _calculate_for_asset_class(self, user_id: int, fy: str, 
                                    asset_class: AssetClass) -> CapitalGainsSummary:
        """Calculate CG summary for specific asset class."""
        start_year = int(fy.split('-')[0])
        fy_start = date(start_year, 4, 1)
        fy_end = date(start_year + 1, 3, 31)
        
        # Query redemption transactions
        # ... implementation details ...
        
        stcg_total = Decimal("0")
        ltcg_total = Decimal("0")
        
        # Calculate exemption for equity LTCG
        ltcg_exemption = Decimal("0")
        if asset_class == AssetClass.EQUITY:
            ltcg_exemption = min(ltcg_total, self.EQUITY_LTCG_EXEMPTION)
        
        taxable_ltcg = max(Decimal("0"), ltcg_total - ltcg_exemption)
        
        # Set tax rates
        if asset_class == AssetClass.EQUITY:
            stcg_rate = self.EQUITY_STCG_RATE
            ltcg_rate = self.EQUITY_LTCG_RATE
        else:
            stcg_rate = self.DEBT_STCG_RATE  # Slab rate
            ltcg_rate = self.DEBT_LTCG_RATE  # Slab rate
        
        return CapitalGainsSummary(
            financial_year=fy,
            asset_class=asset_class,
            stcg_amount=stcg_total,
            ltcg_amount=ltcg_total,
            ltcg_exemption=ltcg_exemption,
            taxable_stcg=stcg_total,  # STCG fully taxable
            taxable_ltcg=taxable_ltcg,
            stcg_tax_rate=stcg_rate,
            ltcg_tax_rate=ltcg_rate
        )
```

---

## Test Cases

### TC-MF-001: CAMS CAS Parse
```python
def test_cams_cas_parse(test_db, fixtures_path):
    """Test CAMS CAS Excel parsing."""
    parser = CAMSParser(test_db)
    transactions = parser.parse(fixtures_path / "mf/cams_cas_sample.xlsx")
    
    assert len(transactions) > 0
    
    # Verify transaction structure
    txn = transactions[0]
    assert txn.folio_number != ""
    assert txn.scheme.name != ""
    assert txn.date is not None
    assert txn.units > 0
```

### TC-MF-003: Equity Fund Classification
```python
def test_equity_fund_classification():
    """Test equity fund auto-classification."""
    from pfas.parsers.mf.classifier import classify_scheme
    
    # Equity fund indicators
    assert classify_scheme("SBI Bluechip Fund Direct Growth") == AssetClass.EQUITY
    assert classify_scheme("HDFC Top 100 Fund") == AssetClass.EQUITY
    assert classify_scheme("Mirae Asset Large Cap Fund") == AssetClass.EQUITY
```

### TC-MF-004: Debt Fund Classification
```python
def test_debt_fund_classification():
    """Test debt fund auto-classification."""
    from pfas.parsers.mf.classifier import classify_scheme
    
    # Debt fund indicators
    assert classify_scheme("HDFC Short Term Debt Fund") == AssetClass.DEBT
    assert classify_scheme("Kotak Corporate Bond Fund") == AssetClass.DEBT
    assert classify_scheme("SBI Liquid Fund") == AssetClass.DEBT
```

### TC-MF-005: Equity STCG Calculation
```python
def test_equity_stcg_calculation(test_db):
    """Test STCG calculation for equity <12 months."""
    calc = CapitalGainsCalculator(test_db)
    
    txn = MFTransaction(
        folio_number="12345",
        scheme=MFScheme(name="Test Equity Fund", amc_name="Test", asset_class=AssetClass.EQUITY),
        transaction_type=TransactionType.REDEMPTION,
        date=date(2024, 7, 15),
        units=Decimal("100"),
        nav=Decimal("150"),
        amount=Decimal("15000"),  # Sale value
        purchase_date=date(2024, 3, 1),  # <12 months
        purchase_units=Decimal("100"),
        purchase_nav=Decimal("120"),  # Cost = ₹12,000
    )
    
    stcg, ltcg = calc.calculate_for_transaction(txn)
    
    assert stcg == Decimal("3000")  # 15000 - 12000
    assert ltcg == Decimal("0")
```

### TC-MF-006: Equity LTCG Calculation
```python
def test_equity_ltcg_calculation(test_db):
    """Test LTCG calculation for equity >12 months."""
    calc = CapitalGainsCalculator(test_db)
    
    txn = MFTransaction(
        folio_number="12345",
        scheme=MFScheme(name="Test Equity Fund", amc_name="Test", asset_class=AssetClass.EQUITY),
        transaction_type=TransactionType.REDEMPTION,
        date=date(2024, 7, 15),
        units=Decimal("100"),
        nav=Decimal("150"),
        amount=Decimal("15000"),  # Sale value
        purchase_date=date(2023, 1, 1),  # >12 months
        purchase_units=Decimal("100"),
        purchase_nav=Decimal("120"),  # Cost = ₹12,000
    )
    
    stcg, ltcg = calc.calculate_for_transaction(txn)
    
    assert stcg == Decimal("0")
    assert ltcg == Decimal("3000")  # 15000 - 12000
```

### TC-MF-008: Grandfathering Pre-31-Jan-2018
```python
def test_grandfathering_pre_31jan2018(test_db):
    """Test grandfathering for pre-31-Jan-2018 purchases."""
    calc = CapitalGainsCalculator(test_db)
    
    txn = MFTransaction(
        folio_number="12345",
        scheme=MFScheme(name="Test Equity Fund", amc_name="Test", asset_class=AssetClass.EQUITY),
        transaction_type=TransactionType.REDEMPTION,
        date=date(2024, 7, 15),
        units=Decimal("100"),
        nav=Decimal("200"),
        amount=Decimal("20000"),  # Sale value
        purchase_date=date(2017, 6, 1),  # Before 31-Jan-2018
        purchase_units=Decimal("100"),
        purchase_nav=Decimal("100"),  # Actual cost = ₹10,000
        grandfathered_nav=Decimal("150"),  # FMV on 31-Jan-2018
        grandfathered_value=Decimal("15000"),  # FMV > actual cost
    )
    
    stcg, ltcg = calc.calculate_for_transaction(txn)
    
    # Should use FMV (₹15,000) as cost since it's higher than actual (₹10,000)
    # LTCG = 20000 - 15000 = 5000
    assert ltcg == Decimal("5000")
```

---

## Verification Commands

```bash
# Run MF CAMS tests
pytest tests/unit/test_parsers/test_mf/ -v

# Run with coverage
pytest tests/unit/test_parsers/test_mf/ --cov=src/pfas/parsers/mf --cov-report=term-missing

# Expected: All 7+ test cases pass, coverage > 80%
```

---

## Success Criteria

- [ ] CAMS CAS Excel parsed correctly (all sheets)
- [ ] CAMS CAS PDF parsed with password handling
- [ ] Equity funds auto-classified based on scheme name
- [ ] Debt funds auto-classified based on scheme name
- [ ] STCG calculated for equity <12 months at 20%
- [ ] LTCG calculated for equity >12 months at 12.5%
- [ ] Grandfathering applied for pre-31-Jan-2018 purchases
- [ ] Capital gains statement generated
- [ ] Journal entries created for redemptions
- [ ] All unit tests passing
- [ ] Code coverage > 80%
