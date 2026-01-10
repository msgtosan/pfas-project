# Sprint 7: Rental Income, SGB & REIT

## Module Overview
**Sprint:** S7 (Week 13-14)
**Phase:** 1
**Requirements:** REQ-RNT-001 to REQ-RNT-006, REQ-SGB-001 to REQ-SGB-005, REQ-REIT-001 to REQ-REIT-005

---

# Rental Income Module

## Requirements

### REQ-RNT-001 to REQ-RNT-006
- Register property details
- Track monthly rental income
- Apply 30% standard deduction
- Track Section 24 home loan interest (max ₹2L for self-occupied)
- Track municipal tax payments
- Handle loss from house property (max ₹2L set-off)

---

## Database Schema

```sql
CREATE TABLE IF NOT EXISTS properties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    property_type TEXT NOT NULL,  -- SELF_OCCUPIED, LET_OUT, DEEMED_LET_OUT
    address TEXT NOT NULL,
    city TEXT,
    pin_code TEXT,
    acquisition_date DATE,
    acquisition_cost DECIMAL(15,2),
    account_id INTEGER REFERENCES accounts(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS rental_income (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER REFERENCES properties(id),
    financial_year TEXT NOT NULL,
    month TEXT,  -- Optional: Apr-2024
    gross_rent DECIMAL(15,2) DEFAULT 0,
    municipal_tax_paid DECIMAL(15,2) DEFAULT 0,
    net_annual_value DECIMAL(15,2) DEFAULT 0,  -- Gross - Municipal
    standard_deduction DECIMAL(15,2) DEFAULT 0,  -- 30% of NAV
    interest_on_home_loan DECIMAL(15,2) DEFAULT 0,
    income_from_hp DECIMAL(15,2) DEFAULT 0,  -- Can be negative (loss)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(property_id, financial_year, month)
);

CREATE TABLE IF NOT EXISTS home_loan_interest (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER REFERENCES properties(id),
    financial_year TEXT NOT NULL,
    lender_name TEXT,
    loan_account_number TEXT,
    total_interest_paid DECIMAL(15,2) DEFAULT 0,
    principal_repaid DECIMAL(15,2) DEFAULT 0,  -- For 80C
    pre_construction_interest DECIMAL(15,2) DEFAULT 0,
    section_24_eligible DECIMAL(15,2) DEFAULT 0,  -- Max 2L
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Implementation

### rental.py
```python
"""Rental income calculation module."""

from dataclasses import dataclass
from decimal import Decimal
from datetime import date

@dataclass
class RentalIncomeCalculation:
    gross_rent: Decimal
    municipal_tax: Decimal
    net_annual_value: Decimal
    standard_deduction: Decimal  # 30% of NAV
    home_loan_interest: Decimal
    income_from_hp: Decimal  # Final (can be negative)

class RentalIncomeCalculator:
    """Calculate income from house property."""
    
    STANDARD_DEDUCTION_RATE = Decimal("0.30")  # 30%
    MAX_INTEREST_SELF_OCCUPIED = Decimal("200000")  # ₹2L
    MAX_LOSS_SETOFF = Decimal("200000")  # ₹2L loss can be set off
    
    def calculate(self, gross_rent: Decimal, municipal_tax: Decimal,
                  home_loan_interest: Decimal, 
                  property_type: str = "LET_OUT") -> RentalIncomeCalculation:
        """
        Calculate income from house property.
        
        For let-out property:
        1. Gross Annual Value = Actual rent received
        2. Less: Municipal taxes paid (actual)
        3. Net Annual Value (NAV)
        4. Less: Standard deduction (30% of NAV)
        5. Less: Interest on home loan
        6. Income from House Property (can be negative)
        """
        # For self-occupied: GAV = 0
        if property_type == "SELF_OCCUPIED":
            gross_rent = Decimal("0")
            municipal_tax = Decimal("0")
        
        # Net Annual Value
        nav = gross_rent - municipal_tax
        
        # Standard deduction (30% of NAV)
        std_deduction = nav * self.STANDARD_DEDUCTION_RATE
        
        # Cap interest for self-occupied
        if property_type == "SELF_OCCUPIED":
            eligible_interest = min(home_loan_interest, self.MAX_INTEREST_SELF_OCCUPIED)
        else:
            eligible_interest = home_loan_interest  # No cap for let-out
        
        # Income from HP
        income_hp = nav - std_deduction - eligible_interest
        
        return RentalIncomeCalculation(
            gross_rent=gross_rent,
            municipal_tax=municipal_tax,
            net_annual_value=nav,
            standard_deduction=std_deduction,
            home_loan_interest=eligible_interest,
            income_from_hp=income_hp  # Can be negative (loss)
        )
    
    def calculate_loss_setoff(self, hp_loss: Decimal) -> Decimal:
        """
        Calculate how much HP loss can be set off against other income.
        
        Max ₹2L can be set off in current year.
        Remaining carried forward for 8 years.
        """
        if hp_loss >= 0:
            return Decimal("0")
        
        return min(abs(hp_loss), self.MAX_LOSS_SETOFF)
```

---

# SGB & RBI Bonds Module

## Requirements

### REQ-SGB-001 to REQ-SGB-005
- Parse SGB holdings from NSDL CAS
- Track semi-annual interest (2.5%)
- Capital gains on maturity (8 years) is EXEMPT
- Parse RBI Floating Rate Bonds
- Track rate changes

---

## Implementation

### sgb.py
```python
"""Sovereign Gold Bonds (SGB) tracker."""

from dataclasses import dataclass
from decimal import Decimal
from datetime import date
from typing import List, Optional

@dataclass
class SGBHolding:
    series: str  # e.g., "SGB 2024-25 Series I"
    isin: str
    issue_date: date
    maturity_date: date
    quantity: int  # in grams
    issue_price: Decimal
    interest_rate: Decimal = Decimal("2.5")  # 2.5% p.a.

@dataclass
class SGBInterest:
    series: str
    payment_date: date
    quantity: int
    rate: Decimal
    amount: Decimal
    tds_deducted: Decimal = Decimal("0")  # TDS only if >threshold

class SGBTracker:
    """Track SGB holdings and calculate returns."""
    
    INTEREST_RATE = Decimal("2.5")  # 2.5% per annum
    MATURITY_YEARS = 8
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def calculate_interest(self, holding: SGBHolding, 
                          gold_price_at_issue: Decimal) -> Decimal:
        """
        Calculate semi-annual interest.
        
        Interest = Issue Price × Quantity × 2.5% ÷ 2
        (Paid semi-annually)
        """
        annual_interest = holding.issue_price * holding.quantity * (self.INTEREST_RATE / 100)
        return annual_interest / 2  # Semi-annual
    
    def calculate_maturity_cg(self, holding: SGBHolding,
                              redemption_price: Decimal) -> tuple[Decimal, bool]:
        """
        Calculate capital gains on redemption.
        
        If held till maturity (8 years): CG is EXEMPT
        If sold before maturity: LTCG at 12.5% (if >12 months)
        
        Returns:
            (capital_gain, is_exempt)
        """
        cost = holding.issue_price * holding.quantity
        sale_value = redemption_price * holding.quantity
        gain = sale_value - cost
        
        # Check if maturity
        holding_days = (date.today() - holding.issue_date).days
        is_maturity = holding_days >= (self.MATURITY_YEARS * 365)
        
        return gain, is_maturity  # If maturity, CG is exempt
```

---

# REIT/InvIT Module

## Requirements

### REQ-REIT-001 to REQ-REIT-005
- Parse REIT holdings
- Track distribution breakdowns (dividend/interest/other)
- Dividend portion: Exempt
- Interest portion: Taxable + TDS
- Capital reduction: Reduces cost basis

---

## Implementation

### reit.py
```python
"""REIT/InvIT distribution tracker."""

from dataclasses import dataclass
from decimal import Decimal
from datetime import date
from enum import Enum

class DistributionType(Enum):
    DIVIDEND = "DIVIDEND"  # Exempt
    INTEREST = "INTEREST"  # Taxable at slab
    OTHER = "OTHER"        # Capital reduction - reduces cost
    CAPITAL_GAIN = "CAPITAL_GAIN"

@dataclass
class REITDistribution:
    symbol: str
    record_date: date
    distribution_type: DistributionType
    gross_amount: Decimal
    tds_deducted: Decimal  # TDS only on interest portion
    net_amount: Decimal

class REITTracker:
    """Track REIT/InvIT holdings and distributions."""
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def process_distribution(self, distribution: REITDistribution,
                             holding_cost: Decimal) -> dict:
        """
        Process REIT distribution based on type.
        
        Returns dict with tax treatment.
        """
        result = {
            'gross_amount': distribution.gross_amount,
            'taxable_amount': Decimal("0"),
            'exempt_amount': Decimal("0"),
            'cost_reduction': Decimal("0"),
            'tds_credit': distribution.tds_deducted
        }
        
        if distribution.distribution_type == DistributionType.DIVIDEND:
            # Dividend is exempt
            result['exempt_amount'] = distribution.gross_amount
        
        elif distribution.distribution_type == DistributionType.INTEREST:
            # Interest is taxable at slab rate
            result['taxable_amount'] = distribution.gross_amount
        
        elif distribution.distribution_type == DistributionType.OTHER:
            # Capital reduction - reduces cost basis
            result['cost_reduction'] = distribution.gross_amount
            # Adjust holding cost
            new_cost = max(Decimal("0"), holding_cost - distribution.gross_amount)
            result['new_cost_basis'] = new_cost
        
        return result
```

---

# Sprint 8: Reports & GNUCash Export

## Module Overview
**Sprint:** S8 (Week 15-16)
**Phase:** 1
**Requirements:** REQ-RPT-001 to REQ-RPT-006

---

## Requirements

### REQ-RPT-001: Net Worth Report
- Asset-wise breakdown
- Liability summary
- Net worth trend

### REQ-RPT-002: Tax Computation
- Old vs New regime comparison
- Income under each head
- Deductions (Chapter VI-A)
- Tax liability calculation

### REQ-RPT-003: GNUCash QIF Export
- Export transactions in QIF format
- Compatible with GNUCash import

### REQ-RPT-004: GNUCash CSV Export
- Export in GNUCash CSV format
- Account mapping

### REQ-RPT-005: Capital Gains Report
- Stock-wise CG
- MF-wise CG
- Quarterly breakdown

### REQ-RPT-006: Advance Tax Calculator
- Quarterly tax calculation
- Due dates (Jun 15, Sep 15, Dec 15, Mar 15)

---

## Implementation

### tax_computation.py
```python
"""Tax computation for Old and New regime."""

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Dict

class TaxRegime(Enum):
    OLD = "OLD"
    NEW = "NEW"

@dataclass
class TaxComputation:
    regime: TaxRegime
    
    # Income heads
    salary_income: Decimal = Decimal("0")
    house_property_income: Decimal = Decimal("0")
    capital_gains_stcg: Decimal = Decimal("0")
    capital_gains_ltcg: Decimal = Decimal("0")
    other_income: Decimal = Decimal("0")
    
    # Gross Total Income
    gross_total_income: Decimal = Decimal("0")
    
    # Deductions (only for old regime)
    deduction_80c: Decimal = Decimal("0")
    deduction_80ccd_1b: Decimal = Decimal("0")
    deduction_80ccd_2: Decimal = Decimal("0")
    deduction_80d: Decimal = Decimal("0")
    deduction_80tta: Decimal = Decimal("0")
    total_deductions: Decimal = Decimal("0")
    
    # Taxable Income
    taxable_income: Decimal = Decimal("0")
    
    # Tax calculation
    tax_on_normal_income: Decimal = Decimal("0")
    tax_on_stcg: Decimal = Decimal("0")
    tax_on_ltcg: Decimal = Decimal("0")
    total_tax: Decimal = Decimal("0")
    
    # Cess and surcharge
    surcharge: Decimal = Decimal("0")
    cess: Decimal = Decimal("0")
    
    # Final tax
    tax_payable: Decimal = Decimal("0")
    
    # Credits
    tds_credit: Decimal = Decimal("0")
    tcs_credit: Decimal = Decimal("0")
    advance_tax_paid: Decimal = Decimal("0")
    
    # Balance
    tax_due: Decimal = Decimal("0")
    refund_due: Decimal = Decimal("0")

class TaxCalculator:
    """Calculate tax for both regimes."""
    
    # Old regime slabs (FY 2024-25)
    OLD_SLABS = [
        (Decimal("250000"), Decimal("0")),
        (Decimal("500000"), Decimal("5")),
        (Decimal("1000000"), Decimal("20")),
        (Decimal("99999999999"), Decimal("30"))
    ]
    
    # New regime slabs (FY 2024-25)
    NEW_SLABS = [
        (Decimal("300000"), Decimal("0")),
        (Decimal("700000"), Decimal("5")),
        (Decimal("1000000"), Decimal("10")),
        (Decimal("1200000"), Decimal("15")),
        (Decimal("1500000"), Decimal("20")),
        (Decimal("99999999999"), Decimal("30"))
    ]
    
    CESS_RATE = Decimal("4")  # 4% Health & Education Cess
    
    # Special rates
    STCG_EQUITY_RATE = Decimal("20")  # 20% for equity STCG
    LTCG_EQUITY_RATE = Decimal("12.5")  # 12.5% for equity LTCG
    
    def calculate(self, income_data: dict, regime: TaxRegime) -> TaxComputation:
        """Calculate tax for given regime."""
        comp = TaxComputation(regime=regime)
        
        # Set income heads
        comp.salary_income = income_data.get('salary', Decimal("0"))
        comp.house_property_income = income_data.get('house_property', Decimal("0"))
        comp.capital_gains_stcg = income_data.get('stcg', Decimal("0"))
        comp.capital_gains_ltcg = income_data.get('ltcg', Decimal("0"))
        comp.other_income = income_data.get('other', Decimal("0"))
        
        # Gross Total Income (excluding special rate CG)
        comp.gross_total_income = (
            comp.salary_income + 
            comp.house_property_income + 
            comp.other_income
        )
        
        # Deductions (Old regime only)
        if regime == TaxRegime.OLD:
            comp.deduction_80c = min(income_data.get('80c', Decimal("0")), Decimal("150000"))
            comp.deduction_80ccd_1b = min(income_data.get('80ccd_1b', Decimal("0")), Decimal("50000"))
            comp.deduction_80ccd_2 = income_data.get('80ccd_2', Decimal("0"))
            comp.deduction_80d = income_data.get('80d', Decimal("0"))
            comp.deduction_80tta = min(income_data.get('80tta', Decimal("0")), Decimal("10000"))
            
            comp.total_deductions = (
                comp.deduction_80c + comp.deduction_80ccd_1b + 
                comp.deduction_80ccd_2 + comp.deduction_80d + comp.deduction_80tta
            )
        else:
            # New regime - only employer NPS allowed
            comp.deduction_80ccd_2 = income_data.get('80ccd_2', Decimal("0"))
            comp.total_deductions = comp.deduction_80ccd_2
        
        # Taxable Income (normal)
        comp.taxable_income = comp.gross_total_income - comp.total_deductions
        
        # Tax on normal income (slab rates)
        slabs = self.OLD_SLABS if regime == TaxRegime.OLD else self.NEW_SLABS
        comp.tax_on_normal_income = self._calculate_slab_tax(comp.taxable_income, slabs)
        
        # Tax on STCG (20% for equity)
        comp.tax_on_stcg = comp.capital_gains_stcg * (self.STCG_EQUITY_RATE / 100)
        
        # Tax on LTCG (12.5% for equity, after ₹1.25L exemption)
        ltcg_taxable = max(Decimal("0"), comp.capital_gains_ltcg - Decimal("125000"))
        comp.tax_on_ltcg = ltcg_taxable * (self.LTCG_EQUITY_RATE / 100)
        
        # Total tax before cess
        comp.total_tax = comp.tax_on_normal_income + comp.tax_on_stcg + comp.tax_on_ltcg
        
        # Health & Education Cess (4%)
        comp.cess = comp.total_tax * (self.CESS_RATE / 100)
        
        # Tax payable
        comp.tax_payable = comp.total_tax + comp.cess
        
        # Credits
        comp.tds_credit = income_data.get('tds', Decimal("0"))
        comp.tcs_credit = income_data.get('tcs', Decimal("0"))
        comp.advance_tax_paid = income_data.get('advance_tax', Decimal("0"))
        
        total_credits = comp.tds_credit + comp.tcs_credit + comp.advance_tax_paid
        
        # Balance
        if comp.tax_payable > total_credits:
            comp.tax_due = comp.tax_payable - total_credits
        else:
            comp.refund_due = total_credits - comp.tax_payable
        
        return comp
    
    def _calculate_slab_tax(self, income: Decimal, slabs: list) -> Decimal:
        """Calculate tax using slab rates."""
        tax = Decimal("0")
        prev_limit = Decimal("0")
        
        for limit, rate in slabs:
            if income <= prev_limit:
                break
            
            taxable_in_slab = min(income, limit) - prev_limit
            tax += taxable_in_slab * (rate / 100)
            prev_limit = limit
        
        return tax
    
    def compare_regimes(self, income_data: dict) -> dict:
        """Compare tax under both regimes."""
        old = self.calculate(income_data, TaxRegime.OLD)
        new = self.calculate(income_data, TaxRegime.NEW)
        
        return {
            'old_regime': old,
            'new_regime': new,
            'better_regime': TaxRegime.OLD if old.tax_payable < new.tax_payable else TaxRegime.NEW,
            'savings': abs(old.tax_payable - new.tax_payable)
        }
```

### gnucash_export.py
```python
"""GNUCash export in QIF and CSV formats."""

from datetime import date
from decimal import Decimal
from typing import List
from io import StringIO

class GNUCashExporter:
    """Export to GNUCash-compatible formats."""
    
    # Account mapping: PFAS code -> GNUCash path
    ACCOUNT_MAP = {
        "1101": "Assets:Current Assets:Bank:Savings",
        "1201": "Assets:Investments:Mutual Funds:Equity",
        "1202": "Assets:Investments:Mutual Funds:Debt",
        "1203": "Assets:Investments:Stocks:Indian",
        "1301": "Assets:Retirement:EPF:Employee",
        "1302": "Assets:Retirement:EPF:Employer",
        "1303": "Assets:Retirement:PPF",
        "1401": "Assets:Investments:Foreign:USA:RSU",
        "4101": "Income:Salary:Basic",
        "4201": "Income:Interest:Bank",
        "4301": "Income:Capital Gains:STCG",
        "4302": "Income:Capital Gains:LTCG",
    }
    
    def export_qif(self, transactions: List[dict], account_type: str = "Bank") -> str:
        """
        Export transactions in QIF format.
        
        QIF Format:
        !Account
        NAccount Name
        TAccount Type
        ^
        !Type:Bank
        Ddate
        Tamount
        Ppayee
        Mmemo
        Lcategory
        ^
        """
        output = StringIO()
        
        # Header
        output.write("!Type:" + account_type + "\n")
        
        for txn in transactions:
            # Date (M/D/YY format)
            d = txn['date']
            output.write(f"D{d.month}/{d.day}/{d.year % 100}\n")
            
            # Amount
            amount = txn.get('credit', Decimal("0")) - txn.get('debit', Decimal("0"))
            output.write(f"T{amount}\n")
            
            # Payee/Description
            if txn.get('payee'):
                output.write(f"P{txn['payee']}\n")
            
            # Memo
            if txn.get('memo'):
                output.write(f"M{txn['memo']}\n")
            
            # Category
            if txn.get('category'):
                output.write(f"L{txn['category']}\n")
            
            # End of transaction
            output.write("^\n")
        
        return output.getvalue()
    
    def export_csv(self, transactions: List[dict]) -> str:
        """
        Export in GNUCash CSV format.
        
        Columns: Date,Description,Account,Deposit,Withdrawal,Balance
        """
        output = StringIO()
        output.write("Date,Description,Account,Deposit,Withdrawal,Balance\n")
        
        for txn in transactions:
            date_str = txn['date'].strftime("%Y-%m-%d")
            desc = txn.get('description', '').replace(',', ';')
            account = self.ACCOUNT_MAP.get(txn.get('account_code', ''), 'Unassigned')
            deposit = txn.get('credit', '')
            withdrawal = txn.get('debit', '')
            balance = txn.get('balance', '')
            
            output.write(f"{date_str},{desc},{account},{deposit},{withdrawal},{balance}\n")
        
        return output.getvalue()
```

---

## Verification Commands

```bash
# Sprint 7
pytest tests/unit/test_parsers/test_rental/ -v
pytest tests/unit/test_parsers/test_sgb/ -v
pytest tests/unit/test_parsers/test_reit/ -v

# Sprint 8
pytest tests/unit/test_reports/ -v --cov=src/pfas/reports

# All Phase 1 tests
pytest tests/ -v --cov=src/pfas
```

---

## Success Criteria

### Sprint 7
- [ ] Rental income calculated with 30% deduction
- [ ] Section 24 interest tracked (max ₹2L)
- [ ] HP loss set-off computed
- [ ] SGB interest calculated
- [ ] SGB maturity CG exemption handled
- [ ] REIT distribution types processed correctly

### Sprint 8
- [ ] Net worth report generated
- [ ] Tax computed for both regimes
- [ ] Better regime recommendation
- [ ] QIF export works with GNUCash
- [ ] CSV export works with GNUCash
- [ ] Capital gains report by security
- [ ] Advance tax quarterly calculation
