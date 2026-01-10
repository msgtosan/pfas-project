# Sprint 5: Salary & Form 16 Parser

## Module Overview
**Sprint:** S5 (Week 9-10)
**Phase:** 1
**Requirements:** REQ-SAL-001 to REQ-SAL-012
**Dependencies:** Core module, EPF/NPS parsers for cross-reference

---

## Requirements to Implement

### REQ-SAL-001: Payslip Parser
- **Input:** Monthly payslip PDF (Qualcomm format)
- **Processing:** Extract all salary components, deductions, taxes
- **Output:** Salary record with all components

### REQ-SAL-002 to REQ-SAL-004: Salary Components
- Basic Salary, HRA, Special Allowance, LTA
- Track each component separately for tax calculation

### REQ-SAL-005: RSU Tax Credit (CRITICAL)
- **Input:** Negative "RSUs Tax" deduction in payslip
- **Processing:** This is a TAX CREDIT (money added back) when RSUs vest
- **Output:** RSU tax credit linked to vest event
- **Note:** Appears as NEGATIVE number in deductions = credit back to employee

### REQ-SAL-006: ESPP Deduction
- **Input:** ESPP Deduction from payslip
- **Processing:** Track as investment in foreign stock
- **Output:** ESPP contribution linked to purchase

### REQ-SAL-007: TCS on ESPP
- **Input:** TCS on ESPP (20% on LRS remittance)
- **Processing:** Track as tax credit (Section 206CQ)
- **Output:** TCS receivable for ITR

### REQ-SAL-008: Professional Tax
- **Input:** Prof Tax deduction
- **Processing:** Track state-wise PT (max ₹2,500/year)
- **Output:** PT expense for 16(iii) deduction

### REQ-SAL-009 to REQ-SAL-012: Form 16 Parsing
- Part A: Quarterly TDS details
- Part B: Salary breakup, deductions
- Form 12BA: Perquisites (RSU, ESPP discounts)

---

## Payslip Format (Qualcomm)

Based on project file `Payslip_20240630.pdf`:

```
Pay Slip for the Month of June 2024

Employee ID: 111030              Days Payable: 30
Employee Name: Shankar Sanjay    Bank Account: 003101008527
Job Title: Director, Engineering PF Number: APHYD00476720000003193
Location: Hyderabad_SEZ          UAN: 100379251525
DOJ: 06.10.2014                  PAN: AAPPS0793R

EARNINGS                UNITS   INR         DEDUCTIONS              INR
Basic Salary                    560,456.00  *RSUs Tax           -1,957,774.65  ← NEGATIVE = CREDIT
Special Allowance              291,584.13  ESPP Deduction         168,136.80
House Rent Allowance           224,182.40  QCOM Trust Fund            400.00
                                           Prof Tax                   200.00
                                           Income Tax           2,167,667.00
                                           EE PF contribution      67,255.00

(*) denotes back pay adjustment

PAY SUMMARY                     INR
Total Gross               1,076,222.53
Less: Total Dedns           445,884.15
NET PAY                     630,338.38

NPS Contribution             28,022.80
```

### Key Observations:
1. **RSUs Tax is NEGATIVE** = Tax credit when shares vest
2. **ESPP Deduction** = Investment in US stock
3. **TCS on ESPP** (from annual summary) = Tax credit
4. **EE PF** may exceed 12% (includes VPF)

---

## Form 16 Structure

### Part A (TDS Certificate)
```
Quarter | TDS Deposited | Date | BSR Code | Challan No | Status
Q1      | 3,500,000     | 07-Jul-24 | ... | ... | F
Q2      | 3,200,000     | 07-Oct-24 | ... | ... | F
Q3      | 3,100,000     | 07-Jan-25 | ... | ... | F
Q4      | 3,415,375     | 07-Apr-25 | ... | ... | F
Total   | 13,215,375
```

### Part B (Salary Details)
Based on `Taxforms_40_2024_Form16B.pdf`:
```
1. Gross Salary
   (a) Salary as per section 17(1): 18,807,413
   (b) Value of perquisites u/s 17(2): 16,403,773
   (c) Profits in lieu of salary u/s 17(3): 0
   
2. Less: Exemptions u/s 10
   (a) HRA exemption: 0 (if claiming under new regime)
   
3. Total Salary: 35,286,186
4. Less: Standard Deduction u/s 16(ia): 75,000
5. Income under Salaries: 35,211,186

6. Deductions under Chapter VI-A
   (a) 80C: 0 (new regime)
   (f) 80CCD(2) - Employer NPS: 292,277.80
```

### Form 12BA (Perquisites)
Based on `Taxforms_2024_12BA.pdf`:
```
8. Valuation of Perquisites:
   17. Stock options (non-qualified): 15,993,899.40  ← RSU perquisite
   18. Employer contribution to fund: 378,257.80    ← ER PF/NPS taxable
   19. Interest accretion: 14,254.17                ← Interest on above
   21. Total perquisites: 16,403,773.17
```

---

## Database Schema

```sql
CREATE TABLE IF NOT EXISTS employers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    tan TEXT UNIQUE NOT NULL,
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS salary_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    employer_id INTEGER REFERENCES employers(id),
    pay_period TEXT NOT NULL,  -- 'Jun-2024'
    pay_date DATE,
    
    -- Earnings
    basic_salary DECIMAL(15,2) DEFAULT 0,
    hra DECIMAL(15,2) DEFAULT 0,
    special_allowance DECIMAL(15,2) DEFAULT 0,
    lta DECIMAL(15,2) DEFAULT 0,
    other_allowances DECIMAL(15,2) DEFAULT 0,
    
    -- Computed
    gross_salary DECIMAL(15,2) DEFAULT 0,
    
    -- Deductions
    pf_employee DECIMAL(15,2) DEFAULT 0,
    pf_employer DECIMAL(15,2) DEFAULT 0,
    nps_employee DECIMAL(15,2) DEFAULT 0,
    nps_employer DECIMAL(15,2) DEFAULT 0,
    professional_tax DECIMAL(15,2) DEFAULT 0,
    income_tax_deducted DECIMAL(15,2) DEFAULT 0,
    espp_deduction DECIMAL(15,2) DEFAULT 0,
    tcs_on_espp DECIMAL(15,2) DEFAULT 0,
    other_deductions DECIMAL(15,2) DEFAULT 0,
    
    -- RSU Tax Credit (NEGATIVE = credit)
    rsu_tax_credit DECIMAL(15,2) DEFAULT 0,  -- Store as positive, interpret as credit
    
    -- Net
    total_deductions DECIMAL(15,2) DEFAULT 0,
    net_pay DECIMAL(15,2) DEFAULT 0,
    
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS rsu_tax_credits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    salary_record_id INTEGER REFERENCES salary_records(id),
    credit_amount DECIMAL(15,2) NOT NULL,  -- Positive value
    credit_date DATE NOT NULL,
    vest_id INTEGER,  -- Link to RSU vest event (Phase 2)
    correlation_status TEXT DEFAULT 'PENDING',  -- PENDING, MATCHED, UNMATCHED
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS form16_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    employer_id INTEGER REFERENCES employers(id),
    assessment_year TEXT NOT NULL,  -- '2025-26'
    
    -- Part A - TDS Summary
    q1_tds DECIMAL(15,2) DEFAULT 0,
    q2_tds DECIMAL(15,2) DEFAULT 0,
    q3_tds DECIMAL(15,2) DEFAULT 0,
    q4_tds DECIMAL(15,2) DEFAULT 0,
    total_tds DECIMAL(15,2) DEFAULT 0,
    
    -- Part B - Income
    salary_17_1 DECIMAL(15,2) DEFAULT 0,
    perquisites_17_2 DECIMAL(15,2) DEFAULT 0,
    profits_17_3 DECIMAL(15,2) DEFAULT 0,
    gross_salary DECIMAL(15,2) DEFAULT 0,
    
    -- Exemptions u/s 10
    hra_exemption DECIMAL(15,2) DEFAULT 0,
    lta_exemption DECIMAL(15,2) DEFAULT 0,
    other_exemptions DECIMAL(15,2) DEFAULT 0,
    
    -- Deductions
    standard_deduction DECIMAL(15,2) DEFAULT 0,
    professional_tax DECIMAL(15,2) DEFAULT 0,
    
    -- Chapter VI-A
    section_80c DECIMAL(15,2) DEFAULT 0,
    section_80ccd_1b DECIMAL(15,2) DEFAULT 0,
    section_80ccd_2 DECIMAL(15,2) DEFAULT 0,
    section_80d DECIMAL(15,2) DEFAULT 0,
    
    -- Net
    taxable_income DECIMAL(15,2) DEFAULT 0,
    tax_payable DECIMAL(15,2) DEFAULT 0,
    
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, employer_id, assessment_year)
);

CREATE TABLE IF NOT EXISTS perquisites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    form16_id INTEGER REFERENCES form16_records(id),
    perquisite_type TEXT NOT NULL,  -- RSU, ESPP_DISCOUNT, EMPLOYER_PF, etc.
    description TEXT,
    gross_value DECIMAL(15,2) NOT NULL,
    recovered_from_employee DECIMAL(15,2) DEFAULT 0,
    taxable_value DECIMAL(15,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Files to Create

```
src/pfas/parsers/salary/
├── __init__.py
├── payslip.py           # Monthly payslip parser
├── form16.py            # Form 16 Part A & B parser
├── form12ba.py          # Form 12BA perquisites parser
├── models.py            # SalaryRecord, Form16Record dataclasses
├── rsu_correlation.py   # RSU tax credit correlation
└── hra_calculator.py    # HRA exemption calculation

tests/unit/test_parsers/test_salary/
├── __init__.py
├── test_payslip.py
├── test_form16.py
├── test_form12ba.py
├── test_rsu_correlation.py
└── test_hra.py
```

---

## Implementation

### payslip.py
```python
"""Monthly payslip PDF parser."""

import re
import pdfplumber
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal
from dataclasses import dataclass
from typing import Optional

@dataclass
class SalaryRecord:
    pay_period: str
    pay_date: Optional[date]
    
    # Earnings
    basic_salary: Decimal = Decimal("0")
    hra: Decimal = Decimal("0")
    special_allowance: Decimal = Decimal("0")
    lta: Decimal = Decimal("0")
    other_allowances: Decimal = Decimal("0")
    gross_salary: Decimal = Decimal("0")
    
    # Deductions
    pf_employee: Decimal = Decimal("0")
    nps_employee: Decimal = Decimal("0")
    professional_tax: Decimal = Decimal("0")
    income_tax_deducted: Decimal = Decimal("0")
    espp_deduction: Decimal = Decimal("0")
    tcs_on_espp: Decimal = Decimal("0")
    
    # RSU Tax Credit - CRITICAL
    rsu_tax_credit: Decimal = Decimal("0")  # Store as POSITIVE
    
    # Net
    total_deductions: Decimal = Decimal("0")
    net_pay: Decimal = Decimal("0")

class PayslipParser:
    """Parser for Qualcomm-format payslips."""
    
    # Component patterns
    PATTERNS = {
        'pay_period': r'Pay Slip for the Month of (\w+ \d{4})',
        'basic_salary': r'Basic Salary\s+[\d,]+\.?\d*\s+([\d,]+\.\d{2})',
        'hra': r'House Rent Allowance\s+([\d,]+\.\d{2})',
        'special_allowance': r'Special Allowance\s+([\d,]+\.\d{2})',
        'rsu_tax': r'\*?RSUs? Tax\s+(-?[\d,]+\.\d{2})',  # Note: can be negative
        'espp_deduction': r'ESPP Deduction\s+([\d,]+\.\d{2})',
        'tcs_espp': r'TCS on ESPP\s+([\d,]+\.\d{2})',
        'pf_employee': r'EE PF contribution\s+([\d,]+\.\d{2})',
        'professional_tax': r'Prof Tax.*?\s+([\d,]+\.\d{2})',
        'income_tax': r'Income Tax\s+([\d,]+\.\d{2})',
        'nps': r'NPS Contribution\s+([\d,]+\.\d{2})',
        'gross_salary': r'Total Gross\s+([\d,]+\.\d{2})',
        'total_deductions': r'Total Dedns\s+([\d,]+\.\d{2})',
        'net_pay': r'NET PAY\s+([\d,]+\.\d{2})',
    }
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def parse(self, file_path: Path) -> SalaryRecord:
        """Parse payslip PDF."""
        with pdfplumber.open(file_path) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() or ""
        
        return self._extract_salary_record(text)
    
    def _extract_salary_record(self, text: str) -> SalaryRecord:
        """Extract salary components from text."""
        record = SalaryRecord(pay_period="")
        
        # Pay period
        match = re.search(self.PATTERNS['pay_period'], text)
        if match:
            record.pay_period = match.group(1)
        
        # Extract each component
        for field, pattern in self.PATTERNS.items():
            if field == 'pay_period':
                continue
            
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                value = self._to_decimal(match.group(1))
                
                # CRITICAL: RSU Tax is NEGATIVE in payslip = CREDIT
                if field == 'rsu_tax':
                    # If negative, it's a credit - store as positive
                    if value < 0:
                        record.rsu_tax_credit = abs(value)
                    # If positive, it's actual tax deduction (rare)
                    else:
                        # Handle as normal deduction
                        pass
                elif field == 'basic_salary':
                    record.basic_salary = value
                elif field == 'hra':
                    record.hra = value
                elif field == 'special_allowance':
                    record.special_allowance = value
                elif field == 'espp_deduction':
                    record.espp_deduction = value
                elif field == 'tcs_espp':
                    record.tcs_on_espp = value
                elif field == 'pf_employee':
                    record.pf_employee = value
                elif field == 'professional_tax':
                    record.professional_tax = value
                elif field == 'income_tax':
                    record.income_tax_deducted = value
                elif field == 'nps':
                    record.nps_employee = value
                elif field == 'gross_salary':
                    record.gross_salary = value
                elif field == 'total_deductions':
                    record.total_deductions = value
                elif field == 'net_pay':
                    record.net_pay = value
        
        return record
    
    def _to_decimal(self, value: str) -> Decimal:
        """Convert string to Decimal, handling negatives."""
        if not value:
            return Decimal("0")
        # Remove commas, handle negative
        clean = value.replace(",", "")
        return Decimal(clean)


class RSUTaxCreditCorrelator:
    """
    Correlate RSU tax credits with vest events.
    
    The RSU tax credit in payslip should match with RSU vest perquisite.
    This is critical for accurate tax reporting.
    """
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def correlate(self, salary_record_id: int, vest_date: date, 
                  vest_perquisite: Decimal) -> bool:
        """
        Try to match RSU tax credit with vest event.
        
        Returns True if correlated successfully.
        """
        # Get RSU tax credit from salary record
        cursor = self.conn.execute("""
            SELECT rsu_tax_credit, pay_period
            FROM salary_records
            WHERE id = ?
        """, (salary_record_id,))
        
        row = cursor.fetchone()
        if not row or row['rsu_tax_credit'] == 0:
            return False
        
        credit_amount = Decimal(str(row['rsu_tax_credit']))
        
        # Check if this credit is within expected range of vest perquisite tax
        # Tax on perquisite should be approximately 30-35% of perquisite
        expected_tax_min = vest_perquisite * Decimal("0.25")
        expected_tax_max = vest_perquisite * Decimal("0.40")
        
        if expected_tax_min <= credit_amount <= expected_tax_max:
            # Mark as correlated
            self.conn.execute("""
                INSERT INTO rsu_tax_credits 
                (salary_record_id, credit_amount, credit_date, correlation_status)
                VALUES (?, ?, ?, 'MATCHED')
            """, (salary_record_id, float(credit_amount), vest_date))
            self.conn.commit()
            return True
        
        return False
```

### form16.py
```python
"""Form 16 Part A & B parser."""

import zipfile
import pdfplumber
from pathlib import Path
from decimal import Decimal
from dataclasses import dataclass
import re

@dataclass
class Form16Record:
    assessment_year: str
    employer_tan: str
    employee_pan: str
    
    # Part A - TDS
    q1_tds: Decimal = Decimal("0")
    q2_tds: Decimal = Decimal("0")
    q3_tds: Decimal = Decimal("0")
    q4_tds: Decimal = Decimal("0")
    total_tds: Decimal = Decimal("0")
    
    # Part B - Income
    salary_17_1: Decimal = Decimal("0")
    perquisites_17_2: Decimal = Decimal("0")
    gross_salary: Decimal = Decimal("0")
    
    # Exemptions
    hra_exemption: Decimal = Decimal("0")
    standard_deduction: Decimal = Decimal("0")
    
    # Deductions
    section_80c: Decimal = Decimal("0")
    section_80ccd_2: Decimal = Decimal("0")
    
    taxable_income: Decimal = Decimal("0")

class Form16Parser:
    """Parser for Form 16 ZIP archive containing Part A & B PDFs."""
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def parse(self, zip_path: Path) -> Form16Record:
        """Parse Form 16 ZIP containing Part A and Part B."""
        record = Form16Record(assessment_year="", employer_tan="", employee_pan="")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_file:
            for filename in zip_file.namelist():
                if filename.lower().endswith('.pdf'):
                    with zip_file.open(filename) as pdf_file:
                        # pdfplumber needs a file path, so extract temporarily
                        import tempfile
                        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
                            tmp.write(pdf_file.read())
                            tmp_path = tmp.name
                        
                        if 'Part_A' in filename or 'PartA' in filename:
                            self._parse_part_a(tmp_path, record)
                        elif 'Part_B' in filename or 'PartB' in filename:
                            self._parse_part_b(tmp_path, record)
        
        return record
    
    def _parse_part_a(self, pdf_path: str, record: Form16Record):
        """Parse Part A - TDS certificate."""
        with pdfplumber.open(pdf_path) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() or ""
        
        # Extract quarterly TDS
        quarters = re.findall(r'Q([1-4]).*?([\d,]+\.\d{2})', text)
        for q, amount in quarters:
            value = Decimal(amount.replace(",", ""))
            if q == '1':
                record.q1_tds = value
            elif q == '2':
                record.q2_tds = value
            elif q == '3':
                record.q3_tds = value
            elif q == '4':
                record.q4_tds = value
        
        record.total_tds = record.q1_tds + record.q2_tds + record.q3_tds + record.q4_tds
    
    def _parse_part_b(self, pdf_path: str, record: Form16Record):
        """Parse Part B - Salary details."""
        with pdfplumber.open(pdf_path) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() or ""
        
        # Extract salary components
        patterns = {
            'salary_17_1': r'section 17\(1\).*?([\d,]+\.\d{2})',
            'perquisites_17_2': r'section 17\(2\).*?([\d,]+\.\d{2})',
            'standard_deduction': r'Standard deduction.*?section 16\(ia\).*?([\d,]+\.\d{2})',
            'section_80ccd_2': r'80CCD\(2\).*?([\d,]+\.\d{2})',
            'taxable_income': r'Gross total income.*?([\d,]+\.\d{2})',
        }
        
        for field, pattern in patterns.items():
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                setattr(record, field, Decimal(match.group(1).replace(",", "")))
        
        record.gross_salary = record.salary_17_1 + record.perquisites_17_2
```

---

## Test Cases

### TC-SAL-001: Payslip Parse
```python
def test_payslip_parse(test_db, fixtures_path):
    """Test monthly payslip parsing."""
    parser = PayslipParser(test_db)
    record = parser.parse(fixtures_path / "salary/payslip_jun2024.pdf")
    
    assert record.pay_period == "June 2024"
    assert record.basic_salary > 0
    assert record.gross_salary > 0
    assert record.net_pay > 0
```

### TC-SAL-005: RSU Tax Credit (CRITICAL)
```python
def test_rsu_tax_credit_extraction(test_db, fixtures_path):
    """Test RSU tax credit (negative deduction) extraction."""
    parser = PayslipParser(test_db)
    record = parser.parse(fixtures_path / "salary/payslip_jun2024.pdf")
    
    # RSU Tax appears as -1,957,774.65 in payslip
    # Should be stored as POSITIVE credit
    assert record.rsu_tax_credit > 0
    assert record.rsu_tax_credit == Decimal("1957774.65")

def test_rsu_tax_credit_is_not_deduction(test_db, fixtures_path):
    """Verify RSU tax credit is not counted as deduction."""
    parser = PayslipParser(test_db)
    record = parser.parse(fixtures_path / "salary/payslip_jun2024.pdf")
    
    # RSU tax credit should NOT reduce gross salary
    # It's money credited back when RSUs vest
    assert record.rsu_tax_credit not in [
        record.pf_employee, 
        record.income_tax_deducted,
        record.total_deductions  # Should not be part of deductions
    ]
```

### TC-SAL-006/007: ESPP Deduction and TCS
```python
def test_espp_tracking(test_db, fixtures_path):
    """Test ESPP deduction and TCS extraction."""
    parser = PayslipParser(test_db)
    record = parser.parse(fixtures_path / "salary/payslip_jun2024.pdf")
    
    assert record.espp_deduction > 0
    # TCS may be 0 in some months, present in others
    # Annual summary should have TCS on ESPP
```

---

## Annual Salary Summary Processing

### Processing Annual Payslips
```python
def process_annual_salary(db_connection, user_id: int, fy: str, payslip_dir: Path):
    """
    Process all monthly payslips for a financial year.
    
    Creates:
    - Monthly salary records
    - RSU tax credit records
    - Annual summary
    """
    parser = PayslipParser(db_connection)
    monthly_records = []
    
    # Process each month's payslip
    for payslip_file in sorted(payslip_dir.glob("Payslip_*.pdf")):
        record = parser.parse(payslip_file)
        monthly_records.append(record)
        
        # Store in database
        # ...
    
    # Calculate annual totals
    annual_basic = sum(r.basic_salary for r in monthly_records)
    annual_hra = sum(r.hra for r in monthly_records)
    annual_rsu_credits = sum(r.rsu_tax_credit for r in monthly_records)
    annual_espp = sum(r.espp_deduction for r in monthly_records)
    annual_tcs = sum(r.tcs_on_espp for r in monthly_records)
    
    return {
        'basic_salary': annual_basic,
        'hra': annual_hra,
        'rsu_tax_credits': annual_rsu_credits,  # Should match Form 12BA perquisite tax
        'espp_investment': annual_espp,
        'tcs_on_espp': annual_tcs  # 206CQ credit
    }
```

---

## Verification Commands

```bash
# Run salary parser tests
pytest tests/unit/test_parsers/test_salary/ -v

# Run with coverage
pytest tests/unit/test_parsers/test_salary/ --cov=src/pfas/parsers/salary --cov-report=term-missing

# Critical: RSU tax credit tests
pytest tests/unit/test_parsers/test_salary/test_rsu_correlation.py -v
```

---

## Success Criteria

- [ ] Monthly payslip PDF parsed correctly
- [ ] All salary components extracted (Basic, HRA, Special, etc.)
- [ ] **RSU Tax Credit extracted as POSITIVE amount from NEGATIVE deduction**
- [ ] ESPP deduction tracked as investment
- [ ] TCS on ESPP tracked as tax credit (206CQ)
- [ ] Professional tax tracked
- [ ] Form 16 Part A TDS extracted
- [ ] Form 16 Part B salary details extracted
- [ ] Form 12BA perquisites extracted
- [ ] RSU tax credits correlated with vest events
- [ ] All tests passing, coverage > 80%

---

## Integration with Other Modules

### Link to Form 26AS
- TDS from Form 16 Part A should match Form 26AS Section 192
- TCS on ESPP should match Form 26AS Section 206CQ

### Link to RSU Vest (Phase 2)
- RSU perquisite in Form 12BA should match RSU vest FMV
- RSU tax credit in payslip should match tax on perquisite
