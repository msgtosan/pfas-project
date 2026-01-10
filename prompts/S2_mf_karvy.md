# Sprint 2: Mutual Fund KARVY/KFINTECH CAS Parser

## Module Overview
**Sprint:** S2 (Week 3-4)
**Phase:** 1
**Requirements:** REQ-MF-002, REQ-MF-003, REQ-MF-004, REQ-MF-005, REQ-MF-006, REQ-MF-007, REQ-MF-008, REQ-MF-009
**Dependencies:** Core module complete, MF base models from S2_mf_cams.md

---

## Requirements to Implement

### REQ-MF-002: KARVY/KFINTECH CAS Parser
- **Input:** KARVY Capital Gains Statement Excel (multiple sheets)
- **Processing:** Extract transactions from all sheets, handle different column formats
- **Output:** Parsed MF holdings and capital gains in database

### REQ-MF-003: Equity Fund Classification
- **Input:** Scheme name, Fund category
- **Processing:** Classify as EQUITY based on scheme name keywords or explicit category
- **Output:** asset_class = 'EQUITY' for tax treatment

### REQ-MF-004: Debt Fund Classification
- **Input:** Scheme name, Fund category (Liquid, Ultra Short, etc.)
- **Processing:** Classify as DEBT if not equity
- **Output:** asset_class = 'DEBT' for tax treatment

### REQ-MF-005: STCG Calculation
- **Input:** Redemption <12 months for Equity, <36 months for Debt (pre-Apr 2023)
- **Processing:** Calculate short-term gain
- **Output:** STCG amount (20% for equity, slab for debt)

### REQ-MF-006: LTCG Calculation
- **Input:** Redemption >12 months for Equity
- **Processing:** Calculate long-term gain at 12.5% with ₹1.25L exemption
- **Output:** LTCG amount with exemption applied

### REQ-MF-007: Debt MF Tax Treatment (Post Apr 2023)
- **Input:** Debt fund redemption after 1-Apr-2023
- **Processing:** No LTCG benefit - all gains taxed at slab rate
- **Output:** Full gain taxable at slab rate (no indexation)

### REQ-MF-008: Grandfathering (Pre-31-Jan-2018)
- **Input:** Purchase before 31-Jan-2018
- **Processing:** Use higher of (actual cost, FMV on 31-Jan-2018) as cost basis
- **Output:** Adjusted cost basis for CG calculation

### REQ-MF-009: Quarterly Capital Gains Statement
- **Input:** All redemption transactions for FY
- **Processing:** Generate quarterly breakdown as per ITR requirements
- **Output:** Capital gains by quarter (Apr-Jun, Jul-Sep, Oct-Dec, Jan-Mar)

---

## KARVY Excel File Structure

Based on project file `MF_KArvy_CG_FY24-25.xlsx`:

### Sheet 1: SUMMARY
**Purpose:** Overall summary of capital gains by quarter

```
Summary Of Capital Gains | 01/04 to 15/06 | 16/06 to 15/09 | 16/09 to 15/12 | 16/12 to 15/03 | 16/03 to 31/03 | Total
Short Term Capital Gain
Full Value Consideration  | 1150000        | 4276114.7      | 6205545.08     | 1769302.46     | 95000          | 13495962.24
Cost of Acquisition       | 1121739.7      | 4082253.28     | 5978843.55     | 1730062.62     | 94494.38       | 13007393.53
Short Term Capital Gain   | 28260.3        | 193861.42      | 226701.53      | 39239.84       | 505.62         | 488568.71
Long Term Capital Gain with Indexation
Full Value Consideration  | 0              | 0              | 0              | 0              | 0              |
Cost of Acquisition       | 0              | 0              | 0              | 0              | 0              |
LongTermWithIndex-CG      | 0              | 0              | 0              | 0              | 0              |
Long Term Capital Gain without Indexation
Full Value Consideration  | 0              | 0              | 0              | 0              | 0              |
Cost of Acquisition       | 0              | 0              | 0              | 0              | 0              |
LongTermWithOutIndex-CG   | 0              | 0              | 0              | 0              | 0              |
```

### Sheet 2: Scheme_Level_Summary
**Purpose:** Scheme-wise summary for classification

```
Scheme Name                                                    | Count | Outflow Amount | Net Value   | Fair Market NAV | Short Gain | Long Gain With Index | Long Gain Without Index
quant Liquid Fund - Direct Plan INF966L01820                  | 22    | 1613390.23     | 1553018.51  | 26.9383         | 60371.72   | 0.00                 | 0.00
Mirae Asset Aggressive Hybrid Fund - Direct Plan INF769K01DH9 | 1     | 28632.76       | 24999.99    | 0               | 3632.77    | 0.00                 | 0.00
```

### Sheet 3: Trasaction_Details (Main Data Sheet)
**Purpose:** Detailed transaction-level data with capital gains

**Column Structure:**
```
Fund | Fund Name | Folio Number | Scheme Name | Trxn.Type | Date | Current Units | Source Scheme units | 
Original Purchase Cost | Original Cost Amount | Grandfathered NAV as on 31/01/2018 | GrandFathered Cost Value |
IT Applicable NAV | IT Applicable Cost Value | Trxn.Type | Date | Units | Amount | Price | 
Tax Perc | Tax | Short Term | Indexed Cost | Long Term With Index | Long Term Without Index
```

**Sample Row:**
```
117 | Mirae Asset Mutual Fund | 7993343482 | Mirae Asset Large and Midcap Fund - Direct Plan (INF769K01BI1) |
Systematic Investment | 10/01/2024 | 107.223 | 107.223 | 139.889 | 15000.07 | 0.0000 | 0.00 |
139.8890 | 15000.07 | Redemption | 12/08/2024 | 107.223 | 17563.99 | 163.808 | | | 2563.92 | 0.00 | 0.00 | 0.00
```

---

## Key Differences: KARVY vs CAMS

| Feature | CAMS | KARVY |
|---------|------|-------|
| Asset Class Column | Explicit "ASSET CLASS" | Inferred from scheme name/category |
| ISIN Location | In "Scheme Name" field | In "Scheme Name" field (same) |
| Grandfathering Data | Separate columns | Combined in transaction row |
| Sheet Names | TRXN_DETAILS | Trasaction_Details (note typo) |
| Column Names | CamelCase | Mixed case with spaces |
| Date Format | DD-MMM-YYYY | DD/MM/YYYY |

---

## Database Schema

Uses same schema as CAMS (defined in S2_mf_cams.md). Additional table for KARVY-specific tracking:

```sql
-- Track source RTA for each folio
ALTER TABLE mf_folios ADD COLUMN rta TEXT DEFAULT 'CAMS';
-- Values: 'CAMS', 'KARVY', 'KFINTECH'

-- Track import source
ALTER TABLE mf_transactions ADD COLUMN rta TEXT;

-- Quarterly capital gains for ITR
CREATE TABLE IF NOT EXISTS mf_quarterly_cg (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    financial_year TEXT NOT NULL,
    quarter TEXT NOT NULL,  -- 'Q1', 'Q2', 'Q3', 'Q4', 'Q4_MAR'
    quarter_start DATE NOT NULL,
    quarter_end DATE NOT NULL,
    asset_class TEXT NOT NULL,  -- 'EQUITY', 'DEBT'
    
    -- Short Term
    stcg_full_value DECIMAL(15,2) DEFAULT 0,
    stcg_cost_acquisition DECIMAL(15,2) DEFAULT 0,
    stcg_gain_loss DECIMAL(15,2) DEFAULT 0,
    
    -- Long Term with Indexation (pre-2023 debt)
    ltcg_indexed_full_value DECIMAL(15,2) DEFAULT 0,
    ltcg_indexed_cost DECIMAL(15,2) DEFAULT 0,
    ltcg_indexed_gain DECIMAL(15,2) DEFAULT 0,
    
    -- Long Term without Indexation (equity, post-2023 debt)
    ltcg_unindexed_full_value DECIMAL(15,2) DEFAULT 0,
    ltcg_unindexed_cost DECIMAL(15,2) DEFAULT 0,
    ltcg_unindexed_gain DECIMAL(15,2) DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, financial_year, quarter, asset_class)
);

-- ITR Quarter boundaries (as per Income Tax rules)
-- Q1: 01-Apr to 15-Jun
-- Q2: 16-Jun to 15-Sep
-- Q3: 16-Sep to 15-Dec
-- Q4: 16-Dec to 15-Mar
-- Q4_MAR: 16-Mar to 31-Mar (special for advance tax)
```

---

## Files to Create

```
src/pfas/parsers/mf/
├── __init__.py
├── base.py              # Base MF parser class (shared)
├── cams.py              # CAMS CAS parser (from S2_mf_cams.md)
├── karvy.py             # KARVY/KFINTECH parser (THIS MODULE)
├── models.py            # MFTransaction, MFScheme dataclasses (shared)
├── classifier.py        # Equity/Debt classification logic (shared)
├── capital_gains.py     # CG calculation engine (shared)
├── grandfathering.py    # Pre-31-Jan-2018 logic (shared)
└── quarterly_report.py  # Quarterly CG breakdown for ITR

tests/unit/test_parsers/test_mf/
├── __init__.py
├── test_cams.py
├── test_karvy.py        # THIS MODULE's tests
├── test_classifier.py
├── test_capital_gains.py
├── test_grandfathering.py
└── test_quarterly_report.py

tests/fixtures/mf/
├── cams_cas_sample.xlsx
├── karvy_cg_sample.xlsx          # Full KARVY CG statement
├── karvy_liquid_fund.xlsx        # Liquid fund (debt) test case
├── karvy_equity_fund.xlsx        # Equity fund test case
└── karvy_grandfathered.xlsx      # Pre-31-Jan-2018 test case
```

---

## Implementation

### karvy.py
```python
"""
KARVY/KFINTECH Capital Gains Statement Parser.

Parses Excel files from KARVY (now KFINTECH) RTA containing:
- Summary sheet with quarterly breakdown
- Scheme-level summary
- Transaction details with pre-calculated capital gains

Key differences from CAMS:
- Different column naming conventions
- Asset class must be inferred from scheme name
- Grandfathering data embedded in transaction rows
- Date format: DD/MM/YYYY vs DD-MMM-YYYY
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field

from .models import MFTransaction, MFScheme, AssetClass, TransactionType
from .classifier import classify_scheme, extract_isin
from .capital_gains import CapitalGainsCalculator

@dataclass
class KARVYSummary:
    """Quarterly capital gains summary from KARVY."""
    quarter: str
    quarter_start: date
    quarter_end: date
    
    # Short Term
    stcg_full_value: Decimal = Decimal("0")
    stcg_cost: Decimal = Decimal("0")
    stcg_gain: Decimal = Decimal("0")
    
    # Long Term with Index
    ltcg_indexed_full_value: Decimal = Decimal("0")
    ltcg_indexed_cost: Decimal = Decimal("0")
    ltcg_indexed_gain: Decimal = Decimal("0")
    
    # Long Term without Index
    ltcg_unindexed_full_value: Decimal = Decimal("0")
    ltcg_unindexed_cost: Decimal = Decimal("0")
    ltcg_unindexed_gain: Decimal = Decimal("0")

@dataclass
class KARVYParseResult:
    """Result of parsing KARVY statement."""
    success: bool
    transactions: List[MFTransaction] = field(default_factory=list)
    quarterly_summary: List[KARVYSummary] = field(default_factory=list)
    scheme_summary: List[Dict] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    source_file: str = ""

class KARVYParser:
    """
    Parser for KARVY/KFINTECH Capital Gains Statement Excel files.
    
    Handles:
    - Multi-sheet Excel files (SUMMARY, Scheme_Level_Summary, Trasaction_Details)
    - Different column naming conventions
    - Scheme classification from name
    - Grandfathering calculations
    - Quarterly breakdown generation
    """
    
    # KARVY column name mappings (KARVY column -> standard name)
    COLUMN_MAP = {
        # Transaction details columns
        'Fund': 'fund_code',
        'Fund Name': 'amc_name',
        'Folio Number': 'folio_number',
        'Folio No': 'folio_number',
        'Scheme Name': 'scheme_name',
        'Trxn.Type': 'transaction_type',
        'Trxn Type': 'transaction_type',
        'Transaction Type': 'transaction_type',
        'Date': 'date',
        'Current Units': 'units',
        'Units': 'units',
        'Source Scheme units': 'source_units',
        'Original Purchase Cost': 'purchase_nav',
        'Original Cost Amount': 'purchase_amount',
        'Grandfathered NAV as on 31/01/2018': 'grandfathered_nav',
        'GrandFathered Cost Value': 'grandfathered_value',
        'IT Applicable NAV': 'applicable_nav',
        'IT Applicable Cost Value': 'applicable_cost',
        'Amount': 'amount',
        'Price': 'nav',
        'NAV': 'nav',
        'Tax Perc': 'tax_percentage',
        'Tax': 'tax_amount',
        'Short Term': 'short_term_gain',
        'Indexed Cost': 'indexed_cost',
        'Long Term With Index': 'long_term_indexed',
        'Long Term Without Index': 'long_term_unindexed',
    }
    
    # ITR Quarter boundaries
    ITR_QUARTERS = {
        'Q1': (4, 1, 6, 15),    # Apr 1 to Jun 15
        'Q2': (6, 16, 9, 15),   # Jun 16 to Sep 15
        'Q3': (9, 16, 12, 15),  # Sep 16 to Dec 15
        'Q4': (12, 16, 3, 15),  # Dec 16 to Mar 15
        'Q4_MAR': (3, 16, 3, 31)  # Mar 16 to Mar 31
    }
    
    # Sheet name variations
    SUMMARY_SHEETS = ['SUMMARY', 'Summary', 'summary']
    SCHEME_SHEETS = ['Scheme_Level_Summary', 'SCHEME_LEVEL_SUMMARY', 'Scheme Level Summary']
    TRANSACTION_SHEETS = ['Trasaction_Details', 'Transaction_Details', 'TRANSACTION_DETAILS', 
                          'Trasaction Details', 'Transaction Details']
    
    def __init__(self, db_connection):
        self.conn = db_connection
        self.cg_calculator = CapitalGainsCalculator(db_connection)
    
    def parse(self, file_path: Path) -> KARVYParseResult:
        """
        Parse KARVY Capital Gains Statement Excel file.
        
        Args:
            file_path: Path to KARVY Excel file
            
        Returns:
            KARVYParseResult with transactions, summaries, and any errors
        """
        result = KARVYParseResult(success=False, source_file=str(file_path))
        
        if not file_path.exists():
            result.errors.append(f"File not found: {file_path}")
            return result
        
        try:
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names
            
            # Parse Summary sheet
            summary_sheet = self._find_sheet(sheet_names, self.SUMMARY_SHEETS)
            if summary_sheet:
                result.quarterly_summary = self._parse_summary_sheet(excel_file, summary_sheet)
            
            # Parse Scheme Level Summary
            scheme_sheet = self._find_sheet(sheet_names, self.SCHEME_SHEETS)
            if scheme_sheet:
                result.scheme_summary = self._parse_scheme_summary(excel_file, scheme_sheet)
            
            # Parse Transaction Details (main data)
            txn_sheet = self._find_sheet(sheet_names, self.TRANSACTION_SHEETS)
            if txn_sheet:
                result.transactions = self._parse_transactions(excel_file, txn_sheet)
            else:
                result.errors.append("Transaction details sheet not found")
                return result
            
            result.success = len(result.transactions) > 0
            
        except Exception as e:
            result.errors.append(f"Parse error: {str(e)}")
        
        return result
    
    def _find_sheet(self, available: List[str], candidates: List[str]) -> Optional[str]:
        """Find matching sheet name from candidates."""
        for candidate in candidates:
            if candidate in available:
                return candidate
        # Try case-insensitive match
        for candidate in candidates:
            for available_sheet in available:
                if candidate.lower() == available_sheet.lower():
                    return available_sheet
        return None
    
    def _parse_summary_sheet(self, excel_file: pd.ExcelFile, 
                             sheet_name: str) -> List[KARVYSummary]:
        """Parse quarterly summary from SUMMARY sheet."""
        summaries = []
        
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
            
            # Find header row with quarter columns
            header_row = None
            for idx, row in df.iterrows():
                row_values = [str(v).strip() for v in row.values if pd.notna(v)]
                if any('01/04' in v or '16/06' in v for v in row_values):
                    header_row = idx
                    break
            
            if header_row is None:
                return summaries
            
            # Extract quarter columns
            quarter_cols = {
                'Q1': self._find_quarter_col(df.iloc[header_row], '01/04'),
                'Q2': self._find_quarter_col(df.iloc[header_row], '16/06'),
                'Q3': self._find_quarter_col(df.iloc[header_row], '16/09'),
                'Q4': self._find_quarter_col(df.iloc[header_row], '16/12'),
                'Q4_MAR': self._find_quarter_col(df.iloc[header_row], '16/03'),
            }
            
            # Parse STCG, LTCG with index, LTCG without index sections
            # (Implementation details...)
            
        except Exception as e:
            pass  # Log error but continue
        
        return summaries
    
    def _parse_scheme_summary(self, excel_file: pd.ExcelFile,
                               sheet_name: str) -> List[Dict]:
        """Parse scheme-level summary for classification hints."""
        schemes = []
        
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            
            for _, row in df.iterrows():
                scheme_name = str(row.get('Scheme Name', ''))
                if not scheme_name or scheme_name == 'nan':
                    continue
                
                schemes.append({
                    'scheme_name': scheme_name,
                    'isin': extract_isin(scheme_name),
                    'count': int(row.get('Count', 0)),
                    'outflow_amount': self._to_decimal(row.get('Outflow Amount')),
                    'short_gain': self._to_decimal(row.get('Short Gain')),
                    'long_gain_indexed': self._to_decimal(row.get('Long Gain With Index')),
                    'long_gain_unindexed': self._to_decimal(row.get('Long Gain Without Index')),
                })
                
        except Exception as e:
            pass
        
        return schemes
    
    def _parse_transactions(self, excel_file: pd.ExcelFile,
                            sheet_name: str) -> List[MFTransaction]:
        """Parse transaction details from main data sheet."""
        transactions = []
        
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        df = self._normalize_columns(df)
        
        # Skip if required columns missing
        required_cols = ['scheme_name', 'date', 'units']
        if not all(col in df.columns for col in required_cols):
            return transactions
        
        for idx, row in df.iterrows():
            try:
                txn = self._parse_transaction_row(row, idx)
                if txn:
                    transactions.append(txn)
            except Exception as e:
                # Log but continue parsing
                continue
        
        return transactions
    
    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names to standard format."""
        rename_map = {}
        
        for col in df.columns:
            col_str = str(col).strip()
            # Try exact match first
            if col_str in self.COLUMN_MAP:
                rename_map[col] = self.COLUMN_MAP[col_str]
            else:
                # Try case-insensitive match
                for karvy_col, std_col in self.COLUMN_MAP.items():
                    if karvy_col.lower() == col_str.lower():
                        rename_map[col] = std_col
                        break
        
        return df.rename(columns=rename_map)
    
    def _parse_transaction_row(self, row: pd.Series, row_idx: int) -> Optional[MFTransaction]:
        """Parse a single transaction row."""
        # Extract scheme info
        scheme_name = str(row.get('scheme_name', ''))
        if not scheme_name or scheme_name == 'nan' or scheme_name.strip() == '':
            return None
        
        # Extract ISIN from scheme name (format: "Scheme Name (ISIN)")
        isin = extract_isin(scheme_name)
        
        # Classify scheme as EQUITY or DEBT
        asset_class = classify_scheme(scheme_name)
        
        # Create scheme object
        scheme = MFScheme(
            name=scheme_name,
            amc_name=str(row.get('amc_name', '')),
            isin=isin,
            asset_class=asset_class,
            nav_31jan2018=self._to_decimal(row.get('grandfathered_nav'))
        )
        
        # Determine transaction type
        txn_type_str = str(row.get('transaction_type', '')).upper()
        txn_type = self._parse_transaction_type(txn_type_str)
        
        # Parse dates (KARVY uses DD/MM/YYYY format)
        txn_date = self._parse_date(row.get('date'))
        if not txn_date:
            return None
        
        # Get units, NAV, amount
        units = self._to_decimal(row.get('units'))
        nav = self._to_decimal(row.get('nav'))
        amount = self._to_decimal(row.get('amount'))
        
        # If amount is 0 but units and nav exist, calculate it
        if amount == 0 and units > 0 and nav > 0:
            amount = units * nav
        
        # Create transaction
        txn = MFTransaction(
            folio_number=str(row.get('folio_number', '')),
            scheme=scheme,
            transaction_type=txn_type,
            date=txn_date,
            units=units,
            nav=nav,
            amount=amount,
            stt=Decimal("0"),  # KARVY doesn't provide STT in CG statement
            
            # Purchase info (for redemptions)
            purchase_date=self._parse_date(row.get('date')),  # Same row has purchase info
            purchase_units=self._to_decimal(row.get('source_units')),
            purchase_nav=self._to_decimal(row.get('purchase_nav')),
            
            # Grandfathering
            grandfathered_nav=self._to_decimal(row.get('grandfathered_nav')),
            grandfathered_value=self._to_decimal(row.get('grandfathered_value')),
            
            # Pre-calculated capital gains from KARVY
            short_term_gain=self._to_decimal(row.get('short_term_gain')),
            long_term_gain=max(
                self._to_decimal(row.get('long_term_indexed')),
                self._to_decimal(row.get('long_term_unindexed'))
            )
        )
        
        return txn
    
    def _parse_transaction_type(self, txn_type_str: str) -> TransactionType:
        """Parse KARVY transaction type string."""
        txn_upper = txn_type_str.upper().strip()
        
        if 'REDEMPTION' in txn_upper or 'RED' in txn_upper:
            return TransactionType.REDEMPTION
        elif 'SWITCH OUT' in txn_upper or 'SWITCHOUT' in txn_upper or 'STP OUT' in txn_upper:
            return TransactionType.SWITCH_OUT
        elif 'SWITCH IN' in txn_upper or 'SWITCHIN' in txn_upper or 'STP IN' in txn_upper:
            return TransactionType.SWITCH_IN
        elif 'DIVIDEND REINVEST' in txn_upper or 'DIV REINVEST' in txn_upper:
            return TransactionType.DIVIDEND_REINVEST
        elif 'DIVIDEND' in txn_upper or 'DIV PAYOUT' in txn_upper:
            return TransactionType.DIVIDEND
        elif any(kw in txn_upper for kw in ['PURCHASE', 'SIP', 'SYSTEMATIC', 'NEW', 'ADDITIONAL']):
            return TransactionType.PURCHASE
        else:
            return TransactionType.PURCHASE  # Default
    
    def _parse_date(self, date_val) -> Optional[date]:
        """
        Parse date from various KARVY formats.
        
        Supported formats:
        - DD/MM/YYYY
        - DD-MM-YYYY
        - DD-MMM-YYYY
        - datetime object
        """
        if pd.isna(date_val) or date_val is None:
            return None
        
        if isinstance(date_val, datetime):
            return date_val.date()
        
        if isinstance(date_val, date):
            return date_val
        
        date_str = str(date_val).strip()
        
        # Try different formats
        formats = [
            '%d/%m/%Y',    # 10/01/2024
            '%d-%m-%Y',    # 10-01-2024
            '%d-%b-%Y',    # 10-Jan-2024
            '%d-%b-%y',    # 10-Jan-24
            '%Y-%m-%d',    # 2024-01-10
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        # Try pandas parser as fallback
        try:
            return pd.to_datetime(date_str, dayfirst=True).date()
        except:
            return None
    
    def _to_decimal(self, value) -> Decimal:
        """Safely convert value to Decimal."""
        if pd.isna(value) or value is None or value == '':
            return Decimal("0")
        
        try:
            # Handle string with commas
            if isinstance(value, str):
                value = value.replace(',', '').strip()
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return Decimal("0")
    
    def _find_quarter_col(self, header_row: pd.Series, pattern: str) -> Optional[int]:
        """Find column index containing quarter pattern."""
        for idx, val in enumerate(header_row):
            if pd.notna(val) and pattern in str(val):
                return idx
        return None
    
    # === Database Operations ===
    
    def save_to_db(self, result: KARVYParseResult, user_id: int) -> int:
        """
        Save parsed KARVY data to database.
        
        Returns:
            Number of transactions saved
        """
        if not result.success:
            return 0
        
        count = 0
        
        for txn in result.transactions:
            try:
                # Get or create scheme
                scheme_id = self._get_or_create_scheme(txn.scheme)
                
                # Get or create folio
                folio_id = self._get_or_create_folio(user_id, scheme_id, txn.folio_number)
                
                # Insert transaction (with duplicate check)
                if self._insert_transaction(folio_id, txn, result.source_file):
                    count += 1
                    
            except Exception as e:
                continue
        
        # Save quarterly summary
        if result.quarterly_summary:
            self._save_quarterly_summary(user_id, result.quarterly_summary)
        
        self.conn.commit()
        return count
    
    def _get_or_create_scheme(self, scheme: MFScheme) -> int:
        """Get existing scheme ID or create new."""
        # Try to find by ISIN first
        if scheme.isin:
            cursor = self.conn.execute(
                "SELECT id FROM mf_schemes WHERE isin = ?",
                (scheme.isin,)
            )
            row = cursor.fetchone()
            if row:
                return row['id']
        
        # Create new scheme
        cursor = self.conn.execute("""
            INSERT INTO mf_schemes (name, isin, asset_class, nav_31jan2018)
            VALUES (?, ?, ?, ?)
        """, (scheme.name, scheme.isin, scheme.asset_class.value, 
              float(scheme.nav_31jan2018) if scheme.nav_31jan2018 else None))
        
        return cursor.lastrowid
    
    def _get_or_create_folio(self, user_id: int, scheme_id: int, folio_number: str) -> int:
        """Get existing folio ID or create new."""
        cursor = self.conn.execute("""
            SELECT id FROM mf_folios 
            WHERE user_id = ? AND scheme_id = ? AND folio_number = ?
        """, (user_id, scheme_id, folio_number))
        
        row = cursor.fetchone()
        if row:
            return row['id']
        
        cursor = self.conn.execute("""
            INSERT INTO mf_folios (user_id, scheme_id, folio_number, rta)
            VALUES (?, ?, ?, 'KARVY')
        """, (user_id, scheme_id, folio_number))
        
        return cursor.lastrowid
    
    def _insert_transaction(self, folio_id: int, txn: MFTransaction, 
                           source_file: str) -> bool:
        """Insert transaction with duplicate prevention."""
        # Check for duplicate
        cursor = self.conn.execute("""
            SELECT id FROM mf_transactions
            WHERE folio_id = ? AND date = ? AND units = ? AND amount = ?
        """, (folio_id, txn.date, float(txn.units), float(txn.amount)))
        
        if cursor.fetchone():
            return False  # Duplicate
        
        # Insert
        self.conn.execute("""
            INSERT INTO mf_transactions (
                folio_id, transaction_type, date, units, nav, amount,
                purchase_date, purchase_units, purchase_nav,
                grandfathered_nav, grandfathered_value,
                short_term_gain, long_term_gain, source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            folio_id, txn.transaction_type.value, txn.date,
            float(txn.units), float(txn.nav), float(txn.amount),
            txn.purchase_date, float(txn.purchase_units) if txn.purchase_units else None,
            float(txn.purchase_nav) if txn.purchase_nav else None,
            float(txn.grandfathered_nav) if txn.grandfathered_nav else None,
            float(txn.grandfathered_value) if txn.grandfathered_value else None,
            float(txn.short_term_gain), float(txn.long_term_gain),
            source_file
        ))
        
        return True
    
    def _save_quarterly_summary(self, user_id: int, summaries: List[KARVYSummary]):
        """Save quarterly CG summary to database."""
        for summary in summaries:
            self.conn.execute("""
                INSERT OR REPLACE INTO mf_quarterly_cg (
                    user_id, financial_year, quarter, quarter_start, quarter_end,
                    asset_class, stcg_full_value, stcg_cost_acquisition, stcg_gain_loss,
                    ltcg_indexed_full_value, ltcg_indexed_cost, ltcg_indexed_gain,
                    ltcg_unindexed_full_value, ltcg_unindexed_cost, ltcg_unindexed_gain
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id, "2024-25", summary.quarter, 
                summary.quarter_start, summary.quarter_end, "COMBINED",
                float(summary.stcg_full_value), float(summary.stcg_cost),
                float(summary.stcg_gain),
                float(summary.ltcg_indexed_full_value), float(summary.ltcg_indexed_cost),
                float(summary.ltcg_indexed_gain),
                float(summary.ltcg_unindexed_full_value), float(summary.ltcg_unindexed_cost),
                float(summary.ltcg_unindexed_gain)
            ))
```

### classifier.py (Shared between CAMS and KARVY)
```python
"""
Mutual Fund scheme classification logic.

Classifies schemes as EQUITY or DEBT based on:
1. Scheme name keywords
2. Category indicators
3. SEBI categorization rules
"""

import re
from .models import AssetClass

# Keywords indicating EQUITY schemes
EQUITY_KEYWORDS = [
    'equity', 'bluechip', 'large cap', 'largecap', 'mid cap', 'midcap',
    'small cap', 'smallcap', 'flexi cap', 'flexicap', 'multi cap', 'multicap',
    'focused', 'contra', 'value', 'dividend yield', 'thematic', 'sectoral',
    'elss', 'tax saver', 'tax saving', 'infrastructure', 'banking', 'pharma',
    'technology', 'consumption', 'fmcg', 'nifty', 'sensex', 'index fund',
    'aggressive hybrid', 'balanced advantage', 'dynamic asset',
    'arbitrage',  # Treated as equity for tax
]

# Keywords indicating DEBT schemes
DEBT_KEYWORDS = [
    'liquid', 'ultra short', 'ultrashort', 'low duration', 'money market',
    'overnight', 'short term', 'short duration', 'medium term', 'medium duration',
    'long term', 'long duration', 'gilt', 'government securities', 'g-sec',
    'corporate bond', 'credit risk', 'banking psu', 'floater', 'floating rate',
    'income fund', 'dynamic bond', 'fixed maturity', 'fmp',
    'conservative hybrid',  # <25% equity
]

# Keywords indicating HYBRID (check equity % for classification)
HYBRID_KEYWORDS = [
    'hybrid', 'balanced', 'asset allocation', 'multi asset',
]

def classify_scheme(scheme_name: str, category: str = None) -> AssetClass:
    """
    Classify mutual fund scheme as EQUITY or DEBT.
    
    For Indian tax purposes:
    - EQUITY: Schemes with >65% equity allocation (STCG 20%, LTCG 12.5%)
    - DEBT: All other schemes (taxed at slab rate, no indexation post Apr 2023)
    
    Args:
        scheme_name: Full scheme name (may include ISIN)
        category: Optional explicit category from RTA
        
    Returns:
        AssetClass.EQUITY or AssetClass.DEBT
    """
    name_lower = scheme_name.lower()
    
    # Remove ISIN if present
    name_lower = re.sub(r'\(?INF[A-Z0-9]+\)?', '', name_lower)
    
    # Check explicit category first
    if category:
        cat_lower = category.lower()
        if cat_lower == 'equity' or cat_lower == 'eq':
            return AssetClass.EQUITY
        elif cat_lower in ['debt', 'ultra liquid', 'liquid', 'money market']:
            return AssetClass.DEBT
    
    # Check debt keywords first (more specific)
    for keyword in DEBT_KEYWORDS:
        if keyword in name_lower:
            return AssetClass.DEBT
    
    # Check equity keywords
    for keyword in EQUITY_KEYWORDS:
        if keyword in name_lower:
            return AssetClass.EQUITY
    
    # Check hybrid - default to debt if ambiguous
    for keyword in HYBRID_KEYWORDS:
        if keyword in name_lower:
            # Aggressive/Balanced Advantage usually have >65% equity
            if 'aggressive' in name_lower or 'balanced advantage' in name_lower:
                return AssetClass.EQUITY
            return AssetClass.DEBT
    
    # Default to DEBT (safer for tax purposes)
    return AssetClass.DEBT

def extract_isin(scheme_name: str) -> str:
    """
    Extract ISIN from scheme name.
    
    ISIN format: INF followed by 9 alphanumeric characters
    Example: "Mirae Asset Large Cap Fund (INF769K01BI1)"
    
    Returns:
        ISIN string or empty string if not found
    """
    match = re.search(r'INF[A-Z0-9]{9}', scheme_name.upper())
    return match.group(0) if match else ""
```

---

## Test Cases

### TC-MF-002: KARVY CAS Parse
```python
def test_karvy_parse_basic(test_db, fixtures_path):
    """Test basic KARVY CG statement parsing."""
    parser = KARVYParser(test_db)
    result = parser.parse(fixtures_path / "mf/karvy_cg_sample.xlsx")
    
    assert result.success
    assert len(result.transactions) > 0
    assert len(result.errors) == 0
    
    # Verify transaction structure
    txn = result.transactions[0]
    assert txn.folio_number != ""
    assert txn.scheme.name != ""
    assert txn.date is not None
    assert txn.units > 0

def test_karvy_column_normalization(test_db):
    """Test column name normalization."""
    parser = KARVYParser(test_db)
    
    df = pd.DataFrame({
        'Folio Number': ['123456'],
        'Scheme Name': ['Test Liquid Fund INF123456789'],
        'Date': ['10/01/2024'],
        'Units': [100.0],
        'Amount': [10500.50],
        'Price': [105.005],
        'Short Term': [500.50],
    })
    
    normalized = parser._normalize_columns(df)
    
    assert 'folio_number' in normalized.columns
    assert 'scheme_name' in normalized.columns
    assert 'date' in normalized.columns
    assert 'units' in normalized.columns
    assert 'amount' in normalized.columns
    assert 'nav' in normalized.columns
    assert 'short_term_gain' in normalized.columns

def test_karvy_date_parsing(test_db):
    """Test various date format parsing."""
    parser = KARVYParser(test_db)
    
    # DD/MM/YYYY
    assert parser._parse_date('10/01/2024') == date(2024, 1, 10)
    
    # DD-MM-YYYY
    assert parser._parse_date('10-01-2024') == date(2024, 1, 10)
    
    # DD-MMM-YYYY
    assert parser._parse_date('10-Jan-2024') == date(2024, 1, 10)
    
    # None handling
    assert parser._parse_date(None) is None
    assert parser._parse_date('') is None
```

### TC-MF-003/004: Classification Tests
```python
def test_equity_fund_classification():
    """Test equity fund classification from scheme name."""
    from pfas.parsers.mf.classifier import classify_scheme, AssetClass
    
    # Clear equity schemes
    assert classify_scheme("SBI Bluechip Fund Direct Growth") == AssetClass.EQUITY
    assert classify_scheme("Mirae Asset Large Cap Fund") == AssetClass.EQUITY
    assert classify_scheme("HDFC Mid-Cap Opportunities Fund") == AssetClass.EQUITY
    assert classify_scheme("Axis Small Cap Fund") == AssetClass.EQUITY
    assert classify_scheme("Parag Parikh Flexi Cap Fund") == AssetClass.EQUITY
    assert classify_scheme("ELSS Tax Saver Fund") == AssetClass.EQUITY
    assert classify_scheme("Nifty 50 Index Fund") == AssetClass.EQUITY

def test_debt_fund_classification():
    """Test debt fund classification from scheme name."""
    from pfas.parsers.mf.classifier import classify_scheme, AssetClass
    
    # Clear debt schemes
    assert classify_scheme("quant Liquid Fund Direct Plan") == AssetClass.DEBT
    assert classify_scheme("HDFC Ultra Short Term Fund") == AssetClass.DEBT
    assert classify_scheme("Kotak Corporate Bond Fund") == AssetClass.DEBT
    assert classify_scheme("SBI Overnight Fund") == AssetClass.DEBT
    assert classify_scheme("ICICI Prudential Gilt Fund") == AssetClass.DEBT
    assert classify_scheme("Axis Short Term Fund") == AssetClass.DEBT

def test_hybrid_fund_classification():
    """Test hybrid fund classification."""
    from pfas.parsers.mf.classifier import classify_scheme, AssetClass
    
    # Aggressive hybrid (>65% equity) -> EQUITY
    assert classify_scheme("Mirae Asset Aggressive Hybrid Fund") == AssetClass.EQUITY
    assert classify_scheme("ICICI Balanced Advantage Fund") == AssetClass.EQUITY
    
    # Conservative hybrid (<25% equity) -> DEBT
    assert classify_scheme("HDFC Conservative Hybrid Fund") == AssetClass.DEBT

def test_isin_extraction():
    """Test ISIN extraction from scheme name."""
    from pfas.parsers.mf.classifier import extract_isin
    
    assert extract_isin("quant Liquid Fund - Direct Plan INF966L01820") == "INF966L01820"
    assert extract_isin("Mirae Asset Fund (INF769K01BI1)") == "INF769K01BI1"
    assert extract_isin("No ISIN Here Fund") == ""
```

### TC-MF-005/006: Capital Gains Tests
```python
def test_karvy_stcg_extraction(test_db, fixtures_path):
    """Test STCG extraction from KARVY statement."""
    parser = KARVYParser(test_db)
    result = parser.parse(fixtures_path / "mf/karvy_cg_sample.xlsx")
    
    # Find redemption transactions
    redemptions = [t for t in result.transactions 
                   if t.transaction_type == TransactionType.REDEMPTION]
    
    # At least one should have STCG
    stcg_txns = [t for t in redemptions if t.short_term_gain > 0]
    assert len(stcg_txns) > 0
    
    # Verify STCG is positive
    for txn in stcg_txns:
        assert txn.short_term_gain > Decimal("0")

def test_karvy_ltcg_extraction(test_db, fixtures_path):
    """Test LTCG extraction from KARVY statement."""
    parser = KARVYParser(test_db)
    result = parser.parse(fixtures_path / "mf/karvy_cg_sample.xlsx")
    
    # Find redemption transactions with LTCG
    ltcg_txns = [t for t in result.transactions 
                 if t.long_term_gain > Decimal("0")]
    
    # Verify LTCG values
    for txn in ltcg_txns:
        assert txn.long_term_gain > Decimal("0")
```

### TC-MF-008: Grandfathering Tests
```python
def test_karvy_grandfathering_data(test_db, fixtures_path):
    """Test extraction of grandfathering data."""
    parser = KARVYParser(test_db)
    result = parser.parse(fixtures_path / "mf/karvy_grandfathered.xlsx")
    
    # Find transactions with grandfathering data
    gf_txns = [t for t in result.transactions 
               if t.grandfathered_nav and t.grandfathered_nav > 0]
    
    for txn in gf_txns:
        assert txn.grandfathered_nav > Decimal("0")
        assert txn.grandfathered_value >= Decimal("0")
```

### TC-MF-009: Quarterly Summary Tests
```python
def test_karvy_quarterly_summary(test_db, fixtures_path):
    """Test quarterly summary extraction."""
    parser = KARVYParser(test_db)
    result = parser.parse(fixtures_path / "mf/karvy_cg_sample.xlsx")
    
    # Should have quarterly breakdowns
    assert len(result.quarterly_summary) > 0
    
    # Verify quarter structure
    for summary in result.quarterly_summary:
        assert summary.quarter in ['Q1', 'Q2', 'Q3', 'Q4', 'Q4_MAR']
        assert summary.quarter_start is not None
        assert summary.quarter_end is not None

def test_quarterly_totals_match_transactions(test_db, fixtures_path):
    """Verify quarterly totals match transaction-level data."""
    parser = KARVYParser(test_db)
    result = parser.parse(fixtures_path / "mf/karvy_cg_sample.xlsx")
    
    # Sum transaction-level STCG
    txn_stcg = sum(t.short_term_gain for t in result.transactions)
    
    # Sum quarterly STCG
    qtr_stcg = sum(s.stcg_gain for s in result.quarterly_summary)
    
    # Should be approximately equal (allow small rounding difference)
    diff = abs(txn_stcg - qtr_stcg)
    assert diff < Decimal("1.0"), f"STCG mismatch: txn={txn_stcg}, qtr={qtr_stcg}"
```

### Integration Test
```python
def test_karvy_full_flow(test_db, fixtures_path):
    """Test complete KARVY parsing and storage flow."""
    parser = KARVYParser(test_db)
    
    # Parse
    result = parser.parse(fixtures_path / "mf/karvy_cg_sample.xlsx")
    assert result.success
    
    # Save to database
    count = parser.save_to_db(result, user_id=1)
    assert count > 0
    
    # Verify data in database
    cursor = test_db.execute("SELECT COUNT(*) as cnt FROM mf_transactions")
    assert cursor.fetchone()['cnt'] == count
    
    # Verify schemes created
    cursor = test_db.execute("SELECT COUNT(*) as cnt FROM mf_schemes")
    assert cursor.fetchone()['cnt'] > 0
    
    # Verify folios created
    cursor = test_db.execute("SELECT COUNT(*) as cnt FROM mf_folios WHERE rta = 'KARVY'")
    assert cursor.fetchone()['cnt'] > 0
```

---

## Verification Commands

```bash
# Run KARVY-specific tests
pytest tests/unit/test_parsers/test_mf/test_karvy.py -v

# Run all MF parser tests
pytest tests/unit/test_parsers/test_mf/ -v

# Run with coverage
pytest tests/unit/test_parsers/test_mf/ --cov=src/pfas/parsers/mf --cov-report=term-missing

# Run specific test
pytest tests/unit/test_parsers/test_mf/test_karvy.py::test_karvy_parse_basic -v

# Expected output:
# - All tests pass
# - Coverage > 80%
```

---

## Success Criteria

- [ ] KARVY Excel files parsed correctly (all 3 sheets)
- [ ] Multiple column naming variations handled
- [ ] Date formats (DD/MM/YYYY, DD-MMM-YYYY) parsed correctly
- [ ] Schemes classified as EQUITY or DEBT from name
- [ ] ISIN extracted from scheme name
- [ ] Transaction types (Purchase, Redemption, SIP, STP) recognized
- [ ] Grandfathering data (NAV on 31-Jan-2018) extracted
- [ ] Pre-calculated STCG/LTCG values captured
- [ ] Quarterly summary extracted
- [ ] Duplicate transactions prevented on re-import
- [ ] Integration with CAMS parser (unified model)
- [ ] All unit tests passing
- [ ] Code coverage > 80%

---

## Integration with CAMS Parser

Both KARVY and CAMS parsers share:
1. **models.py** - `MFTransaction`, `MFScheme`, `AssetClass` dataclasses
2. **classifier.py** - Scheme classification logic
3. **capital_gains.py** - CG calculation engine
4. **grandfathering.py** - Pre-31-Jan-2018 cost basis logic

The parsers produce the same output format, allowing unified reporting and ITR generation regardless of RTA source.

### Combined MF Report Generation
```python
def generate_combined_mf_report(user_id: int, fy: str):
    """Generate unified MF report from both CAMS and KARVY."""
    # Query all transactions regardless of source
    cursor = db.execute("""
        SELECT t.*, f.rta, s.name as scheme_name, s.asset_class
        FROM mf_transactions t
        JOIN mf_folios f ON t.folio_id = f.id
        JOIN mf_schemes s ON f.scheme_id = s.id
        WHERE f.user_id = ? AND t.date BETWEEN ? AND ?
        ORDER BY t.date
    """, (user_id, fy_start, fy_end))
    
    # Process uniformly...
```
