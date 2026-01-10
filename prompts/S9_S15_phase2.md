# Phase 2: Foreign Assets, DTAA & ITR Export

## Overview
**Sprints:** S9-S15 (Week 17-30)
**Phase:** 2
**Focus:** RSU, ESPP, DRIP, DTAA Credit, Schedule FA, ITR-2 JSON Export

---

# Sprint 9: Multi-Currency & E*TRADE Parser

## Requirements

### Currency Module (REQ-CORE-004 Enhancement)
- Implement SBI TT Buying Rate lookup
- Historical rate storage
- Manual rate entry fallback

### E*TRADE Parser (REQ-RSU-001, REQ-ESPP-001)
- Parse E*TRADE/Morgan Stanley PDF statements
- Extract RSU vests, ESPP purchases
- Extract stock sales, dividends, DRIP

---

## SBI TT Buying Rate

```python
"""SBI TT Buying Rate for USD to INR conversion."""

from decimal import Decimal
from datetime import date
from typing import Optional
import requests

class SBITTRateProvider:
    """
    Fetch SBI TT Buying Rate.
    
    This rate is used for all foreign currency conversions:
    - RSU perquisite valuation
    - ESPP perquisite valuation
    - Foreign dividend conversion
    - Capital gains calculation
    """
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def get_rate(self, txn_date: date, currency: str = "USD") -> Decimal:
        """
        Get TT Buying Rate for date.
        
        Priority:
        1. Check local database cache
        2. If not found, try to fetch (if network enabled)
        3. Fallback to nearest available rate
        """
        # Check cache first
        cached = self._get_cached_rate(txn_date, currency)
        if cached:
            return cached
        
        # Try nearest available rate
        nearest = self._get_nearest_rate(txn_date, currency)
        if nearest:
            return nearest
        
        raise ValueError(f"No exchange rate available for {currency} on {txn_date}")
    
    def add_manual_rate(self, rate_date: date, currency: str, rate: Decimal):
        """Add manual rate entry."""
        self.conn.execute("""
            INSERT OR REPLACE INTO exchange_rates 
            (date, from_currency, to_currency, rate, source)
            VALUES (?, ?, 'INR', ?, 'MANUAL')
        """, (rate_date, currency, float(rate)))
        self.conn.commit()
    
    def _get_cached_rate(self, rate_date: date, currency: str) -> Optional[Decimal]:
        cursor = self.conn.execute("""
            SELECT rate FROM exchange_rates
            WHERE date = ? AND from_currency = ?
        """, (rate_date, currency))
        row = cursor.fetchone()
        return Decimal(str(row['rate'])) if row else None
```

---

## Morgan Stanley Statement Parser

Based on project files `ClientStatements_6492_*.pdf`:

```python
"""Morgan Stanley Client Statement parser."""

import pdfplumber
import re
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class StockPlanDetails:
    grant_date: date
    grant_number: str
    grant_type: str  # RSU
    symbol: str
    potential_quantity: Decimal
    grant_price: Decimal
    market_price: Decimal
    total_value: Decimal

@dataclass
class CashFlowActivity:
    activity_date: date
    settlement_date: Optional[date]
    activity_type: str  # Qualified Dividend, Sold, Interest Income, etc.
    description: str
    quantity: Optional[Decimal]
    price: Optional[Decimal]
    amount: Decimal  # Credits positive, Debits negative

class MorganStanleyParser:
    """Parser for Morgan Stanley Client Statements."""
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def parse(self, file_path: Path) -> dict:
        """Parse client statement PDF."""
        with pdfplumber.open(file_path) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() or ""
        
        return {
            'statement_period': self._extract_period(text),
            'account_number': self._extract_account(text),
            'stock_plan_details': self._extract_stock_plan(text),
            'cash_flow_activities': self._extract_activities(text)
        }
    
    def _extract_stock_plan(self, text: str) -> List[StockPlanDetails]:
        """Extract stock plan details section."""
        details = []
        
        # Pattern: Grant Date | Number | Type | Symbol | Qty | Grant Price | Market Price | Value
        pattern = r'(\d{2}/\d{2}/\d{2})\s+(RU\d+)\s+(RSU)\s+(\w+)\s+([\d.]+)\s+\$([\d.]+)\s+\$([\d.]+)\s+\$([\d,.]+)'
        
        for match in re.finditer(pattern, text):
            details.append(StockPlanDetails(
                grant_date=datetime.strptime(match.group(1), "%m/%d/%y").date(),
                grant_number=match.group(2),
                grant_type=match.group(3),
                symbol=match.group(4),
                potential_quantity=Decimal(match.group(5)),
                grant_price=Decimal(match.group(6)),
                market_price=Decimal(match.group(7)),
                total_value=Decimal(match.group(8).replace(",", ""))
            ))
        
        return details
    
    def _extract_activities(self, text: str) -> List[CashFlowActivity]:
        """Extract cash flow activities."""
        activities = []
        
        # Pattern for activities section
        # Activity Date | Settlement Date | Type | Description | Qty | Price | Amount
        
        # Dividend pattern
        div_pattern = r'(\d{1,2}/\d{1,2})\s+Qualified Dividend\s+(\w+\s+\w+)\s+\$([\d,.]+)'
        for match in re.finditer(div_pattern, text):
            activities.append(CashFlowActivity(
                activity_date=self._parse_short_date(match.group(1)),
                settlement_date=None,
                activity_type="Qualified Dividend",
                description=match.group(2),
                quantity=None,
                price=None,
                amount=Decimal(match.group(3).replace(",", ""))
            ))
        
        # Sold pattern
        sold_pattern = r'(\d{1,2}/\d{1,2})\s+(\d{1,2}/\d{1,2})\s+Sold\s+(.+?)\s+([\d.]+)\s+([\d.]+)\s+([\d,.]+)'
        for match in re.finditer(sold_pattern, text):
            activities.append(CashFlowActivity(
                activity_date=self._parse_short_date(match.group(1)),
                settlement_date=self._parse_short_date(match.group(2)),
                activity_type="Sold",
                description=match.group(3),
                quantity=Decimal(match.group(4)),
                price=Decimal(match.group(5)),
                amount=Decimal(match.group(6).replace(",", ""))
            ))
        
        return activities
```

---

# Sprint 10: RSU Processing

## Requirements (REQ-RSU-001 to REQ-RSU-006)

- Parse RSU vest events
- Calculate perquisite in INR (FMV × Shares × TT Rate)
- Correlate with payslip RSU tax credit
- Track cost basis for future sale
- Calculate LTCG on sale (>24 months)
- Process DRIP transactions

---

## Implementation

```python
"""RSU (Restricted Stock Unit) processing."""

from dataclasses import dataclass
from decimal import Decimal
from datetime import date
from typing import List, Optional

@dataclass
class RSUVest:
    grant_number: str
    vest_date: date
    shares_vested: Decimal
    fmv_usd: Decimal  # Fair Market Value at vest
    shares_withheld_for_tax: Decimal
    net_shares: Decimal
    tt_rate: Decimal  # SBI TT Buying Rate on vest date
    perquisite_inr: Decimal  # FMV × Shares × TT Rate
    
    @property
    def cost_basis_per_share_usd(self) -> Decimal:
        """Cost basis = FMV at vest (for future CG calculation)."""
        return self.fmv_usd
    
    @property
    def cost_basis_per_share_inr(self) -> Decimal:
        """Cost basis in INR."""
        return self.fmv_usd * self.tt_rate

@dataclass
class RSUSale:
    sell_date: date
    shares_sold: Decimal
    sell_price_usd: Decimal
    # Matched vest info
    vest_date: date
    cost_basis_usd: Decimal
    # Calculated
    holding_period_days: int
    is_ltcg: bool  # >24 months for foreign
    gain_usd: Decimal
    gain_inr: Decimal
    tt_rate_at_sale: Decimal

class RSUProcessor:
    """Process RSU vests and sales."""
    
    LTCG_THRESHOLD_DAYS = 730  # 24 months for foreign stocks
    
    def __init__(self, db_connection, rate_provider):
        self.conn = db_connection
        self.rate_provider = rate_provider
    
    def process_vest(self, vest_data: dict) -> RSUVest:
        """
        Process RSU vest event.
        
        1. Get TT rate for vest date
        2. Calculate perquisite in INR
        3. Store for correlation with payslip
        """
        vest_date = vest_data['vest_date']
        tt_rate = self.rate_provider.get_rate(vest_date, "USD")
        
        shares = Decimal(str(vest_data['shares_vested']))
        fmv = Decimal(str(vest_data['fmv_usd']))
        
        perquisite_inr = shares * fmv * tt_rate
        
        return RSUVest(
            grant_number=vest_data['grant_number'],
            vest_date=vest_date,
            shares_vested=shares,
            fmv_usd=fmv,
            shares_withheld_for_tax=Decimal(str(vest_data.get('shares_withheld', 0))),
            net_shares=shares - Decimal(str(vest_data.get('shares_withheld', 0))),
            tt_rate=tt_rate,
            perquisite_inr=perquisite_inr
        )
    
    def correlate_with_payslip(self, vest: RSUVest, salary_record_id: int) -> bool:
        """
        Correlate RSU vest with payslip RSU tax credit.
        
        The RSU tax credit in payslip should approximately match
        the tax on perquisite (30-35% of perquisite value).
        """
        # Get payslip RSU tax credit
        cursor = self.conn.execute("""
            SELECT rsu_tax_credit FROM salary_records WHERE id = ?
        """, (salary_record_id,))
        
        row = cursor.fetchone()
        if not row:
            return False
        
        payslip_credit = Decimal(str(row['rsu_tax_credit']))
        
        # Expected tax on perquisite (approximately 30-35%)
        expected_min = vest.perquisite_inr * Decimal("0.25")
        expected_max = vest.perquisite_inr * Decimal("0.40")
        
        if expected_min <= payslip_credit <= expected_max:
            # Mark as correlated
            self.conn.execute("""
                UPDATE rsu_vests SET 
                    salary_record_id = ?,
                    correlation_status = 'MATCHED'
                WHERE id = ?
            """, (salary_record_id, vest.id))
            self.conn.commit()
            return True
        
        return False
    
    def process_sale(self, sale_data: dict, vest: RSUVest) -> RSUSale:
        """
        Process RSU sale and calculate capital gain.
        
        LTCG if held >24 months from vest date.
        """
        sell_date = sale_data['sell_date']
        tt_rate_sale = self.rate_provider.get_rate(sell_date, "USD")
        
        shares_sold = Decimal(str(sale_data['shares_sold']))
        sell_price = Decimal(str(sale_data['sell_price_usd']))
        
        # Holding period from vest date
        holding_days = (sell_date - vest.vest_date).days
        is_ltcg = holding_days > self.LTCG_THRESHOLD_DAYS
        
        # Capital gain in USD
        sale_value_usd = shares_sold * sell_price
        cost_basis_usd = shares_sold * vest.cost_basis_per_share_usd
        gain_usd = sale_value_usd - cost_basis_usd
        
        # Convert gain to INR (using sale date TT rate)
        gain_inr = gain_usd * tt_rate_sale
        
        return RSUSale(
            sell_date=sell_date,
            shares_sold=shares_sold,
            sell_price_usd=sell_price,
            vest_date=vest.vest_date,
            cost_basis_usd=cost_basis_usd,
            holding_period_days=holding_days,
            is_ltcg=is_ltcg,
            gain_usd=gain_usd,
            gain_inr=gain_inr,
            tt_rate_at_sale=tt_rate_sale
        )
```

---

# Sprint 11: ESPP Processing

## Requirements (REQ-ESPP-001 to REQ-ESPP-005)

- Parse ESPP purchase events
- Calculate discount perquisite (15% typically)
- Track cost basis
- Calculate CG on sale
- Track TCS on LRS (206CQ)

---

## Implementation

```python
"""ESPP (Employee Stock Purchase Plan) processing."""

from dataclasses import dataclass
from decimal import Decimal
from datetime import date

@dataclass
class ESPPPurchase:
    purchase_date: date
    shares_purchased: Decimal
    purchase_price_usd: Decimal  # Discounted price
    market_price_usd: Decimal    # FMV at purchase
    discount_percentage: Decimal
    perquisite_per_share_usd: Decimal  # Market - Purchase
    total_perquisite_usd: Decimal
    tt_rate: Decimal
    perquisite_inr: Decimal  # Taxable as salary
    
    # TCS tracking
    lrs_amount_inr: Decimal  # Total remittance
    tcs_collected: Decimal   # 20% TCS if >₹7L

class ESPPProcessor:
    """Process ESPP purchases and sales."""
    
    TCS_THRESHOLD = Decimal("700000")  # ₹7L
    TCS_RATE = Decimal("20")  # 20%
    
    def __init__(self, db_connection, rate_provider):
        self.conn = db_connection
        self.rate_provider = rate_provider
    
    def process_purchase(self, purchase_data: dict) -> ESPPPurchase:
        """
        Process ESPP purchase.
        
        1. Calculate discount perquisite
        2. Convert to INR
        3. Calculate TCS if LRS >₹7L
        """
        purchase_date = purchase_data['purchase_date']
        tt_rate = self.rate_provider.get_rate(purchase_date, "USD")
        
        shares = Decimal(str(purchase_data['shares']))
        purchase_price = Decimal(str(purchase_data['purchase_price_usd']))
        market_price = Decimal(str(purchase_data['market_price_usd']))
        
        # Perquisite = Market Price - Purchase Price (the discount)
        perquisite_per_share = market_price - purchase_price
        total_perquisite_usd = perquisite_per_share * shares
        perquisite_inr = total_perquisite_usd * tt_rate
        
        # LRS remittance (total amount sent abroad)
        lrs_amount_usd = purchase_price * shares
        lrs_amount_inr = lrs_amount_usd * tt_rate
        
        # TCS calculation (20% if >₹7L in FY)
        tcs = Decimal("0")
        if lrs_amount_inr > self.TCS_THRESHOLD:
            # TCS on amount exceeding threshold
            taxable_lrs = lrs_amount_inr - self.TCS_THRESHOLD
            tcs = taxable_lrs * (self.TCS_RATE / 100)
        
        return ESPPPurchase(
            purchase_date=purchase_date,
            shares_purchased=shares,
            purchase_price_usd=purchase_price,
            market_price_usd=market_price,
            discount_percentage=(perquisite_per_share / market_price * 100),
            perquisite_per_share_usd=perquisite_per_share,
            total_perquisite_usd=total_perquisite_usd,
            tt_rate=tt_rate,
            perquisite_inr=perquisite_inr,
            lrs_amount_inr=lrs_amount_inr,
            tcs_collected=tcs
        )
```

---

# Sprint 12: DTAA Credit

## Requirements (REQ-DTAA-001 to REQ-DTAA-004)

- Parse US withholding from Form 1042-S
- Calculate DTAA credit (lower of US tax paid or India tax)
- Generate Form 67 data
- Track foreign tax credit ledger

---

## Implementation

```python
"""DTAA (Double Taxation Avoidance Agreement) credit calculation."""

from dataclasses import dataclass
from decimal import Decimal
from datetime import date
from typing import List

@dataclass
class USWithholding:
    income_code: str  # 06 = Dividend
    gross_income_usd: Decimal
    tax_rate: Decimal  # 25% typically
    tax_withheld_usd: Decimal
    tax_withheld_inr: Decimal

@dataclass
class DTAACredit:
    country: str  # "USA"
    income_type: str  # "DIVIDEND", "CAPITAL_GAIN"
    foreign_income_inr: Decimal
    foreign_tax_paid_inr: Decimal
    india_tax_on_income: Decimal
    dtaa_credit_allowed: Decimal  # Lower of foreign tax or India tax
    relief_type: str  # "DTAA" or "UNILATERAL"

class DTAACreditCalculator:
    """Calculate DTAA tax credit."""
    
    # US-India DTAA rates
    US_DIVIDEND_RATE = Decimal("25")  # 25% withholding
    
    def __init__(self, db_connection, rate_provider):
        self.conn = db_connection
        self.rate_provider = rate_provider
    
    def parse_form_1042s(self, text: str) -> List[USWithholding]:
        """Parse Form 1042-S data."""
        # Based on project file TaxDocuments_6492_031225_FY2425.pdf
        # Income Code 06 = Dividend, 25% rate
        
        withholdings = []
        # Parse logic...
        return withholdings
    
    def calculate_credit(self, withholding: USWithholding,
                         india_tax_rate: Decimal) -> DTAACredit:
        """
        Calculate DTAA credit for foreign tax paid.
        
        Credit = Lower of:
        1. Actual foreign tax paid
        2. India tax on same income
        """
        # India tax on the foreign income
        india_tax = withholding.gross_income_usd * (india_tax_rate / 100)
        
        # DTAA credit = lower of foreign tax or India tax
        dtaa_credit = min(withholding.tax_withheld_usd, india_tax)
        
        return DTAACredit(
            country="USA",
            income_type="DIVIDEND" if withholding.income_code == "06" else "OTHER",
            foreign_income_inr=withholding.gross_income_usd,  # Will convert
            foreign_tax_paid_inr=withholding.tax_withheld_inr,
            india_tax_on_income=india_tax,
            dtaa_credit_allowed=dtaa_credit,
            relief_type="DTAA"
        )
    
    def generate_form_67_data(self, credits: List[DTAACredit]) -> dict:
        """Generate data for Form 67 (Foreign Tax Credit claim)."""
        return {
            'country_code': 'US',
            'treaty_relief_claimed': True,
            'total_foreign_income': sum(c.foreign_income_inr for c in credits),
            'total_foreign_tax': sum(c.foreign_tax_paid_inr for c in credits),
            'total_credit_claimed': sum(c.dtaa_credit_allowed for c in credits),
            'article_of_treaty': 'Article 25 - Relief from Double Taxation'
        }
```

---

# Sprint 13: Schedule FA & Unlisted Shares

## Requirements

### Schedule FA (REQ-FA-001 to REQ-FA-004)
- Report foreign bank accounts (peak balance)
- Report foreign equity holdings
- Report foreign income
- Generate Schedule FA JSON

### Unlisted Shares (REQ-UNL-001 to REQ-UNL-004)
- Track unlisted share holdings
- Calculate FMV per CBDT rules
- LTCG after 24 months at 12.5%
- Generate Schedule UA JSON

---

## Implementation

```python
"""Schedule FA (Foreign Assets) generation."""

from dataclasses import dataclass
from decimal import Decimal
from datetime import date
from typing import List

@dataclass
class ForeignBankAccount:
    country: str
    bank_name: str
    account_number: str
    peak_balance_usd: Decimal
    peak_balance_inr: Decimal
    interest_income_usd: Decimal
    interest_income_inr: Decimal

@dataclass
class ForeignEquityHolding:
    country: str
    entity_name: str  # e.g., "Qualcomm Inc"
    nature: str  # "Equity Shares"
    date_of_acquisition: date
    initial_value_usd: Decimal
    peak_value_usd: Decimal
    peak_value_inr: Decimal
    closing_value_usd: Decimal
    closing_value_inr: Decimal
    income_during_year_inr: Decimal  # Dividends, etc.

class ScheduleFAGenerator:
    """Generate Schedule FA for ITR."""
    
    def __init__(self, db_connection, rate_provider):
        self.conn = db_connection
        self.rate_provider = rate_provider
    
    def generate(self, user_id: int, fy: str) -> dict:
        """Generate Schedule FA data structure."""
        # Get FY end date for closing balances
        start_year = int(fy.split('-')[0])
        fy_end = date(start_year + 1, 3, 31)
        tt_rate = self.rate_provider.get_rate(fy_end, "USD")
        
        return {
            'ForeignBankAccounts': self._get_bank_accounts(user_id, fy, tt_rate),
            'ForeignEquity': self._get_equity_holdings(user_id, fy, tt_rate),
            'ForeignIncome': self._get_foreign_income(user_id, fy),
        }
    
    def to_itr_json(self, schedule_fa: dict) -> dict:
        """Convert to ITR JSON format."""
        # Match ITR-2 JSON schema structure
        return {
            'ScheduleFA': {
                'DetailsForiegnBank': [
                    {
                        'CountryName': acc['country'],
                        'NameOfInstitution': acc['bank_name'],
                        'AccountNumber': acc['account_number'],
                        'PeakBalanceDuringPeriod': int(acc['peak_balance_inr']),
                        'ClosingBalance': int(acc['closing_balance_inr']),
                        'InterestAccrued': int(acc['interest_income_inr'])
                    }
                    for acc in schedule_fa['ForeignBankAccounts']
                ],
                'DtlsForeignEquityDebtInt': [
                    {
                        'CountryName': eq['country'],
                        'NameOfEntity': eq['entity_name'],
                        'NatureOfEntity': eq['nature'],
                        'DateOfAcquisition': eq['date_of_acquisition'].strftime('%Y-%m-%d'),
                        'InitialValue': int(eq['initial_value_inr']),
                        'PeakValue': int(eq['peak_value_inr']),
                        'ClosingValue': int(eq['closing_value_inr']),
                        'TotalGrossAmountPaid': int(eq['income_during_year_inr'])
                    }
                    for eq in schedule_fa['ForeignEquity']
                ]
            }
        }
```

---

# Sprint 14: ITR-2 JSON Export

## Requirements (REQ-RPT-007)

- Generate complete ITR-2 JSON
- Validate against CBDT schema
- Include all schedules

---

## Implementation

```python
"""ITR-2 JSON export for e-filing."""

import json
import jsonschema
from pathlib import Path
from decimal import Decimal
from datetime import date

class ITR2Exporter:
    """Generate ITR-2 JSON for e-filing."""
    
    SCHEMA_VERSION = "V1.2"
    ASSESSMENT_YEAR = "2025-26"
    
    def __init__(self, db_connection):
        self.conn = db_connection
        self.schema = self._load_schema()
    
    def _load_schema(self) -> dict:
        """Load CBDT ITR-2 JSON schema."""
        schema_path = Path("schemas/ITR-2_2025_Main_V1.2.json")
        with open(schema_path) as f:
            return json.load(f)
    
    def generate(self, user_id: int, fy: str) -> dict:
        """Generate complete ITR-2 JSON."""
        # Collect all data
        personal = self._get_personal_info(user_id)
        salary = self._get_salary_schedule(user_id, fy)
        hp = self._get_house_property(user_id, fy)
        cg = self._get_capital_gains(user_id, fy)
        other = self._get_other_income(user_id, fy)
        deductions = self._get_deductions(user_id, fy)
        tds = self._get_tds_schedule(user_id, fy)
        fa = self._get_schedule_fa(user_id, fy)
        
        itr = {
            'ITR': {
                'ITR2': {
                    'PersonalInfo': personal,
                    'FilingStatus': {
                        'ReturnFileSec': 11,  # 139(1) - on or before due date
                        'OptOutNewTaxRegime': 'N',  # Default new regime
                    },
                    'ScheduleS': salary,
                    'ScheduleHP': hp,
                    'ScheduleCGFor23': cg,
                    'ScheduleOS': other,
                    'ScheduleVIA': deductions,
                    'ScheduleTDS1': tds['tds_salary'],
                    'ScheduleTDS2': tds['tds_other'],
                    'ScheduleTDS3': tds['tds_sale'],
                    'ScheduleTCS': tds['tcs'],
                    'ScheduleIT': tds['advance_tax'],
                    'ScheduleFA': fa,
                    'PartB-TI': self._calculate_total_income(salary, hp, cg, other, deductions),
                    'PartB-TTI': self._calculate_tax(...)
                }
            },
            'CreationInfo': {
                'SWVersionNo': 'PFAS_v6.0',
                'SWCreatedBy': 'PFAS',
                'IntermediaryCity': 'Hyderabad'
            }
        }
        
        return itr
    
    def validate(self, itr_json: dict) -> bool:
        """Validate against CBDT schema."""
        try:
            jsonschema.validate(itr_json, self.schema)
            return True
        except jsonschema.ValidationError as e:
            print(f"Validation error: {e.message}")
            return False
    
    def export(self, itr_json: dict, output_path: Path):
        """Export to JSON file."""
        with open(output_path, 'w') as f:
            json.dump(itr_json, f, indent=2, default=str)
```

---

# Sprint 15: Documentation & Final Testing

## Requirements

- Complete user documentation
- API documentation
- Full regression testing
- UAT with actual FY24-25 data

---

## Verification Commands

```bash
# Phase 2 tests
pytest tests/unit/test_parsers/test_rsu/ -v
pytest tests/unit/test_parsers/test_espp/ -v
pytest tests/unit/test_services/test_dtaa/ -v
pytest tests/unit/test_services/test_schedule_fa/ -v
pytest tests/unit/test_services/test_itr_export/ -v

# Full regression
pytest tests/ -v --cov=src/pfas --cov-report=html

# ITR JSON validation
python -m pfas.services.itr_export --validate --fy 2024-25
```

---

## Success Criteria

### Sprint 9-10: RSU
- [ ] SBI TT rate lookup working
- [ ] Morgan Stanley statements parsed
- [ ] RSU perquisite calculated in INR
- [ ] RSU-payslip correlation working
- [ ] RSU sale LTCG calculated (>24 months)

### Sprint 11: ESPP
- [ ] ESPP purchase parsed
- [ ] Discount perquisite calculated
- [ ] TCS on LRS tracked (206CQ)

### Sprint 12: DTAA
- [ ] Form 1042-S parsed
- [ ] DTAA credit calculated
- [ ] Form 67 data generated

### Sprint 13: Schedule FA/Unlisted
- [ ] Foreign bank accounts reported
- [ ] Foreign equity reported
- [ ] Unlisted shares tracked
- [ ] FMV calculated per CBDT rules

### Sprint 14: ITR-2
- [ ] Complete ITR-2 JSON generated
- [ ] Validates against CBDT schema
- [ ] All schedules populated correctly

### Sprint 15: Final
- [ ] All tests passing
- [ ] Coverage >80%
- [ ] Documentation complete
- [ ] UAT passed with real data
