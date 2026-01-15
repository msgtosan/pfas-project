"""
Data models for Asset Parsers.

Supports Rental Income, SGB, REIT, and Dividends.
New Tax Regime only.
"""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional, List
from enum import Enum


# ============================================================================
# Rental Income Models
# ============================================================================

class PropertyType(Enum):
    """Property type classification."""
    SELF_OCCUPIED = "SELF_OCCUPIED"
    LET_OUT = "LET_OUT"
    DEEMED_LET_OUT = "DEEMED_LET_OUT"


@dataclass
class Property:
    """Property master record."""
    id: Optional[int] = None
    user_id: Optional[int] = None
    property_type: PropertyType = PropertyType.LET_OUT
    address: str = ""
    city: str = ""
    pin_code: str = ""
    tenant_name: Optional[str] = None
    acquisition_date: Optional[date] = None
    acquisition_cost: Decimal = Decimal("0")
    account_id: Optional[int] = None

    def __post_init__(self):
        if isinstance(self.property_type, str):
            self.property_type = PropertyType(self.property_type)
        if not isinstance(self.acquisition_cost, Decimal):
            self.acquisition_cost = Decimal(str(self.acquisition_cost))


@dataclass
class RentalIncome:
    """Monthly or annual rental income record."""
    id: Optional[int] = None
    property_id: Optional[int] = None
    financial_year: str = ""
    month: Optional[str] = None  # e.g., "Apr-2024" or None for annual
    gross_rent: Decimal = Decimal("0")
    municipal_tax_paid: Decimal = Decimal("0")
    source: str = "BANK_STATEMENT"  # BANK_STATEMENT, MANUAL

    def __post_init__(self):
        if not isinstance(self.gross_rent, Decimal):
            self.gross_rent = Decimal(str(self.gross_rent))
        if not isinstance(self.municipal_tax_paid, Decimal):
            self.municipal_tax_paid = Decimal(str(self.municipal_tax_paid))


@dataclass
class RentalIncomeCalculation:
    """Calculated rental income with deductions."""
    gross_rent: Decimal
    municipal_tax: Decimal
    net_annual_value: Decimal
    standard_deduction: Decimal  # 30% of NAV
    home_loan_interest: Decimal
    income_from_hp: Decimal  # Final (can be negative for loss)

    @property
    def is_loss(self) -> bool:
        """Check if this results in a loss from house property."""
        return self.income_from_hp < 0


# ============================================================================
# SGB (Sovereign Gold Bonds) Models
# ============================================================================

@dataclass
class SGBHolding:
    """SGB holding record."""
    id: Optional[int] = None
    user_id: Optional[int] = None
    series: str = ""  # e.g., "2.50% Sov. Gold Bond 8 Sep 28"
    isin: Optional[str] = None
    issue_date: Optional[date] = None
    maturity_date: Optional[date] = None
    quantity: int = 0  # in grams
    issue_price: Decimal = Decimal("0")  # per gram
    current_price: Optional[Decimal] = None
    interest_rate: Decimal = Decimal("2.5")  # 2.5% p.a.
    interest_earned: Decimal = Decimal("0")
    accrued_interest: Decimal = Decimal("0")
    unrealized_gain: Decimal = Decimal("0")

    def __post_init__(self):
        for attr in ['issue_price', 'current_price', 'interest_rate',
                     'interest_earned', 'accrued_interest', 'unrealized_gain']:
            val = getattr(self, attr)
            if val is not None and not isinstance(val, Decimal):
                setattr(self, attr, Decimal(str(val)))

    @property
    def cost_value(self) -> Decimal:
        """Total cost of holding."""
        return self.issue_price * self.quantity

    @property
    def market_value(self) -> Optional[Decimal]:
        """Current market value if price available."""
        if self.current_price:
            return self.current_price * self.quantity
        return None


@dataclass
class SGBInterest:
    """SGB interest payment record (semi-annual)."""
    id: Optional[int] = None
    sgb_holding_id: Optional[int] = None
    series: str = ""
    payment_date: date = None
    financial_year: str = ""
    quantity: int = 0
    rate: Decimal = Decimal("2.5")
    amount: Decimal = Decimal("0")
    tds_deducted: Decimal = Decimal("0")
    source: str = "BANK_STATEMENT"

    def __post_init__(self):
        for attr in ['rate', 'amount', 'tds_deducted']:
            val = getattr(self, attr)
            if val is not None and not isinstance(val, Decimal):
                setattr(self, attr, Decimal(str(val)))


@dataclass
class SGBSummary:
    """SGB summary for a financial year."""
    financial_year: str
    total_holdings: int = 0
    total_quantity: int = 0
    total_cost: Decimal = Decimal("0")
    total_market_value: Decimal = Decimal("0")
    total_interest_earned: Decimal = Decimal("0")
    total_unrealized_gain: Decimal = Decimal("0")
    holdings: List[SGBHolding] = field(default_factory=list)


# ============================================================================
# REIT/InvIT Models
# ============================================================================

class DistributionType(Enum):
    """REIT/InvIT distribution type."""
    DIVIDEND = "DIVIDEND"      # Exempt from tax
    INTEREST = "INTEREST"      # Taxable at slab rate
    OTHER = "OTHER"            # Capital reduction - reduces cost basis
    CAPITAL_GAIN = "CAPITAL_GAIN"


@dataclass
class REITHolding:
    """REIT/InvIT holding record."""
    id: Optional[int] = None
    user_id: Optional[int] = None
    symbol: str = ""
    name: str = ""
    isin: Optional[str] = None
    units: Decimal = Decimal("0")
    purchase_date: Optional[date] = None
    purchase_price: Decimal = Decimal("0")  # per unit
    current_price: Optional[Decimal] = None
    cost_basis: Decimal = Decimal("0")  # Adjusted for capital reductions

    def __post_init__(self):
        for attr in ['units', 'purchase_price', 'current_price', 'cost_basis']:
            val = getattr(self, attr)
            if val is not None and not isinstance(val, Decimal):
                setattr(self, attr, Decimal(str(val)))

    @property
    def market_value(self) -> Optional[Decimal]:
        """Current market value if price available."""
        if self.current_price:
            return self.current_price * self.units
        return None


@dataclass
class REITDistribution:
    """REIT/InvIT distribution record."""
    id: Optional[int] = None
    reit_holding_id: Optional[int] = None
    symbol: str = ""
    record_date: date = None
    payment_date: Optional[date] = None
    financial_year: str = ""
    distribution_type: DistributionType = DistributionType.DIVIDEND
    gross_amount: Decimal = Decimal("0")
    tds_deducted: Decimal = Decimal("0")
    net_amount: Decimal = Decimal("0")
    source: str = "BANK_STATEMENT"

    def __post_init__(self):
        if isinstance(self.distribution_type, str):
            self.distribution_type = DistributionType(self.distribution_type)
        for attr in ['gross_amount', 'tds_deducted', 'net_amount']:
            val = getattr(self, attr)
            if val is not None and not isinstance(val, Decimal):
                setattr(self, attr, Decimal(str(val)))


# ============================================================================
# Dividend Models
# ============================================================================

@dataclass
class DividendRecord:
    """Individual dividend payment record."""
    id: Optional[int] = None
    user_id: Optional[int] = None
    symbol: str = ""
    company_name: Optional[str] = None
    isin: Optional[str] = None
    record_date: Optional[date] = None
    payment_date: date = None
    financial_year: str = ""
    dividend_type: str = "INTERIM"  # INTERIM, FINAL, SPECIAL
    gross_amount: Decimal = Decimal("0")
    tds_deducted: Decimal = Decimal("0")  # TDS u/s 194
    net_amount: Decimal = Decimal("0")
    source: str = "BANK_STATEMENT"

    def __post_init__(self):
        for attr in ['gross_amount', 'tds_deducted', 'net_amount']:
            val = getattr(self, attr)
            if val is not None and not isinstance(val, Decimal):
                setattr(self, attr, Decimal(str(val)))


@dataclass
class DividendSummary:
    """Dividend summary for a financial year."""
    financial_year: str
    total_dividend_income: Decimal = Decimal("0")
    total_tds_deducted: Decimal = Decimal("0")
    dividend_count: int = 0
    dividends: List[DividendRecord] = field(default_factory=list)

    @property
    def net_dividend_income(self) -> Decimal:
        """Net dividend after TDS."""
        return self.total_dividend_income - self.total_tds_deducted


# ============================================================================
# Tax Computation Models (New Regime Only)
# ============================================================================

@dataclass
class AssetIncomeSummary:
    """Summary of all asset income for tax computation (New Regime)."""
    financial_year: str

    # Income from House Property
    rental_gross: Decimal = Decimal("0")
    rental_municipal_tax: Decimal = Decimal("0")
    rental_standard_deduction: Decimal = Decimal("0")
    rental_net_income: Decimal = Decimal("0")  # Can be negative (loss)

    # SGB Interest (taxable at slab rate)
    sgb_interest: Decimal = Decimal("0")

    # REIT Distributions
    reit_dividend: Decimal = Decimal("0")  # Exempt
    reit_interest: Decimal = Decimal("0")  # Taxable at slab rate
    reit_other: Decimal = Decimal("0")  # Cost reduction (not income)

    # Dividends (taxable at slab rate from AY 2021-22)
    dividend_income: Decimal = Decimal("0")

    # TDS Credits
    tds_on_rent: Decimal = Decimal("0")
    tds_on_sgb: Decimal = Decimal("0")
    tds_on_reit: Decimal = Decimal("0")
    tds_on_dividend: Decimal = Decimal("0")

    @property
    def total_taxable_income(self) -> Decimal:
        """Total taxable income under 'Other Sources' head."""
        return (
            self.sgb_interest +
            self.reit_interest +
            self.dividend_income
        )

    @property
    def total_exempt_income(self) -> Decimal:
        """Total exempt income (REIT dividends only in new regime)."""
        return self.reit_dividend

    @property
    def total_tds_credit(self) -> Decimal:
        """Total TDS credit available."""
        return (
            self.tds_on_rent +
            self.tds_on_sgb +
            self.tds_on_reit +
            self.tds_on_dividend
        )

    @property
    def house_property_loss(self) -> Decimal:
        """Loss from house property (if any)."""
        if self.rental_net_income < 0:
            return abs(self.rental_net_income)
        return Decimal("0")
