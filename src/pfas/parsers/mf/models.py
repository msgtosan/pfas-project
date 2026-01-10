"""Mutual Fund data models."""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional
from enum import Enum


class AssetClass(Enum):
    """Mutual fund asset class for tax treatment."""
    EQUITY = "EQUITY"
    DEBT = "DEBT"
    HYBRID = "HYBRID"
    OTHER = "OTHER"


class TransactionType(Enum):
    """Mutual fund transaction types."""
    PURCHASE = "PURCHASE"
    REDEMPTION = "REDEMPTION"
    SWITCH_IN = "SWITCH_IN"
    SWITCH_OUT = "SWITCH_OUT"
    DIVIDEND = "DIVIDEND"
    DIVIDEND_REINVEST = "DIVIDEND_REINVEST"


@dataclass
class MFScheme:
    """
    Represents a mutual fund scheme.

    Attributes:
        name: Full scheme name
        amc_name: Asset Management Company name
        isin: ISIN code (e.g., INF178L01BY0)
        asset_class: EQUITY, DEBT, HYBRID, or OTHER
        scheme_type: GROWTH, DIVIDEND, IDCW
        nav_31jan2018: NAV on 31-Jan-2018 for grandfathering
    """
    name: str
    amc_name: str
    isin: Optional[str] = None
    asset_class: AssetClass = AssetClass.OTHER
    scheme_type: Optional[str] = None
    nav_31jan2018: Optional[Decimal] = None

    def __post_init__(self):
        """Auto-classify based on scheme name if not set."""
        if self.asset_class == AssetClass.OTHER:
            from .classifier import classify_scheme
            self.asset_class = classify_scheme(self.name)


@dataclass
class MFTransaction:
    """
    Represents a mutual fund transaction.

    Attributes:
        folio_number: Folio number
        scheme: MF scheme details
        transaction_type: Purchase, redemption, etc.
        date: Transaction date
        units: Number of units
        nav: Net Asset Value per unit
        amount: Transaction amount
        stt: Securities Transaction Tax
        purchase_date: Purchase date (for redemptions)
        purchase_units: Units purchased (for redemptions)
        purchase_nav: Purchase NAV (for redemptions)
        grandfathered_units: Units held on 31-Jan-2018
        grandfathered_nav: NAV on 31-Jan-2018
        grandfathered_value: Market value on 31-Jan-2018
        short_term_gain: Short-term capital gain
        long_term_gain: Long-term capital gain
    """
    folio_number: str
    scheme: MFScheme
    transaction_type: TransactionType
    date: date
    units: Decimal
    nav: Decimal
    amount: Decimal
    stt: Decimal = Decimal("0")
    stamp_duty: Decimal = Decimal("0")

    # Purchase details (for redemptions)
    purchase_date: Optional[date] = None
    purchase_units: Optional[Decimal] = None
    purchase_nav: Optional[Decimal] = None
    purchase_amount: Optional[Decimal] = None

    # Grandfathering (pre-31-Jan-2018)
    grandfathered_units: Optional[Decimal] = None
    grandfathered_nav: Optional[Decimal] = None
    grandfathered_value: Optional[Decimal] = None

    # Computed capital gains
    short_term_gain: Decimal = Decimal("0")
    long_term_gain: Decimal = Decimal("0")

    @property
    def holding_period_days(self) -> Optional[int]:
        """
        Calculate holding period in days.

        Returns:
            Number of days between purchase and redemption, or None if not a redemption
        """
        if self.purchase_date and self.transaction_type == TransactionType.REDEMPTION:
            return (self.date - self.purchase_date).days
        return None

    @property
    def is_long_term(self) -> bool:
        """
        Check if qualifies for Long-Term Capital Gains.

        Threshold:
        - Equity: >12 months (365 days)
        - Debt: >24 months (730 days) - old rule before April 2023
                Now taxed at slab rate regardless

        Returns:
            True if long-term holding, False otherwise
        """
        if self.holding_period_days is None:
            return False

        # Equity: >12 months for LTCG
        if self.scheme.asset_class == AssetClass.EQUITY:
            return self.holding_period_days > 365

        # Debt: >24 months (old rule, now at slab rate)
        # For debt, we still track LTCG vs STCG but both taxed at slab
        return self.holding_period_days > 730

    @property
    def is_grandfathered(self) -> bool:
        """
        Check if transaction qualifies for grandfathering.

        Returns:
            True if purchased before 31-Jan-2018 and has grandfathered value
        """
        if not self.purchase_date:
            return False

        GRANDFATHERING_DATE = date(2018, 1, 31)
        return self.purchase_date <= GRANDFATHERING_DATE and self.grandfathered_value is not None


@dataclass
class ParseResult:
    """Result of parsing a CAMS CAS file."""
    success: bool
    transactions: list[MFTransaction] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    source_file: str = ""

    def add_error(self, error: str):
        """Add an error message."""
        self.errors.append(error)
        self.success = False

    def add_warning(self, warning: str):
        """Add a warning message."""
        self.warnings.append(warning)
