"""Mutual Fund data models."""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional, List, Dict, Any
from enum import Enum


class CASFileType(Enum):
    """CAS file format type."""
    UNKNOWN = "UNKNOWN"
    SUMMARY = "SUMMARY"
    DETAILED = "DETAILED"


class CASSource(Enum):
    """Source/registrar of CAS file."""
    UNKNOWN = "UNKNOWN"
    CAMS = "CAMS"
    KFINTECH = "KFINTECH"
    NSDL = "NSDL"
    CDSL = "CDSL"


class AssetClass(Enum):
    """Mutual fund asset class for tax treatment."""
    EQUITY = "EQUITY"
    DEBT = "DEBT"
    HYBRID = "HYBRID"
    OTHER = "OTHER"


class TransactionType(Enum):
    """
    Mutual fund transaction types.

    Expanded to cover all CAS statement transaction types including
    tax deductions, scheme mergers, segregation, and reversals.
    """
    # Purchase types
    PURCHASE = "PURCHASE"
    PURCHASE_SIP = "PURCHASE_SIP"

    # Redemption
    REDEMPTION = "REDEMPTION"

    # Switch transactions
    SWITCH_IN = "SWITCH_IN"
    SWITCH_OUT = "SWITCH_OUT"
    SWITCH_IN_MERGER = "SWITCH_IN_MERGER"
    SWITCH_OUT_MERGER = "SWITCH_OUT_MERGER"

    # Dividend transactions
    DIVIDEND = "DIVIDEND"
    DIVIDEND_PAYOUT = "DIVIDEND_PAYOUT"
    DIVIDEND_REINVEST = "DIVIDEND_REINVEST"

    # Tax deductions
    STT_TAX = "STT_TAX"
    STAMP_DUTY_TAX = "STAMP_DUTY_TAX"
    TDS_TAX = "TDS_TAX"

    # Special transactions
    SEGREGATION = "SEGREGATION"
    REVERSAL = "REVERSAL"
    MISC = "MISC"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_description(cls, description: str, units: Decimal) -> "TransactionType":
        """
        Classify transaction type from description text and unit sign.

        Args:
            description: Transaction description from CAS
            units: Number of units (positive=buy, negative=sell, zero=tax/misc)

        Returns:
            Appropriate TransactionType enum value
        """
        desc_upper = description.upper()

        # Tax transactions (zero units)
        if units == Decimal("0") or "TAX" in desc_upper:
            if "STT" in desc_upper:
                return cls.STT_TAX
            if "STAMP" in desc_upper:
                return cls.STAMP_DUTY_TAX
            if "TDS" in desc_upper:
                return cls.TDS_TAX

        # Dividend transactions
        if "DIVIDEND" in desc_upper or "IDCW" in desc_upper or "PAYOUT" in desc_upper:
            if "REINVEST" in desc_upper or units > Decimal("0"):
                return cls.DIVIDEND_REINVEST
            return cls.DIVIDEND_PAYOUT

        # Switch transactions
        if "SWITCH" in desc_upper or "TRANSFER" in desc_upper:
            if "MERGER" in desc_upper or "CONSOLIDAT" in desc_upper:
                if units > Decimal("0"):
                    return cls.SWITCH_IN_MERGER
                return cls.SWITCH_OUT_MERGER
            if units > Decimal("0") or "IN" in desc_upper:
                return cls.SWITCH_IN
            return cls.SWITCH_OUT

        # Redemption
        if units < Decimal("0") or "REDEMP" in desc_upper or "SELL" in desc_upper:
            return cls.REDEMPTION

        # Purchase types
        if units > Decimal("0") or "PURCHASE" in desc_upper or "BUY" in desc_upper:
            if "SIP" in desc_upper or "SYSTEMATIC" in desc_upper:
                return cls.PURCHASE_SIP
            return cls.PURCHASE

        # Special cases
        if "SEGREG" in desc_upper:
            return cls.SEGREGATION
        if "REVERS" in desc_upper:
            return cls.REVERSAL

        return cls.UNKNOWN


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


# ============================================================================
# CAS-Specific Data Models
# ============================================================================

@dataclass
class InvestorInfo:
    """Investor information from CAS statement."""
    name: str
    email: str = ""
    address: str = ""
    mobile: str = ""
    pan: str = ""


@dataclass
class StatementPeriod:
    """CAS statement period."""
    from_date: date
    to_date: date

    def __str__(self) -> str:
        return f"{self.from_date.strftime('%d-%b-%Y')} to {self.to_date.strftime('%d-%b-%Y')}"


@dataclass
class CASTransaction:
    """
    Transaction record from CAS statement.

    This is a simpler transaction record used during parsing,
    before being converted to MFTransaction.
    """
    date: date
    description: str
    amount: Optional[Decimal] = None
    units: Optional[Decimal] = None
    nav: Optional[Decimal] = None
    balance: Optional[Decimal] = None
    transaction_type: TransactionType = TransactionType.UNKNOWN
    dividend_rate: Optional[Decimal] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "date": self.date.isoformat() if self.date else None,
            "description": self.description,
            "amount": str(self.amount) if self.amount else None,
            "units": str(self.units) if self.units else None,
            "nav": str(self.nav) if self.nav else None,
            "balance": str(self.balance) if self.balance else None,
            "type": self.transaction_type.value,
        }


@dataclass
class SchemeValuation:
    """Point-in-time scheme valuation."""
    date: date
    nav: Decimal
    cost: Optional[Decimal] = None
    value: Decimal = Decimal("0")


@dataclass
class CASScheme:
    """
    Scheme data from CAS statement.

    Contains scheme metadata, valuation, and transactions.
    """
    scheme: str
    rta_code: str = ""
    rta: str = ""
    isin: Optional[str] = None
    amfi: Optional[str] = None
    advisor: Optional[str] = None
    scheme_type: Optional[str] = None
    nominees: List[str] = field(default_factory=list)

    # Unit balances
    open: Decimal = Decimal("0")
    close: Decimal = Decimal("0")
    close_calculated: Decimal = Decimal("0")

    # Valuation
    valuation: Optional[SchemeValuation] = None

    # Transactions
    transactions: List[CASTransaction] = field(default_factory=list)

    @property
    def balance_mismatch(self) -> Decimal:
        """Check if calculated close matches stated close."""
        return abs(self.close - self.close_calculated)

    @property
    def has_mismatch(self) -> bool:
        """Return True if there's a balance mismatch > 0.001 units."""
        return self.balance_mismatch > Decimal("0.001")


@dataclass
class CASFolio:
    """
    Folio data from CAS statement.

    Contains folio metadata and list of schemes.
    """
    folio: str
    amc: str
    pan: Optional[str] = None
    kyc: Optional[str] = None
    pankyc: Optional[str] = None
    schemes: List[CASScheme] = field(default_factory=list)


@dataclass
class CASData:
    """
    Complete parsed CAS data.

    This is the primary output of the CAS PDF parser containing
    all folios, schemes, transactions, and investor info.
    """
    statement_period: StatementPeriod
    investor_info: InvestorInfo
    folios: List[CASFolio] = field(default_factory=list)
    cas_type: CASFileType = CASFileType.UNKNOWN
    cas_source: CASSource = CASSource.UNKNOWN

    @property
    def total_schemes(self) -> int:
        """Total number of schemes across all folios."""
        return sum(len(f.schemes) for f in self.folios)

    @property
    def total_transactions(self) -> int:
        """Total number of transactions."""
        return sum(
            len(s.transactions)
            for f in self.folios
            for s in f.schemes
        )

    @property
    def total_value(self) -> Decimal:
        """Total portfolio value."""
        total = Decimal("0")
        for folio in self.folios:
            for scheme in folio.schemes:
                if scheme.valuation:
                    total += scheme.valuation.value
        return total

    def get_schemes_with_mismatch(self) -> List[CASScheme]:
        """Get schemes with balance mismatches."""
        mismatches = []
        for folio in self.folios:
            for scheme in folio.schemes:
                if scheme.has_mismatch:
                    mismatches.append(scheme)
        return mismatches

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "statement_period": {
                "from": self.statement_period.from_date.isoformat(),
                "to": self.statement_period.to_date.isoformat(),
            },
            "investor_info": {
                "name": self.investor_info.name,
                "email": self.investor_info.email,
                "mobile": self.investor_info.mobile,
                "pan": self.investor_info.pan,
            },
            "cas_type": self.cas_type.value,
            "cas_source": self.cas_source.value,
            "folios": len(self.folios),
            "total_schemes": self.total_schemes,
            "total_transactions": self.total_transactions,
            "total_value": str(self.total_value),
        }
