"""
Golden Reference Data Models.

Provides dataclasses for golden reference reconciliation engine:
- MetricType, AssetClass, SourceType enums
- GoldenHolding, ReconciliationEvent, SuspenseItem dataclasses
- Result types for reconciliation operations
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, List, Dict, Any


class MetricType(Enum):
    """Types of metrics that can be reconciled."""

    NET_WORTH = "NET_WORTH"
    CAPITAL_GAINS = "CAPITAL_GAINS"
    UNITS = "UNITS"
    COST_BASIS = "COST_BASIS"
    DIVIDENDS = "DIVIDENDS"
    TRANSACTIONS = "TRANSACTIONS"


class AssetClass(Enum):
    """Asset classes supported by golden reference."""

    MUTUAL_FUND = "MUTUAL_FUND"
    STOCKS = "STOCKS"
    NPS = "NPS"
    EPF = "EPF"
    PPF = "PPF"
    SGB = "SGB"
    US_STOCKS = "US_STOCKS"
    BONDS = "BONDS"


class SourceType(Enum):
    """Source types for golden reference data."""

    NSDL_CAS = "NSDL_CAS"
    CDSL_CAS = "CDSL_CAS"
    RTA_CAS = "RTA_CAS"  # CAMS, KFintech
    BROKER = "BROKER"
    BROKER_STATEMENT = "BROKER_STATEMENT"
    DEPOSITORY = "DEPOSITORY"
    BANK_STATEMENT = "BANK_STATEMENT"
    EPFO_PASSBOOK = "EPFO_PASSBOOK"
    NPS_STATEMENT = "NPS_STATEMENT"
    SYSTEM = "SYSTEM"


class MatchResult(Enum):
    """Result of a reconciliation comparison."""

    EXACT = "EXACT"
    WITHIN_TOLERANCE = "WITHIN_TOLERANCE"
    MISMATCH = "MISMATCH"
    MISSING_SYSTEM = "MISSING_SYSTEM"
    MISSING_GOLDEN = "MISSING_GOLDEN"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class ReconciliationStatus(Enum):
    """Status of a reconciliation event."""

    PENDING = "PENDING"
    MATCHED = "MATCHED"
    MISMATCH = "MISMATCH"
    RESOLVED = "RESOLVED"
    SUSPENSE = "SUSPENSE"


class Severity(Enum):
    """Severity level for reconciliation issues."""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class SuspenseStatus(Enum):
    """Status of a suspense item."""

    OPEN = "OPEN"
    IN_PROGRESS = "IN_PROGRESS"
    RESOLVED = "RESOLVED"
    WRITTEN_OFF = "WRITTEN_OFF"


@dataclass
class TruthSourceConfig:
    """Configuration for a truth source."""

    metric_type: MetricType
    asset_class: AssetClass
    source_priority: List[SourceType]
    description: str = ""
    user_id: Optional[int] = None
    is_default: bool = True

    @classmethod
    def from_db_row(cls, row) -> "TruthSourceConfig":
        """Create from database row."""
        import json
        sources = json.loads(row["source_priority"]) if isinstance(row["source_priority"], str) else row["source_priority"]
        return cls(
            metric_type=MetricType(row["metric_type"]),
            asset_class=AssetClass(row["asset_class"]),
            source_priority=[SourceType(s) for s in sources],
            description=row.get("description", ""),
            user_id=row.get("user_id"),
            is_default=bool(row.get("is_default", True)),
        )


@dataclass
class GoldenReference:
    """Represents an external golden reference statement."""

    id: Optional[int] = None
    user_id: int = 0
    source_type: SourceType = SourceType.SYSTEM
    statement_date: date = field(default_factory=date.today)
    period_start: Optional[date] = None
    period_end: Optional[date] = None
    file_path: Optional[str] = None
    file_hash: Optional[str] = None
    raw_data: Optional[Dict[str, Any]] = None
    investor_name: Optional[str] = None
    investor_pan: Optional[str] = None
    status: str = "ACTIVE"
    ingested_at: Optional[datetime] = None
    validated_at: Optional[datetime] = None
    notes: Optional[str] = None


@dataclass
class GoldenHolding:
    """Represents a holding parsed from a golden reference source."""

    id: Optional[int] = None
    golden_ref_id: int = 0
    user_id: int = 0
    asset_type: AssetClass = AssetClass.MUTUAL_FUND
    isin: Optional[str] = None
    symbol: Optional[str] = None
    name: str = ""
    folio_number: Optional[str] = None
    account_number: Optional[str] = None
    units: Decimal = Decimal("0")
    nav: Optional[Decimal] = None
    market_value: Decimal = Decimal("0")
    cost_basis: Optional[Decimal] = None
    unrealized_gain: Optional[Decimal] = None
    currency: str = "INR"
    exchange_rate: Decimal = Decimal("1")
    value_inr: Optional[Decimal] = None
    as_of_date: date = field(default_factory=date.today)
    financial_year: Optional[str] = None

    def __post_init__(self):
        """Ensure decimal types and calculate INR value."""
        if not isinstance(self.units, Decimal):
            self.units = Decimal(str(self.units)) if self.units else Decimal("0")
        if not isinstance(self.market_value, Decimal):
            self.market_value = Decimal(str(self.market_value)) if self.market_value else Decimal("0")
        if not isinstance(self.exchange_rate, Decimal):
            self.exchange_rate = Decimal(str(self.exchange_rate)) if self.exchange_rate else Decimal("1")

        # Calculate INR value if not set
        if self.value_inr is None:
            self.value_inr = self.market_value * self.exchange_rate

    @property
    def reconciliation_key(self) -> str:
        """Generate a unique key for reconciliation matching."""
        if self.isin:
            return f"ISIN:{self.isin}"
        if self.folio_number:
            return f"FOLIO:{self.folio_number}"
        if self.symbol:
            return f"SYMBOL:{self.symbol}"
        return f"NAME:{self.name}"


@dataclass
class SystemHolding:
    """Represents a holding from the system (internal database)."""

    asset_type: AssetClass
    isin: Optional[str] = None
    symbol: Optional[str] = None
    name: str = ""
    folio_number: Optional[str] = None
    units: Decimal = Decimal("0")
    nav: Optional[Decimal] = None
    market_value: Decimal = Decimal("0")
    cost_basis: Decimal = Decimal("0")
    unrealized_gain: Optional[Decimal] = None
    currency: str = "INR"
    as_of_date: date = field(default_factory=date.today)

    @property
    def reconciliation_key(self) -> str:
        """Generate a unique key for reconciliation matching."""
        if self.isin:
            return f"ISIN:{self.isin}"
        if self.folio_number:
            return f"FOLIO:{self.folio_number}"
        if self.symbol:
            return f"SYMBOL:{self.symbol}"
        return f"NAME:{self.name}"


@dataclass
class ReconciliationEvent:
    """Represents a reconciliation comparison event."""

    id: Optional[int] = None
    user_id: int = 0
    reconciliation_date: date = field(default_factory=date.today)
    metric_type: MetricType = MetricType.NET_WORTH
    asset_class: AssetClass = AssetClass.MUTUAL_FUND
    source_type: SourceType = SourceType.SYSTEM
    golden_ref_id: Optional[int] = None

    # Matching keys
    isin: Optional[str] = None
    folio_number: Optional[str] = None
    symbol: Optional[str] = None

    # Comparison values
    system_value: Optional[Decimal] = None
    golden_value: Optional[Decimal] = None
    difference: Optional[Decimal] = None
    difference_pct: Optional[Decimal] = None
    tolerance_used: Decimal = Decimal("0.01")

    # Result
    status: ReconciliationStatus = ReconciliationStatus.PENDING
    match_result: MatchResult = MatchResult.NOT_APPLICABLE
    severity: Severity = Severity.INFO

    # Resolution
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[str] = None
    resolution_action: Optional[str] = None
    resolution_notes: Optional[str] = None

    def calculate_difference(self) -> None:
        """Calculate difference and percentage."""
        if self.system_value is not None and self.golden_value is not None:
            self.difference = self.system_value - self.golden_value
            if self.golden_value != 0:
                self.difference_pct = (self.difference / self.golden_value) * 100
            else:
                self.difference_pct = Decimal("100") if self.system_value != 0 else Decimal("0")


@dataclass
class SuspenseItem:
    """Represents an item in the reconciliation suspense account."""

    id: Optional[int] = None
    user_id: int = 0
    event_id: int = 0
    asset_type: AssetClass = AssetClass.MUTUAL_FUND
    isin: Optional[str] = None
    symbol: Optional[str] = None
    name: Optional[str] = None
    folio_number: Optional[str] = None
    suspense_units: Optional[Decimal] = None
    suspense_value: Optional[Decimal] = None
    suspense_currency: str = "INR"
    suspense_reason: Optional[str] = None
    opened_date: date = field(default_factory=date.today)
    target_resolution_date: Optional[date] = None
    actual_resolution_date: Optional[date] = None
    status: SuspenseStatus = SuspenseStatus.OPEN
    priority: str = "NORMAL"
    assigned_to: Optional[str] = None


@dataclass
class ReconciliationSummary:
    """Summary of a reconciliation run."""

    user_id: int
    reconciliation_date: date
    asset_class: AssetClass
    source_type: SourceType
    golden_ref_id: int

    # Counts
    total_items: int = 0
    matched_exact: int = 0
    matched_tolerance: int = 0
    mismatches: int = 0
    missing_system: int = 0
    missing_golden: int = 0

    # Aggregates
    total_system_value: Decimal = Decimal("0")
    total_golden_value: Decimal = Decimal("0")
    total_difference: Decimal = Decimal("0")

    # Details
    events: List[ReconciliationEvent] = field(default_factory=list)

    @property
    def match_rate(self) -> float:
        """Calculate match rate percentage."""
        if self.total_items == 0:
            return 100.0
        matched = self.matched_exact + self.matched_tolerance
        return (matched / self.total_items) * 100


@dataclass
class GoldenCapitalGains:
    """Capital gains from a golden reference source."""

    id: Optional[int] = None
    golden_ref_id: int = 0
    user_id: int = 0
    financial_year: str = ""
    asset_type: AssetClass = AssetClass.MUTUAL_FUND

    # Identification
    isin: Optional[str] = None
    symbol: Optional[str] = None
    name: Optional[str] = None
    folio_number: Optional[str] = None

    # Capital gains breakdown
    stcg_equity: Decimal = Decimal("0")
    stcg_other: Decimal = Decimal("0")
    ltcg_equity: Decimal = Decimal("0")
    ltcg_other: Decimal = Decimal("0")
    total_gain: Optional[Decimal] = None

    # Currency
    currency: str = "INR"
    exchange_rate: Decimal = Decimal("1")
    gain_inr: Optional[Decimal] = None

    def __post_init__(self):
        """Calculate totals."""
        if self.total_gain is None:
            self.total_gain = (
                self.stcg_equity + self.stcg_other +
                self.ltcg_equity + self.ltcg_other
            )
        if self.gain_inr is None and self.total_gain is not None:
            self.gain_inr = self.total_gain * self.exchange_rate
