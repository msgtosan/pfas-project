"""Stock trade data models."""

from dataclasses import dataclass, field
from datetime import date as date_type
from decimal import Decimal
from enum import Enum
from typing import Optional


class TradeType(Enum):
    """Type of stock trade."""
    BUY = "BUY"
    SELL = "SELL"


class TradeCategory(Enum):
    """Category of stock trade for tax purposes."""
    INTRADAY = "INTRADAY"  # Speculative income
    DELIVERY = "DELIVERY"  # STCG/LTCG
    FNO = "FNO"  # Futures & Options - Non-speculative business income


@dataclass
class StockDividend:
    """
    Stock dividend record.

    Represents dividend income from stocks. Dividends are taxed as
    'Income from Other Sources' at slab rate. TDS of 10% is deducted
    if total dividend exceeds Rs 5,000 in a financial year.

    Tax Treatment (from FY 2020-21):
    - Dividends are fully taxable at slab rate
    - TDS @ 10% if dividend > Rs 5,000 per company in a year
    """

    symbol: str
    isin: str
    dividend_date: date_type
    quantity: int
    dividend_per_share: Decimal
    gross_amount: Decimal  # Before TDS
    tds_amount: Decimal = Decimal("0")  # TDS deducted (10% if > Rs 5000)
    net_amount: Decimal = Decimal("0")  # After TDS

    def __post_init__(self):
        """Calculate gross and net amounts."""
        if self.gross_amount == Decimal("0"):
            self.gross_amount = self.quantity * self.dividend_per_share

        if self.net_amount == Decimal("0"):
            self.net_amount = self.gross_amount - self.tds_amount


@dataclass
class StockHolding:
    """
    Current stock holding with cost basis.

    Represents shares held at a given date with average cost.
    Used for computing unrealized gains and portfolio value.
    """

    symbol: str
    isin: Optional[str]
    quantity: int
    average_cost: Decimal  # Average purchase price per share
    total_cost: Decimal  # Total cost basis
    current_price: Optional[Decimal] = None
    current_value: Optional[Decimal] = None
    unrealized_gain: Optional[Decimal] = None
    first_purchase_date: Optional[date_type] = None

    def calculate_unrealized_gain(self) -> Optional[Decimal]:
        """Calculate unrealized gain if current price is set."""
        if self.current_price is not None:
            self.current_value = self.quantity * self.current_price
            self.unrealized_gain = self.current_value - self.total_cost
            return self.unrealized_gain
        return None


@dataclass
class STTEntry:
    """
    Securities Transaction Tax (STT) ledger entry.

    STT is a tax levied on purchase/sale of securities.
    - Equity Delivery: 0.1% on both buy and sell
    - Equity Intraday: 0.025% on sell only
    - F&O: Various rates

    STT paid is a deductible expense for business income.
    """

    trade_date: date_type
    symbol: str
    trade_type: TradeType
    trade_category: TradeCategory
    trade_value: Decimal
    stt_amount: Decimal
    source_file: Optional[str] = None


@dataclass
class StockTrade:
    """
    Stock trade transaction.

    Represents a single buy or sell trade of stocks.
    For delivery trades, LTCG threshold is 12 months.

    Tax Treatment:
    - Intraday: Speculative business income (taxed at slab rate)
    - Delivery STCG (<12 months): 20% tax
    - Delivery LTCG (>12 months): 12.5% tax (₹1.25L exemption)
    - F&O: Non-speculative business income (taxed at slab rate)
    """

    symbol: str
    trade_date: date_type
    trade_type: TradeType
    quantity: int
    price: Decimal
    amount: Decimal  # quantity * price

    # Charges
    brokerage: Decimal = Decimal("0")
    stt: Decimal = Decimal("0")
    exchange_charges: Decimal = Decimal("0")
    gst: Decimal = Decimal("0")
    sebi_charges: Decimal = Decimal("0")
    stamp_duty: Decimal = Decimal("0")

    # Net amount (amount + charges for buy, amount - charges for sell)
    net_amount: Decimal = Decimal("0")

    # Optional fields
    isin: Optional[str] = None
    trade_category: Optional[TradeCategory] = None

    # For SELL trades - matching purchase info
    buy_date: Optional[date_type] = None
    buy_price: Optional[Decimal] = None
    cost_of_acquisition: Optional[Decimal] = None

    # Capital gains (for SELL trades)
    holding_period_days: Optional[int] = None
    is_long_term: Optional[bool] = None
    capital_gain: Optional[Decimal] = None

    def __post_init__(self):
        """Calculate net amount if not provided."""
        if self.net_amount == Decimal("0"):
            total_charges = (
                self.brokerage + self.stt + self.exchange_charges +
                self.gst + self.sebi_charges + self.stamp_duty
            )
            if self.trade_type == TradeType.BUY:
                self.net_amount = self.amount + total_charges
            else:  # SELL
                self.net_amount = self.amount - total_charges

    @property
    def is_intraday(self) -> bool:
        """Check if trade is intraday/speculative."""
        return self.trade_category == TradeCategory.INTRADAY

    @property
    def is_delivery(self) -> bool:
        """Check if trade is delivery-based."""
        return self.trade_category == TradeCategory.DELIVERY

    @property
    def is_fno(self) -> bool:
        """Check if trade is F&O."""
        return self.trade_category == TradeCategory.FNO

    def calculate_holding_period(self) -> Optional[int]:
        """
        Calculate holding period in days.

        Returns:
            Days between buy and sell, or None if buy_date not available
        """
        if not self.buy_date or self.trade_type != TradeType.SELL:
            return None

        delta = self.trade_date - self.buy_date
        return delta.days

    def is_ltcg(self) -> bool:
        """
        Check if qualifies for LTCG (>12 months for stocks).

        Returns:
            True if holding period > 365 days
        """
        if self.holding_period_days is None:
            self.holding_period_days = self.calculate_holding_period()

        if self.holding_period_days is None:
            return False

        return self.holding_period_days > 365


@dataclass
class ParseResult:
    """Result of parsing a stock trades file."""

    success: bool
    trades: list[StockTrade] = field(default_factory=list)
    dividends: list[StockDividend] = field(default_factory=list)
    stt_entries: list[STTEntry] = field(default_factory=list)
    source_file: str = ""
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def add_error(self, error: str):
        """Add an error message."""
        self.errors.append(error)
        self.success = False

    def add_warning(self, warning: str):
        """Add a warning message."""
        self.warnings.append(warning)


@dataclass
class DividendSummary:
    """Summary of dividend income for a financial year."""

    financial_year: str
    total_dividend: Decimal = Decimal("0")
    total_tds: Decimal = Decimal("0")
    net_dividend: Decimal = Decimal("0")
    dividend_count: int = 0

    def add_dividend(self, dividend: StockDividend):
        """Add a dividend to the summary."""
        self.total_dividend += dividend.gross_amount
        self.total_tds += dividend.tds_amount
        self.net_dividend += dividend.net_amount
        self.dividend_count += 1


@dataclass
class STTSummary:
    """Summary of STT paid for a financial year."""

    financial_year: str
    delivery_stt: Decimal = Decimal("0")
    intraday_stt: Decimal = Decimal("0")
    fno_stt: Decimal = Decimal("0")
    total_stt: Decimal = Decimal("0")

    def add_stt(self, entry: STTEntry):
        """Add an STT entry to the summary."""
        if entry.trade_category == TradeCategory.DELIVERY:
            self.delivery_stt += entry.stt_amount
        elif entry.trade_category == TradeCategory.INTRADAY:
            self.intraday_stt += entry.stt_amount
        elif entry.trade_category == TradeCategory.FNO:
            self.fno_stt += entry.stt_amount
        self.total_stt += entry.stt_amount


@dataclass
class CapitalGainsSummary:
    """Summary of capital gains for a financial year."""

    financial_year: str
    trade_category: TradeCategory

    # Short-term capital gains
    stcg_amount: Decimal = Decimal("0")
    stcg_tax_rate: Decimal = Decimal("20")  # 20% for equity STCG

    # Long-term capital gains
    ltcg_amount: Decimal = Decimal("0")
    ltcg_exemption: Decimal = Decimal("125000")  # ₹1.25L for equity LTCG
    ltcg_tax_rate: Decimal = Decimal("12.5")  # 12.5% for equity LTCG

    # Speculative income (intraday)
    speculative_income: Decimal = Decimal("0")

    # Taxable amounts
    taxable_stcg: Decimal = Decimal("0")
    taxable_ltcg: Decimal = Decimal("0")

    def calculate_taxable_amounts(self):
        """Calculate taxable STCG and LTCG after exemptions."""
        self.taxable_stcg = self.stcg_amount

        # Apply ₹1.25L exemption to LTCG
        if self.ltcg_amount > self.ltcg_exemption:
            self.taxable_ltcg = self.ltcg_amount - self.ltcg_exemption
        else:
            self.taxable_ltcg = Decimal("0")

    def calculate_tax(self) -> Decimal:
        """
        Calculate total tax liability.

        Returns:
            Total tax amount
        """
        stcg_tax = self.taxable_stcg * (self.stcg_tax_rate / Decimal("100"))
        ltcg_tax = self.taxable_ltcg * (self.ltcg_tax_rate / Decimal("100"))

        return stcg_tax + ltcg_tax
