"""Data models for foreign asset processing.

Covers RSU, ESPP, foreign dividends, and stock sales.
"""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Optional, List


class GrantType(Enum):
    """Type of stock grant."""
    RSU = "RSU"
    ESPP = "ESPP"
    ESOP = "ESOP"
    DRIP = "DRIP"


class ActivityType(Enum):
    """Type of cash flow activity."""
    VEST = "VEST"
    SALE = "SALE"
    DIVIDEND = "DIVIDEND"
    DIVIDEND_REINVEST = "DIVIDEND_REINVEST"
    PURCHASE = "PURCHASE"
    INTEREST = "INTEREST"
    FEE = "FEE"
    TAX_WITHHOLD = "TAX_WITHHOLD"
    TRANSFER = "TRANSFER"


@dataclass
class StockPlanDetails:
    """Stock plan grant details from broker statement."""

    grant_date: date
    grant_number: str
    grant_type: GrantType
    symbol: str
    potential_quantity: Decimal
    grant_price: Decimal
    market_price: Decimal
    total_value: Decimal
    vested_quantity: Decimal = Decimal("0")
    unvested_quantity: Decimal = Decimal("0")


@dataclass
class CashFlowActivity:
    """Cash flow activity from broker statement."""

    activity_date: date
    activity_type: ActivityType
    description: str
    symbol: Optional[str] = None
    quantity: Optional[Decimal] = None
    price: Optional[Decimal] = None
    amount: Decimal = Decimal("0")  # Credits positive, Debits negative
    settlement_date: Optional[date] = None
    fees: Decimal = Decimal("0")
    withholding: Decimal = Decimal("0")


@dataclass
class RSUVest:
    """
    RSU vest event.

    Tax Treatment:
    - Perquisite = FMV × Shares × TT Rate (taxed as salary)
    - Cost basis = FMV at vest (for future CG calculation)
    - LTCG if held >24 months from vest date
    """

    grant_number: str
    vest_date: date
    shares_vested: Decimal
    fmv_usd: Decimal  # Fair Market Value at vest
    shares_withheld_for_tax: Decimal = Decimal("0")
    net_shares: Decimal = Decimal("0")

    # INR conversion
    tt_rate: Optional[Decimal] = None
    perquisite_inr: Optional[Decimal] = None

    # Correlation with payslip
    salary_record_id: Optional[int] = None
    correlation_status: str = "PENDING"

    @property
    def cost_basis_per_share_usd(self) -> Decimal:
        """Cost basis per share = FMV at vest."""
        return self.fmv_usd

    @property
    def cost_basis_per_share_inr(self) -> Optional[Decimal]:
        """Cost basis in INR."""
        if self.tt_rate:
            return self.fmv_usd * self.tt_rate
        return None

    def calculate_perquisite(self, tt_rate: Decimal) -> Decimal:
        """Calculate perquisite in INR."""
        self.tt_rate = tt_rate
        self.perquisite_inr = self.shares_vested * self.fmv_usd * tt_rate
        return self.perquisite_inr

    def __post_init__(self):
        """Calculate net shares if not provided."""
        if self.net_shares == Decimal("0"):
            self.net_shares = self.shares_vested - self.shares_withheld_for_tax


@dataclass
class RSUSale:
    """
    RSU sale transaction.

    LTCG applies if held >24 months from vest date (foreign stocks).
    """

    sell_date: date
    shares_sold: Decimal
    sell_price_usd: Decimal
    sell_value_usd: Decimal

    # Matched vest info
    vest_date: date
    cost_basis_per_share_usd: Decimal
    cost_basis_usd: Decimal

    # Calculated fields
    holding_period_days: int = 0
    is_ltcg: bool = False
    gain_usd: Decimal = Decimal("0")
    gain_inr: Decimal = Decimal("0")
    tt_rate_at_sale: Optional[Decimal] = None

    # Fees and withholding
    fees_usd: Decimal = Decimal("0")
    withholding_usd: Decimal = Decimal("0")

    LTCG_THRESHOLD_DAYS = 730  # 24 months

    def calculate_gain(self, tt_rate: Decimal) -> None:
        """Calculate capital gain."""
        self.tt_rate_at_sale = tt_rate
        self.holding_period_days = (self.sell_date - self.vest_date).days
        self.is_ltcg = self.holding_period_days > self.LTCG_THRESHOLD_DAYS

        self.cost_basis_usd = self.shares_sold * self.cost_basis_per_share_usd
        self.sell_value_usd = self.shares_sold * self.sell_price_usd
        self.gain_usd = self.sell_value_usd - self.cost_basis_usd - self.fees_usd
        self.gain_inr = self.gain_usd * tt_rate


@dataclass
class ESPPPurchase:
    """
    ESPP purchase event.

    Tax Treatment:
    - Discount is taxable as perquisite (typically 15%)
    - Cost basis = Purchase price (not market price)
    - TCS applicable on LRS remittance >₹7L (20%)
    """

    purchase_date: date
    shares_purchased: Decimal
    purchase_price_usd: Decimal  # Discounted price
    market_price_usd: Decimal    # FMV at purchase

    # Calculated
    discount_percentage: Decimal = Decimal("0")
    perquisite_per_share_usd: Decimal = Decimal("0")
    total_perquisite_usd: Decimal = Decimal("0")

    # INR conversion
    tt_rate: Optional[Decimal] = None
    perquisite_inr: Decimal = Decimal("0")
    purchase_value_inr: Decimal = Decimal("0")

    # TCS tracking (Section 206CQ)
    lrs_amount_inr: Decimal = Decimal("0")
    tcs_collected: Decimal = Decimal("0")

    TCS_THRESHOLD = Decimal("700000")  # ₹7L
    TCS_RATE = Decimal("0.20")  # 20%

    def calculate_perquisite(self, tt_rate: Decimal) -> None:
        """Calculate perquisite and TCS."""
        self.tt_rate = tt_rate

        # Perquisite = Market Price - Purchase Price
        self.perquisite_per_share_usd = self.market_price_usd - self.purchase_price_usd
        self.total_perquisite_usd = self.perquisite_per_share_usd * self.shares_purchased
        self.perquisite_inr = self.total_perquisite_usd * tt_rate

        # Discount percentage
        if self.market_price_usd > Decimal("0"):
            self.discount_percentage = (
                self.perquisite_per_share_usd / self.market_price_usd * Decimal("100")
            )

        # LRS amount (money sent abroad)
        self.purchase_value_inr = self.shares_purchased * self.purchase_price_usd * tt_rate
        self.lrs_amount_inr = self.purchase_value_inr

        # TCS calculation (20% on amount exceeding ₹7L)
        if self.lrs_amount_inr > self.TCS_THRESHOLD:
            taxable_lrs = self.lrs_amount_inr - self.TCS_THRESHOLD
            self.tcs_collected = taxable_lrs * self.TCS_RATE


@dataclass
class ESPPSale:
    """ESPP sale transaction."""

    sell_date: date
    shares_sold: Decimal
    sell_price_usd: Decimal
    sell_value_usd: Decimal

    # Purchase info
    purchase_date: date
    cost_basis_per_share_usd: Decimal  # Purchase price (discounted)
    cost_basis_usd: Decimal

    # Calculated
    holding_period_days: int = 0
    is_ltcg: bool = False
    gain_usd: Decimal = Decimal("0")
    gain_inr: Decimal = Decimal("0")
    tt_rate_at_sale: Optional[Decimal] = None

    LTCG_THRESHOLD_DAYS = 730  # 24 months for foreign

    def calculate_gain(self, tt_rate: Decimal) -> None:
        """Calculate capital gain."""
        self.tt_rate_at_sale = tt_rate
        self.holding_period_days = (self.sell_date - self.purchase_date).days
        self.is_ltcg = self.holding_period_days > self.LTCG_THRESHOLD_DAYS

        self.cost_basis_usd = self.shares_sold * self.cost_basis_per_share_usd
        self.sell_value_usd = self.shares_sold * self.sell_price_usd
        self.gain_usd = self.sell_value_usd - self.cost_basis_usd
        self.gain_inr = self.gain_usd * tt_rate


@dataclass
class ForeignDividend:
    """
    Foreign dividend income.

    Tax Treatment:
    - Taxable as 'Income from Other Sources'
    - US withholds 25% (can claim DTAA credit)
    - Report in Schedule FA
    """

    dividend_date: date
    symbol: str
    shares_held: Decimal
    dividend_per_share_usd: Decimal
    gross_dividend_usd: Decimal
    withholding_tax_usd: Decimal  # 25% US withholding
    net_dividend_usd: Decimal

    # INR conversion
    tt_rate: Optional[Decimal] = None
    gross_dividend_inr: Decimal = Decimal("0")
    withholding_tax_inr: Decimal = Decimal("0")
    net_dividend_inr: Decimal = Decimal("0")

    # DTAA credit
    dtaa_credit_inr: Decimal = Decimal("0")

    # DRIP (Dividend Reinvestment)
    is_reinvested: bool = False
    shares_purchased: Decimal = Decimal("0")

    def convert_to_inr(self, tt_rate: Decimal) -> None:
        """Convert to INR."""
        self.tt_rate = tt_rate
        self.gross_dividend_inr = self.gross_dividend_usd * tt_rate
        self.withholding_tax_inr = self.withholding_tax_usd * tt_rate
        self.net_dividend_inr = self.net_dividend_usd * tt_rate


@dataclass
class ForeignParseResult:
    """Result of parsing foreign asset documents."""

    success: bool
    statement_period: Optional[str] = None
    account_number: Optional[str] = None

    stock_plan_details: List[StockPlanDetails] = field(default_factory=list)
    rsu_vests: List[RSUVest] = field(default_factory=list)
    rsu_sales: List[RSUSale] = field(default_factory=list)
    espp_purchases: List[ESPPPurchase] = field(default_factory=list)
    espp_sales: List[ESPPSale] = field(default_factory=list)
    dividends: List[ForeignDividend] = field(default_factory=list)
    activities: List[CashFlowActivity] = field(default_factory=list)

    source_file: str = ""
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_error(self, error: str) -> None:
        """Add an error."""
        self.errors.append(error)
        self.success = False

    def add_warning(self, warning: str) -> None:
        """Add a warning."""
        self.warnings.append(warning)
