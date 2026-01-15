"""
Core models for PFAS - unified transaction and financial concepts.

This module provides:
- NormalizedTransaction: Unified transaction format across all parsers
- CashFlow: Cash flow entry for statement generation
- AssetHolding: Point-in-time asset holding for balance sheet
- Liability: Liability record for balance sheet
- BalanceSheetSnapshot: Balance sheet at a point in time
- CashFlowStatement: Cash flow statement for a period

All monetary values use Decimal for precision.
"""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any, List


class ActivityType(Enum):
    """Cash flow activity classification per accounting standards."""
    OPERATING = "OPERATING"    # Day-to-day business activities
    INVESTING = "INVESTING"    # Buying/selling assets
    FINANCING = "FINANCING"    # Loans, repayments


class FlowDirection(Enum):
    """Cash flow direction."""
    INFLOW = "INFLOW"
    OUTFLOW = "OUTFLOW"


class AssetCategory(Enum):
    """Asset categories for portfolio tracking."""
    BANK_SAVINGS = "BANK_SAVINGS"
    BANK_CURRENT = "BANK_CURRENT"
    BANK_FD = "BANK_FD"
    MUTUAL_FUND_EQUITY = "MF_EQUITY"
    MUTUAL_FUND_DEBT = "MF_DEBT"
    MUTUAL_FUND_HYBRID = "MF_HYBRID"
    MUTUAL_FUND_LIQUID = "MF_LIQUID"
    STOCK_INDIAN = "STOCK_INDIAN"
    STOCK_FOREIGN = "STOCK_FOREIGN"
    EPF = "EPF"
    PPF = "PPF"
    NPS_TIER1 = "NPS_TIER1"
    NPS_TIER2 = "NPS_TIER2"
    SGB = "SGB"
    RBI_BOND = "RBI_BOND"
    REIT = "REIT"
    INVIT = "INVIT"
    REAL_ESTATE = "REAL_ESTATE"
    GOLD_PHYSICAL = "GOLD_PHYSICAL"
    GOLD_ETF = "GOLD_ETF"
    CRYPTO = "CRYPTO"
    INSURANCE = "INSURANCE"
    OTHER = "OTHER"


class LiabilityType(Enum):
    """Liability types for tracking."""
    HOME_LOAN = "HOME_LOAN"
    CAR_LOAN = "CAR_LOAN"
    PERSONAL_LOAN = "PERSONAL_LOAN"
    EDUCATION_LOAN = "EDUCATION_LOAN"
    CREDIT_CARD = "CREDIT_CARD"
    OVERDRAFT = "OVERDRAFT"
    OTHER = "OTHER"


class CashFlowCategory(Enum):
    """Detailed cash flow categories for classification."""
    # Operating - Inflows
    SALARY = "SALARY"
    DIVIDEND_INDIAN = "DIVIDEND_INDIAN"
    DIVIDEND_FOREIGN = "DIVIDEND_FOREIGN"
    INTEREST_BANK = "INTEREST_BANK"
    INTEREST_FD = "INTEREST_FD"
    INTEREST_SGB = "INTEREST_SGB"
    INTEREST_OTHER = "INTEREST_OTHER"
    RENT_RECEIVED = "RENT_RECEIVED"
    BUSINESS_INCOME = "BUSINESS_INCOME"
    OTHER_OPERATING_INFLOW = "OTHER_OPERATING_INFLOW"

    # Operating - Outflows
    TAX_PAID = "TAX_PAID"
    INSURANCE_PREMIUM = "INSURANCE_PREMIUM"
    RENT_PAID = "RENT_PAID"
    HOUSEHOLD_EXPENSE = "HOUSEHOLD_EXPENSE"
    OTHER_OPERATING_OUTFLOW = "OTHER_OPERATING_OUTFLOW"

    # Investing - Inflows
    MF_REDEMPTION = "MF_REDEMPTION"
    STOCK_SALE = "STOCK_SALE"
    FD_MATURITY = "FD_MATURITY"
    PROPERTY_SALE = "PROPERTY_SALE"
    OTHER_INVESTMENT_INFLOW = "OTHER_INVESTMENT_INFLOW"

    # Investing - Outflows
    MF_PURCHASE = "MF_PURCHASE"
    STOCK_PURCHASE = "STOCK_PURCHASE"
    FD_INVESTMENT = "FD_INVESTMENT"
    PPF_DEPOSIT = "PPF_DEPOSIT"
    NPS_CONTRIBUTION = "NPS_CONTRIBUTION"
    EPF_CONTRIBUTION = "EPF_CONTRIBUTION"
    SGB_PURCHASE = "SGB_PURCHASE"
    PROPERTY_PURCHASE = "PROPERTY_PURCHASE"
    OTHER_INVESTMENT_OUTFLOW = "OTHER_INVESTMENT_OUTFLOW"

    # Financing - Inflows
    LOAN_DISBURSEMENT = "LOAN_DISBURSEMENT"
    OTHER_FINANCING_INFLOW = "OTHER_FINANCING_INFLOW"

    # Financing - Outflows
    LOAN_EMI = "LOAN_EMI"
    LOAN_PREPAYMENT = "LOAN_PREPAYMENT"
    CREDIT_CARD_PAYMENT = "CREDIT_CARD_PAYMENT"
    OTHER_FINANCING_OUTFLOW = "OTHER_FINANCING_OUTFLOW"

    # Unclassified
    TRANSFER = "TRANSFER"
    UNKNOWN = "UNKNOWN"


@dataclass
class NormalizedTransaction:
    """
    Unified transaction format for all source types.

    This is the common intermediate representation that parsers
    can convert to, enabling cross-source analysis and reporting.

    Attributes:
        date: Transaction date
        amount: Transaction amount (always positive, use flow_direction for sign)
        transaction_type: Type string (BUY, SELL, CREDIT, DEBIT, etc.)
        asset_category: Asset classification
        asset_identifier: ISIN, symbol, account number, etc.
        asset_name: Human-readable name
        quantity: Units/shares (for securities)
        unit_price: Price per unit (for securities)
        activity_type: Operating/Investing/Financing
        flow_direction: Inflow/Outflow
        holding_period_days: Days held (for capital gains)
        is_long_term: LTCG qualification
        capital_gain: Gain/loss amount
        tds_deducted: TDS withheld
        source_type: Parser source (CAMS, ZERODHA, etc.)
        source_file: Original file path
        raw_data: Original parsed data for reference
    """
    date: date
    amount: Decimal
    transaction_type: str

    # Asset identification
    asset_category: AssetCategory
    asset_identifier: str = ""
    asset_name: str = ""

    # Optional transaction details
    quantity: Optional[Decimal] = None
    unit_price: Optional[Decimal] = None

    # Cash flow classification
    activity_type: Optional[ActivityType] = None
    flow_direction: Optional[FlowDirection] = None
    cash_flow_category: Optional[CashFlowCategory] = None

    # Tax-related
    holding_period_days: Optional[int] = None
    is_long_term: Optional[bool] = None
    capital_gain: Optional[Decimal] = None
    tds_deducted: Decimal = Decimal("0")

    # Source tracking
    source_type: str = ""
    source_file: str = ""
    raw_data: Dict[str, Any] = field(default_factory=dict)

    def to_cash_flow(self) -> Optional["CashFlow"]:
        """Convert to CashFlow if activity type is set."""
        if self.activity_type is None or self.flow_direction is None:
            return None
        return CashFlow(
            flow_date=self.date,
            activity_type=self.activity_type,
            flow_direction=self.flow_direction,
            amount=self.amount,
            category=self.cash_flow_category.value if self.cash_flow_category else self.asset_category.value,
            description=self.asset_name or self.asset_identifier,
            source_table=self.source_type,
        )


@dataclass
class CashFlow:
    """
    Cash flow entry for statement generation.

    Represents a single cash movement that can be aggregated
    into a cash flow statement.
    """
    flow_date: date
    activity_type: ActivityType
    flow_direction: FlowDirection
    amount: Decimal
    category: str
    sub_category: str = ""
    description: str = ""
    source_table: str = ""
    source_id: Optional[int] = None
    bank_account_id: Optional[int] = None
    financial_year: str = ""

    @property
    def signed_amount(self) -> Decimal:
        """Amount with sign based on flow direction."""
        if self.flow_direction == FlowDirection.OUTFLOW:
            return -self.amount
        return self.amount


@dataclass
class AssetHolding:
    """
    Point-in-time asset holding for balance sheet.

    Represents a single holding with current valuation
    and cost basis for unrealized gain calculation.
    """
    asset_type: AssetCategory
    asset_identifier: str
    asset_name: str
    quantity: Decimal
    unit_price: Decimal
    total_value: Decimal
    cost_basis: Decimal
    unrealized_gain: Decimal
    currency: str = "INR"
    source_table: str = ""
    source_id: Optional[int] = None
    as_of_date: Optional[date] = None

    @property
    def return_percentage(self) -> Optional[Decimal]:
        """Calculate return percentage."""
        if self.cost_basis and self.cost_basis != 0:
            return ((self.total_value - self.cost_basis) / self.cost_basis * 100).quantize(Decimal("0.01"))
        return None


@dataclass
class Liability:
    """
    Liability record for balance sheet.

    Represents a loan, credit card balance, or other liability.
    """
    id: Optional[int] = None
    liability_type: LiabilityType = LiabilityType.OTHER
    lender_name: str = ""
    account_number: str = ""
    principal_amount: Decimal = Decimal("0")
    outstanding_amount: Decimal = Decimal("0")
    interest_rate: Decimal = Decimal("0")
    emi_amount: Optional[Decimal] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    tenure_months: Optional[int] = None
    is_active: bool = True

    @property
    def amount_paid(self) -> Decimal:
        """Total amount paid so far."""
        return self.principal_amount - self.outstanding_amount


@dataclass
class LiabilityTransaction:
    """Transaction on a liability (EMI, prepayment, etc.)."""
    liability_id: int
    transaction_date: date
    transaction_type: str  # EMI, PREPAYMENT, DISBURSEMENT, INTEREST, FEE
    amount: Decimal
    principal_component: Optional[Decimal] = None
    interest_component: Optional[Decimal] = None
    outstanding_after: Optional[Decimal] = None
    reference_number: str = ""


@dataclass
class BalanceSheetSnapshot:
    """
    Balance sheet at a point in time.

    Provides a complete picture of assets and liabilities
    with computed net worth.
    """
    snapshot_date: date

    # Bank & Cash
    bank_savings: Decimal = Decimal("0")
    bank_current: Decimal = Decimal("0")
    bank_fd: Decimal = Decimal("0")
    cash_in_hand: Decimal = Decimal("0")

    # Investments - Domestic
    mutual_funds_equity: Decimal = Decimal("0")
    mutual_funds_debt: Decimal = Decimal("0")
    mutual_funds_hybrid: Decimal = Decimal("0")
    mutual_funds_liquid: Decimal = Decimal("0")
    stocks_indian: Decimal = Decimal("0")

    # Investments - Foreign
    stocks_foreign: Decimal = Decimal("0")

    # Retirement
    epf_balance: Decimal = Decimal("0")
    ppf_balance: Decimal = Decimal("0")
    nps_tier1: Decimal = Decimal("0")
    nps_tier2: Decimal = Decimal("0")

    # Other Investments
    sgb_holdings: Decimal = Decimal("0")
    rbi_bonds: Decimal = Decimal("0")
    reit_holdings: Decimal = Decimal("0")
    gold_holdings: Decimal = Decimal("0")

    # Real Assets
    real_estate: Decimal = Decimal("0")
    vehicles: Decimal = Decimal("0")
    other_assets: Decimal = Decimal("0")

    # Liabilities
    home_loans: Decimal = Decimal("0")
    car_loans: Decimal = Decimal("0")
    personal_loans: Decimal = Decimal("0")
    education_loans: Decimal = Decimal("0")
    credit_cards: Decimal = Decimal("0")
    other_liabilities: Decimal = Decimal("0")

    # Holdings details (for drill-down)
    asset_holdings: List[AssetHolding] = field(default_factory=list)
    liability_details: List[Liability] = field(default_factory=list)

    @property
    def total_bank_balances(self) -> Decimal:
        """Total cash and bank balances."""
        return self.bank_savings + self.bank_current + self.bank_fd + self.cash_in_hand

    @property
    def total_mutual_funds(self) -> Decimal:
        """Total mutual fund investments."""
        return (self.mutual_funds_equity + self.mutual_funds_debt +
                self.mutual_funds_hybrid + self.mutual_funds_liquid)

    @property
    def total_retirement_funds(self) -> Decimal:
        """Total retirement corpus."""
        return self.epf_balance + self.ppf_balance + self.nps_tier1 + self.nps_tier2

    @property
    def total_investments(self) -> Decimal:
        """Total financial investments."""
        return (self.total_mutual_funds + self.stocks_indian + self.stocks_foreign +
                self.sgb_holdings + self.rbi_bonds + self.reit_holdings + self.gold_holdings)

    @property
    def total_assets(self) -> Decimal:
        """Total assets."""
        return (self.total_bank_balances + self.total_investments +
                self.total_retirement_funds + self.real_estate +
                self.vehicles + self.other_assets)

    @property
    def total_liabilities(self) -> Decimal:
        """Total liabilities."""
        return (self.home_loans + self.car_loans + self.personal_loans +
                self.education_loans + self.credit_cards + self.other_liabilities)

    @property
    def net_worth(self) -> Decimal:
        """Net worth (assets - liabilities)."""
        return self.total_assets - self.total_liabilities

    def to_breakdown_dict(self) -> Dict[str, Dict[str, float]]:
        """Convert to breakdown dictionaries for storage."""
        return {
            "assets": {
                "bank_savings": float(self.bank_savings),
                "bank_current": float(self.bank_current),
                "bank_fd": float(self.bank_fd),
                "mutual_funds_equity": float(self.mutual_funds_equity),
                "mutual_funds_debt": float(self.mutual_funds_debt),
                "mutual_funds_hybrid": float(self.mutual_funds_hybrid),
                "mutual_funds_liquid": float(self.mutual_funds_liquid),
                "stocks_indian": float(self.stocks_indian),
                "stocks_foreign": float(self.stocks_foreign),
                "epf_balance": float(self.epf_balance),
                "ppf_balance": float(self.ppf_balance),
                "nps_tier1": float(self.nps_tier1),
                "nps_tier2": float(self.nps_tier2),
                "sgb_holdings": float(self.sgb_holdings),
                "reit_holdings": float(self.reit_holdings),
                "real_estate": float(self.real_estate),
                "other_assets": float(self.other_assets),
            },
            "liabilities": {
                "home_loans": float(self.home_loans),
                "car_loans": float(self.car_loans),
                "personal_loans": float(self.personal_loans),
                "education_loans": float(self.education_loans),
                "credit_cards": float(self.credit_cards),
                "other_liabilities": float(self.other_liabilities),
            }
        }


@dataclass
class CashFlowStatement:
    """
    Cash flow statement for a financial year.

    Categorizes all cash movements into Operating, Investing,
    and Financing activities per accounting standards.
    """
    period_start: date
    period_end: date
    financial_year: str

    # Operating Activities - Inflows
    salary_received: Decimal = Decimal("0")
    dividends_received: Decimal = Decimal("0")
    interest_received: Decimal = Decimal("0")
    rent_received: Decimal = Decimal("0")
    business_income: Decimal = Decimal("0")
    other_operating_inflow: Decimal = Decimal("0")

    # Operating Activities - Outflows
    taxes_paid: Decimal = Decimal("0")
    insurance_paid: Decimal = Decimal("0")
    rent_paid: Decimal = Decimal("0")
    household_expenses: Decimal = Decimal("0")
    other_operating_outflow: Decimal = Decimal("0")

    # Investing Activities - Inflows
    mf_redemptions: Decimal = Decimal("0")
    stock_sells: Decimal = Decimal("0")
    fd_maturities: Decimal = Decimal("0")
    property_sales: Decimal = Decimal("0")
    other_investing_inflow: Decimal = Decimal("0")

    # Investing Activities - Outflows
    mf_purchases: Decimal = Decimal("0")
    stock_buys: Decimal = Decimal("0")
    fd_investments: Decimal = Decimal("0")
    ppf_deposits: Decimal = Decimal("0")
    nps_contributions: Decimal = Decimal("0")
    epf_contributions: Decimal = Decimal("0")
    sgb_purchases: Decimal = Decimal("0")
    property_purchases: Decimal = Decimal("0")
    other_investing_outflow: Decimal = Decimal("0")

    # Financing Activities - Inflows
    loan_proceeds: Decimal = Decimal("0")
    other_financing_inflow: Decimal = Decimal("0")

    # Financing Activities - Outflows
    loan_repayments: Decimal = Decimal("0")
    loan_prepayments: Decimal = Decimal("0")
    credit_card_payments: Decimal = Decimal("0")
    other_financing_outflow: Decimal = Decimal("0")

    # Opening and closing cash
    opening_cash: Decimal = Decimal("0")
    closing_cash: Decimal = Decimal("0")

    # Detail lists for drill-down
    operating_details: List[CashFlow] = field(default_factory=list)
    investing_details: List[CashFlow] = field(default_factory=list)
    financing_details: List[CashFlow] = field(default_factory=list)

    @property
    def total_operating_inflow(self) -> Decimal:
        """Total operating inflows."""
        return (self.salary_received + self.dividends_received +
                self.interest_received + self.rent_received +
                self.business_income + self.other_operating_inflow)

    @property
    def total_operating_outflow(self) -> Decimal:
        """Total operating outflows."""
        return (self.taxes_paid + self.insurance_paid + self.rent_paid +
                self.household_expenses + self.other_operating_outflow)

    @property
    def net_operating(self) -> Decimal:
        """Net cash from operating activities."""
        return self.total_operating_inflow - self.total_operating_outflow

    @property
    def total_investing_inflow(self) -> Decimal:
        """Total investing inflows."""
        return (self.mf_redemptions + self.stock_sells + self.fd_maturities +
                self.property_sales + self.other_investing_inflow)

    @property
    def total_investing_outflow(self) -> Decimal:
        """Total investing outflows."""
        return (self.mf_purchases + self.stock_buys + self.fd_investments +
                self.ppf_deposits + self.nps_contributions + self.epf_contributions +
                self.sgb_purchases + self.property_purchases + self.other_investing_outflow)

    @property
    def net_investing(self) -> Decimal:
        """Net cash from investing activities."""
        return self.total_investing_inflow - self.total_investing_outflow

    @property
    def total_financing_inflow(self) -> Decimal:
        """Total financing inflows."""
        return self.loan_proceeds + self.other_financing_inflow

    @property
    def total_financing_outflow(self) -> Decimal:
        """Total financing outflows."""
        return (self.loan_repayments + self.loan_prepayments +
                self.credit_card_payments + self.other_financing_outflow)

    @property
    def net_financing(self) -> Decimal:
        """Net cash from financing activities."""
        return self.total_financing_inflow - self.total_financing_outflow

    @property
    def net_change_in_cash(self) -> Decimal:
        """Net change in cash and cash equivalents."""
        return self.net_operating + self.net_investing + self.net_financing

    def validate_cash_reconciliation(self) -> bool:
        """Validate that opening + change = closing."""
        expected_closing = self.opening_cash + self.net_change_in_cash
        return abs(expected_closing - self.closing_cash) < Decimal("0.01")

    def to_breakdown_dict(self) -> Dict[str, Dict[str, float]]:
        """Convert to breakdown dictionaries for storage."""
        return {
            "operating": {
                "salary_received": float(self.salary_received),
                "dividends_received": float(self.dividends_received),
                "interest_received": float(self.interest_received),
                "rent_received": float(self.rent_received),
                "taxes_paid": float(self.taxes_paid),
                "net": float(self.net_operating),
            },
            "investing": {
                "mf_redemptions": float(self.mf_redemptions),
                "mf_purchases": float(self.mf_purchases),
                "stock_sells": float(self.stock_sells),
                "stock_buys": float(self.stock_buys),
                "net": float(self.net_investing),
            },
            "financing": {
                "loan_proceeds": float(self.loan_proceeds),
                "loan_repayments": float(self.loan_repayments),
                "net": float(self.net_financing),
            }
        }


# Utility functions for financial year handling
def get_financial_year(dt: date) -> str:
    """
    Get financial year string for a date.

    Indian FY runs April 1 to March 31.
    April 2024 -> "2024-25"
    March 2025 -> "2024-25"

    Args:
        dt: Date to get FY for

    Returns:
        Financial year string (e.g., "2024-25")
    """
    if dt.month >= 4:
        return f"{dt.year}-{str(dt.year + 1)[2:]}"
    else:
        return f"{dt.year - 1}-{str(dt.year)[2:]}"


def get_fy_dates(financial_year: str) -> tuple:
    """
    Get start and end dates for a financial year.

    Args:
        financial_year: FY string (e.g., "2024-25")

    Returns:
        Tuple of (start_date, end_date)
    """
    start_year = int(financial_year.split('-')[0])
    return (date(start_year, 4, 1), date(start_year + 1, 3, 31))
