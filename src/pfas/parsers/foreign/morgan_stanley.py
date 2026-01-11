"""Morgan Stanley / E*TRADE statement parser.

Parses broker statements for:
- Stock plan details (RSU/ESPP grants)
- Cash flow activity (vests, sales, dividends)
- Tax withholding information
"""

import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional, List, Tuple

from .models import (
    GrantType,
    ActivityType,
    StockPlanDetails,
    CashFlowActivity,
    RSUVest,
    RSUSale,
    ESPPPurchase,
    ForeignDividend,
    ForeignParseResult,
)


class MorganStanleyParser:
    """
    Parser for Morgan Stanley / E*TRADE broker statements.

    Extracts:
    - Stock plan details (grants, vesting schedules)
    - Cash flow activity (deposits, withdrawals, trades)
    - RSU vest events
    - Stock sales
    - Dividend payments
    """

    # Common patterns for parsing
    PATTERNS = {
        'date': r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        'amount': r'[\$]?([\d,]+\.?\d*)',
        'shares': r'([\d,]+\.?\d*)\s*(?:shares?|shs?)?',
        'symbol': r'([A-Z]{1,5})',
    }

    # Activity type mappings (order matters - more specific first)
    ACTIVITY_KEYWORDS = {
        'DIVIDEND_REINVEST': ['reinvest', 'drip'],  # Check before DIVIDEND and VEST
        'DIVIDEND': ['dividend', 'div'],
        'VEST': ['vest', 'vesting', 'release'],
        'SALE': ['sale', 'sold', 'sell'],
        'PURCHASE': ['purchase', 'buy', 'espp'],
        'TAX_WITHHOLD': ['withhold', 'tax', 'w/h'],
        'FEE': ['fee', 'commission', 'charge'],
        'TRANSFER': ['transfer', 'wire', 'deposit', 'withdrawal'],
    }

    def __init__(self):
        """Initialize parser."""
        self._result: Optional[ForeignParseResult] = None

    def parse(self, content: str, source_file: str = "") -> ForeignParseResult:
        """
        Parse Morgan Stanley statement content.

        Args:
            content: Statement text content
            source_file: Source file path

        Returns:
            ForeignParseResult with parsed data
        """
        self._result = ForeignParseResult(success=True, source_file=source_file)

        try:
            # Extract statement metadata
            self._parse_metadata(content)

            # Parse stock plan details
            self._parse_stock_plans(content)

            # Parse cash flow activity
            self._parse_activities(content)

            # Correlate activities to create vest/sale records
            self._correlate_activities()

        except Exception as e:
            self._result.add_error(f"Parse error: {str(e)}")

        return self._result

    def parse_csv(self, csv_content: str, source_file: str = "") -> ForeignParseResult:
        """
        Parse CSV export from Morgan Stanley.

        Args:
            csv_content: CSV file content
            source_file: Source file path

        Returns:
            ForeignParseResult with parsed data
        """
        import csv
        from io import StringIO

        self._result = ForeignParseResult(success=True, source_file=source_file)

        try:
            reader = csv.DictReader(StringIO(csv_content))
            for row in reader:
                activity = self._parse_csv_row(row)
                if activity:
                    self._result.activities.append(activity)

            # Correlate activities
            self._correlate_activities()

        except Exception as e:
            self._result.add_error(f"CSV parse error: {str(e)}")

        return self._result

    def _parse_metadata(self, content: str) -> None:
        """Extract statement metadata."""
        # Statement period
        period_match = re.search(
            r'(?:statement\s+period|period)[\s:]+(\w+\s+\d+,?\s*\d{4})\s*[-–to]+\s*(\w+\s+\d+,?\s*\d{4})',
            content, re.IGNORECASE
        )
        if period_match:
            self._result.statement_period = f"{period_match.group(1)} - {period_match.group(2)}"

        # Account number
        account_match = re.search(
            r'(?:account|acct)[\s#:]+([A-Z0-9-]+)',
            content, re.IGNORECASE
        )
        if account_match:
            self._result.account_number = account_match.group(1)

    def _parse_stock_plans(self, content: str) -> None:
        """Parse stock plan details section."""
        # Look for stock plan table
        plan_section = re.search(
            r'(?:stock\s+plan|equity\s+awards?|grant\s+details?)(.+?)(?:cash\s+flow|activity|$)',
            content, re.IGNORECASE | re.DOTALL
        )

        if not plan_section:
            return

        section_text = plan_section.group(1)

        # Parse individual grants
        grant_pattern = re.compile(
            r'(RSU|ESPP|ESOP)\s+' +
            self.PATTERNS['date'] + r'\s+' +
            r'([A-Z0-9-]+)\s+' +  # Grant number
            self.PATTERNS['symbol'] + r'\s+' +
            self.PATTERNS['shares'] + r'\s+' +
            self.PATTERNS['amount'],  # Price/Value
            re.IGNORECASE
        )

        for match in grant_pattern.finditer(section_text):
            try:
                grant_type = GrantType[match.group(1).upper()]
                grant_date = self._parse_date(match.group(2))
                grant_number = match.group(3)
                symbol = match.group(4)
                quantity = self._to_decimal(match.group(5))
                price = self._to_decimal(match.group(6))

                plan = StockPlanDetails(
                    grant_date=grant_date,
                    grant_number=grant_number,
                    grant_type=grant_type,
                    symbol=symbol,
                    potential_quantity=quantity,
                    grant_price=price,
                    market_price=price,  # Will be updated if available
                    total_value=quantity * price,
                )
                self._result.stock_plan_details.append(plan)

            except (ValueError, KeyError) as e:
                self._result.add_warning(f"Could not parse grant: {str(e)}")

    def _parse_activities(self, content: str) -> None:
        """Parse cash flow activity section."""
        # Look for activity table
        activity_section = re.search(
            r'(?:cash\s+flow|activity|transactions?)(.+?)(?:summary|total|$)',
            content, re.IGNORECASE | re.DOTALL
        )

        if not activity_section:
            return

        section_text = activity_section.group(1)

        # Split into lines and parse each
        for line in section_text.split('\n'):
            activity = self._parse_activity_line(line)
            if activity:
                self._result.activities.append(activity)

    def _parse_activity_line(self, line: str) -> Optional[CashFlowActivity]:
        """Parse a single activity line."""
        line = line.strip()
        if not line or len(line) < 10:
            return None

        # Try to extract date
        date_match = re.search(self.PATTERNS['date'], line)
        if not date_match:
            return None

        try:
            activity_date = self._parse_date(date_match.group(1))
        except ValueError:
            return None

        # Determine activity type
        activity_type = self._determine_activity_type(line)

        # Extract amounts
        amounts = re.findall(self.PATTERNS['amount'], line)
        amount = Decimal("0")
        if amounts:
            # Last amount is usually the net amount
            amount = self._to_decimal(amounts[-1])

        # Check for debit indicator
        if re.search(r'\(|\bdr\b|\bdebit\b', line, re.IGNORECASE):
            amount = -abs(amount)

        # Extract symbol if present
        symbol_match = re.search(r'\b([A-Z]{1,5})\b', line)
        symbol = symbol_match.group(1) if symbol_match else None

        # Extract quantity if present
        quantity = None
        qty_match = re.search(self.PATTERNS['shares'], line)
        if qty_match:
            quantity = self._to_decimal(qty_match.group(1))

        return CashFlowActivity(
            activity_date=activity_date,
            activity_type=activity_type,
            description=line[:100],  # Truncate long descriptions
            symbol=symbol,
            quantity=quantity,
            amount=amount,
        )

    def _parse_csv_row(self, row: dict) -> Optional[CashFlowActivity]:
        """Parse a CSV row into CashFlowActivity."""
        # Common column name variations
        date_cols = ['Date', 'Trade Date', 'Activity Date', 'Transaction Date']
        desc_cols = ['Description', 'Activity', 'Transaction Type', 'Type']
        amount_cols = ['Amount', 'Net Amount', 'Value', 'Total']
        symbol_cols = ['Symbol', 'Ticker', 'Security']
        qty_cols = ['Quantity', 'Shares', 'Units']

        # Find date
        activity_date = None
        for col in date_cols:
            if col in row and row[col]:
                try:
                    activity_date = self._parse_date(row[col])
                    break
                except ValueError:
                    continue

        if not activity_date:
            return None

        # Find description
        description = ""
        for col in desc_cols:
            if col in row and row[col]:
                description = row[col]
                break

        # Determine activity type from description
        activity_type = self._determine_activity_type(description)

        # Find amount
        amount = Decimal("0")
        for col in amount_cols:
            if col in row and row[col]:
                try:
                    amount = self._to_decimal(row[col])
                    break
                except (ValueError, InvalidOperation):
                    continue

        # Find symbol
        symbol = None
        for col in symbol_cols:
            if col in row and row[col]:
                symbol = row[col].upper()
                break

        # Find quantity
        quantity = None
        for col in qty_cols:
            if col in row and row[col]:
                try:
                    quantity = self._to_decimal(row[col])
                    break
                except (ValueError, InvalidOperation):
                    continue

        # Find price if available
        price = None
        if 'Price' in row and row['Price']:
            try:
                price = self._to_decimal(row['Price'])
            except (ValueError, InvalidOperation):
                pass

        return CashFlowActivity(
            activity_date=activity_date,
            activity_type=activity_type,
            description=description,
            symbol=symbol,
            quantity=quantity,
            price=price,
            amount=amount,
        )

    def _correlate_activities(self) -> None:
        """
        Correlate activities to create vest/sale/dividend records.

        Groups related activities (e.g., vest + tax withholding).
        """
        # Group by date and symbol
        vest_activities = [a for a in self._result.activities
                          if a.activity_type == ActivityType.VEST]
        sale_activities = [a for a in self._result.activities
                          if a.activity_type == ActivityType.SALE]
        dividend_activities = [a for a in self._result.activities
                               if a.activity_type in (ActivityType.DIVIDEND,
                                                      ActivityType.DIVIDEND_REINVEST)]

        # Create RSU vest records
        for vest in vest_activities:
            if vest.quantity and vest.price:
                rsu_vest = RSUVest(
                    grant_number=self._find_grant_number(vest),
                    vest_date=vest.activity_date,
                    shares_vested=vest.quantity,
                    fmv_usd=vest.price,
                    shares_withheld_for_tax=self._find_withheld_shares(vest),
                )
                self._result.rsu_vests.append(rsu_vest)

        # Create sale records
        for sale in sale_activities:
            if sale.quantity and sale.price:
                # Try to match with vest for cost basis
                vest_info = self._find_matching_vest(sale)

                rsu_sale = RSUSale(
                    sell_date=sale.activity_date,
                    shares_sold=sale.quantity,
                    sell_price_usd=sale.price,
                    sell_value_usd=sale.quantity * sale.price,
                    vest_date=vest_info[0] if vest_info else sale.activity_date,
                    cost_basis_per_share_usd=vest_info[1] if vest_info else sale.price,
                    cost_basis_usd=vest_info[1] * sale.quantity if vest_info else sale.price * sale.quantity,
                    fees_usd=abs(sale.fees),
                )
                self._result.rsu_sales.append(rsu_sale)

        # Create dividend records
        for div in dividend_activities:
            if div.symbol:
                # Find withholding for this dividend
                withholding = self._find_dividend_withholding(div)

                dividend = ForeignDividend(
                    dividend_date=div.activity_date,
                    symbol=div.symbol,
                    shares_held=div.quantity or Decimal("0"),
                    dividend_per_share_usd=div.price or Decimal("0"),
                    gross_dividend_usd=abs(div.amount) + withholding,
                    withholding_tax_usd=withholding,
                    net_dividend_usd=abs(div.amount),
                    is_reinvested=div.activity_type == ActivityType.DIVIDEND_REINVEST,
                )
                self._result.dividends.append(dividend)

    def _determine_activity_type(self, text: str) -> ActivityType:
        """Determine activity type from description text."""
        text_lower = text.lower()

        for activity_type, keywords in self.ACTIVITY_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text_lower:
                    return ActivityType[activity_type]

        return ActivityType.TRANSFER  # Default

    def _find_grant_number(self, activity: CashFlowActivity) -> str:
        """Find grant number for an activity."""
        # Check description for grant number pattern
        grant_match = re.search(r'([A-Z0-9]+-[A-Z0-9]+)', activity.description)
        if grant_match:
            return grant_match.group(1)

        # Check stock plans
        for plan in self._result.stock_plan_details:
            if plan.symbol == activity.symbol:
                return plan.grant_number

        return "UNKNOWN"

    def _find_withheld_shares(self, vest: CashFlowActivity) -> Decimal:
        """Find shares withheld for tax for a vest event."""
        # Look for tax withholding on same date
        for activity in self._result.activities:
            if (activity.activity_type == ActivityType.TAX_WITHHOLD and
                activity.activity_date == vest.activity_date and
                activity.symbol == vest.symbol and
                activity.quantity):
                return abs(activity.quantity)

        return Decimal("0")

    def _find_matching_vest(self, sale: CashFlowActivity) -> Optional[Tuple[date, Decimal]]:
        """Find matching vest for a sale (for cost basis)."""
        # Use FIFO matching
        for vest in self._result.rsu_vests:
            if vest.shares_vested >= sale.quantity:
                return (vest.vest_date, vest.fmv_usd)

        return None

    def _find_dividend_withholding(self, dividend: CashFlowActivity) -> Decimal:
        """Find tax withholding for a dividend."""
        # Look for withholding on same date
        for activity in self._result.activities:
            if (activity.activity_type == ActivityType.TAX_WITHHOLD and
                activity.activity_date == dividend.activity_date and
                activity.symbol == dividend.symbol):
                return abs(activity.amount)

        return Decimal("0")

    def _parse_date(self, date_str: str) -> date:
        """Parse date from various formats."""
        date_str = date_str.strip()

        # Common formats
        formats = [
            '%m/%d/%Y',
            '%m-%d-%Y',
            '%d/%m/%Y',
            '%d-%m-%Y',
            '%Y-%m-%d',
            '%m/%d/%y',
            '%m-%d-%y',
            '%b %d, %Y',
            '%B %d, %Y',
        ]

        from datetime import datetime
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        raise ValueError(f"Unable to parse date: {date_str}")

    def _to_decimal(self, value: str) -> Decimal:
        """Convert string to Decimal, handling currency formatting."""
        if not value:
            return Decimal("0")

        # Remove currency symbols and formatting
        cleaned = re.sub(r'[$,\s]', '', str(value))

        # Handle parentheses for negative
        if cleaned.startswith('(') and cleaned.endswith(')'):
            cleaned = '-' + cleaned[1:-1]

        return Decimal(cleaned)


class ETradeParser(MorganStanleyParser):
    """
    Parser for E*TRADE statements.

    E*TRADE uses similar format to Morgan Stanley.
    This class can override specific parsing logic if needed.
    """

    def __init__(self):
        """Initialize E*TRADE parser."""
        super().__init__()

    # E*TRADE specific parsing can be added here
