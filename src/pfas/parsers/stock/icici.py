"""ICICI Direct Capital Gains parser."""

import pandas as pd
from pathlib import Path
from datetime import datetime, date as date_type
from decimal import Decimal
from typing import Optional
import sqlite3

from .models import StockTrade, TradeType, TradeCategory, ParseResult, CapitalGainsSummary


class ICICIDirectParser:
    """
    Parser for ICICI Direct Capital Gains CSV reports.

    ICICI Direct provides capital gains reports with pre-matched buy-sell pairs,
    separated into STCG and LTCG sections.

    File Structure:
    - Row 0-2: Account info (Account, Name, Capital Gain year)
    - Row 3: Column headers
    - Row 4+: Data with section markers

    Section Markers:
    - "Short Term Capital Gain (STT paid)" - STCG section
    - "Long Term Capital Gain (STT paid)" - LTCG section
    - "Total" - Section totals (skip)
    - "Grand Total" - File totals (skip)
    - "Note:" - Footer notes (skip)
    """

    # Section markers to identify STCG/LTCG sections
    STCG_MARKER = "Short Term Capital Gain (STT paid)"
    LTCG_MARKER = "Long Term Capital Gain (STT paid)"
    SKIP_MARKERS = ["Total", "Grand Total", "Note:"]

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize ICICI Direct parser.

        Args:
            db_connection: Database connection
        """
        self.conn = db_connection

    def parse(self, file_path: Path) -> ParseResult:
        """
        Parse ICICI Direct Capital Gains CSV file.

        Args:
            file_path: Path to ICICI Direct Capital Gains CSV

        Returns:
            ParseResult with trades

        Examples:
            >>> parser = ICICIDirectParser(conn)
            >>> result = parser.parse(Path("ICICIDirect_FY24.csv"))
            >>> print(f"Parsed {len(result.trades)} trades")
        """
        file_path = Path(file_path)
        result = ParseResult(success=True, source_file=str(file_path))

        if not file_path.exists():
            result.add_error(f"File not found: {file_path}")
            return result

        suffix = file_path.suffix.lower()
        if suffix != '.csv':
            result.add_error(f"Unsupported file format: {suffix}. Expected .csv")
            return result

        try:
            # Read CSV with header at row 3
            df = pd.read_csv(file_path, header=3)
            trades = self._parse_csv(df, result)
            result.trades.extend(trades)

            if len(result.trades) == 0:
                result.add_warning("No trades found in file")

        except Exception as e:
            result.add_error(f"Failed to parse CSV: {str(e)}")

        return result

    def _parse_csv(self, df: pd.DataFrame, result: ParseResult) -> list[StockTrade]:
        """
        Parse CSV data into StockTrade objects.

        Handles section markers to determine STCG vs LTCG trades.

        Args:
            df: DataFrame from CSV
            result: ParseResult to add warnings to

        Returns:
            List of StockTrade objects
        """
        trades = []
        current_section = None  # 'STCG' or 'LTCG'

        for idx, row in df.iterrows():
            try:
                stock_symbol = row.get('Stock Symbol')

                # Skip empty rows
                if pd.isna(stock_symbol):
                    continue

                stock_symbol = str(stock_symbol).strip()

                # Check for section markers
                if self.STCG_MARKER in stock_symbol:
                    current_section = 'STCG'
                    continue
                elif self.LTCG_MARKER in stock_symbol:
                    current_section = 'LTCG'
                    continue

                # Skip total rows and notes
                if any(marker in stock_symbol for marker in self.SKIP_MARKERS):
                    continue

                # Skip rows without ISIN (section headers, notes)
                isin = row.get('ISIN')
                if pd.isna(isin):
                    continue

                # Parse trade data
                trade = self._parse_trade_row(row, current_section, result, idx)
                if trade:
                    trades.append(trade)

            except Exception as e:
                result.add_warning(f"Row {idx}: {str(e)}")
                continue

        return trades

    def _parse_trade_row(
        self,
        row: pd.Series,
        section: Optional[str],
        result: ParseResult,
        idx: int
    ) -> Optional[StockTrade]:
        """
        Parse a single trade row.

        Creates a SELL trade with pre-matched buy information.

        Args:
            row: DataFrame row
            section: Current section ('STCG' or 'LTCG')
            result: ParseResult for warnings
            idx: Row index for error messages

        Returns:
            StockTrade object or None if invalid
        """
        try:
            symbol = str(row['Stock Symbol']).strip()
            isin = str(row['ISIN']).strip()
            quantity = int(float(row['Qty']))

            # Parse dates
            sale_date = self._parse_date(row.get('Sale Date'))
            purchase_date = self._parse_date(row.get('Purchase Date'))

            if not sale_date:
                result.add_warning(f"Row {idx}: Invalid sale date")
                return None

            # Parse amounts
            sale_rate = self._to_decimal(row.get('Sale Rate'))
            sale_value = self._to_decimal(row.get('Sale Value'))
            sale_expenses = self._to_decimal(row.get('Sale Expenses'))

            purchase_rate = self._to_decimal(row.get('Purchase Rate'))
            purchase_value = self._to_decimal(row.get('Purchase Value'))
            purchase_expenses = self._to_decimal(row.get('Purchase Expenses'))

            profit_loss = self._to_decimal(row.get('Profit/Loss(-)'))

            # Grandfathering for LTCG
            fmv_31jan2018 = self._to_decimal(row.get('Price as on 31st Jan 2018'))
            purchase_price_considered = self._to_decimal(row.get('Purchase Price Considered'))

            # Determine if long term based on section
            is_long_term = section == 'LTCG'

            # Calculate holding period
            holding_days = None
            if sale_date and purchase_date:
                holding_days = (sale_date - purchase_date).days

            # Calculate cost of acquisition
            # For LTCG with grandfathering, use purchase_price_considered if available
            if purchase_price_considered and purchase_price_considered > 0:
                cost_of_acquisition = purchase_price_considered * quantity
            else:
                cost_of_acquisition = purchase_value + purchase_expenses

            # Net amount = Sale Value - Sale Expenses
            net_amount = sale_value - sale_expenses

            trade = StockTrade(
                symbol=symbol,
                isin=isin,
                trade_date=sale_date,
                trade_type=TradeType.SELL,
                quantity=quantity,
                price=sale_rate,
                amount=sale_value,
                brokerage=sale_expenses,  # ICICI bundles all charges
                stt=Decimal("0"),  # Included in sale_expenses
                net_amount=net_amount,
                trade_category=TradeCategory.DELIVERY,
                # Pre-matched buy info
                buy_date=purchase_date,
                buy_price=purchase_rate,
                cost_of_acquisition=cost_of_acquisition,
                holding_period_days=holding_days,
                is_long_term=is_long_term,
                capital_gain=profit_loss
            )

            return trade

        except Exception as e:
            result.add_warning(f"Row {idx}: Failed to parse - {str(e)}")
            return None

    def _parse_date(self, value) -> Optional[date_type]:
        """
        Parse date from ICICI format (DD-MMM-YY).

        Args:
            value: Date value (string or datetime)

        Returns:
            date object or None

        Examples:
            >>> parser._parse_date("21-May-24")
            date(2024, 5, 21)
        """
        if pd.isna(value) or value is None or value == '':
            return None

        if isinstance(value, date_type):
            return value

        if isinstance(value, datetime):
            return value.date()

        if isinstance(value, pd.Timestamp):
            return value.date()

        # Parse DD-MMM-YY format
        try:
            dt = datetime.strptime(str(value).strip(), "%d-%b-%y")
            return dt.date()
        except ValueError:
            pass

        # Try other common formats
        for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]:
            try:
                dt = datetime.strptime(str(value).strip(), fmt)
                return dt.date()
            except ValueError:
                continue

        return None

    def _to_decimal(self, value) -> Decimal:
        """
        Convert value to Decimal safely.

        Args:
            value: Numeric value (float, int, string, or None)

        Returns:
            Decimal value (0 if invalid)
        """
        if pd.isna(value) or value == '' or value is None:
            return Decimal("0")

        try:
            # Handle string with commas
            if isinstance(value, str):
                value = value.replace(',', '')
            return Decimal(str(value))
        except:
            return Decimal("0")

    def calculate_capital_gains(
        self,
        trades: list[StockTrade],
        fy: str = "2024-25"
    ) -> CapitalGainsSummary:
        """
        Calculate capital gains summary from trades.

        Args:
            trades: List of StockTrade objects
            fy: Financial year (default: 2024-25)

        Returns:
            CapitalGainsSummary with STCG/LTCG totals
        """
        summary = CapitalGainsSummary(
            financial_year=fy,
            trade_category=TradeCategory.DELIVERY
        )

        for trade in trades:
            if trade.trade_type != TradeType.SELL:
                continue

            if trade.capital_gain is None:
                continue

            if trade.is_long_term:
                summary.ltcg_amount += trade.capital_gain
            else:
                summary.stcg_amount += trade.capital_gain

        summary.calculate_taxable_amounts()
        return summary

    def save_to_db(
        self,
        result: ParseResult,
        user_id: Optional[int] = None,
        broker_name: str = "ICICI Direct"
    ) -> int:
        """
        Save parsed stock trades to database.

        Args:
            result: ParseResult from parsing
            user_id: User ID (optional)
            broker_name: Broker name (default: ICICI Direct)

        Returns:
            Number of trades saved

        Examples:
            >>> result = parser.parse(Path("capital_gains.csv"))
            >>> count = parser.save_to_db(result, user_id=1)
            >>> print(f"Saved {count} trades")
        """
        if not result.success or not result.trades:
            return 0

        cursor = self.conn.cursor()
        in_transaction = self.conn.in_transaction

        try:
            if not in_transaction:
                cursor.execute("BEGIN IMMEDIATE")

            # Get or create broker
            broker_id = self._get_or_create_broker(broker_name)

            count = 0
            for trade in result.trades:
                if self._insert_trade(broker_id, user_id, trade, result.source_file):
                    count += 1

            if not in_transaction:
                self.conn.commit()

            return count

        except Exception as e:
            if not in_transaction:
                self.conn.rollback()
            raise Exception(f"Failed to save stock trades: {e}") from e

    def _get_or_create_broker(self, broker_name: str) -> int:
        """Get or create stock broker and return ID."""
        cursor = self.conn.execute(
            "SELECT id FROM stock_brokers WHERE name = ?",
            (broker_name,)
        )
        row = cursor.fetchone()

        if row:
            return row['id']

        cursor = self.conn.execute(
            "INSERT INTO stock_brokers (name) VALUES (?)",
            (broker_name,)
        )
        return cursor.lastrowid

    def _insert_trade(
        self,
        broker_id: int,
        user_id: Optional[int],
        trade: StockTrade,
        source_file: str
    ) -> bool:
        """Insert stock trade into database."""
        try:
            self.conn.execute(
                """INSERT INTO stock_trades
                (broker_id, user_id, symbol, isin, trade_date, trade_type, quantity,
                 price, amount, brokerage, stt, exchange_charges, gst, sebi_charges,
                 stamp_duty, net_amount, trade_category, buy_date, buy_price,
                 cost_of_acquisition, holding_period_days, is_long_term, capital_gain,
                 source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    broker_id,
                    user_id,
                    trade.symbol,
                    trade.isin,
                    trade.trade_date.isoformat(),
                    trade.trade_type.value,
                    trade.quantity,
                    str(trade.price),
                    str(trade.amount),
                    str(trade.brokerage),
                    str(trade.stt),
                    str(trade.exchange_charges),
                    str(trade.gst),
                    str(trade.sebi_charges),
                    str(trade.stamp_duty),
                    str(trade.net_amount),
                    trade.trade_category.value if trade.trade_category else None,
                    trade.buy_date.isoformat() if trade.buy_date else None,
                    str(trade.buy_price) if trade.buy_price else None,
                    str(trade.cost_of_acquisition) if trade.cost_of_acquisition else None,
                    trade.holding_period_days,
                    trade.is_long_term,
                    str(trade.capital_gain) if trade.capital_gain else None,
                    source_file
                )
            )
            return True
        except sqlite3.IntegrityError:
            # Duplicate trade
            return False
