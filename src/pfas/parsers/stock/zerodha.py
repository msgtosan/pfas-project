"""Zerodha Tax P&L parser."""

import pandas as pd
from pathlib import Path
from datetime import date as date_type
from decimal import Decimal
from typing import Optional
import sqlite3
import re

from .models import (
    StockTrade,
    StockDividend,
    STTEntry,
    TradeType,
    TradeCategory,
    ParseResult,
    CapitalGainsSummary,
    DividendSummary,
    STTSummary,
)


class ZerodhaParser:
    """
    Parser for Zerodha Tax P&L Excel.

    Zerodha Tax P&L provides pre-matched trades with capital gains.

    Supported formats:
    - New format (2024+): "Tradewise Exits from YYYY-MM-DD" sheet with header at row 14
    - Old format: TRADEWISE, SCRIPWISE, SPECULATIVE sheets

    Column mapping (new format):
    - Symbol, ISIN, Entry Date, Exit Date, Quantity
    - Buy Value, Sell Value, Profit
    - Period of Holding, Fair Market Value, Taxable Profit, Turnover
    """

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize Zerodha parser.

        Args:
            db_connection: Database connection
        """
        self.conn = db_connection

    def parse(self, file_path: Path) -> ParseResult:
        """
        Parse Zerodha Tax P&L Excel file.

        Automatically detects old vs new format based on sheet names.

        Args:
            file_path: Path to Zerodha Tax P&L Excel

        Returns:
            ParseResult with trades

        Examples:
            >>> parser = ZerodhaParser(conn)
            >>> result = parser.parse(Path("taxpnl.xlsx"))
            >>> print(f"Parsed {len(result.trades)} trades")
        """
        file_path = Path(file_path)
        result = ParseResult(success=True, source_file=str(file_path))

        if not file_path.exists():
            result.add_error(f"File not found: {file_path}")
            return result

        try:
            excel = pd.ExcelFile(file_path)

            # Detect format by sheet names
            tradewise_sheet = self._find_tradewise_sheet(excel.sheet_names)

            if tradewise_sheet:
                if tradewise_sheet.startswith('Tradewise Exits'):
                    # New format (2024+)
                    df = pd.read_excel(excel, sheet_name=tradewise_sheet, header=14)
                    trades = self._parse_tradewise_new(df, result)
                    result.trades.extend(trades)
                else:
                    # Old format (TRADEWISE)
                    df = pd.read_excel(excel, sheet_name=tradewise_sheet)
                    trades = self._parse_tradewise_old(df, result)
                    result.trades.extend(trades)
            else:
                result.add_warning("No tradewise sheet found")

            # Parse SPECULATIVE sheet (intraday trades) - old format only
            if 'SPECULATIVE' in excel.sheet_names:
                df_spec = pd.read_excel(excel, sheet_name='SPECULATIVE')
                speculative_trades = self._parse_speculative(df_spec, result)
                result.trades.extend(speculative_trades)

            # Parse Equity Dividends sheet (REQ-STK-003)
            if 'Equity Dividends' in excel.sheet_names:
                df_div = pd.read_excel(excel, sheet_name='Equity Dividends', header=14)
                dividends = self._parse_dividends(df_div, result)
                result.dividends.extend(dividends)

            # Generate STT entries from trades (REQ-STK-006)
            for trade in result.trades:
                if trade.stt > Decimal("0"):
                    stt_entry = STTEntry(
                        trade_date=trade.trade_date,
                        symbol=trade.symbol,
                        trade_type=trade.trade_type,
                        trade_category=trade.trade_category or TradeCategory.DELIVERY,
                        trade_value=trade.amount,
                        stt_amount=trade.stt,
                        source_file=str(file_path),
                    )
                    result.stt_entries.append(stt_entry)

            if len(result.trades) == 0 and len(result.dividends) == 0:
                result.add_warning("No trades or dividends found in file")

        except Exception as e:
            result.add_error(f"Failed to parse Excel: {str(e)}")

        return result

    def _find_tradewise_sheet(self, sheet_names: list[str]) -> Optional[str]:
        """
        Find the tradewise exits sheet.

        Handles both old (TRADEWISE) and new (Tradewise Exits from...) formats.

        Args:
            sheet_names: List of sheet names from Excel file

        Returns:
            Sheet name or None if not found
        """
        # Look for new format first
        for name in sheet_names:
            if name.startswith('Tradewise Exits'):
                return name

        # Fall back to old format
        if 'TRADEWISE' in sheet_names:
            return 'TRADEWISE'

        return None

    def _parse_tradewise_new(self, df: pd.DataFrame, result: ParseResult) -> list[StockTrade]:
        """
        Parse new format Tradewise Exits sheet (2024+).

        Columns: Symbol, ISIN, Entry Date, Exit Date, Quantity, Buy Value,
                 Sell Value, Profit, Period of Holding, Fair Market Value,
                 Taxable Profit, Turnover

        Args:
            df: DataFrame from Tradewise Exits sheet
            result: ParseResult to add warnings to

        Returns:
            List of StockTrade objects (SELL trades only with pre-matched buy info)
        """
        trades = []

        for idx, row in df.iterrows():
            try:
                # Skip empty rows and section headers
                symbol = row.get('Symbol')
                if pd.isna(symbol):
                    continue

                symbol = str(symbol).strip()

                # Skip section markers and repeated headers
                if symbol in ['Symbol', 'Equity', 'Mutual Funds', 'Currency',
                              'Commodity', 'Equity - Buyback', 'F&O']:
                    continue

                # Validate ISIN (must start with INE or INF)
                isin = row.get('ISIN')
                if pd.isna(isin):
                    continue
                isin = str(isin).strip()
                if not (isin.startswith('INE') or isin.startswith('INF')):
                    continue

                # Parse dates
                entry_date = self._parse_date(row.get('Entry Date'))
                exit_date = self._parse_date(row.get('Exit Date'))

                if not exit_date:
                    continue

                # Parse quantities and amounts
                quantity = self._to_int(row.get('Quantity'))
                if quantity <= 0:
                    continue

                buy_value = self._to_decimal(row.get('Buy Value'))
                sell_value = self._to_decimal(row.get('Sell Value'))
                profit = self._to_decimal(row.get('Profit'))
                holding_days = self._to_int(row.get('Period of Holding'))

                # Calculate prices from values
                buy_price = buy_value / quantity if quantity > 0 else Decimal("0")
                sell_price = sell_value / quantity if quantity > 0 else Decimal("0")

                # Determine if long-term (>365 days)
                is_long_term = holding_days > 365 if holding_days else False

                # Create SELL trade with pre-matched buy info
                trade = StockTrade(
                    symbol=symbol,
                    isin=isin,
                    trade_date=exit_date,
                    trade_type=TradeType.SELL,
                    quantity=quantity,
                    price=sell_price,
                    amount=sell_value,
                    net_amount=sell_value,
                    trade_category=TradeCategory.DELIVERY,
                    # Pre-matched buy info
                    buy_date=entry_date,
                    buy_price=buy_price,
                    cost_of_acquisition=buy_value,
                    holding_period_days=holding_days,
                    is_long_term=is_long_term,
                    capital_gain=profit
                )
                trades.append(trade)

            except Exception as e:
                result.add_warning(f"Row {idx}: {str(e)}")
                continue

        return trades

    def _parse_tradewise_old(self, df: pd.DataFrame, result: ParseResult) -> list[StockTrade]:
        """
        Parse old format TRADEWISE sheet (delivery trades with pre-matched buy-sell).

        Expected columns:
        - Symbol, ISIN, Trade Type, Quantity
        - Buy Date, Buy Price, Buy Value
        - Sell Date, Sell Price, Sell Value
        - Profit/Loss, STT

        Args:
            df: DataFrame from TRADEWISE sheet
            result: ParseResult to add warnings to

        Returns:
            List of StockTrade objects
        """
        trades = []

        for idx, row in df.iterrows():
            try:
                # Skip empty rows
                if pd.isna(row.get('Symbol')):
                    continue

                symbol = str(row['Symbol']).strip()
                isin = str(row.get('ISIN', '')).strip() if not pd.isna(row.get('ISIN')) else None
                quantity = int(row['Quantity'])

                # Parse buy trade
                buy_date = self._parse_date(row.get('Buy Date'))
                buy_price = self._to_decimal(row.get('Buy Price'))
                buy_value = self._to_decimal(row.get('Buy Value'))

                if buy_date and buy_price and buy_value:
                    buy_trade = StockTrade(
                        symbol=symbol,
                        isin=isin,
                        trade_date=buy_date,
                        trade_type=TradeType.BUY,
                        quantity=quantity,
                        price=buy_price,
                        amount=buy_value,
                        net_amount=buy_value,
                        trade_category=TradeCategory.DELIVERY
                    )
                    trades.append(buy_trade)

                # Parse sell trade
                sell_date = self._parse_date(row.get('Sell Date'))
                sell_price = self._to_decimal(row.get('Sell Price'))
                sell_value = self._to_decimal(row.get('Sell Value'))
                stt = self._to_decimal(row.get('STT'))
                profit_loss = self._to_decimal(row.get('Profit/Loss'))

                if sell_date and sell_price and sell_value:
                    # Calculate holding period
                    holding_days = (sell_date - buy_date).days if buy_date else None
                    is_long_term = holding_days > 365 if holding_days else False

                    sell_trade = StockTrade(
                        symbol=symbol,
                        isin=isin,
                        trade_date=sell_date,
                        trade_type=TradeType.SELL,
                        quantity=quantity,
                        price=sell_price,
                        amount=sell_value,
                        stt=stt,
                        net_amount=sell_value - stt,
                        trade_category=TradeCategory.DELIVERY,
                        # Pre-matched buy info
                        buy_date=buy_date,
                        buy_price=buy_price,
                        cost_of_acquisition=buy_value,
                        holding_period_days=holding_days,
                        is_long_term=is_long_term,
                        capital_gain=profit_loss
                    )
                    trades.append(sell_trade)

            except Exception as e:
                result.add_warning(f"Row {idx}: {str(e)}")
                continue

        return trades

    def _parse_speculative(self, df: pd.DataFrame, result: ParseResult) -> list[StockTrade]:
        """
        Parse SPECULATIVE sheet (intraday trades).

        Intraday trades are taxed as speculative business income.

        Args:
            df: DataFrame from SPECULATIVE sheet
            result: ParseResult to add warnings to

        Returns:
            List of StockTrade objects
        """
        trades = []

        for idx, row in df.iterrows():
            try:
                # Skip empty rows
                if pd.isna(row.get('Symbol')):
                    continue

                symbol = str(row['Symbol']).strip()
                isin = str(row.get('ISIN', '')).strip() if not pd.isna(row.get('ISIN')) else None
                quantity = int(row['Quantity'])
                trade_date = self._parse_date(row.get('Trade Date', row.get('Date')))

                if not trade_date:
                    continue

                # Buy trade
                buy_price = self._to_decimal(row.get('Buy Price'))
                buy_value = self._to_decimal(row.get('Buy Value'))

                if buy_price and buy_value:
                    buy_trade = StockTrade(
                        symbol=symbol,
                        isin=isin,
                        trade_date=trade_date,
                        trade_type=TradeType.BUY,
                        quantity=quantity,
                        price=buy_price,
                        amount=buy_value,
                        net_amount=buy_value,
                        trade_category=TradeCategory.INTRADAY
                    )
                    trades.append(buy_trade)

                # Sell trade
                sell_price = self._to_decimal(row.get('Sell Price'))
                sell_value = self._to_decimal(row.get('Sell Value'))
                stt = self._to_decimal(row.get('STT'))
                profit_loss = self._to_decimal(row.get('Profit/Loss'))

                if sell_price and sell_value:
                    sell_trade = StockTrade(
                        symbol=symbol,
                        isin=isin,
                        trade_date=trade_date,
                        trade_type=TradeType.SELL,
                        quantity=quantity,
                        price=sell_price,
                        amount=sell_value,
                        stt=stt,
                        net_amount=sell_value - stt,
                        trade_category=TradeCategory.INTRADAY,
                        # Same-day buy
                        buy_date=trade_date,
                        buy_price=buy_price,
                        cost_of_acquisition=buy_value,
                        holding_period_days=0,
                        is_long_term=False,
                        capital_gain=profit_loss
                    )
                    trades.append(sell_trade)

            except Exception as e:
                result.add_warning(f"Speculative row {idx}: {str(e)}")
                continue

        return trades

    def _parse_dividends(self, df: pd.DataFrame, result: ParseResult) -> list[StockDividend]:
        """
        Parse Equity Dividends sheet.

        Columns (after header row 14):
        - Symbol, ISIN, Date, Quantity, Dividend Per Share, Net Dividend Amount

        Note: Zerodha provides net dividend after TDS deduction.
        We estimate TDS as 10% if annual dividend per company > Rs 5000.

        Args:
            df: DataFrame from Equity Dividends sheet
            result: ParseResult to add warnings to

        Returns:
            List of StockDividend objects
        """
        dividends = []

        # Rename columns to match expected format
        if len(df.columns) >= 7:
            df.columns = ['_', 'Symbol', 'ISIN', 'Date', 'Quantity', 'Dividend_Per_Share', 'Net_Amount']

        for idx, row in df.iterrows():
            try:
                symbol = row.get('Symbol')
                if pd.isna(symbol) or str(symbol).strip() == '':
                    continue

                symbol = str(symbol).strip()

                # Skip header row that may appear in data
                if symbol == 'Symbol':
                    continue

                isin = str(row.get('ISIN', '')).strip() if not pd.isna(row.get('ISIN')) else ''

                # Validate ISIN
                if not (isin.startswith('INE') or isin.startswith('INF')):
                    continue

                dividend_date = self._parse_date(row.get('Date'))
                if not dividend_date:
                    continue

                quantity = self._to_int(row.get('Quantity'))
                dividend_per_share = self._to_decimal(row.get('Dividend_Per_Share'))
                net_amount = self._to_decimal(row.get('Net_Amount'))

                # Calculate gross amount (net + estimated TDS)
                # Zerodha provides net amount after TDS
                # TDS is 10% if dividend > Rs 5000 per company per year
                # For simplicity, we use net amount as provided
                gross_amount = net_amount
                tds_amount = Decimal("0")

                dividend = StockDividend(
                    symbol=symbol,
                    isin=isin,
                    dividend_date=dividend_date,
                    quantity=quantity,
                    dividend_per_share=dividend_per_share,
                    gross_amount=gross_amount,
                    tds_amount=tds_amount,
                    net_amount=net_amount,
                )
                dividends.append(dividend)

            except Exception as e:
                result.add_warning(f"Dividend row {idx}: {str(e)}")
                continue

        return dividends

    def calculate_dividend_summary(
        self,
        dividends: list[StockDividend],
        fy: str = "2024-25"
    ) -> DividendSummary:
        """
        Calculate dividend income summary.

        Args:
            dividends: List of StockDividend objects
            fy: Financial year (default: 2024-25)

        Returns:
            DividendSummary with totals
        """
        summary = DividendSummary(financial_year=fy)

        for dividend in dividends:
            summary.add_dividend(dividend)

        return summary

    def calculate_stt_summary(
        self,
        stt_entries: list[STTEntry],
        fy: str = "2024-25"
    ) -> STTSummary:
        """
        Calculate STT payment summary.

        Args:
            stt_entries: List of STTEntry objects
            fy: Financial year (default: 2024-25)

        Returns:
            STTSummary with category-wise totals
        """
        summary = STTSummary(financial_year=fy)

        for entry in stt_entries:
            summary.add_stt(entry)

        return summary

    def _parse_date(self, value) -> Optional[date_type]:
        """
        Parse date from various formats.

        Args:
            value: Date value (string, datetime, or pd.Timestamp)

        Returns:
            date object or None
        """
        if pd.isna(value) or value is None or value == '':
            return None

        if isinstance(value, date_type):
            return value

        if isinstance(value, pd.Timestamp):
            return value.date()

        # Try parsing string
        try:
            return pd.to_datetime(value).date()
        except:
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

    def _to_int(self, value) -> int:
        """
        Convert value to int safely.

        Args:
            value: Numeric value

        Returns:
            int value (0 if invalid)
        """
        if pd.isna(value) or value == '' or value is None:
            return 0

        try:
            return int(float(value))
        except:
            return 0

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

    def save_to_db(self, result: ParseResult, user_id: Optional[int] = None, broker_name: str = "Zerodha") -> int:
        """
        Save parsed stock trades to database.

        Args:
            result: ParseResult from parsing
            user_id: User ID (optional)
            broker_name: Broker name (default: Zerodha)

        Returns:
            Number of trades saved

        Examples:
            >>> result = parser.parse(Path("taxpnl.xlsx"))
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

    def _insert_trade(self, broker_id: int, user_id: Optional[int],
                     trade: StockTrade, source_file: str) -> bool:
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

    def save_dividends_to_db(
        self,
        result: ParseResult,
        user_id: Optional[int] = None,
        broker_name: str = "Zerodha"
    ) -> int:
        """
        Save parsed dividends to database.

        Args:
            result: ParseResult from parsing
            user_id: User ID (optional)
            broker_name: Broker name (default: Zerodha)

        Returns:
            Number of dividends saved
        """
        if not result.success or not result.dividends:
            return 0

        cursor = self.conn.cursor()
        in_transaction = self.conn.in_transaction

        try:
            if not in_transaction:
                cursor.execute("BEGIN IMMEDIATE")

            broker_id = self._get_or_create_broker(broker_name)

            count = 0
            for dividend in result.dividends:
                try:
                    self.conn.execute(
                        """INSERT INTO stock_dividends
                        (user_id, broker_id, symbol, isin, dividend_date, quantity,
                         dividend_per_share, gross_amount, tds_amount, net_amount, source_file)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            user_id,
                            broker_id,
                            dividend.symbol,
                            dividend.isin,
                            dividend.dividend_date.isoformat(),
                            dividend.quantity,
                            str(dividend.dividend_per_share),
                            str(dividend.gross_amount),
                            str(dividend.tds_amount),
                            str(dividend.net_amount),
                            result.source_file
                        )
                    )
                    count += 1
                except sqlite3.IntegrityError:
                    # Duplicate dividend
                    continue

            if not in_transaction:
                self.conn.commit()

            return count

        except Exception as e:
            if not in_transaction:
                self.conn.rollback()
            raise Exception(f"Failed to save dividends: {e}") from e

    def save_stt_to_db(
        self,
        result: ParseResult,
        user_id: Optional[int] = None,
        broker_name: str = "Zerodha"
    ) -> int:
        """
        Save STT entries to database.

        Args:
            result: ParseResult from parsing
            user_id: User ID (optional)
            broker_name: Broker name (default: Zerodha)

        Returns:
            Number of STT entries saved
        """
        if not result.success or not result.stt_entries:
            return 0

        cursor = self.conn.cursor()
        in_transaction = self.conn.in_transaction

        try:
            if not in_transaction:
                cursor.execute("BEGIN IMMEDIATE")

            broker_id = self._get_or_create_broker(broker_name)

            count = 0
            for entry in result.stt_entries:
                try:
                    self.conn.execute(
                        """INSERT INTO stock_stt_ledger
                        (user_id, broker_id, trade_date, symbol, trade_type,
                         trade_category, trade_value, stt_amount, source_file)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            user_id,
                            broker_id,
                            entry.trade_date.isoformat(),
                            entry.symbol,
                            entry.trade_type.value,
                            entry.trade_category.value,
                            str(entry.trade_value),
                            str(entry.stt_amount),
                            entry.source_file
                        )
                    )
                    count += 1
                except sqlite3.IntegrityError:
                    # Duplicate entry
                    continue

            if not in_transaction:
                self.conn.commit()

            return count

        except Exception as e:
            if not in_transaction:
                self.conn.rollback()
            raise Exception(f"Failed to save STT entries: {e}") from e

    def save_all_to_db(
        self,
        result: ParseResult,
        user_id: Optional[int] = None,
        broker_name: str = "Zerodha"
    ) -> dict:
        """
        Save all parsed data (trades, dividends, STT) to database.

        Args:
            result: ParseResult from parsing
            user_id: User ID (optional)
            broker_name: Broker name (default: Zerodha)

        Returns:
            Dictionary with counts: {'trades': N, 'dividends': N, 'stt': N}
        """
        counts = {
            'trades': self.save_to_db(result, user_id, broker_name),
            'dividends': self.save_dividends_to_db(result, user_id, broker_name),
            'stt': self.save_stt_to_db(result, user_id, broker_name),
        }
        return counts
