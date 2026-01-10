"""Zerodha Tax P&L parser."""

import pandas as pd
from pathlib import Path
from datetime import date as date_type
from decimal import Decimal
from typing import Optional
import sqlite3

from .models import StockTrade, TradeType, TradeCategory, ParseResult


class ZerodhaParser:
    """
    Parser for Zerodha Tax P&L Excel.

    Zerodha Tax P&L provides pre-matched trades with capital gains.
    Supports TRADEWISE, SCRIPWISE, SPECULATIVE, and SUMMARY sheets.

    Primary sheet: TRADEWISE (matched buy-sell pairs)
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

            # Parse TRADEWISE sheet (main trade data)
            if 'TRADEWISE' in excel.sheet_names:
                df = pd.read_excel(excel, sheet_name='TRADEWISE')
                trades = self._parse_tradewise(df, result)
                result.trades.extend(trades)
            else:
                result.add_warning("TRADEWISE sheet not found")

            # Parse SPECULATIVE sheet (intraday trades)
            if 'SPECULATIVE' in excel.sheet_names:
                df_spec = pd.read_excel(excel, sheet_name='SPECULATIVE')
                speculative_trades = self._parse_speculative(df_spec, result)
                result.trades.extend(speculative_trades)

            if len(result.trades) == 0:
                result.add_warning("No trades found in file")

        except Exception as e:
            result.add_error(f"Failed to parse Excel: {str(e)}")

        return result

    def _parse_tradewise(self, df: pd.DataFrame, result: ParseResult) -> list[StockTrade]:
        """
        Parse TRADEWISE sheet (delivery trades with pre-matched buy-sell).

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
            return Decimal(str(value))
        except:
            return Decimal("0")

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
