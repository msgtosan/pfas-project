"""Zerodha Tax P&L parser."""

import pandas as pd
from pathlib import Path
from datetime import date as date_type
from decimal import Decimal
from typing import Optional, List
import sqlite3
import hashlib

from pfas.core.transaction_service import (
    TransactionService,
    TransactionSource,
    AssetRecord,
    IdempotencyKeyGenerator,
)
from pfas.core.journal import JournalEntry
from pfas.core.accounts import get_account_by_code

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
        Save parsed stock trades to database via TransactionService.

        All inserts flow through TransactionService.record() ensuring:
        - Idempotency (duplicate prevention)
        - Audit logging
        - Double-entry accounting (for BUY/SELL trades)
        - Atomic transactions

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

        if user_id is None:
            user_id = 1  # Default user

        # Initialize TransactionService
        txn_service = TransactionService(self.conn)

        # Get or create broker via TransactionService
        broker_id = self._get_or_create_broker_via_service(txn_service, broker_name, user_id)

        # Generate file hash for idempotency keys
        file_hash = self._generate_file_hash(result.source_file)

        count = 0
        for idx, trade in enumerate(result.trades):
            if self._record_trade(txn_service, broker_id, user_id, trade, result.source_file, file_hash, idx):
                count += 1

        return count

    def _generate_file_hash(self, file_path: str) -> str:
        """Generate short hash for file path (for idempotency keys)."""
        return hashlib.sha256(file_path.encode()).hexdigest()[:8]

    def _get_or_create_broker_via_service(
        self,
        txn_service: TransactionService,
        broker_name: str,
        user_id: int
    ) -> int:
        """Get or create stock broker via TransactionService."""
        # Check if broker exists
        cursor = self.conn.execute(
            "SELECT id FROM stock_brokers WHERE name = ?",
            (broker_name,)
        )
        row = cursor.fetchone()

        if row:
            return row['id'] if isinstance(row, dict) else row[0]

        # Create via TransactionService
        idempotency_key = f"broker:{broker_name.lower().replace(' ', '_')}"

        asset_record = AssetRecord(
            table_name="stock_brokers",
            data={"name": broker_name},
            on_conflict="IGNORE"
        )

        result = txn_service.record_asset_only(
            user_id=user_id,
            asset_records=[asset_record],
            idempotency_key=idempotency_key,
            source=TransactionSource.PARSER_ZERODHA,
            description=f"Stock broker: {broker_name}",
        )

        if result.asset_record_ids.get("stock_brokers"):
            return result.asset_record_ids["stock_brokers"]

        # If insert was ignored, fetch existing
        cursor = self.conn.execute(
            "SELECT id FROM stock_brokers WHERE name = ?",
            (broker_name,)
        )
        row = cursor.fetchone()
        return row['id'] if isinstance(row, dict) else row[0] if row else 0

    def _record_trade(
        self,
        txn_service: TransactionService,
        broker_id: int,
        user_id: int,
        trade: StockTrade,
        source_file: str,
        file_hash: str,
        row_idx: int
    ) -> bool:
        """
        Record stock trade via TransactionService with journal entry.

        For BUY trades:
            Dr Indian Stocks (1203)  | Amount
            Cr Bank Account (1101)   | Amount

        For SELL trades:
            Dr Bank Account (1101)   | Proceeds
            Cr Indian Stocks (1203)  | Cost Basis
            Cr/Dr Capital Gains      | Gain/Loss
        """
        # Generate idempotency key
        idempotency_key = f"stock:{file_hash}:{row_idx}:{trade.symbol}:{trade.trade_date.isoformat()}:{trade.quantity}:{trade.trade_type.value}"

        # Create journal entries
        entries = self._create_journal_entries(trade, user_id)

        # Create asset record for stock_trades table
        asset_record = AssetRecord(
            table_name="stock_trades",
            data={
                "broker_id": broker_id,
                "user_id": user_id,
                "symbol": trade.symbol,
                "isin": trade.isin,
                "trade_date": trade.trade_date.isoformat(),
                "trade_type": trade.trade_type.value,
                "quantity": trade.quantity,
                "price": str(trade.price),
                "amount": str(trade.amount),
                "brokerage": str(trade.brokerage),
                "stt": str(trade.stt),
                "exchange_charges": str(trade.exchange_charges),
                "gst": str(trade.gst),
                "sebi_charges": str(trade.sebi_charges),
                "stamp_duty": str(trade.stamp_duty),
                "net_amount": str(trade.net_amount),
                "trade_category": trade.trade_category.value if trade.trade_category else None,
                "buy_date": trade.buy_date.isoformat() if trade.buy_date else None,
                "buy_price": str(trade.buy_price) if trade.buy_price else None,
                "cost_of_acquisition": str(trade.cost_of_acquisition) if trade.cost_of_acquisition else None,
                "holding_period_days": trade.holding_period_days,
                "is_long_term": trade.is_long_term,
                "capital_gain": str(trade.capital_gain) if trade.capital_gain else None,
                "source_file": source_file,
            },
            on_conflict="IGNORE"
        )

        # Record via TransactionService
        description = f"Stock {trade.trade_type.value}: {trade.symbol} x {trade.quantity}"
        result = txn_service.record(
            user_id=user_id,
            entries=entries,
            description=description[:100],
            source=TransactionSource.PARSER_ZERODHA,
            idempotency_key=idempotency_key,
            txn_date=trade.trade_date,
            reference_type=f"STOCK_{trade.trade_type.value}",
            asset_records=[asset_record],
        )

        return result.result.value == "success"

    def _create_journal_entries(self, trade: StockTrade, user_id: int) -> List[JournalEntry]:
        """Create journal entries for stock trade."""
        entries = []

        # Get account IDs
        stock_account = get_account_by_code(self.conn, "1203")  # Indian Stocks
        bank_account = get_account_by_code(self.conn, "1101")  # Bank - Savings

        if not stock_account or not bank_account:
            return entries

        if trade.trade_type == TradeType.BUY:
            # BUY: Dr Stock Asset | Cr Bank
            entries.append(JournalEntry(
                account_id=stock_account.id,
                debit=trade.net_amount,
                narration=f"Buy: {trade.symbol} x {trade.quantity} @ {trade.price}"
            ))
            entries.append(JournalEntry(
                account_id=bank_account.id,
                credit=trade.net_amount,
                narration=f"Payment for stock: {trade.symbol}"
            ))

        elif trade.trade_type == TradeType.SELL:
            # SELL: Dr Bank | Cr Stock Asset | Cr/Dr Capital Gains
            cost_basis = trade.cost_of_acquisition or Decimal("0")
            capital_gain = trade.capital_gain or (trade.net_amount - cost_basis)

            # Dr Bank for proceeds
            entries.append(JournalEntry(
                account_id=bank_account.id,
                debit=trade.net_amount,
                narration=f"Proceeds from sale: {trade.symbol}"
            ))

            # Cr Stock asset for cost basis
            if cost_basis > 0:
                entries.append(JournalEntry(
                    account_id=stock_account.id,
                    credit=cost_basis,
                    narration=f"Cost basis: {trade.symbol} x {trade.quantity}"
                ))

            # Capital gains/loss
            if capital_gain != Decimal("0"):
                # Determine STCG vs LTCG
                cg_code = "4302" if trade.is_long_term else "4301"  # LTCG vs STCG
                cg_account = get_account_by_code(self.conn, cg_code)

                if cg_account:
                    if capital_gain > 0:
                        # Gain: Credit capital gains
                        entries.append(JournalEntry(
                            account_id=cg_account.id,
                            credit=capital_gain,
                            narration=f"{'LTCG' if trade.is_long_term else 'STCG'}: {trade.symbol}"
                        ))
                    else:
                        # Loss: Debit capital gains
                        entries.append(JournalEntry(
                            account_id=cg_account.id,
                            debit=abs(capital_gain),
                            narration=f"{'LTCL' if trade.is_long_term else 'STCL'}: {trade.symbol}"
                        ))

        return entries

    def save_dividends_to_db(
        self,
        result: ParseResult,
        user_id: Optional[int] = None,
        broker_name: str = "Zerodha"
    ) -> int:
        """
        Save parsed dividends to database via TransactionService.

        Creates journal entry:
            Dr Bank Account (1101)       | Net Amount
            Dr TDS Receivable (1601)     | TDS Amount
            Cr Dividend Income (4203)    | Gross Amount

        Args:
            result: ParseResult from parsing
            user_id: User ID (optional)
            broker_name: Broker name (default: Zerodha)

        Returns:
            Number of dividends saved
        """
        if not result.success or not result.dividends:
            return 0

        if user_id is None:
            user_id = 1

        txn_service = TransactionService(self.conn)
        broker_id = self._get_or_create_broker_via_service(txn_service, broker_name, user_id)
        file_hash = self._generate_file_hash(result.source_file)

        count = 0
        for idx, dividend in enumerate(result.dividends):
            if self._record_dividend(txn_service, broker_id, user_id, dividend, result.source_file, file_hash, idx):
                count += 1

        return count

    def _record_dividend(
        self,
        txn_service: TransactionService,
        broker_id: int,
        user_id: int,
        dividend: StockDividend,
        source_file: str,
        file_hash: str,
        row_idx: int
    ) -> bool:
        """Record dividend via TransactionService with journal entry."""
        # Generate idempotency key
        idempotency_key = f"dividend:{file_hash}:{row_idx}:{dividend.symbol}:{dividend.dividend_date.isoformat()}:{dividend.net_amount}"

        # Create journal entries
        entries = []

        bank_account = get_account_by_code(self.conn, "1101")  # Bank - Savings
        tds_account = get_account_by_code(self.conn, "1601")   # TDS Receivable
        dividend_income = get_account_by_code(self.conn, "4203")  # Dividend - Indian

        if bank_account and dividend_income:
            # Dr Bank for net amount
            if dividend.net_amount > Decimal("0"):
                entries.append(JournalEntry(
                    account_id=bank_account.id,
                    debit=dividend.net_amount,
                    narration=f"Dividend received: {dividend.symbol}"
                ))

            # Dr TDS Receivable for TDS
            if tds_account and dividend.tds_amount > Decimal("0"):
                entries.append(JournalEntry(
                    account_id=tds_account.id,
                    debit=dividend.tds_amount,
                    narration=f"TDS on dividend: {dividend.symbol}"
                ))

            # Cr Dividend income for gross
            entries.append(JournalEntry(
                account_id=dividend_income.id,
                credit=dividend.gross_amount,
                narration=f"Dividend from {dividend.symbol}"
            ))

        # Create asset record
        asset_record = AssetRecord(
            table_name="stock_dividends",
            data={
                "user_id": user_id,
                "broker_id": broker_id,
                "symbol": dividend.symbol,
                "isin": dividend.isin,
                "dividend_date": dividend.dividend_date.isoformat(),
                "quantity": dividend.quantity,
                "dividend_per_share": str(dividend.dividend_per_share),
                "gross_amount": str(dividend.gross_amount),
                "tds_amount": str(dividend.tds_amount),
                "net_amount": str(dividend.net_amount),
                "source_file": source_file,
            },
            on_conflict="IGNORE"
        )

        result = txn_service.record(
            user_id=user_id,
            entries=entries,
            description=f"Dividend: {dividend.symbol} - {dividend.net_amount}",
            source=TransactionSource.PARSER_ZERODHA,
            idempotency_key=idempotency_key,
            txn_date=dividend.dividend_date,
            reference_type="STOCK_DIVIDEND",
            asset_records=[asset_record],
        )

        return result.result.value == "success"

    def save_stt_to_db(
        self,
        result: ParseResult,
        user_id: Optional[int] = None,
        broker_name: str = "Zerodha"
    ) -> int:
        """
        Save STT entries to database via TransactionService.

        STT is recorded as expense:
            Dr STT Paid (5xxx)    | STT Amount
            Cr Bank (1101)        | STT Amount

        Note: STT is typically included in net_amount calculation for trades,
        so this ledger entry is for tracking/reporting purposes only.

        Args:
            result: ParseResult from parsing
            user_id: User ID (optional)
            broker_name: Broker name (default: Zerodha)

        Returns:
            Number of STT entries saved
        """
        if not result.success or not result.stt_entries:
            return 0

        if user_id is None:
            user_id = 1

        txn_service = TransactionService(self.conn)
        broker_id = self._get_or_create_broker_via_service(txn_service, broker_name, user_id)
        file_hash = self._generate_file_hash(result.source_file)

        count = 0
        for idx, entry in enumerate(result.stt_entries):
            if self._record_stt(txn_service, broker_id, user_id, entry, file_hash, idx):
                count += 1

        return count

    def _record_stt(
        self,
        txn_service: TransactionService,
        broker_id: int,
        user_id: int,
        entry: STTEntry,
        file_hash: str,
        row_idx: int
    ) -> bool:
        """Record STT entry via TransactionService."""
        # Generate idempotency key
        idempotency_key = f"stt:{file_hash}:{row_idx}:{entry.symbol}:{entry.trade_date.isoformat()}:{entry.stt_amount}"

        # STT is recorded as part of trade, so we only create asset record here
        # (journal entry for STT is embedded in trade's net_amount calculation)

        asset_record = AssetRecord(
            table_name="stock_stt_ledger",
            data={
                "user_id": user_id,
                "broker_id": broker_id,
                "trade_date": entry.trade_date.isoformat(),
                "symbol": entry.symbol,
                "trade_type": entry.trade_type.value,
                "trade_category": entry.trade_category.value,
                "trade_value": str(entry.trade_value),
                "stt_amount": str(entry.stt_amount),
                "source_file": entry.source_file,
            },
            on_conflict="IGNORE"
        )

        # Record asset-only (STT amounts already included in trade journal entries)
        result = txn_service.record_asset_only(
            user_id=user_id,
            asset_records=[asset_record],
            idempotency_key=idempotency_key,
            source=TransactionSource.PARSER_ZERODHA,
            description=f"STT: {entry.symbol} - {entry.stt_amount}",
        )

        return result.result.value == "success"

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
