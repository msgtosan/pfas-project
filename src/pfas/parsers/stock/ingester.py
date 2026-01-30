"""Indian Stock Statement Ingester."""

import logging
import hashlib
from pathlib import Path
from decimal import Decimal
from typing import List, Optional, Dict, Any

try:
    import sqlcipher3 as sqlite3
except ImportError:
    import sqlite3

from pfas.services.generic_ingester import GenericAssetIngester, GenericIngestionResult
from .zerodha import ZerodhaParser
from .icici import ICICIDirectParser

# Ledger integration imports
from pfas.core.transaction_service import TransactionService, TransactionSource, AssetRecord
from pfas.parsers.ledger_integration import (
    record_stock_buy,
    record_stock_sell,
)

logger = logging.getLogger(__name__)


class IndianStockIngester(GenericAssetIngester):
    """
    Indian stock statement ingester.

    Supports:
    - Zerodha (holdings, P&L)
    - ICICI Direct (holdings, contract notes)
    - Generic holdings Excel
    """

    def __init__(self, conn: sqlite3.Connection, user_id: int, inbox_path: Path):
        super().__init__(conn, user_id, inbox_path, "Indian-Stocks")
        self.parsers = {
            'Zerodha': ZerodhaParser(conn),
            'ICICIDirect': ICICIDirectParser(conn),
        }

    def get_supported_extensions(self) -> List[str]:
        return ['.xlsx', '.xls', '.csv', '.pdf']

    def detect_source_from_path(self, file_path: Path) -> Optional[str]:
        """Detect broker from file path or name."""
        path_upper = str(file_path).upper()
        name_upper = file_path.name.upper()

        if 'ZERODHA' in path_upper or 'ZERODHA' in name_upper or 'QY' in name_upper:
            return 'Zerodha'
        elif 'ICICIDIRECT' in path_upper or 'ICICID' in name_upper or 'ICICI' in name_upper:
            return 'ICICIDirect'
        elif 'UNLISTED' in name_upper:
            return 'Unlisted'

        return 'Generic'

    def parse_file(self, file_path: Path, source: Optional[str]) -> Dict[str, Any]:
        """Parse stock statement file."""
        result = {'success': False, 'records': [], 'errors': []}

        try:
            parser = self.parsers.get(source)

            if not parser and source in ('Generic', 'Unlisted'):
                # Handle generic Excel files
                result['success'] = True
                result['records'] = self._parse_generic_holdings(file_path)
                return result

            if not parser:
                result['errors'].append(f"No parser available for {source}")
                return result

            # Parse the file (may return ParseResult or list)
            parse_output = parser.parse(file_path)

            # Handle ParseResult objects
            if hasattr(parse_output, 'transactions'):
                records = parse_output.transactions
            elif hasattr(parse_output, 'holdings'):
                records = parse_output.holdings
            elif isinstance(parse_output, list):
                records = parse_output
            else:
                records = []

            result['success'] = True
            result['records'] = records

        except Exception as e:
            result['errors'].append(f"Parse error: {str(e)}")
            logger.exception(f"Failed to parse {file_path}")

        return result

    def _parse_generic_holdings(self, file_path: Path) -> List[Dict]:
        """Parse generic holdings Excel."""
        import pandas as pd

        try:
            df = pd.read_excel(file_path)

            # Try to find standard columns
            records = []
            for _, row in df.iterrows():
                record = {
                    'symbol': row.get('Symbol') or row.get('Stock') or row.get('Company'),
                    'quantity': row.get('Quantity') or row.get('Units') or row.get('Shares'),
                    'buy_price': row.get('Buy Price') or row.get('Avg Cost') or row.get('Purchase Price'),
                    'current_price': row.get('LTP') or row.get('Current Price') or row.get('Market Price'),
                    'source_file': str(file_path)
                }

                if record['symbol']:
                    records.append(record)

            return records

        except Exception as e:
            logger.warning(f"Failed to parse generic holdings: {e}")
            return []

    def save_to_db(self, records: List[Any], source_file: str = "") -> int:
        """
        Save stock holdings/trades to database with double-entry ledger.

        All inserts flow through TransactionService ensuring:
        - Idempotency (duplicate prevention)
        - Audit logging
        - Double-entry accounting (for trades)
        - Atomic transactions

        Args:
            records: List of stock records (holdings or trades)
            source_file: Path to source file for idempotency

        Returns:
            Number of records saved
        """
        if not records:
            return 0

        # Initialize TransactionService for ledger entries
        txn_service = TransactionService(self.conn)
        file_hash = hashlib.sha256(source_file.encode()).hexdigest()[:8]

        inserted = 0
        for row_idx, item in enumerate(records):
            try:
                # Determine if it's a holding or trade
                if 'quantity' in item and 'trade_date' not in item:
                    # It's a holding - holdings don't create journal entries
                    # They represent current state, not transactions
                    symbol = item.get('symbol', '')
                    idempotency_key = f"stock_holding:{file_hash}:{row_idx}:{symbol}"

                    asset_record = AssetRecord(
                        table_name="stock_holdings",
                        data={
                            "user_id": self.user_id,
                            "symbol": symbol,
                            "quantity": item.get('quantity', 0),
                            "buy_price": item.get('buy_price', 0),
                            "current_price": item.get('current_price', 0),
                            "broker": item.get('broker', 'Generic'),
                            "source_file": str(item.get('source_file', source_file)),
                        },
                        on_conflict="REPLACE"
                    )

                    result = txn_service.record_asset_only(
                        user_id=self.user_id,
                        asset_records=[asset_record],
                        idempotency_key=idempotency_key,
                        source=self._get_transaction_source(item.get('broker', 'Generic')),
                        description=f"Stock holding: {symbol}",
                    )

                    if result.result.value == "success":
                        inserted += 1

                else:
                    # It's a trade - record to ledger
                    symbol = item.get('symbol', '')
                    trade_date = item.get('trade_date')
                    txn_type = item.get('transaction_type', 'BUY').upper()
                    quantity = int(item.get('quantity', 0))
                    price = Decimal(str(item.get('price', 0)))
                    amount = Decimal(str(item.get('amount', 0))) or (price * quantity)
                    broker = item.get('broker', 'Generic')
                    trade_id = item.get('trade_id', '')
                    brokerage = Decimal(str(item.get('brokerage', 0)))
                    stt = Decimal(str(item.get('stt', 0)))
                    cost_basis = Decimal(str(item.get('cost_basis', amount)))
                    is_long_term = item.get('is_long_term', False)

                    # Parse trade_date if string
                    if isinstance(trade_date, str):
                        import pandas as pd
                        trade_date = pd.to_datetime(trade_date).date()

                    # Determine source based on broker
                    source = self._get_transaction_source(broker)

                    # Record to ledger (creates journal entries)
                    if txn_type == 'BUY':
                        ledger_result = record_stock_buy(
                            txn_service=txn_service,
                            conn=self.conn,
                            user_id=self.user_id,
                            symbol=symbol,
                            txn_date=trade_date,
                            quantity=quantity,
                            price=price,
                            amount=amount,
                            brokerage=brokerage,
                            source_file=source_file or str(item.get('source_file', '')),
                            row_idx=row_idx,
                            broker=broker,
                            trade_id=trade_id,
                            source=source,
                        )
                    else:  # SELL
                        ledger_result = record_stock_sell(
                            txn_service=txn_service,
                            conn=self.conn,
                            user_id=self.user_id,
                            symbol=symbol,
                            txn_date=trade_date,
                            quantity=quantity,
                            price=price,
                            proceeds=amount,
                            cost_basis=cost_basis,
                            brokerage=brokerage,
                            stt=stt,
                            is_long_term=is_long_term,
                            source_file=source_file or str(item.get('source_file', '')),
                            row_idx=row_idx,
                            broker=broker,
                            trade_id=trade_id,
                            source=source,
                        )

                    # Skip if duplicate in ledger
                    if ledger_result.is_duplicate:
                        logger.debug(f"Duplicate stock transaction skipped: {symbol} {txn_type}")
                        continue

                    # Insert to stock_transactions via AssetRecord for backward compatibility
                    txn_idempotency_key = f"stock_txn:{file_hash}:{row_idx}:{symbol}:{txn_type}"

                    asset_record = AssetRecord(
                        table_name="stock_transactions",
                        data={
                            "user_id": self.user_id,
                            "symbol": symbol,
                            "trade_date": trade_date.isoformat() if hasattr(trade_date, 'isoformat') else str(trade_date),
                            "transaction_type": txn_type,
                            "quantity": quantity,
                            "price": str(price),
                            "amount": str(amount),
                            "broker": broker,
                            "source_file": str(item.get('source_file', source_file)),
                        },
                        on_conflict="IGNORE"
                    )

                    txn_service.record_asset_only(
                        user_id=self.user_id,
                        asset_records=[asset_record],
                        idempotency_key=txn_idempotency_key,
                        source=source,
                        description=f"Stock {txn_type}: {symbol}",
                    )
                    inserted += 1

            except sqlite3.IntegrityError:
                # Duplicate, skip
                pass
            except Exception as e:
                logger.warning(f"Failed to insert record: {e}")
                logger.debug(f"Record data: {item}")

        self.conn.commit()
        return inserted

    def _get_transaction_source(self, broker: str) -> TransactionSource:
        """Get transaction source based on broker."""
        broker_upper = broker.upper()
        if 'ZERODHA' in broker_upper:
            return TransactionSource.PARSER_ZERODHA
        elif 'ICICI' in broker_upper:
            return TransactionSource.PARSER_ICICI
        else:
            return TransactionSource.PARSER_ZERODHA  # Default


def ingest_indian_stocks(
    conn: sqlite3.Connection,
    user_id: int,
    inbox_path: Path,
    force: bool = False
) -> GenericIngestionResult:
    """
    Convenience function to ingest Indian stock statements.

    Args:
        conn: Database connection
        user_id: User ID
        inbox_path: Path to inbox/Indian-Stocks/
        force: If True, reprocess all files

    Returns:
        GenericIngestionResult
    """
    ingester = IndianStockIngester(conn, user_id, inbox_path)
    return ingester.ingest(force)
