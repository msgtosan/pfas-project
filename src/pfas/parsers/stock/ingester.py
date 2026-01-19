"""Indian Stock Statement Ingester."""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

try:
    import sqlcipher3 as sqlite3
except ImportError:
    import sqlite3

from pfas.services.generic_ingester import GenericAssetIngester, GenericIngestionResult
from .zerodha import ZerodhaParser
from .icici import ICICIDirectParser

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

    def save_to_db(self, records: List[Any]) -> int:
        """Save stock holdings/trades to database."""
        if not records:
            return 0

        inserted = 0
        for item in records:
            try:
                # Determine if it's a holding or trade
                if 'quantity' in item:
                    # It's a holding
                    self.conn.execute(
                        """
                        INSERT OR REPLACE INTO stock_holdings
                        (user_id, symbol, quantity, buy_price, current_price,
                         broker, source_file, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """,
                        (
                            self.user_id,
                            item.get('symbol'),
                            item.get('quantity', 0),
                            item.get('buy_price', 0),
                            item.get('current_price', 0),
                            item.get('broker', 'Generic'),
                            str(item.get('source_file', ''))
                        )
                    )
                else:
                    # It's a trade
                    self.conn.execute(
                        """
                        INSERT OR IGNORE INTO stock_transactions
                        (user_id, symbol, trade_date, transaction_type, quantity,
                         price, amount, broker, source_file, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """,
                        (
                            self.user_id,
                            item.get('symbol'),
                            item.get('trade_date'),
                            item.get('transaction_type', 'BUY'),
                            item.get('quantity', 0),
                            item.get('price', 0),
                            item.get('amount', 0),
                            item.get('broker', 'Generic'),
                            str(item.get('source_file', ''))
                        )
                    )

                inserted += 1

            except sqlite3.IntegrityError:
                # Duplicate, skip
                pass
            except Exception as e:
                logger.warning(f"Failed to insert record: {e}")

        self.conn.commit()
        return inserted


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
