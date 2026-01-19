"""Bank Statement Ingester."""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

try:
    import sqlcipher3 as sqlite3
except ImportError:
    import sqlite3

from pfas.services.generic_ingester import GenericAssetIngester, GenericIngestionResult
from .icici_excel import ICICIExcelParser
from .icici import ICICIParser
from .hdfc import HDFCParser
from .sbi import SBIParser

logger = logging.getLogger(__name__)


class BankIngester(GenericAssetIngester):
    """
    Bank statement ingester.

    Supports:
    - ICICI Excel (.xls, .xlsx)
    - ICICI PDF
    - HDFC Excel/PDF
    - SBI Excel/PDF
    """

    def __init__(self, conn: sqlite3.Connection, user_id: int, inbox_path: Path):
        super().__init__(conn, user_id, inbox_path, "Bank")
        self.parsers = {
            'ICICI': {'excel': ICICIExcelParser(conn), 'pdf': ICICIParser(conn)},
            'HDFC': {'default': HDFCParser(conn)},
            'SBI': {'default': SBIParser(conn)},
        }

    def get_supported_extensions(self) -> List[str]:
        return ['.xls', '.xlsx', '.pdf', '.csv']

    def detect_source_from_path(self, file_path: Path) -> Optional[str]:
        """Detect bank from file path or name."""
        path_upper = str(file_path).upper()
        name_upper = file_path.name.upper()

        if 'ICICI' in path_upper or 'ICICI' in name_upper:
            return 'ICICI'
        elif 'HDFC' in path_upper or 'HDFC' in name_upper:
            return 'HDFC'
        elif 'SBI' in path_upper or 'SBI' in name_upper:
            return 'SBI'

        return None

    def parse_file(self, file_path: Path, source: Optional[str]) -> Dict[str, Any]:
        """Parse bank statement file."""
        result = {'success': False, 'records': [], 'errors': []}

        if not source:
            result['errors'].append(f"Could not detect bank from filename: {file_path.name}")
            return result

        try:
            # Get appropriate parser
            parser = None
            if source == 'ICICI':
                if file_path.suffix.lower() in ['.xls', '.xlsx']:
                    parser = self.parsers['ICICI']['excel']
                else:
                    parser = self.parsers['ICICI']['pdf']
            else:
                parser = self.parsers.get(source, {}).get('default')

            if not parser:
                result['errors'].append(f"No parser available for {source}")
                return result

            # Parse the file (returns ParseResult object)
            parse_result = parser.parse(file_path)

            # Extract transactions list from ParseResult
            if hasattr(parse_result, 'transactions'):
                transactions = parse_result.transactions
            elif isinstance(parse_result, list):
                transactions = parse_result
            else:
                transactions = []

            result['success'] = True
            result['records'] = transactions

        except Exception as e:
            result['errors'].append(f"Parse error: {str(e)}")
            logger.exception(f"Failed to parse {file_path}")

        return result

    def save_to_db(self, records: List[Any]) -> int:
        """Save bank transactions to database."""
        if not records:
            return 0

        inserted = 0
        for txn in records:
            try:
                # Handle BankTransaction dataclass objects
                if hasattr(txn, 'date'):
                    # It's a BankTransaction object
                    date_val = txn.date.isoformat() if hasattr(txn.date, 'isoformat') else str(txn.date)
                    description = txn.description
                    debit = str(txn.debit)
                    credit = str(txn.credit)
                    balance = str(txn.balance) if txn.balance else None
                    category = txn.category.value if hasattr(txn.category, 'value') else str(txn.category)
                else:
                    # It's a dict
                    date_val = txn.get('date')
                    description = txn.get('description')
                    debit = str(txn.get('debit', 0))
                    credit = str(txn.get('credit', 0))
                    balance = str(txn.get('balance', 0))
                    category = txn.get('category', 'OTHER')

                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO bank_transactions
                    (user_id, account_id, date, description, debit, credit, balance,
                     category, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        self.user_id,
                        1,  # Default to account 1
                        date_val,
                        description,
                        debit,
                        credit,
                        balance,
                        category
                    )
                )
                inserted += 1
            except sqlite3.IntegrityError:
                # Duplicate, skip
                pass
            except Exception as e:
                logger.warning(f"Failed to insert transaction: {e}")
                logger.debug(f"Transaction data: {txn}")

        self.conn.commit()
        return inserted


def ingest_bank_statements(
    conn: sqlite3.Connection,
    user_id: int,
    inbox_path: Path,
    force: bool = False
) -> GenericIngestionResult:
    """
    Convenience function to ingest bank statements.

    Args:
        conn: Database connection
        user_id: User ID
        inbox_path: Path to inbox/Bank/
        force: If True, reprocess all files

    Returns:
        GenericIngestionResult
    """
    ingester = BankIngester(conn, user_id, inbox_path)
    return ingester.ingest(force)
