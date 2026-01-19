"""
Multi-Asset Ingester for EPF, NPS, PPF, SGB, FD-Bonds, USA-Stocks.
"""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

try:
    import sqlcipher3 as sqlite3
except ImportError:
    import sqlite3

from pfas.services.generic_ingester import GenericAssetIngester, GenericIngestionResult

logger = logging.getLogger(__name__)


class EPFIngester(GenericAssetIngester):
    """EPF passbook ingester."""

    def __init__(self, conn: sqlite3.Connection, user_id: int, inbox_path: Path):
        super().__init__(conn, user_id, inbox_path, "EPF")

    def get_supported_extensions(self) -> List[str]:
        return ['.pdf', '.xlsx', '.xls']

    def detect_source_from_path(self, file_path: Path) -> Optional[str]:
        return 'EPFO'

    def parse_file(self, file_path: Path, source: Optional[str]) -> Dict[str, Any]:
        result = {'success': False, 'records': [], 'errors': [], 'warnings': []}
        try:
            from pfas.parsers.epf.epf import EPFParser
            parser = EPFParser(self.conn)
            parse_output = parser.parse(file_path)

            # Check if parser reported failure
            if hasattr(parse_output, 'success') and not parse_output.success:
                result['errors'].extend(getattr(parse_output, 'errors', ['Parse failed']))
                result['warnings'].extend(getattr(parse_output, 'warnings', []))
                return result

            # Handle ParseResult objects
            if hasattr(parse_output, 'transactions'):
                transactions = parse_output.transactions
            elif isinstance(parse_output, list):
                transactions = parse_output
            else:
                transactions = []

            # CRITICAL: Only mark success if we have actual data
            if not transactions:
                result['errors'].append("No transactions extracted from EPF file")
                result['warnings'].extend(getattr(parse_output, 'warnings', []))
                return result

            result['success'] = True
            result['records'] = transactions
            result['warnings'].extend(getattr(parse_output, 'warnings', []))
        except Exception as e:
            logger.exception(f"EPF parse error for {file_path}")
            result['errors'].append(f"EPF parse exception: {str(e)}")
        return result

    def save_to_db(self, records: List[Any]) -> int:
        # EPF parser handles DB insertion
        return len(records) if isinstance(records, list) else 0


class NPSIngester(GenericAssetIngester):
    """NPS statement ingester."""

    def __init__(self, conn: sqlite3.Connection, user_id: int, inbox_path: Path):
        super().__init__(conn, user_id, inbox_path, "NPS")

    def get_supported_extensions(self) -> List[str]:
        return ['.csv', '.xlsx', '.xls']  # NPS uses CSV primarily

    def detect_source_from_path(self, file_path: Path) -> Optional[str]:
        return 'NPS'

    def parse_file(self, file_path: Path, source: Optional[str]) -> Dict[str, Any]:
        result = {'success': False, 'records': [], 'errors': [], 'warnings': []}
        try:
            from pfas.parsers.nps.nps import NPSParser
            parser = NPSParser(self.conn)
            parse_output = parser.parse(file_path)

            # Check if parser reported failure
            if hasattr(parse_output, 'success') and not parse_output.success:
                result['errors'].extend(getattr(parse_output, 'errors', ['Parse failed']))
                result['warnings'].extend(getattr(parse_output, 'warnings', []))
                return result

            # Handle ParseResult objects
            if hasattr(parse_output, 'transactions'):
                transactions = parse_output.transactions
            elif isinstance(parse_output, list):
                transactions = parse_output
            else:
                transactions = []

            # CRITICAL: Only mark success if we have actual data
            if not transactions:
                result['errors'].append("No transactions extracted from NPS file")
                result['warnings'].extend(getattr(parse_output, 'warnings', []))
                return result

            result['success'] = True
            result['records'] = transactions
            result['warnings'].extend(getattr(parse_output, 'warnings', []))
        except Exception as e:
            logger.exception(f"NPS parse error for {file_path}")
            result['errors'].append(f"NPS parse exception: {str(e)}")
        return result

    def save_to_db(self, records: List[Any]) -> int:
        return len(records) if isinstance(records, list) else 0


class PPFIngester(GenericAssetIngester):
    """PPF passbook ingester."""

    def __init__(self, conn: sqlite3.Connection, user_id: int, inbox_path: Path):
        super().__init__(conn, user_id, inbox_path, "PPF")

    def get_supported_extensions(self) -> List[str]:
        return ['.xlsx', '.xls', '.csv']  # PPF uses Excel/CSV primarily

    def detect_source_from_path(self, file_path: Path) -> Optional[str]:
        return 'PPF'

    def parse_file(self, file_path: Path, source: Optional[str]) -> Dict[str, Any]:
        result = {'success': False, 'records': [], 'errors': [], 'warnings': []}
        try:
            from pfas.parsers.ppf.ppf import PPFParser
            parser = PPFParser(self.conn)
            # PPF parser requires account_number - try to extract from filename
            account_number = self._extract_account_number(file_path)
            parse_output = parser.parse(file_path, account_number=account_number)

            # Check if parser reported failure
            if hasattr(parse_output, 'success') and not parse_output.success:
                result['errors'].extend(getattr(parse_output, 'errors', ['Parse failed']))
                result['warnings'].extend(getattr(parse_output, 'warnings', []))
                return result

            # Handle ParseResult objects
            if hasattr(parse_output, 'transactions'):
                transactions = parse_output.transactions
            elif isinstance(parse_output, list):
                transactions = parse_output
            else:
                transactions = []

            # CRITICAL: Only mark success if we have actual data
            if not transactions:
                result['errors'].append("No transactions extracted from PPF file")
                result['warnings'].extend(getattr(parse_output, 'warnings', []))
                return result

            result['success'] = True
            result['records'] = transactions
            result['warnings'].extend(getattr(parse_output, 'warnings', []))
        except Exception as e:
            logger.exception(f"PPF parse error for {file_path}")
            result['errors'].append(f"PPF parse exception: {str(e)}")
        return result

    def _extract_account_number(self, file_path: Path) -> str:
        """Extract account number from filename or return default."""
        import re
        # Try to find account number pattern in filename
        # e.g., PPF_12345678_statement.xlsx
        match = re.search(r'PPF[_-]?(\d{8,12})', file_path.name, re.IGNORECASE)
        if match:
            return match.group(1)
        return f"PPF-{file_path.stem}"

    def save_to_db(self, records: List[Any]) -> int:
        return len(records) if isinstance(records, list) else 0


class SGBIngester(GenericAssetIngester):
    """Sovereign Gold Bond ingester."""

    def __init__(self, conn: sqlite3.Connection, user_id: int, inbox_path: Path):
        super().__init__(conn, user_id, inbox_path, "SGB")

    def get_supported_extensions(self) -> List[str]:
        return ['.pdf', '.xlsx', '.xls']

    def detect_source_from_path(self, file_path: Path) -> Optional[str]:
        return 'SGB'

    def parse_file(self, file_path: Path, source: Optional[str]) -> Dict[str, Any]:
        result = {'success': False, 'records': [], 'errors': [], 'warnings': []}
        try:
            from pfas.parsers.assets.sgb import SGBParser
            # Try with conn, fall back to no-arg constructor
            try:
                parser = SGBParser(self.conn)
            except TypeError:
                parser = SGBParser()

            parse_output = parser.parse(file_path)

            # Check if parser reported failure
            if hasattr(parse_output, 'success') and not parse_output.success:
                result['errors'].extend(getattr(parse_output, 'errors', ['Parse failed']))
                result['warnings'].extend(getattr(parse_output, 'warnings', []))
                return result

            # Handle ParseResult objects
            if hasattr(parse_output, 'holdings'):
                holdings = parse_output.holdings
            elif isinstance(parse_output, list):
                holdings = parse_output
            else:
                holdings = []

            # CRITICAL: Only mark success if we have actual data
            if not holdings:
                result['errors'].append("No holdings extracted from SGB file")
                result['warnings'].extend(getattr(parse_output, 'warnings', []))
                return result

            result['success'] = True
            result['records'] = holdings
            result['warnings'].extend(getattr(parse_output, 'warnings', []))
        except Exception as e:
            logger.exception(f"SGB parse error for {file_path}")
            result['errors'].append(f"SGB parse exception: {str(e)}")
        return result

    def save_to_db(self, records: List[Any]) -> int:
        return len(records)


class USAStockIngester(GenericAssetIngester):
    """USA Stock ingester (Morgan Stanley, E-Trade)."""

    def __init__(self, conn: sqlite3.Connection, user_id: int, inbox_path: Path):
        super().__init__(conn, user_id, inbox_path, "USA-Stocks")

    def get_supported_extensions(self) -> List[str]:
        return ['.pdf', '.xlsx', '.xls', '.csv']

    def detect_source_from_path(self, file_path: Path) -> Optional[str]:
        name_upper = file_path.name.upper()
        if 'MORGAN' in name_upper or 'MS' in name_upper:
            return 'MorganStanley'
        elif 'ETRADE' in name_upper:
            return 'ETrade'
        return 'Generic'

    def parse_file(self, file_path: Path, source: Optional[str]) -> Dict[str, Any]:
        result = {'success': False, 'records': [], 'errors': [], 'warnings': []}
        try:
            from pfas.parsers.foreign.morgan_stanley import MorganStanleyParser
            # Try with conn, fall back to no-arg constructor
            try:
                parser = MorganStanleyParser(self.conn)
            except TypeError:
                parser = MorganStanleyParser()

            parse_output = parser.parse(file_path)

            # Check if parser reported failure
            if hasattr(parse_output, 'success') and not parse_output.success:
                result['errors'].extend(getattr(parse_output, 'errors', ['Parse failed']))
                result['warnings'].extend(getattr(parse_output, 'warnings', []))
                return result

            # Handle ParseResult objects
            if hasattr(parse_output, 'transactions'):
                transactions = parse_output.transactions
            elif isinstance(parse_output, list):
                transactions = parse_output
            else:
                transactions = []

            # CRITICAL: Only mark success if we have actual data
            if not transactions:
                result['errors'].append("No transactions extracted from USA-Stocks file")
                result['warnings'].extend(getattr(parse_output, 'warnings', []))
                return result

            result['success'] = True
            result['records'] = transactions
            result['warnings'].extend(getattr(parse_output, 'warnings', []))
        except Exception as e:
            logger.exception(f"USA-Stocks parse error for {file_path}")
            result['errors'].append(f"USA-Stocks parse exception: {str(e)}")
        return result

    def save_to_db(self, records: List[Any]) -> int:
        return len(records)


class FDBondsIngester(GenericAssetIngester):
    """Fixed Deposit and Bonds ingester."""

    def __init__(self, conn: sqlite3.Connection, user_id: int, inbox_path: Path):
        super().__init__(conn, user_id, inbox_path, "FD-Bonds")

    def get_supported_extensions(self) -> List[str]:
        return ['.pdf', '.xlsx', '.xls']

    def detect_source_from_path(self, file_path: Path) -> Optional[str]:
        name_upper = file_path.name.upper()
        if 'FD' in name_upper or 'FIXED' in name_upper:
            return 'FD'
        elif 'BOND' in name_upper:
            return 'Bond'
        return 'Generic'

    def parse_file(self, file_path: Path, source: Optional[str]) -> Dict[str, Any]:
        # FD-Bonds parsing not yet fully implemented - return proper failure
        result = {'success': False, 'records': [], 'errors': [], 'warnings': []}
        result['warnings'].append("FD-Bonds parsing not yet fully implemented - file skipped")
        return result

    def save_to_db(self, records: List[Any]) -> int:
        return 0


# Convenience functions
def ingest_epf(conn: sqlite3.Connection, user_id: int, inbox_path: Path, force: bool = False) -> GenericIngestionResult:
    return EPFIngester(conn, user_id, inbox_path).ingest(force)

def ingest_nps(conn: sqlite3.Connection, user_id: int, inbox_path: Path, force: bool = False) -> GenericIngestionResult:
    return NPSIngester(conn, user_id, inbox_path).ingest(force)

def ingest_ppf(conn: sqlite3.Connection, user_id: int, inbox_path: Path, force: bool = False) -> GenericIngestionResult:
    return PPFIngester(conn, user_id, inbox_path).ingest(force)

def ingest_sgb(conn: sqlite3.Connection, user_id: int, inbox_path: Path, force: bool = False) -> GenericIngestionResult:
    return SGBIngester(conn, user_id, inbox_path).ingest(force)

def ingest_usa_stocks(conn: sqlite3.Connection, user_id: int, inbox_path: Path, force: bool = False) -> GenericIngestionResult:
    return USAStockIngester(conn, user_id, inbox_path).ingest(force)

def ingest_fd_bonds(conn: sqlite3.Connection, user_id: int, inbox_path: Path, force: bool = False) -> GenericIngestionResult:
    return FDBondsIngester(conn, user_id, inbox_path).ingest(force)
