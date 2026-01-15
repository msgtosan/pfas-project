"""Karvy (KFintech) Consolidated Account Statement parser."""

import pandas as pd
from pathlib import Path
from datetime import datetime, date as date_type
from decimal import Decimal
from typing import List, Optional
import re
import logging
import warnings

# Handle both SQLCipher and regular sqlite3
try:
    import sqlcipher3 as sqlite3
    from sqlcipher3.dbapi2 import IntegrityError as SQLCipherIntegrityError
except ImportError:
    import sqlite3
    SQLCipherIntegrityError = sqlite3.IntegrityError  # Fallback to same type

from .models import MFTransaction, MFScheme, AssetClass, TransactionType, ParseResult
from .classifier import classify_scheme

logger = logging.getLogger(__name__)


class KarvyParser:
    """
    Parser for Karvy/KFintech CAS statements (Excel format).

    KFintech (formerly Karvy) provides consolidated account statements
    for mutual funds. This parser extracts transaction data, capital gains,
    and scheme information from Karvy Excel files.

    Supported formats:
    - Excel (.xlsx, .xls) with Trasaction_Details sheet (note: sheet name has typo)
    - PDF (not yet implemented)

    File Structure Notes:
    - Karvy Capital Gains Excel files have headers at row 5 (0-indexed: 4)
    - First 4 rows contain name, PAN, section headers
    - Sheet name is typically 'Trasaction_Details' (with typo)
    """

    def __init__(self, db_connection: sqlite3.Connection, master_key: bytes = None):
        """
        Initialize Karvy parser.

        Args:
            db_connection: Database connection
            master_key: Encryption key (not used for MF data)
        """
        self.conn = db_connection
        self.master_key = master_key or b"default_key_32_bytes_long_here"
        self._duplicate_count = 0

    def parse(self, file_path: Path, password: Optional[str] = None) -> ParseResult:
        """
        Parse Karvy CAS file.

        Args:
            file_path: Path to Karvy CAS file (Excel or PDF)
            password: Password for encrypted PDFs (optional)

        Returns:
            ParseResult with transactions
        """
        file_path = Path(file_path)

        if not file_path.exists():
            result = ParseResult(success=False, source_file=str(file_path))
            result.add_error(f"File not found: {file_path}")
            return result

        try:
            if file_path.suffix.lower() in ['.xlsx', '.xls']:
                return self._parse_excel(file_path)
            elif file_path.suffix.lower() == '.pdf':
                return self._parse_pdf(file_path, password)
            else:
                result = ParseResult(success=False, source_file=str(file_path))
                result.add_error(f"Unsupported file format: {file_path.suffix}")
                return result

        except Exception as e:
            result = ParseResult(success=False, source_file=str(file_path))
            result.add_error(f"Error parsing file: {str(e)}")
            return result

    def _parse_excel(self, file_path: Path) -> ParseResult:
        """
        Parse Excel CAS with capital gains data.

        Expected sheet structure:
        - Trasaction_Details: Transaction data with CG calculations
        - Headers at row 5 (skip first 4 rows for name/PAN/section info)

        Args:
            file_path: Path to Excel file

        Returns:
            ParseResult with transactions
        """
        result = ParseResult(success=True, source_file=str(file_path))
        transactions = []

        try:
            warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

            df = self._read_excel_with_fallback(file_path)

            if df is None or df.empty:
                result.add_error("No data found in Excel file")
                result.success = False
                return result

            for idx, row in df.iterrows():
                try:
                    txn = self._parse_transaction_row(row)
                    if txn:
                        transactions.append(txn)
                except Exception as e:
                    result.add_warning(f"Row {idx}: {str(e)}")
                    continue

            result.transactions = transactions

            if len(transactions) == 0:
                result.add_warning("No transactions found in file")

        except Exception as e:
            result.add_error(f"Failed to read Excel file: {str(e)}")
            result.success = False

        return result

    def _parse_pdf(self, file_path: Path, password: Optional[str] = None) -> ParseResult:
        """
        Parse PDF CAS file using pdfplumber.

        Note: PDF parsing extracts basic transaction data from standard
        Karvy CAS PDFs. For Capital Gains calculations, Excel format is
        recommended as it includes detailed purchase/redemption lot matching.

        Args:
            file_path: Path to PDF file
            password: PDF password (often PAN number)

        Returns:
            ParseResult with transactions
        """
        try:
            from .pdf_extractor import get_pdf_extractor, check_pdf_support

            if not check_pdf_support():
                result = ParseResult(success=False, source_file=str(file_path))
                result.add_error(
                    "PDF support requires pdfplumber. "
                    "Install with: pip install pdfplumber"
                )
                return result

            extractor = get_pdf_extractor()
            result = extractor.extract_from_pdf(file_path, password)

            # Add warning about Capital Gains data
            if result.success and result.transactions:
                result.add_warning(
                    "PDF parsing provides basic transaction data. "
                    "For accurate Capital Gains calculations, use the Excel format."
                )

            return result

        except ImportError as e:
            result = ParseResult(success=False, source_file=str(file_path))
            result.add_error(f"PDF support not available: {str(e)}")
            return result
        except Exception as e:
            result = ParseResult(success=False, source_file=str(file_path))
            result.add_error(f"Failed to parse PDF: {str(e)}")
            return result

    def _read_excel_with_fallback(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        Read Excel file with multiple fallback strategies.

        Args:
            file_path: Path to Excel file

        Returns:
            DataFrame or None if all strategies fail
        """
        # Karvy sheet names to try (note the typo in actual file)
        sheet_names_to_try = ['Trasaction_Details', 'Transaction_Details', 2, 0]
        header_rows_to_try = [4, 3, 5, 0]  # Row 5 (index 4) is typical for Karvy CG files

        # Try calamine engine first
        for sheet in sheet_names_to_try:
            for header in header_rows_to_try:
                try:
                    df = pd.read_excel(
                        file_path,
                        sheet_name=sheet,
                        header=header,
                        engine='calamine'
                    )
                    if self._validate_karvy_dataframe(df):
                        logger.debug(f"Successfully read with calamine: sheet={sheet}, header={header}")
                        return df
                except Exception:
                    continue

        # Fallback to openpyxl
        for sheet in sheet_names_to_try:
            for header in header_rows_to_try:
                try:
                    df = pd.read_excel(
                        file_path,
                        sheet_name=sheet,
                        header=header,
                        engine='openpyxl'
                    )
                    if self._validate_karvy_dataframe(df):
                        logger.debug(f"Successfully read with openpyxl: sheet={sheet}, header={header}")
                        return df
                except Exception:
                    continue

        return None

    def _validate_karvy_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Validate that DataFrame has expected Karvy columns.

        Args:
            df: DataFrame to validate

        Returns:
            True if valid Karvy format
        """
        if df is None or df.empty:
            return False

        # Check for required columns (case-insensitive)
        required_cols = {'scheme name', 'amount'}
        df_cols_lower = {str(c).lower().strip() for c in df.columns}

        return required_cols.issubset(df_cols_lower)

    def _parse_transaction_row(self, row: pd.Series) -> Optional[MFTransaction]:
        """
        Parse a single transaction row from Karvy Excel.

        Args:
            row: Pandas Series representing one row

        Returns:
            MFTransaction or None if row should be skipped
        """
        # Extract scheme info
        scheme_name = self._get_column_value(row, ['Scheme Name', 'scheme_name', 'SCHEME NAME'])
        if not scheme_name:
            return None

        # Extract ISIN from scheme name (Karvy format: "... ( INF123456789)")
        isin = self._extract_isin(scheme_name)

        # Karvy doesn't typically include asset class - classify from name
        asset_class = classify_scheme(scheme_name)

        # Get AMC name
        amc_name = self._get_column_value(row, [' Fund Name', 'Fund Name', 'AMC Name'])
        amc_name = amc_name.strip() if amc_name else ''

        # Create scheme object
        scheme = MFScheme(
            name=scheme_name,
            amc_name=amc_name,
            isin=isin,
            asset_class=asset_class
        )

        # Determine transaction type from outflow column (Section B)
        # Karvy has purchase type in 'Trxn.Type' and redemption type in 'Trxn.Type.1'
        outflow_type = self._get_column_value(row, ['Trxn.Type.1', 'Trxn Type'])
        if outflow_type and 'redemption' in outflow_type.lower():
            txn_type = TransactionType.REDEMPTION
        else:
            # Use purchase type
            purchase_type = self._get_column_value(row, ['Trxn.Type', 'Transaction Type'])
            txn_type = self._determine_transaction_type(purchase_type or '')

        # Parse redemption date (from Section B)
        txn_date_val = self._get_column_value(row, ['Date.1', 'Date_1', 'Redemption Date'])
        txn_date = self._parse_date(txn_date_val)

        # If no redemption date, try purchase date
        if not txn_date:
            txn_date_val = self._get_column_value(row, ['Date', 'Purchase Date'])
            txn_date = self._parse_date(txn_date_val)

        if not txn_date:
            return None  # Skip if no date

        # Get purchase date (from Section A)
        purchase_date_val = self._get_column_value(row, ['Date', 'Purchase Date'])
        purchase_date = self._parse_date(purchase_date_val)

        # Get folio number
        folio = self._get_column_value(row, ['Folio Number', 'Folio No', 'folio_number'])
        folio = folio.strip() if folio else ''

        # Create transaction
        txn = MFTransaction(
            folio_number=folio,
            scheme=scheme,
            transaction_type=txn_type,
            date=txn_date,
            units=self._to_decimal(self._get_column_value(row, ['Units', 'Current Units'])),
            nav=self._to_decimal(self._get_column_value(row, ['Price', 'NAV'])),
            amount=self._to_decimal(self._get_column_value(row, ['Amount'])),
            stt=Decimal("0"),  # Karvy doesn't typically include STT in CG file
            # Purchase info
            purchase_date=purchase_date,
            purchase_units=self._to_decimal(self._get_column_value(row, ['Source Scheme units', 'Current Units'])),
            purchase_nav=self._to_decimal(self._get_column_value(row, ['Original Purchase Cost', 'IT Applicable\nNAV'])),
            # Grandfathering
            grandfathered_units=None,  # Karvy format doesn't have separate units
            grandfathered_nav=self._to_decimal(self._get_column_value(row, [
                ' Grandfathered\n NAV as on 31/01/2018',
                'Grandfathered NAV'
            ])),
            grandfathered_value=self._to_decimal(self._get_column_value(row, ['GrandFathered Cost Value'])),
            # Capital gains from Karvy
            short_term_gain=self._to_decimal(self._get_column_value(row, ['Short Term'])),
            long_term_gain=self._to_decimal(self._get_column_value(row, ['Long Term Without Index', 'Long Term']))
        )

        return txn

    def _get_column_value(self, row: pd.Series, column_names: List[str]) -> Optional[str]:
        """
        Get value from row trying multiple column name variations.

        Args:
            row: Pandas Series
            column_names: List of possible column names to try

        Returns:
            Value as string or None
        """
        for col_name in column_names:
            if col_name in row.index:
                val = row.get(col_name)
                if pd.notna(val) and str(val).strip() and str(val).strip().lower() != 'nan':
                    return str(val).strip()
        return None

    def _extract_isin(self, scheme_name: str) -> Optional[str]:
        """
        Extract ISIN from scheme name.

        Karvy format: "Scheme Name ( INF123456789)"

        Args:
            scheme_name: Scheme name

        Returns:
            ISIN code or None
        """
        # Match ISIN in parentheses at end of name
        match = re.search(r'\(\s*([A-Z0-9]{12})\s*\)', scheme_name)
        if match:
            return match.group(1)

        # Also try CAMS format
        match = re.search(r'ISIN\s*:\s*([A-Z0-9]{12})', scheme_name, re.IGNORECASE)
        return match.group(1) if match else None

    def _determine_transaction_type(self, description: str) -> TransactionType:
        """
        Determine transaction type from description.

        Args:
            description: Transaction description

        Returns:
            TransactionType enum
        """
        if not description:
            return TransactionType.PURCHASE

        desc = description.upper().strip()

        if 'REDEMPTION' in desc:
            return TransactionType.REDEMPTION
        elif 'SWITCH OUT' in desc or 'SWITCH-OUT' in desc or 'LATERAL SHIFT OUT' in desc:
            return TransactionType.SWITCH_OUT
        elif 'SWITCH IN' in desc or 'SWITCH-IN' in desc or 'LATERAL SHIFT IN' in desc:
            return TransactionType.SWITCH_IN
        elif 'STP IN' in desc:
            return TransactionType.SWITCH_IN
        elif 'STP OUT' in desc:
            return TransactionType.SWITCH_OUT
        elif 'DIVIDEND' in desc and 'REINVEST' in desc:
            return TransactionType.DIVIDEND_REINVEST
        elif 'DIVIDEND' in desc:
            return TransactionType.DIVIDEND
        else:
            return TransactionType.PURCHASE

    def _parse_date(self, value) -> Optional[date_type]:
        """
        Parse date from various formats.

        Args:
            value: Date value

        Returns:
            date object or None
        """
        if pd.isna(value) or value is None or value == '':
            return None

        if isinstance(value, date_type):
            return value

        if isinstance(value, pd.Timestamp):
            return value.date()

        # Try parsing string with various formats
        try:
            return pd.to_datetime(value, dayfirst=True).date()
        except:
            pass

        try:
            return pd.to_datetime(value).date()
        except:
            return None

    def _to_decimal(self, value) -> Decimal:
        """
        Convert value to Decimal safely.

        Args:
            value: Numeric value

        Returns:
            Decimal value (0 if invalid)
        """
        if pd.isna(value) or value == '' or value is None:
            return Decimal("0")

        try:
            return Decimal(str(value))
        except:
            return Decimal("0")

    def save_to_db(self, result: ParseResult, user_id: int = None) -> int:
        """
        Save parsed MF transactions to database.

        Args:
            result: ParseResult from parsing
            user_id: User ID

        Returns:
            Number of transactions saved
        """
        if not result.success or not result.transactions:
            return 0

        cursor = self.conn.cursor()
        in_transaction = self.conn.in_transaction

        try:
            if not in_transaction:
                cursor.execute("BEGIN IMMEDIATE")

            count = 0
            for txn in result.transactions:
                amc_id = self._get_or_create_amc(txn.scheme.amc_name)
                scheme_id = self._get_or_create_scheme(txn.scheme, amc_id, user_id)
                folio_id = self._get_or_create_folio(user_id, scheme_id, txn.folio_number)

                if self._insert_transaction(folio_id, txn, result.source_file, user_id):
                    count += 1

            if not in_transaction:
                self.conn.commit()

            return count

        except Exception as e:
            if not in_transaction:
                self.conn.rollback()
            raise Exception(f"Failed to save MF transactions: {e}") from e

    def _get_or_create_amc(self, amc_name: str) -> int:
        """Get or create AMC and return ID."""
        cursor = self.conn.execute(
            "SELECT id FROM mf_amcs WHERE name = ?",
            (amc_name,)
        )
        row = cursor.fetchone()

        if row:
            return row['id']

        cursor = self.conn.execute(
            "INSERT INTO mf_amcs (name) VALUES (?)",
            (amc_name,)
        )
        return cursor.lastrowid

    def _get_or_create_scheme(self, scheme: MFScheme, amc_id: int, user_id: Optional[int]) -> int:
        """Get or create scheme and return ID."""
        if scheme.isin:
            cursor = self.conn.execute(
                "SELECT id FROM mf_schemes WHERE isin = ?",
                (scheme.isin,)
            )
            row = cursor.fetchone()
            if row:
                return row['id']

        cursor = self.conn.execute(
            """INSERT INTO mf_schemes
            (amc_id, name, isin, asset_class, user_id)
            VALUES (?, ?, ?, ?, ?)""",
            (
                amc_id,
                scheme.name,
                scheme.isin,
                scheme.asset_class.value,
                user_id
            )
        )
        return cursor.lastrowid

    def _get_or_create_folio(self, user_id: Optional[int], scheme_id: int, folio_number: str) -> int:
        """Get or create folio and return ID."""
        cursor = self.conn.execute(
            "SELECT id FROM mf_folios WHERE user_id = ? AND scheme_id = ? AND folio_number = ?",
            (user_id, scheme_id, folio_number)
        )
        row = cursor.fetchone()

        if row:
            return row['id']

        cursor = self.conn.execute(
            """INSERT INTO mf_folios (user_id, scheme_id, folio_number)
            VALUES (?, ?, ?)""",
            (user_id, scheme_id, folio_number)
        )
        return cursor.lastrowid

    def _insert_transaction(
        self, folio_id: int, txn: MFTransaction, source_file: str, user_id: Optional[int]
    ) -> bool:
        """
        Insert MF transaction into database.

        Args:
            folio_id: Folio ID
            txn: Transaction to insert
            source_file: Source file path
            user_id: User ID

        Returns:
            True if inserted, False if duplicate
        """
        try:
            self.conn.execute(
                """INSERT INTO mf_transactions
                (folio_id, transaction_type, date, units, nav, amount, stt, stamp_duty,
                 purchase_date, purchase_units, purchase_nav, purchase_amount,
                 grandfathered_units, grandfathered_nav, grandfathered_value,
                 holding_period_days, is_long_term,
                 short_term_gain, long_term_gain, user_id, source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    folio_id,
                    txn.transaction_type.value,
                    txn.date.isoformat(),
                    str(txn.units),
                    str(txn.nav),
                    str(txn.amount),
                    str(txn.stt),
                    str(txn.stamp_duty),
                    txn.purchase_date.isoformat() if txn.purchase_date else None,
                    str(txn.purchase_units) if txn.purchase_units else None,
                    str(txn.purchase_nav) if txn.purchase_nav else None,
                    str(txn.purchase_amount) if txn.purchase_amount else None,
                    str(txn.grandfathered_units) if txn.grandfathered_units else None,
                    str(txn.grandfathered_nav) if txn.grandfathered_nav else None,
                    str(txn.grandfathered_value) if txn.grandfathered_value else None,
                    txn.holding_period_days,
                    txn.is_long_term,
                    str(txn.short_term_gain),
                    str(txn.long_term_gain),
                    user_id,
                    source_file
                )
            )
            return True
        except (sqlite3.IntegrityError, SQLCipherIntegrityError):
            self._duplicate_count += 1
            logger.debug(
                f"Duplicate transaction skipped: {txn.scheme.name} "
                f"on {txn.date.isoformat()} for {txn.amount}"
            )
            return False

    def get_duplicate_count(self) -> int:
        """Get the count of duplicate transactions encountered."""
        return self._duplicate_count

    def reset_duplicate_count(self) -> None:
        """Reset the duplicate transaction counter."""
        self._duplicate_count = 0
