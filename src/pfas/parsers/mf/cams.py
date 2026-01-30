"""CAMS Consolidated Account Statement parser."""

import pandas as pd
from pathlib import Path
from datetime import datetime, date as date_type
from decimal import Decimal
from typing import List, Optional, Dict, Any
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
from .capital_gains import CapitalGainsCalculator

# Ledger integration imports
from pfas.core.transaction_service import TransactionService, TransactionSource
from pfas.parsers.ledger_integration import (
    record_mf_purchase,
    record_mf_redemption,
    record_mf_switch,
    record_mf_dividend,
)

logger = logging.getLogger(__name__)


class CAMSParser:
    """
    Parser for CAMS CAS statements (Excel format).

    CAMS (Computer Age Management Services) provides consolidated account
    statements for mutual funds. This parser extracts transaction data,
    capital gains, and scheme information from CAMS Excel files.

    Supported formats:
    - Excel (.xlsx, .xls) with TRXN_DETAILS sheet
    - PDF (not yet implemented)

    File Structure Notes:
    - CAMS Capital Gains Excel files have headers at row 4 (0-indexed: 3)
    - First 3 rows contain title and period information
    - Sheet name is typically 'TRXN_DETAILS' at index 1
    """

    # Asset class mapping for CAMS-specific values
    ASSET_CLASS_MAPPING = {
        'EQUITY': AssetClass.EQUITY,
        'DEBT': AssetClass.DEBT,
        'HYBRID': AssetClass.HYBRID,
        'CASH': AssetClass.DEBT,  # CASH funds are treated as DEBT for tax purposes
        'OTHER': AssetClass.OTHER,
    }

    def __init__(self, db_connection: sqlite3.Connection, master_key: bytes = None):
        """
        Initialize CAMS parser.

        Args:
            db_connection: Database connection
            master_key: Encryption key (not used for MF data)
        """
        self.conn = db_connection
        self.master_key = master_key or b"default_key_32_bytes_long_here"
        self._duplicate_count = 0

    def parse(self, file_path: Path, password: Optional[str] = None) -> ParseResult:
        """
        Parse CAMS CAS file.

        Args:
            file_path: Path to CAMS CAS file (Excel or PDF)
            password: Password for encrypted PDFs (optional)

        Returns:
            ParseResult with transactions

        Examples:
            >>> parser = CAMSParser(conn)
            >>> result = parser.parse(Path("cams_cas.xlsx"))
            >>> print(f"Parsed {len(result.transactions)} transactions")
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
        - TRXN_DETAILS: Transaction data with CG calculations
        - Headers at row 4 (skip first 3 rows for title/period info)

        Args:
            file_path: Path to Excel file

        Returns:
            ParseResult with transactions
        """
        result = ParseResult(success=True, source_file=str(file_path))
        transactions = []

        try:
            # Suppress openpyxl warnings for non-standard files
            warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

            # Try different engines and configurations
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
        CAMS CAS PDFs. For Capital Gains calculations, Excel format is
        recommended as it includes detailed purchase/redemption lot matching.

        Args:
            file_path: Path to PDF file
            password: PDF password (often PAN number for CAMS)

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

        Handles various Excel file formats and structures:
        1. Try calamine engine (fastest, most compatible)
        2. Try openpyxl with various sheet configurations
        3. Try xlrd for .xls files

        Args:
            file_path: Path to Excel file

        Returns:
            DataFrame or None if all strategies fail
        """
        sheet_names_to_try = ['TRXN_DETAILS', 1, 'Transaction_Details', 0]
        header_rows_to_try = [3, 0, 4, 2]  # Row 4 (index 3) is typical for CAMS CG files

        # Try calamine engine first (best compatibility)
        for sheet in sheet_names_to_try:
            for header in header_rows_to_try:
                try:
                    df = pd.read_excel(
                        file_path,
                        sheet_name=sheet,
                        header=header,
                        engine='calamine'
                    )
                    if self._validate_cams_dataframe(df):
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
                    if self._validate_cams_dataframe(df):
                        logger.debug(f"Successfully read with openpyxl: sheet={sheet}, header={header}")
                        return df
                except Exception:
                    continue

        # Last resort for .xls files
        if file_path.suffix.lower() == '.xls':
            try:
                df = pd.read_excel(file_path, sheet_name=0, header=3, engine='xlrd')
                if self._validate_cams_dataframe(df):
                    return df
            except Exception:
                pass

        return None

    def _validate_cams_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Validate that DataFrame has expected CAMS columns.

        Args:
            df: DataFrame to validate

        Returns:
            True if valid CAMS format
        """
        if df is None or df.empty:
            return False

        # Check for required columns (case-insensitive)
        required_cols = {'scheme name', 'date', 'amount'}
        df_cols_lower = {str(c).lower().strip() for c in df.columns}

        return required_cols.issubset(df_cols_lower)

    def _parse_transaction_row(self, row: pd.Series) -> Optional[MFTransaction]:
        """
        Parse a single transaction row from CAMS Excel.

        Args:
            row: Pandas Series representing one row

        Returns:
            MFTransaction or None if row should be skipped
        """
        # Extract scheme info (handle various column name formats)
        scheme_name = self._get_column_value(row, ['Scheme Name', 'scheme_name', 'SCHEME NAME'])
        if not scheme_name:
            return None  # Skip empty rows

        # Extract ISIN from scheme name
        isin = self._extract_isin(scheme_name)

        # Get asset class from CAMS or classify
        asset_class_str = self._get_column_value(row, ['ASSET CLASS', 'Asset Class', 'asset_class'])
        asset_class_str = str(asset_class_str).upper().strip() if asset_class_str else ''

        # Use mapping for CAMS-specific values (including CASH -> DEBT)
        if asset_class_str in self.ASSET_CLASS_MAPPING:
            asset_class = self.ASSET_CLASS_MAPPING[asset_class_str]
        else:
            asset_class = classify_scheme(scheme_name)

        # Get AMC name
        amc_name = self._get_column_value(row, ['AMC Name', 'amc_name', 'AMC NAME', ' Fund Name'])
        amc_name = amc_name if amc_name else ''

        # Get grandfathering NAV (try multiple column name formats)
        nav_31jan = self._to_decimal(self._get_column_value(row, [
            'NAV As On 31/01/2018 (Grandfathered NAV)',
            'NAV As On 31/01/2018',
            'Grandfathered NAV'
        ]))

        # Create scheme object
        scheme = MFScheme(
            name=scheme_name,
            amc_name=amc_name,
            isin=isin,
            asset_class=asset_class,
            nav_31jan2018=nav_31jan if nav_31jan > 0 else None
        )

        # Determine transaction type (handle trailing spaces)
        desc = self._get_column_value(row, ['Desc', 'desc', 'Trxn.Type', 'Transaction Type'])
        desc = desc if desc else ''
        txn_type = self._determine_transaction_type(desc)

        # Parse dates
        txn_date_val = self._get_column_value(row, ['Date', 'date', 'Date.1'])
        txn_date = self._parse_date(txn_date_val)
        if not txn_date:
            return None  # Skip if no date

        purchase_date_val = self._get_column_value(row, ['Date_1', 'Date.1', 'Purchase Date'])
        purchase_date = self._parse_date(purchase_date_val)

        # Get folio number
        folio = self._get_column_value(row, ['Folio No', 'Folio Number', 'folio_number'])
        folio = folio if folio else ''

        # Create transaction
        txn = MFTransaction(
            folio_number=folio,
            scheme=scheme,
            transaction_type=txn_type,
            date=txn_date,
            units=self._to_decimal(self._get_column_value(row, ['Units', 'units', 'Current Units'])),
            nav=self._to_decimal(self._get_column_value(row, ['Price', 'NAV', 'nav'])),
            amount=self._to_decimal(self._get_column_value(row, ['Amount', 'amount'])),
            stt=self._to_decimal(self._get_column_value(row, ['STT', 'stt'])),
            # Purchase info (for redemptions)
            purchase_date=purchase_date,
            purchase_units=self._to_decimal(self._get_column_value(row, ['PurhUnit', 'Purchase Units', 'Source Scheme units'])),
            purchase_nav=self._to_decimal(self._get_column_value(row, ['Unit Cost', 'Original Purchase Cost', 'Purchase NAV'])),
            # Grandfathering
            grandfathered_units=self._to_decimal(self._get_column_value(row, [
                'Units As On 31/01/2018 (Grandfathered Units)',
                'Grandfathered Units'
            ])),
            grandfathered_nav=self._to_decimal(self._get_column_value(row, [
                'NAV As On 31/01/2018 (Grandfathered NAV)',
                ' Grandfathered\n NAV as on 31/01/2018'
            ])),
            grandfathered_value=self._to_decimal(self._get_column_value(row, [
                'Market Value As On 31/01/2018 (Grandfathered Value)',
                'GrandFathered Cost Value'
            ])),
            # Capital gains from CAMS
            short_term_gain=self._to_decimal(self._get_column_value(row, ['Short Term', 'short_term'])),
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

        Args:
            scheme_name: Scheme name (e.g., "SBI Fund ISIN : INF123456789")

        Returns:
            ISIN code or None

        Examples:
            >>> parser._extract_isin("SBI Fund ISIN : INF178L01BY0")
            'INF178L01BY0'
        """
        match = re.search(r'ISIN\s*:\s*([A-Z0-9]{12})', scheme_name, re.IGNORECASE)
        return match.group(1) if match else None

    def _determine_transaction_type(self, description: str) -> TransactionType:
        """
        Determine transaction type from description.

        Args:
            description: Transaction description from CAMS

        Returns:
            TransactionType enum

        Examples:
            >>> parser._determine_transaction_type("Redemption")
            TransactionType.REDEMPTION

            >>> parser._determine_transaction_type("Purchase - Systematic")
            TransactionType.PURCHASE
        """
        desc = description.upper()

        if 'REDEMPTION' in desc:
            return TransactionType.REDEMPTION
        elif 'SWITCH OUT' in desc or 'SWITCH-OUT' in desc:
            return TransactionType.SWITCH_OUT
        elif 'SWITCH IN' in desc or 'SWITCH-IN' in desc:
            return TransactionType.SWITCH_IN
        elif 'DIVIDEND' in desc and 'REINVEST' in desc:
            return TransactionType.DIVIDEND_REINVEST
        elif 'DIVIDEND' in desc:
            return TransactionType.DIVIDEND
        else:
            # Default to purchase for any other type
            return TransactionType.PURCHASE

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

        Examples:
            >>> parser._to_decimal(123.45)
            Decimal('123.45')

            >>> parser._to_decimal(None)
            Decimal('0')
        """
        if pd.isna(value) or value == '' or value is None:
            return Decimal("0")

        try:
            return Decimal(str(value))
        except:
            return Decimal("0")

    def save_to_db(self, result: ParseResult, user_id: int = None) -> int:
        """
        Save parsed MF transactions to database with double-entry ledger.

        Process:
        1. Get or create AMC
        2. Get or create scheme
        3. Get or create folio
        4. Record to ledger via TransactionService
        5. Insert transactions to mf_transactions for compatibility

        Args:
            result: ParseResult from parsing
            user_id: User ID (optional)

        Returns:
            Number of transactions saved

        Examples:
            >>> result = parser.parse(Path("cams.xlsx"))
            >>> count = parser.save_to_db(result, user_id=1)
            >>> print(f"Saved {count} transactions")
        """
        if not result.success or not result.transactions:
            return 0

        cursor = self.conn.cursor()
        in_transaction = self.conn.in_transaction

        # Initialize TransactionService for ledger entries
        txn_service = TransactionService(self.conn)

        try:
            if not in_transaction:
                cursor.execute("BEGIN IMMEDIATE")

            count = 0
            for row_idx, txn in enumerate(result.transactions):
                # Get or create AMC
                amc_id = self._get_or_create_amc(txn.scheme.amc_name)

                # Get or create scheme
                scheme_id = self._get_or_create_scheme(txn.scheme, amc_id, user_id)

                # Get or create folio
                folio_id = self._get_or_create_folio(
                    user_id, scheme_id, txn.folio_number
                )

                # Record to double-entry ledger
                ledger_result = self._record_to_ledger(
                    txn_service, user_id, txn, result.source_file, row_idx
                )

                # Skip if duplicate in ledger
                if ledger_result.is_duplicate:
                    self._duplicate_count += 1
                    logger.debug(
                        f"Duplicate transaction skipped (ledger): {txn.scheme.name} "
                        f"on {txn.date.isoformat()} for {txn.amount}"
                    )
                    continue

                # Insert to mf_transactions for backward compatibility
                if self._insert_transaction(folio_id, txn, result.source_file, user_id):
                    count += 1

            if not in_transaction:
                self.conn.commit()

            return count

        except Exception as e:
            if not in_transaction:
                self.conn.rollback()
            raise Exception(f"Failed to save MF transactions: {e}") from e

    def _record_to_ledger(
        self,
        txn_service: TransactionService,
        user_id: int,
        txn: MFTransaction,
        source_file: str,
        row_idx: int
    ):
        """
        Record MF transaction to double-entry ledger.

        Args:
            txn_service: TransactionService instance
            user_id: User ID
            txn: MFTransaction to record
            source_file: Source file path
            row_idx: Row index in source file

        Returns:
            LedgerRecordResult
        """
        from pfas.parsers.ledger_integration import LedgerRecordResult

        is_equity = txn.scheme.asset_class == AssetClass.EQUITY

        # Handle different transaction types
        if txn.transaction_type == TransactionType.PURCHASE:
            return record_mf_purchase(
                txn_service=txn_service,
                conn=self.conn,
                user_id=user_id,
                folio_number=txn.folio_number,
                scheme_name=txn.scheme.name,
                txn_date=txn.date,
                amount=abs(txn.amount),
                units=abs(txn.units),
                is_equity=is_equity,
                source_file=source_file,
                row_idx=row_idx,
                source=TransactionSource.PARSER_CAMS,
            )

        elif txn.transaction_type == TransactionType.REDEMPTION:
            # Calculate cost basis from purchase info if available
            cost_basis = txn.purchase_amount or (txn.purchase_units * txn.purchase_nav if txn.purchase_units and txn.purchase_nav else abs(txn.amount))

            return record_mf_redemption(
                txn_service=txn_service,
                conn=self.conn,
                user_id=user_id,
                folio_number=txn.folio_number,
                scheme_name=txn.scheme.name,
                txn_date=txn.date,
                proceeds=abs(txn.amount),
                cost_basis=cost_basis,
                units=abs(txn.units),
                is_equity=is_equity,
                is_long_term=txn.is_long_term,
                source_file=source_file,
                row_idx=row_idx,
                stt=txn.stt,
                source=TransactionSource.PARSER_CAMS,
            )

        elif txn.transaction_type in (TransactionType.SWITCH_IN, TransactionType.SWITCH_OUT):
            # For switch transactions, record as purchase (switch-in) or redemption (switch-out)
            if txn.transaction_type == TransactionType.SWITCH_IN:
                return record_mf_purchase(
                    txn_service=txn_service,
                    conn=self.conn,
                    user_id=user_id,
                    folio_number=txn.folio_number,
                    scheme_name=txn.scheme.name,
                    txn_date=txn.date,
                    amount=abs(txn.amount),
                    units=abs(txn.units),
                    is_equity=is_equity,
                    source_file=source_file,
                    row_idx=row_idx,
                    source=TransactionSource.PARSER_CAMS,
                )
            else:
                cost_basis = txn.purchase_amount or abs(txn.amount)
                return record_mf_redemption(
                    txn_service=txn_service,
                    conn=self.conn,
                    user_id=user_id,
                    folio_number=txn.folio_number,
                    scheme_name=txn.scheme.name,
                    txn_date=txn.date,
                    proceeds=abs(txn.amount),
                    cost_basis=cost_basis,
                    units=abs(txn.units),
                    is_equity=is_equity,
                    is_long_term=txn.is_long_term,
                    source_file=source_file,
                    row_idx=row_idx,
                    stt=txn.stt,
                    source=TransactionSource.PARSER_CAMS,
                )

        elif txn.transaction_type in (TransactionType.DIVIDEND, TransactionType.DIVIDEND_REINVEST, TransactionType.DIVIDEND_PAYOUT):
            is_reinvested = txn.transaction_type == TransactionType.DIVIDEND_REINVEST
            return record_mf_dividend(
                txn_service=txn_service,
                conn=self.conn,
                user_id=user_id,
                folio_number=txn.folio_number,
                scheme_name=txn.scheme.name,
                txn_date=txn.date,
                amount=abs(txn.amount) if txn.amount else abs(txn.units * txn.nav),
                is_reinvested=is_reinvested,
                is_equity=is_equity,
                source_file=source_file,
                row_idx=row_idx,
                source=TransactionSource.PARSER_CAMS,
            )

        else:
            # For other transaction types, treat as purchase if units > 0, else redemption
            if txn.units > 0:
                return record_mf_purchase(
                    txn_service=txn_service,
                    conn=self.conn,
                    user_id=user_id,
                    folio_number=txn.folio_number,
                    scheme_name=txn.scheme.name,
                    txn_date=txn.date,
                    amount=abs(txn.amount),
                    units=abs(txn.units),
                    is_equity=is_equity,
                    source_file=source_file,
                    row_idx=row_idx,
                    source=TransactionSource.PARSER_CAMS,
                )
            else:
                cost_basis = txn.purchase_amount or abs(txn.amount)
                return record_mf_redemption(
                    txn_service=txn_service,
                    conn=self.conn,
                    user_id=user_id,
                    folio_number=txn.folio_number,
                    scheme_name=txn.scheme.name,
                    txn_date=txn.date,
                    proceeds=abs(txn.amount),
                    cost_basis=cost_basis,
                    units=abs(txn.units),
                    is_equity=is_equity,
                    is_long_term=txn.is_long_term,
                    source_file=source_file,
                    row_idx=row_idx,
                    stt=txn.stt,
                    source=TransactionSource.PARSER_CAMS,
                )

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
            (amc_id, name, isin, asset_class, nav_31jan2018, user_id)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (
                amc_id,
                scheme.name,
                scheme.isin,
                scheme.asset_class.value,
                str(scheme.nav_31jan2018) if scheme.nav_31jan2018 else None,
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
            # Duplicate transaction - log for visibility
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
