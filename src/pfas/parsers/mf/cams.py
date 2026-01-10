"""CAMS Consolidated Account Statement parser."""

import pandas as pd
from pathlib import Path
from datetime import datetime, date as date_type
from decimal import Decimal
from typing import List, Optional
import sqlite3
import re

from .models import MFTransaction, MFScheme, AssetClass, TransactionType, ParseResult
from .classifier import classify_scheme
from .capital_gains import CapitalGainsCalculator


class CAMSParser:
    """
    Parser for CAMS CAS statements (Excel format).

    CAMS (Computer Age Management Services) provides consolidated account
    statements for mutual funds. This parser extracts transaction data,
    capital gains, and scheme information from CAMS Excel files.

    Supported formats:
    - Excel (.xlsx, .xls) with TRXN_DETAILS sheet
    - PDF (basic extraction, Excel recommended)
    """

    def __init__(self, db_connection: sqlite3.Connection, master_key: bytes = None):
        """
        Initialize CAMS parser.

        Args:
            db_connection: Database connection
            master_key: Encryption key (not used for MF data)
        """
        self.conn = db_connection
        self.master_key = master_key or b"default_key_32_bytes_long_here"

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
                result = ParseResult(success=False, source_file=str(file_path))
                result.add_error("PDF parsing not yet implemented. Please use Excel format.")
                return result
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

        Args:
            file_path: Path to Excel file

        Returns:
            ParseResult with transactions
        """
        result = ParseResult(success=True, source_file=str(file_path))
        transactions = []

        try:
            # Read transaction sheet
            df = pd.read_excel(file_path, sheet_name='TRXN_DETAILS')

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

    def _parse_transaction_row(self, row: pd.Series) -> Optional[MFTransaction]:
        """
        Parse a single transaction row from CAMS Excel.

        Args:
            row: Pandas Series representing one row

        Returns:
            MFTransaction or None if row should be skipped
        """
        # Extract scheme info
        scheme_name = str(row.get('Scheme Name', '')).strip()
        if not scheme_name or scheme_name == 'nan':
            return None  # Skip empty rows

        # Extract ISIN from scheme name
        isin = self._extract_isin(scheme_name)

        # Get asset class from CAMS or classify
        asset_class_str = str(row.get('ASSET CLASS', '')).upper().strip()
        if asset_class_str in ['EQUITY', 'DEBT', 'HYBRID', 'OTHER']:
            asset_class = AssetClass[asset_class_str]
        else:
            asset_class = classify_scheme(scheme_name)

        # Create scheme object
        scheme = MFScheme(
            name=scheme_name,
            amc_name=str(row.get('AMC Name', '')).strip(),
            isin=isin,
            asset_class=asset_class,
            nav_31jan2018=self._to_decimal(row.get('NAV As On 31/01/2018 (Grandfathered NAV)'))
        )

        # Determine transaction type
        desc = str(row.get('Desc', '')).strip()
        txn_type = self._determine_transaction_type(desc)

        # Parse dates
        txn_date = self._parse_date(row.get('Date'))
        if not txn_date:
            return None  # Skip if no date

        purchase_date = self._parse_date(row.get('Date_1'))

        # Create transaction
        txn = MFTransaction(
            folio_number=str(row.get('Folio No', '')).strip(),
            scheme=scheme,
            transaction_type=txn_type,
            date=txn_date,
            units=self._to_decimal(row.get('Units')),
            nav=self._to_decimal(row.get('Price')),
            amount=self._to_decimal(row.get('Amount')),
            stt=self._to_decimal(row.get('STT')),
            # Purchase info (for redemptions)
            purchase_date=purchase_date,
            purchase_units=self._to_decimal(row.get('PurhUnit')),
            purchase_nav=self._to_decimal(row.get('Unit Cost')),
            # Grandfathering
            grandfathered_units=self._to_decimal(row.get('Units As On 31/01/2018 (Grandfathered Units)')),
            grandfathered_nav=self._to_decimal(row.get('NAV As On 31/01/2018 (Grandfathered NAV)')),
            grandfathered_value=self._to_decimal(row.get('Market Value As On 31/01/2018 (Grandfathered Value)')),
            # Capital gains from CAMS
            short_term_gain=self._to_decimal(row.get('Short Term')),
            long_term_gain=self._to_decimal(row.get('Long Term Without Index'))
        )

        return txn

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
        Save parsed MF transactions to database.

        Process:
        1. Get or create AMC
        2. Get or create scheme
        3. Get or create folio
        4. Insert transactions
        5. Calculate and save capital gains

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

        try:
            if not in_transaction:
                cursor.execute("BEGIN IMMEDIATE")

            count = 0
            for txn in result.transactions:
                # Get or create AMC
                amc_id = self._get_or_create_amc(txn.scheme.amc_name)

                # Get or create scheme
                scheme_id = self._get_or_create_scheme(txn.scheme, amc_id, user_id)

                # Get or create folio
                folio_id = self._get_or_create_folio(
                    user_id, scheme_id, txn.folio_number
                )

                # Insert transaction
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
        """Insert MF transaction into database."""
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
        except sqlite3.IntegrityError:
            # Duplicate transaction
            return False
