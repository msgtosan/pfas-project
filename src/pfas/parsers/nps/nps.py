"""NPS Statement parser."""

import pandas as pd
import hashlib
from pathlib import Path
from datetime import date as date_type
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass, field
from typing import List, Optional, Dict
import sqlite3

from pfas.core.transaction_service import (
    TransactionService,
    TransactionSource,
    AssetRecord,
)
from pfas.core.journal import JournalEntry
from pfas.core.accounts import get_account_by_code


@dataclass
class NPSAccount:
    """NPS account information."""
    pran: str  # Permanent Retirement Account Number
    nodal_office: str = ""
    scheme_preference: str = ""
    opening_date: Optional[date_type] = None


@dataclass
class NPSTransaction:
    """NPS transaction (contribution, redemption, or switch)."""
    pran: str
    date: date_type
    transaction_type: str  # CONTRIBUTION, REDEMPTION, SWITCH
    tier: str  # I or II
    contribution_type: Optional[str] = None  # EMPLOYEE or EMPLOYER
    amount: Decimal = Decimal("0")
    units: Decimal = Decimal("0")
    nav: Decimal = Decimal("0")
    scheme: str = ""
    financial_year: str = ""


@dataclass
class ParseResult:
    """Result of parsing NPS statement."""
    success: bool
    account: Optional[NPSAccount] = None
    transactions: List[NPSTransaction] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    source_file: str = ""

    def add_error(self, error: str):
        """Add an error message."""
        self.errors.append(error)
        self.success = False

    def add_warning(self, warning: str):
        """Add a warning message."""
        self.warnings.append(warning)


class NPSParser:
    """
    Parser for NPS statements.

    Parses CSV statements from NPS portal.
    Tracks Tier I and Tier II contributions for tax deductions.

    Tax benefits:
    - 80CCD(1): Employee Tier I (part of ₹1.5L limit)
    - 80CCD(1B): Additional ₹50,000 (Tier I only)
    - 80CCD(2): Employer contribution (max 10% of Basic, no limit)
    """

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize NPS parser.

        Args:
            db_connection: Database connection
        """
        self.conn = db_connection

    def parse(self, file_path: Path) -> ParseResult:
        """
        Parse NPS CSV statement.

        Args:
            file_path: Path to NPS CSV file

        Returns:
            ParseResult with account and transactions

        Examples:
            >>> parser = NPSParser(conn)
            >>> result = parser.parse(Path("nps.csv"))
            >>> print(f"PRAN: {result.account.pran}")
            >>> print(f"Transactions: {len(result.transactions)}")
        """
        file_path = Path(file_path)
        result = ParseResult(success=True, source_file=str(file_path))

        if not file_path.exists():
            result.add_error(f"File not found: {file_path}")
            return result

        try:
            # Detect file type and read accordingly
            suffix = file_path.suffix.lower()
            if suffix == '.csv':
                df = pd.read_csv(file_path, header=None)  # Read without header first
            elif suffix in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path, header=None)  # Read without header first
            else:
                result.add_error(f"Unsupported file format: {suffix}")
                return result

            # Validate we have data
            if len(df) == 0:
                result.add_error("File is empty (no data rows)")
                return result

            # Find the header row by looking for PRAN or Transaction Date keywords
            header_row = self._find_header_row(df)
            if header_row is None:
                result.add_error("Could not find header row with expected columns (PRAN, Transaction Date, etc.)")
                return result

            # Re-read with correct header
            if suffix == '.csv':
                df = pd.read_csv(file_path, header=header_row)
            else:
                df = pd.read_excel(file_path, header=header_row)

            # Normalize column names for comparison
            df.columns = df.columns.str.strip()
            actual_cols = set(df.columns.str.upper())

            # Check for required columns (flexible matching)
            required_cols = {'PRAN', 'TRANSACTION DATE', 'TIER', 'AMOUNT'}
            # Try alternative column names
            col_aliases = {
                'PRAN': ['PRAN', 'PRAN NO', 'PRAN NUMBER', 'ACCOUNT NO'],
                'TRANSACTION DATE': ['TRANSACTION DATE', 'DATE', 'TXN DATE', 'TRANS DATE'],
                'TIER': ['TIER', 'TIER TYPE', 'ACCOUNT TIER'],
                'AMOUNT': ['AMOUNT', 'CONTRIBUTION', 'VALUE', 'CREDIT']
            }

            # Find and rename columns to standard names
            col_mapping = {}
            missing_cols = []
            for req_col, aliases in col_aliases.items():
                found = False
                for alias in aliases:
                    if alias in actual_cols:
                        # Find the actual case-sensitive column name
                        for col in df.columns:
                            if col.upper() == alias:
                                col_mapping[col] = req_col.title().replace('_', ' ')
                                if req_col == 'PRAN':
                                    col_mapping[col] = 'PRAN'
                                elif req_col == 'TRANSACTION DATE':
                                    col_mapping[col] = 'Transaction Date'
                                elif req_col == 'TIER':
                                    col_mapping[col] = 'Tier'
                                elif req_col == 'AMOUNT':
                                    col_mapping[col] = 'Amount'
                                found = True
                                break
                        if found:
                            break
                if not found:
                    missing_cols.append(req_col)

            if missing_cols:
                result.add_error(f"Missing required columns: {missing_cols}. Found: {list(df.columns)}")
                return result

            # Rename columns to standard names
            if col_mapping:
                df = df.rename(columns=col_mapping)

            # Parse transactions
            transactions = self._parse_transactions(df, result)
            result.transactions = transactions

            # Extract PRAN from first transaction
            if transactions:
                result.account = NPSAccount(pran=transactions[0].pran)

            if len(transactions) == 0:
                result.add_warning("No valid transactions found in file")

        except pd.errors.EmptyDataError:
            result.add_error("File is empty or contains no valid data")
        except pd.errors.ParserError as e:
            result.add_error(f"CSV/Excel parsing error: {str(e)}")
        except Exception as e:
            result.add_error(f"Failed to parse file: {str(e)}")

        return result

    def _parse_transactions(self, df: pd.DataFrame, result: ParseResult) -> List[NPSTransaction]:
        """
        Parse transactions from DataFrame.

        Expected CSV format (from NPS portal):
        PRAN, Transaction Date, Transaction Type, Tier, Amount, Units, NAV, Scheme

        Args:
            df: DataFrame from CSV
            result: ParseResult to add warnings to

        Returns:
            List of NPSTransaction objects
        """
        transactions = []

        for idx, row in df.iterrows():
            try:
                # Skip empty rows
                if pd.isna(row.get('PRAN')) or pd.isna(row.get('Transaction Date')):
                    continue

                pran = str(row['PRAN']).strip()
                txn_date = pd.to_datetime(row['Transaction Date'], dayfirst=True).date()
                txn_type = str(row['Transaction Type']).strip().upper()
                tier = str(row['Tier']).strip().upper()

                # Parse amounts
                amount = self._to_decimal(row.get('Amount'))
                units = self._to_decimal(row.get('Units'))
                nav = self._to_decimal(row.get('NAV'))

                # Scheme info
                scheme = str(row.get('Scheme', '')).strip()

                # Determine contribution type (employee vs employer)
                # This may need to be inferred from scheme name or description
                contribution_type = self._determine_contribution_type(row, scheme)

                # Financial year
                fy = self._get_financial_year(txn_date)

                # Create transaction
                txn = NPSTransaction(
                    pran=pran,
                    date=txn_date,
                    transaction_type=txn_type,
                    tier=tier,
                    contribution_type=contribution_type,
                    amount=amount,
                    units=units,
                    nav=nav,
                    scheme=scheme,
                    financial_year=fy
                )
                transactions.append(txn)

            except Exception as e:
                result.add_warning(f"Row {idx}: {str(e)}")
                continue

        return transactions

    def _determine_contribution_type(self, row: pd.Series, scheme: str) -> Optional[str]:
        """
        Determine if contribution is from employee or employer.

        This is typically inferred from narration or description field.

        Args:
            row: DataFrame row
            scheme: Scheme name

        Returns:
            'EMPLOYEE' or 'EMPLOYER' or None
        """
        # Check for description/narration column
        desc = ""
        for col in ['Description', 'Narration', 'Details']:
            if col in row.index and not pd.isna(row[col]):
                desc = str(row[col]).upper()
                break

        # Look for keywords
        if any(keyword in desc for keyword in ['EMPLOYER', 'ER CONTRIBUTION', 'COMPANY']):
            return 'EMPLOYER'
        elif any(keyword in desc for keyword in ['EMPLOYEE', 'EE CONTRIBUTION', 'SELF']):
            return 'EMPLOYEE'

        # If not found in description, assume employee contribution
        return 'EMPLOYEE'

    def _find_header_row(self, df: pd.DataFrame) -> Optional[int]:
        """
        Find the row index containing column headers.

        Scans first 20 rows for keywords like PRAN, Transaction Date, etc.

        Args:
            df: DataFrame read without headers

        Returns:
            Row index of header row, or None if not found
        """
        header_keywords = ['PRAN', 'TRANSACTION', 'DATE', 'TIER', 'AMOUNT', 'NAV', 'UNITS']

        for idx in range(min(20, len(df))):
            row_values = df.iloc[idx].astype(str).str.upper()
            # Check if this row contains multiple header keywords
            matches = sum(1 for kw in header_keywords if any(kw in val for val in row_values))
            if matches >= 3:  # At least 3 keywords found
                return idx

        return None

    def _get_financial_year(self, txn_date: date_type) -> str:
        """Get financial year for a transaction date."""
        if txn_date.month >= 4:  # Apr-Mar
            return f"{txn_date.year}-{str(txn_date.year + 1)[-2:]}"
        else:
            return f"{txn_date.year - 1}-{str(txn_date.year)[-2:]}"

    def _to_decimal(self, value) -> Decimal:
        """Convert value to Decimal safely."""
        if pd.isna(value) or value == '' or value is None:
            return Decimal("0")

        try:
            # Remove commas and convert
            clean_value = str(value).replace(",", "").strip()
            # Handle negative values in parentheses: (1000) -> -1000
            if clean_value.startswith('(') and clean_value.endswith(')'):
                clean_value = '-' + clean_value[1:-1]
            return Decimal(clean_value)
        except (ValueError, InvalidOperation):
            return Decimal("0")

    def calculate_deductions(self, transactions: List[NPSTransaction],
                            basic_salary: Decimal, fy: str) -> Dict[str, Decimal]:
        """
        Calculate NPS tax deductions.

        Args:
            transactions: List of NPS transactions
            basic_salary: Annual basic salary
            fy: Financial year (e.g., "2024-25")

        Returns:
            Dict with 80CCD(1), 80CCD(1B), 80CCD(2) amounts

        Examples:
            >>> deductions = parser.calculate_deductions(
            ...     result.transactions,
            ...     Decimal("1200000"),
            ...     "2024-25"
            ... )
            >>> print(f"80CCD(1B): ₹{deductions['80CCD_1B']:,.2f}")
        """
        # Filter Tier I contributions in FY
        tier1_ee = sum(
            txn.amount for txn in transactions
            if txn.tier == 'I' and txn.contribution_type == 'EMPLOYEE'
            and txn.transaction_type.upper() in ['CONTRIBUTION', 'PURCHASE']
            and txn.financial_year == fy
        )

        tier1_er = sum(
            txn.amount for txn in transactions
            if txn.tier == 'I' and txn.contribution_type == 'EMPLOYER'
            and txn.transaction_type.upper() in ['CONTRIBUTION', 'PURCHASE']
            and txn.financial_year == fy
        )

        # 80CCD(1) - Employee contribution (part of ₹1.5L limit with 80C)
        ccd_1 = tier1_ee

        # 80CCD(1B) - Additional ₹50,000 (Tier I employee contribution)
        ccd_1b = min(tier1_ee, Decimal("50000"))

        # 80CCD(2) - Employer contribution (max 10% of Basic)
        max_80ccd2 = basic_salary * Decimal("0.10")
        ccd_2 = min(tier1_er, max_80ccd2)

        return {
            '80CCD_1': ccd_1,  # Part of ₹1.5L limit
            '80CCD_1B': ccd_1b,  # Additional ₹50K
            '80CCD_2': ccd_2,  # ER contribution
            '80CCD_2_limit': max_80ccd2
        }

    def save_to_db(self, result: ParseResult, user_id: Optional[int] = None) -> int:
        """
        Save parsed NPS data to database via TransactionService.

        All inserts flow through TransactionService.record() ensuring:
        - Idempotency (duplicate prevention)
        - Audit logging
        - Double-entry accounting (Dr NPS Asset | Cr Bank)
        - Atomic transactions

        Args:
            result: ParseResult from parsing
            user_id: User ID (optional)

        Returns:
            Number of transactions saved

        Examples:
            >>> result = parser.parse(Path("nps.csv"))
            >>> count = parser.save_to_db(result, user_id=1)
            >>> print(f"Saved {count} transactions")
        """
        if not result.success or not result.account:
            return 0

        if user_id is None:
            user_id = 1  # Default user

        txn_service = TransactionService(self.conn)
        file_hash = hashlib.sha256(result.source_file.encode()).hexdigest()[:8]

        # Get or create NPS account via TransactionService
        nps_account_id = self._get_or_create_account_via_service(
            txn_service, result.account, user_id
        )

        # Insert transactions
        count = 0
        for idx, txn in enumerate(result.transactions):
            if self._record_transaction(txn_service, nps_account_id, txn, result.source_file, file_hash, idx, user_id):
                count += 1

        return count

    def _get_or_create_account_via_service(
        self,
        txn_service: TransactionService,
        account: NPSAccount,
        user_id: int
    ) -> int:
        """Get or create NPS account via TransactionService."""
        # Check if account exists
        cursor = self.conn.execute(
            "SELECT id FROM nps_accounts WHERE pran = ?",
            (account.pran,)
        )
        row = cursor.fetchone()

        if row:
            return row['id'] if isinstance(row, dict) else row[0]

        # Create via TransactionService
        idempotency_key = f"nps_account:{account.pran}"

        asset_record = AssetRecord(
            table_name="nps_accounts",
            data={
                "user_id": user_id,
                "pran": account.pran,
                "nodal_office": account.nodal_office,
                "scheme_preference": account.scheme_preference,
                "opening_date": account.opening_date.isoformat() if account.opening_date else None,
            },
            on_conflict="IGNORE"
        )

        result = txn_service.record_asset_only(
            user_id=user_id,
            asset_records=[asset_record],
            idempotency_key=idempotency_key,
            source=TransactionSource.PARSER_NPS,
            description=f"NPS account: {account.pran}",
        )

        if result.asset_record_ids.get("nps_accounts"):
            return result.asset_record_ids["nps_accounts"]

        # If insert was ignored, fetch existing
        cursor = self.conn.execute(
            "SELECT id FROM nps_accounts WHERE pran = ?",
            (account.pran,)
        )
        row = cursor.fetchone()
        return row['id'] if isinstance(row, dict) else row[0] if row else 0

    def _record_transaction(
        self,
        txn_service: TransactionService,
        nps_account_id: int,
        txn: NPSTransaction,
        source_file: str,
        file_hash: str,
        row_idx: int,
        user_id: int
    ) -> bool:
        """
        Record NPS transaction via TransactionService with journal entry.

        For CONTRIBUTION:
            Dr NPS Asset (1304/1305)  | Amount
            Cr Bank Account (1101)    | Amount
        """
        # Generate idempotency key
        idempotency_key = f"nps:{file_hash}:{row_idx}:{txn.pran}:{txn.date.isoformat()}:{txn.amount}"

        # Create journal entries
        entries = self._create_journal_entries(txn, user_id)

        # Create asset record
        asset_record = AssetRecord(
            table_name="nps_transactions",
            data={
                "nps_account_id": nps_account_id,
                "transaction_date": txn.date.isoformat(),
                "transaction_type": txn.transaction_type,
                "tier": txn.tier,
                "contribution_type": txn.contribution_type,
                "amount": str(txn.amount),
                "units": str(txn.units),
                "nav": str(txn.nav),
                "scheme": txn.scheme,
                "financial_year": txn.financial_year,
                "source_file": source_file,
                "user_id": user_id,
            },
            on_conflict="IGNORE"
        )

        # Record via TransactionService
        description = f"NPS {txn.transaction_type}: Tier {txn.tier} - {txn.amount}"
        result = txn_service.record(
            user_id=user_id,
            entries=entries,
            description=description[:100],
            source=TransactionSource.PARSER_NPS,
            idempotency_key=idempotency_key,
            txn_date=txn.date,
            reference_type=f"NPS_{txn.transaction_type}",
            asset_records=[asset_record],
        )

        return result.result.value == "success"

    def _create_journal_entries(self, txn: NPSTransaction, user_id: int) -> List[JournalEntry]:
        """Create journal entries for NPS transaction."""
        entries = []

        # Get account IDs - Tier I is 1304, Tier II is 1305
        nps_account_code = "1304" if txn.tier == "I" else "1305"
        nps_account = get_account_by_code(self.conn, nps_account_code)
        bank_account = get_account_by_code(self.conn, "1101")  # Bank - Savings

        if not nps_account or not bank_account:
            return entries

        if txn.transaction_type == "CONTRIBUTION":
            # Dr NPS Asset | Cr Bank
            entries.append(JournalEntry(
                account_id=nps_account.id,
                debit=txn.amount,
                narration=f"NPS Tier {txn.tier} contribution: {txn.contribution_type}"
            ))
            entries.append(JournalEntry(
                account_id=bank_account.id,
                credit=txn.amount,
                narration=f"Payment to NPS"
            ))
        elif txn.transaction_type == "REDEMPTION":
            # Dr Bank | Cr NPS Asset
            entries.append(JournalEntry(
                account_id=bank_account.id,
                debit=txn.amount,
                narration=f"NPS Tier {txn.tier} withdrawal"
            ))
            entries.append(JournalEntry(
                account_id=nps_account.id,
                credit=txn.amount,
                narration=f"NPS withdrawal"
            ))

        return entries
