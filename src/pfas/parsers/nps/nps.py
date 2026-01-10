"""NPS Statement parser."""

import pandas as pd
from pathlib import Path
from datetime import date as date_type
from decimal import Decimal
from dataclasses import dataclass, field
from typing import List, Optional, Dict
import sqlite3


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
            # Read CSV
            df = pd.read_csv(file_path)

            # Parse transactions
            transactions = self._parse_transactions(df, result)
            result.transactions = transactions

            # Extract PRAN from first transaction
            if transactions:
                result.account = NPSAccount(pran=transactions[0].pran)

            if len(transactions) == 0:
                result.add_warning("No transactions found")

        except Exception as e:
            result.add_error(f"Failed to parse CSV: {str(e)}")

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
            return Decimal(clean_value)
        except:
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
        Save parsed NPS data to database.

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

        cursor = self.conn.cursor()
        in_transaction = self.conn.in_transaction

        try:
            if not in_transaction:
                cursor.execute("BEGIN IMMEDIATE")

            # Get or create NPS account
            nps_account_id = self._get_or_create_account(result.account, user_id)

            # Insert transactions
            count = 0
            for txn in result.transactions:
                if self._insert_transaction(nps_account_id, txn, result.source_file, user_id):
                    count += 1

            if not in_transaction:
                self.conn.commit()

            return count

        except Exception as e:
            if not in_transaction:
                self.conn.rollback()
            raise Exception(f"Failed to save NPS data: {e}") from e

    def _get_or_create_account(self, account: NPSAccount, user_id: Optional[int]) -> int:
        """Get or create NPS account and return ID."""
        cursor = self.conn.execute(
            "SELECT id FROM nps_accounts WHERE pran = ?",
            (account.pran,)
        )
        row = cursor.fetchone()

        if row:
            return row['id']

        cursor = self.conn.execute(
            """INSERT INTO nps_accounts
            (user_id, pran, nodal_office, scheme_preference, opening_date)
            VALUES (?, ?, ?, ?, ?)""",
            (
                user_id,
                account.pran,
                account.nodal_office,
                account.scheme_preference,
                account.opening_date.isoformat() if account.opening_date else None
            )
        )
        return cursor.lastrowid

    def _insert_transaction(self, nps_account_id: int, txn: NPSTransaction,
                           source_file: str, user_id: Optional[int]) -> bool:
        """Insert NPS transaction into database."""
        try:
            self.conn.execute(
                """INSERT INTO nps_transactions
                (nps_account_id, transaction_date, transaction_type, tier,
                 contribution_type, amount, units, nav, scheme, financial_year,
                 source_file, user_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    nps_account_id,
                    txn.date.isoformat(),
                    txn.transaction_type,
                    txn.tier,
                    txn.contribution_type,
                    str(txn.amount),
                    str(txn.units),
                    str(txn.nav),
                    txn.scheme,
                    txn.financial_year,
                    source_file,
                    user_id
                )
            )
            return True
        except sqlite3.IntegrityError:
            # Duplicate transaction
            return False
