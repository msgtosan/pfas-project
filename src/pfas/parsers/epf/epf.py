"""EPF Passbook PDF parser."""

import re
import pdfplumber
from pathlib import Path
from datetime import datetime, date as date_type
from decimal import Decimal
from dataclasses import dataclass, field
from typing import List, Optional
import sqlite3


@dataclass
class EPFAccount:
    """EPF account information."""
    uan: str
    establishment_id: str
    establishment_name: str
    member_id: str
    member_name: str
    date_of_joining: Optional[date_type] = None


@dataclass
class EPFTransaction:
    """EPF monthly contribution transaction."""
    wage_month: str
    transaction_date: date_type
    transaction_type: str  # CR (Credit), DR (Debit), INT (Interest)
    wages: Decimal = Decimal("0")
    eps_wages: Decimal = Decimal("0")
    employee_contribution: Decimal = Decimal("0")
    employer_contribution: Decimal = Decimal("0")
    pension_contribution: Decimal = Decimal("0")
    vpf_contribution: Decimal = Decimal("0")  # VPF if contribution > 12%
    employee_balance: Decimal = Decimal("0")
    employer_balance: Decimal = Decimal("0")
    pension_balance: Decimal = Decimal("0")


@dataclass
class EPFInterest:
    """EPF annual interest information."""
    financial_year: str
    employee_interest: Decimal = Decimal("0")
    employer_interest: Decimal = Decimal("0")
    taxable_interest: Decimal = Decimal("0")  # Interest on contribution >₹2.5L
    tds_deducted: Decimal = Decimal("0")


@dataclass
class ParseResult:
    """Result of parsing EPF passbook."""
    success: bool
    account: Optional[EPFAccount] = None
    transactions: List[EPFTransaction] = field(default_factory=list)
    interest: Optional[EPFInterest] = None
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


class EPFParser:
    """
    Parser for EPFO Member Passbook PDF.

    Supports bilingual (Hindi/English) PDF format from EPFO portal.
    Extracts account info, monthly contributions, and interest details.

    Tax implications:
    - EE contribution: 80C eligible (max ₹1.5L combined)
    - VPF (>12%): Also 80C eligible
    - Interest on contribution >₹2.5L/year: Taxable
    """

    # Regex patterns for bilingual content
    ACCOUNT_PATTERNS = {
        'uan': r'UAN\s*[:\|]\s*(\d+)',
        'establishment_id': r'Establishment ID[/Name]*\s*[:\|]\s*(\w+)',
        'establishment_name': r'Establishment ID/Name\s*[:\|]\s*\w+\s*/\s*(.+?)(?:\n|,)',
        'member_id': r'Member ID[/Name]*\s*[:\|]\s*(\w+)',
        'member_name': r'Member ID/Name\s*[:\|]\s*\w+\s*/\s*(.+?)(?:\n|UAN)',
    }

    # Financial year pattern
    FY_PATTERN = r'Financial Year\s*-\s*(\d{4})-(\d{4})'

    # Transaction line pattern: Wage Month | Date | Type | Employee Balance | Employer Balance | ...
    # This is a simplified pattern - actual PDF structure may vary
    TXN_PATTERN = r'(\w{3}-\d{4})\s+(\d{2}-\d{2}-\d{2,4})\s+(CR|DR|INT)'

    # Interest pattern
    INTEREST_PATTERN = r'Interest.*?EE.*?(\d[\d,]+\.?\d*)\s+ER.*?(\d[\d,]+\.?\d*)'
    TDS_PATTERN = r'TDS.*?(-?\d[\d,]+\.?\d*)'

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize EPF parser.

        Args:
            db_connection: Database connection
        """
        self.conn = db_connection

    def parse(self, file_path: Path) -> ParseResult:
        """
        Parse EPF passbook PDF.

        Args:
            file_path: Path to EPF passbook PDF

        Returns:
            ParseResult with account info, transactions, and interest

        Examples:
            >>> parser = EPFParser(conn)
            >>> result = parser.parse(Path("epf_passbook.pdf"))
            >>> print(f"UAN: {result.account.uan}")
            >>> print(f"Transactions: {len(result.transactions)}")
        """
        file_path = Path(file_path)
        result = ParseResult(success=True, source_file=str(file_path))

        if not file_path.exists():
            result.add_error(f"File not found: {file_path}")
            return result

        try:
            # Extract text from PDF
            text = self._extract_text(file_path)

            # Parse account information
            result.account = self._parse_account_info(text)

            # Parse financial year
            fy_match = re.search(self.FY_PATTERN, text)
            financial_year = f"{fy_match.group(1)}-{fy_match.group(2)}" if fy_match else ""

            # Parse transactions
            result.transactions = self._parse_transactions(text)

            # Parse interest
            result.interest = self._parse_interest(text, financial_year)

            if not result.account:
                result.add_error("Failed to extract account information")

        except Exception as e:
            result.add_error(f"Failed to parse PDF: {str(e)}")

        return result

    def _extract_text(self, file_path: Path) -> str:
        """Extract text from PDF using pdfplumber."""
        text = ""
        try:
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
        except Exception as e:
            raise Exception(f"PDF extraction failed: {e}")

        return text

    def _parse_account_info(self, text: str) -> Optional[EPFAccount]:
        """
        Extract account header information.

        Args:
            text: Extracted PDF text

        Returns:
            EPFAccount or None
        """
        try:
            info = {}
            for field, pattern in self.ACCOUNT_PATTERNS.items():
                match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                info[field] = match.group(1).strip() if match else ""

            if not info.get('uan') or not info.get('member_id'):
                return None

            return EPFAccount(
                uan=info['uan'],
                establishment_id=info['establishment_id'],
                establishment_name=info['establishment_name'],
                member_id=info['member_id'],
                member_name=info['member_name']
            )
        except Exception:
            return None

    def _parse_transactions(self, text: str) -> List[EPFTransaction]:
        """
        Extract monthly transactions.

        Note: This is a simplified parser. Actual EPFO PDF structure
        may require more sophisticated table extraction.

        Args:
            text: Extracted PDF text

        Returns:
            List of EPFTransaction objects
        """
        transactions = []

        # Find transaction lines
        for match in re.finditer(self.TXN_PATTERN, text, re.MULTILINE):
            wage_month = match.group(1)
            date_str = match.group(2)
            txn_type = match.group(3)

            # Parse date
            try:
                # Handle both DD-MM-YY and DD-MM-YYYY formats
                if len(date_str.split('-')[2]) == 2:
                    txn_date = datetime.strptime(date_str, "%d-%m-%y").date()
                else:
                    txn_date = datetime.strptime(date_str, "%d-%m-%Y").date()
            except:
                continue

            # Create basic transaction
            # Note: Amounts would need to be extracted from the same line or subsequent parsing
            txn = EPFTransaction(
                wage_month=wage_month,
                transaction_date=txn_date,
                transaction_type=txn_type
            )
            transactions.append(txn)

        return transactions

    def _parse_interest(self, text: str, financial_year: str) -> Optional[EPFInterest]:
        """
        Extract interest and TDS information.

        Args:
            text: Extracted PDF text
            financial_year: Financial year (e.g., "2024-2025")

        Returns:
            EPFInterest or None
        """
        try:
            # Look for interest line
            int_match = re.search(self.INTEREST_PATTERN, text, re.IGNORECASE)
            tds_match = re.search(self.TDS_PATTERN, text, re.IGNORECASE)

            if int_match:
                employee_interest = self._to_decimal(int_match.group(1))
                employer_interest = self._to_decimal(int_match.group(2))
                tds = abs(self._to_decimal(tds_match.group(1))) if tds_match else Decimal("0")

                return EPFInterest(
                    financial_year=financial_year,
                    employee_interest=employee_interest,
                    employer_interest=employer_interest,
                    tds_deducted=tds,
                    taxable_interest=tds * Decimal("10")  # Approximate (TDS @ 10%)
                )
        except Exception:
            pass

        return None

    def _to_decimal(self, value: str) -> Decimal:
        """Convert string to Decimal, handling commas."""
        if not value:
            return Decimal("0")

        # Remove commas and convert
        clean_value = value.replace(",", "").strip()
        try:
            return Decimal(clean_value)
        except:
            return Decimal("0")

    def calculate_80c_eligible(self, transactions: List[EPFTransaction]) -> Decimal:
        """
        Calculate 80C eligible amount (EE contribution + VPF).

        Args:
            transactions: List of EPF transactions

        Returns:
            Total 80C eligible amount

        Examples:
            >>> eligible = parser.calculate_80c_eligible(result.transactions)
            >>> print(f"80C eligible: ₹{eligible:,.2f}")
        """
        total_ee = sum(
            txn.employee_contribution + txn.vpf_contribution
            for txn in transactions
            if txn.transaction_type == 'CR'
        )
        return total_ee

    def save_to_db(self, result: ParseResult, user_id: Optional[int] = None) -> int:
        """
        Save parsed EPF data to database.

        Args:
            result: ParseResult from parsing
            user_id: User ID (optional)

        Returns:
            Number of transactions saved

        Examples:
            >>> result = parser.parse(Path("epf.pdf"))
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

            # Get or create EPF account
            epf_account_id = self._get_or_create_account(result.account, user_id)

            # Insert transactions
            count = 0
            for txn in result.transactions:
                if self._insert_transaction(epf_account_id, txn, result.source_file, user_id):
                    count += 1

            # Insert interest if available
            if result.interest:
                self._insert_interest(epf_account_id, result.interest, user_id)

            if not in_transaction:
                self.conn.commit()

            return count

        except Exception as e:
            if not in_transaction:
                self.conn.rollback()
            raise Exception(f"Failed to save EPF data: {e}") from e

    def _get_or_create_account(self, account: EPFAccount, user_id: Optional[int]) -> int:
        """Get or create EPF account and return ID."""
        cursor = self.conn.execute(
            "SELECT id FROM epf_accounts WHERE uan = ?",
            (account.uan,)
        )
        row = cursor.fetchone()

        if row:
            return row['id']

        cursor = self.conn.execute(
            """INSERT INTO epf_accounts
            (user_id, uan, establishment_id, establishment_name, member_id, member_name)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, account.uan, account.establishment_id,
             account.establishment_name, account.member_id, account.member_name)
        )
        return cursor.lastrowid

    def _insert_transaction(self, epf_account_id: int, txn: EPFTransaction,
                           source_file: str, user_id: Optional[int]) -> bool:
        """Insert EPF transaction into database."""
        try:
            self.conn.execute(
                """INSERT INTO epf_transactions
                (epf_account_id, wage_month, transaction_date, transaction_type,
                 wages, eps_wages, employee_contribution, employer_contribution,
                 pension_contribution, vpf_contribution, employee_balance,
                 employer_balance, pension_balance, source_file, user_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    epf_account_id,
                    txn.wage_month,
                    txn.transaction_date.isoformat(),
                    txn.transaction_type,
                    str(txn.wages),
                    str(txn.eps_wages),
                    str(txn.employee_contribution),
                    str(txn.employer_contribution),
                    str(txn.pension_contribution),
                    str(txn.vpf_contribution),
                    str(txn.employee_balance),
                    str(txn.employer_balance),
                    str(txn.pension_balance),
                    source_file,
                    user_id
                )
            )
            return True
        except sqlite3.IntegrityError:
            # Duplicate transaction
            return False

    def _insert_interest(self, epf_account_id: int, interest: EPFInterest,
                        user_id: Optional[int]) -> bool:
        """Insert EPF interest record."""
        try:
            self.conn.execute(
                """INSERT OR REPLACE INTO epf_interest
                (epf_account_id, financial_year, employee_interest, employer_interest,
                 taxable_interest, tds_deducted, user_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    epf_account_id,
                    interest.financial_year,
                    str(interest.employee_interest),
                    str(interest.employer_interest),
                    str(interest.taxable_interest),
                    str(interest.tds_deducted),
                    user_id
                )
            )
            return True
        except Exception:
            return False
