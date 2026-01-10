"""
HDFC Bank statement parser.

Parses HDFC Bank PDF statements with transaction details.
"""

import re
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from pfas.parsers.bank.base import BankStatementParser
from pfas.parsers.bank.models import ParseResult, BankTransaction, BankAccount


class HDFCParser(BankStatementParser):
    """Parser for HDFC Bank PDF statements."""

    BANK_NAME = "HDFC Bank"

    # Regex patterns for HDFC format
    ACCOUNT_PATTERN = r"Account\s+(?:No|Number)[:\s]+(\d+)"
    IFSC_PATTERN = r"IFSC[:\s]+([A-Z]{4}\d{7})"
    BRANCH_PATTERN = r"Branch[:\s]+(.+?)(?:\n|$)"

    # Transaction pattern for HDFC
    # Columns: Date | Narration | Chq./Ref.No. | Value Dt | Withdrawal | Deposit | Closing Balance
    TRANSACTION_PATTERN = r"(\d{2}/\d{2}/\d{2,4})\s+(.+?)\s+([\w\d]+)?\s*(\d{2}/\d{2}/\d{2,4})?\s*([\d,]+\\.\\d{2})?\s*([\d,]+\\.\\d{2})?\s+([\d,]+\\.\\d{2})"

    def _parse_content(self, content: dict, source_file: str) -> ParseResult:
        """Parse HDFC bank statement content."""
        result = ParseResult(success=False, source_file=source_file)
        text = content.get("text", "")

        if not text:
            result.add_error("No text content found in PDF")
            return result

        # Extract account information
        account = self._extract_account_info(text)
        if not account:
            result.add_error("Could not extract account number")
            return result

        result.account = account

        # Parse transactions
        transactions = self._parse_transactions(text)

        if not transactions:
            # Try parsing from tables if available
            if content.get("tables"):
                transactions = self._parse_transactions_from_tables(content["tables"])

        if not transactions:
            result.add_error("No transactions found in statement")
            return result

        result.transactions = transactions
        result.success = True

        # Set statement period
        if transactions:
            dates = [t.date for t in transactions]
            result.statement_period_start = min(dates)
            result.statement_period_end = max(dates)

        return result

    def _extract_account_info(self, text: str) -> Optional[BankAccount]:
        """Extract account information from statement text."""
        # Extract account number
        account_match = re.search(self.ACCOUNT_PATTERN, text, re.IGNORECASE)
        if not account_match:
            return None

        account_number = account_match.group(1).strip()

        # Extract IFSC (optional)
        ifsc_match = re.search(self.IFSC_PATTERN, text, re.IGNORECASE)
        ifsc = ifsc_match.group(1) if ifsc_match else None

        # Extract branch (optional)
        branch_match = re.search(self.BRANCH_PATTERN, text, re.IGNORECASE)
        branch = branch_match.group(1).strip() if branch_match else None

        return BankAccount(
            account_number=account_number,
            bank_name=self.BANK_NAME,
            ifsc_code=ifsc,
            branch=branch
        )

    def _parse_transactions(self, text: str) -> List[BankTransaction]:
        """Parse transactions from text using regex."""
        transactions = []

        for match in re.finditer(self.TRANSACTION_PATTERN, text):
            try:
                date_str = match.group(1)
                description = match.group(2).strip()
                ref_no = match.group(3)
                value_date_str = match.group(4)
                withdrawal_str = match.group(5)
                deposit_str = match.group(6)
                balance_str = match.group(7)

                # Parse dates
                txn_date = self._parse_date(date_str)
                if not txn_date:
                    continue

                value_date = self._parse_date(value_date_str) if value_date_str else txn_date

                # Parse amounts
                debit = self._parse_amount(withdrawal_str)
                credit = self._parse_amount(deposit_str)
                balance = self._parse_amount(balance_str)

                txn = BankTransaction(
                    date=txn_date,
                    value_date=value_date,
                    description=description,
                    reference_number=ref_no.strip() if ref_no else None,
                    debit=debit,
                    credit=credit,
                    balance=balance
                )
                transactions.append(txn)

            except (ValueError, AttributeError):
                continue

        return transactions

    def _parse_transactions_from_tables(self, tables: List[List]) -> List[BankTransaction]:
        """Parse transactions from extracted PDF tables."""
        transactions = []

        for table in tables:
            if not table or len(table) < 2:
                continue

            # Find header row
            header_idx = self._find_header_row(table)
            if header_idx is None:
                continue

            # Parse data rows
            for i in range(header_idx + 1, len(table)):
                row = table[i]
                if not row or len(row) < 6:
                    continue

                try:
                    # Expected columns: Date | Narration | Ref | Value Dt | Withdrawal | Deposit | Balance
                    txn_date = self._parse_date(row[0])
                    description = str(row[1] or "").strip()
                    ref_no = str(row[2] or "").strip() if len(row) > 2 else None
                    value_date = self._parse_date(row[3]) if len(row) > 3 else None
                    debit = self._parse_amount(row[4]) if len(row) > 4 else Decimal("0")
                    credit = self._parse_amount(row[5]) if len(row) > 5 else Decimal("0")
                    balance = self._parse_amount(row[6]) if len(row) > 6 else None

                    if not txn_date or not description:
                        continue

                    txn = BankTransaction(
                        date=txn_date,
                        value_date=value_date or txn_date,
                        description=description,
                        reference_number=ref_no if ref_no else None,
                        debit=debit,
                        credit=credit,
                        balance=balance
                    )
                    transactions.append(txn)

                except (ValueError, IndexError):
                    continue

        return transactions

    def _find_header_row(self, table: List[List]) -> Optional[int]:
        """Find the header row in a table."""
        for i, row in enumerate(table):
            if not row:
                continue

            row_text = " ".join(str(cell or "").upper() for cell in row)
            if "DATE" in row_text and ("NARRATION" in row_text or "DESCRIPTION" in row_text):
                return i

        return None

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date from various formats."""
        if not date_str or not isinstance(date_str, str):
            return None

        date_str = str(date_str).strip()
        if not date_str:
            return None

        # Try different date formats
        formats = [
            "%d/%m/%Y",
            "%d/%m/%y",
            "%d-%m-%Y",
            "%d-%m-%y",
            "%Y-%m-%d"
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        return None

    def _parse_amount(self, amount_str: str) -> Decimal:
        """Parse amount string to Decimal."""
        if not amount_str or not isinstance(amount_str, str):
            return Decimal("0")

        amount_str = str(amount_str).strip()
        if not amount_str or amount_str == "-":
            return Decimal("0")

        # Remove commas and any non-numeric characters except decimal point
        cleaned = re.sub(r"[^\d.]", "", amount_str.replace(",", ""))

        try:
            return Decimal(cleaned) if cleaned else Decimal("0")
        except:
            return Decimal("0")
