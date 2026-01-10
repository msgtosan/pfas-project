"""
ICICI Bank Excel statement parser.

Parses ICICI Bank Excel statements (downloaded from NetBanking).
"""

import re
from datetime import datetime
from decimal import Decimal
from typing import List, Optional
import pandas as pd

from pfas.parsers.bank.base import BankStatementParser
from pfas.parsers.bank.models import ParseResult, BankTransaction, BankAccount


class ICICIExcelParser(BankStatementParser):
    """Parser for ICICI Bank Excel statements."""

    BANK_NAME = "ICICI Bank"

    def _parse_content(self, content: dict, source_file: str) -> ParseResult:
        """Parse ICICI Excel statement content."""
        result = ParseResult(success=False, source_file=source_file)

        df = content.get("dataframe")
        if df is None or df.empty:
            result.add_error("No data found in Excel file")
            return result

        # Extract account information
        account = self._extract_account_info(df)
        if not account:
            result.add_error("Could not extract account number from Excel")
            return result

        result.account = account

        # Parse transactions
        transactions = self._parse_transactions(df)

        if not transactions:
            result.add_error("No transactions found in Excel")
            return result

        result.transactions = transactions
        result.success = True

        # Set statement period from first and last transaction
        if transactions:
            dates = [t.date for t in transactions]
            result.statement_period_start = min(dates)
            result.statement_period_end = max(dates)

        return result

    def _extract_account_info(self, df: pd.DataFrame) -> Optional[BankAccount]:
        """Extract account information from DataFrame."""
        # Look for account number in first 10 rows
        for i in range(min(10, len(df))):
            for col in df.columns:
                cell_value = str(df.iloc[i][col])

                # ICICI format: "003101204539 ( INR ) - SANJAY SHANKAR"
                if "account number" in cell_value.lower():
                    # Check next row for actual number
                    if i + 1 < len(df):
                        for next_col in df.columns:
                            next_value = str(df.iloc[i + 1][next_col])
                            # Look for pattern like "003101204539 ( INR )"
                            match = re.search(r'(\d{12,})\s*\(\s*INR\s*\)', next_value)
                            if match:
                                account_number = match.group(1)

                                # Extract name if present
                                name_match = re.search(r'-\s*(.+?)$', next_value)

                                return BankAccount(
                                    account_number=account_number,
                                    bank_name=self.BANK_NAME
                                )

                # Direct pattern match in same cell
                match = re.search(r'(\d{12,})\s*\(\s*INR\s*\)', cell_value)
                if match:
                    account_number = match.group(1)
                    return BankAccount(
                        account_number=account_number,
                        bank_name=self.BANK_NAME
                    )

        return None

    def _parse_transactions(self, df: pd.DataFrame) -> List[BankTransaction]:
        """Parse transactions from DataFrame."""
        # Find the header row
        header_row = self._find_header_row(df)
        if header_row is None:
            return []

        # Create new DataFrame with proper headers
        df_txn = df.iloc[header_row + 1:].copy()
        df_txn.columns = df.iloc[header_row]

        # Reset index
        df_txn = df_txn.reset_index(drop=True)

        # Map columns
        column_map = {
            "date": None,
            "value_date": None,
            "description": None,
            "cheque_no": None,
            "debit": None,
            "credit": None,
            "balance": None,
        }

        # Find column indices
        for col in df_txn.columns:
            col_str = str(col).strip().upper()

            if "TRANSACTION DATE" in col_str:
                column_map["date"] = col
            elif "VALUE DATE" in col_str:
                column_map["value_date"] = col
            elif "TRANSACTION REMARKS" in col_str or "DESCRIPTION" in col_str or "PARTICULARS" in col_str:
                column_map["description"] = col
            elif "CHEQUE" in col_str:
                column_map["cheque_no"] = col
            elif "WITHDRAWAL" in col_str or "DEBIT" in col_str:
                column_map["debit"] = col
            elif "DEPOSIT" in col_str or "CREDIT" in col_str:
                column_map["credit"] = col
            elif "BALANCE" in col_str:
                column_map["balance"] = col

        if not column_map["date"] or not column_map["description"]:
            return []

        transactions = []

        for idx, row in df_txn.iterrows():
            try:
                # Skip empty rows
                if row.isna().all():
                    continue

                # Extract values
                date_val = row.get(column_map["date"])
                value_date_val = row.get(column_map["value_date"])
                description_val = row.get(column_map["description"])
                cheque_no_val = row.get(column_map["cheque_no"])
                debit_val = row.get(column_map["debit"])
                credit_val = row.get(column_map["credit"])
                balance_val = row.get(column_map["balance"])

                # Parse date
                txn_date = self._parse_date_value(date_val)
                if not txn_date:
                    continue

                value_date = self._parse_date_value(value_date_val) or txn_date

                # Parse description
                description = str(description_val).strip() if pd.notna(description_val) else ""
                if not description or description == "nan":
                    continue

                # Parse amounts
                debit = self._parse_amount_value(debit_val)
                credit = self._parse_amount_value(credit_val)
                balance = self._parse_amount_value(balance_val)

                # Parse reference number
                ref_no = str(cheque_no_val).strip() if pd.notna(cheque_no_val) and str(cheque_no_val) != "nan" else None

                txn = BankTransaction(
                    date=txn_date,
                    value_date=value_date,
                    description=description,
                    debit=debit,
                    credit=credit,
                    balance=balance,
                    reference_number=ref_no,
                )

                transactions.append(txn)

            except (ValueError, TypeError) as e:
                # Skip malformed rows
                continue

        return transactions

    def _find_header_row(self, df: pd.DataFrame) -> Optional[int]:
        """Find the header row in the DataFrame."""
        for i in range(min(20, len(df))):
            row_text = " ".join(str(cell or "").upper() for cell in df.iloc[i])

            # ICICI header contains "Transaction Date" and "Balance"
            if ("TRANSACTION DATE" in row_text or "VALUE DATE" in row_text) and \
               ("BALANCE" in row_text or "WITHDRAWAL" in row_text or "DEPOSIT" in row_text):
                return i

        return None

    def _parse_date_value(self, date_val) -> Optional[datetime]:
        """Parse date value from Excel cell."""
        if pd.isna(date_val):
            return None

        date_str = str(date_val).strip()
        if not date_str or date_str == "nan":
            return None

        # Try different date formats
        formats = [
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y-%m-%d",
            "%d/%m/%y",
            "%d-%m-%y",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        return None

    def _parse_amount_value(self, amount_val) -> Decimal:
        """Parse amount value from Excel cell."""
        if pd.isna(amount_val):
            return Decimal("0")

        amount_str = str(amount_val).strip()
        if not amount_str or amount_str == "nan":
            return Decimal("0")

        # Remove commas and convert
        try:
            cleaned = amount_str.replace(",", "")
            return Decimal(cleaned)
        except:
            return Decimal("0")
