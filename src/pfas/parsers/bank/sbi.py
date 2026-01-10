"""
SBI (State Bank of India) statement parser.

Parses SBI Excel statements with transaction details.
"""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional
import pandas as pd

from pfas.parsers.bank.base import BankStatementParser
from pfas.parsers.bank.models import ParseResult, BankTransaction, BankAccount


class SBIParser(BankStatementParser):
    """Parser for SBI Excel statements."""

    BANK_NAME = "State Bank of India"

    # Expected column names (case insensitive matching)
    EXPECTED_COLUMNS = {
        "date": ["txn date", "date", "txn_date", "transaction date"],
        "value_date": ["value date", "value_date", "val date"],
        "description": ["description", "narration", "particulars"],
        "ref_no": ["ref no", "cheque no", "ref_no", "chq no", "reference"],
        "debit": ["debit", "withdrawal", "dr"],
        "credit": ["credit", "deposit", "cr"],
        "balance": ["balance", "closing balance", "bal"]
    }

    def _parse_content(self, content: dict, source_file: str) -> ParseResult:
        """Parse SBI Excel statement content."""
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
        # Look for account number in first few rows
        for i in range(min(10, len(df))):
            for col in df.columns:
                cell_value = str(df.iloc[i][col])
                if "account" in cell_value.lower() and "number" in cell_value.lower():
                    # Try to extract account number from next cell or same row
                    # Pattern: "Account Number: 12345678901"
                    import re
                    match = re.search(r'(\d{9,})', cell_value)
                    if match:
                        return BankAccount(
                            account_number=match.group(1),
                            bank_name=self.BANK_NAME
                        )

                    # Check next column
                    if i < len(df.columns) - 1:
                        next_value = str(df.iloc[i][df.columns[list(df.columns).index(col) + 1]])
                        match = re.search(r'(\d{9,})', next_value)
                        if match:
                            return BankAccount(
                                account_number=match.group(1),
                                bank_name=self.BANK_NAME
                            )

        # If not found in metadata, return None
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

        # Map columns to expected names
        column_map = self._map_columns(df_txn.columns)

        transactions = []

        for idx, row in df_txn.iterrows():
            try:
                # Skip empty rows
                if row.isna().all():
                    continue

                # Extract values
                date_val = row.get(column_map.get("date"))
                value_date_val = row.get(column_map.get("value_date"))
                description_val = row.get(column_map.get("description"))
                ref_no_val = row.get(column_map.get("ref_no"))
                debit_val = row.get(column_map.get("debit"))
                credit_val = row.get(column_map.get("credit"))
                balance_val = row.get(column_map.get("balance"))

                # Parse date
                txn_date = self._parse_date_value(date_val)
                if not txn_date:
                    continue

                value_date = self._parse_date_value(value_date_val) or txn_date

                # Parse description
                description = str(description_val).strip() if pd.notna(description_val) else ""
                if not description:
                    continue

                # Parse amounts
                debit = self._parse_amount_value(debit_val)
                credit = self._parse_amount_value(credit_val)
                balance = self._parse_amount_value(balance_val)

                # Parse reference number
                ref_no = str(ref_no_val).strip() if pd.notna(ref_no_val) else None

                txn = BankTransaction(
                    date=txn_date,
                    value_date=value_date,
                    description=description,
                    reference_number=ref_no,
                    debit=debit,
                    credit=credit,
                    balance=balance
                )
                transactions.append(txn)

            except Exception as e:
                # Skip problematic rows
                continue

        return transactions

    def _find_header_row(self, df: pd.DataFrame) -> Optional[int]:
        """Find the row containing column headers."""
        for i in range(min(20, len(df))):
            row_text = " ".join(str(val).lower() for val in df.iloc[i] if pd.notna(val))

            if ("date" in row_text and "description" in row_text) or \
               ("date" in row_text and "narration" in row_text) or \
               ("date" in row_text and "particulars" in row_text):
                return i

        return None

    def _map_columns(self, columns: pd.Index) -> dict:
        """Map DataFrame columns to expected column names."""
        column_map = {}

        for expected_name, possible_names in self.EXPECTED_COLUMNS.items():
            for col in columns:
                col_lower = str(col).lower().strip()
                if any(pn in col_lower for pn in possible_names):
                    column_map[expected_name] = col
                    break

        return column_map

    def _parse_date_value(self, value) -> Optional[datetime]:
        """Parse date from cell value."""
        if pd.isna(value):
            return None

        # If already datetime
        if isinstance(value, (pd.Timestamp, datetime)):
            return value.date() if hasattr(value, 'date') else value

        # Try parsing string
        date_str = str(value).strip()
        if not date_str:
            return None

        formats = [
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%d-%b-%Y",
            "%d %b %Y",
            "%d.%m.%Y"
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        return None

    def _parse_amount_value(self, value) -> Decimal:
        """Parse amount from cell value."""
        if pd.isna(value):
            return Decimal("0")

        # If already numeric
        if isinstance(value, (int, float)):
            return Decimal(str(value))

        # Parse string
        amount_str = str(value).strip()
        if not amount_str or amount_str == "-":
            return Decimal("0")

        # Remove commas and spaces
        cleaned = amount_str.replace(",", "").replace(" ", "")

        try:
            return Decimal(cleaned) if cleaned else Decimal("0")
        except:
            return Decimal("0")
