"""
Intelligent Bank Statement Analyzer.

Provides dynamic header detection, category classification,
and intelligent ingestion of bank statements into SQLite database.
"""

import os
import sqlite3
import json
from pathlib import Path
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional, List, Tuple, Dict, Any
import hashlib

import pandas as pd

try:
    from rapidfuzz import fuzz, process
    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False

from .models import (
    BankTransactionIntel, UserBankConfig, IngestionResult, TransactionType
)
from .category_rules import CategoryClassifier


# Header keywords for fuzzy matching
HEADER_KEYWORDS = [
    "REMARK", "REMARKS", "NARRATION", "DESCRIPTION", "PARTICULARS",
    "WITHDRAWAL", "DR", "DEBIT", "WITHDRAWAL/DR",
    "DEPOSIT", "CR", "CREDIT", "DEPOSIT/CR",
    "DATE", "TXN DATE", "TRANSACTION DATE", "VALUE DATE",
    "BALANCE", "CLOSING BALANCE", "AVAILABLE BALANCE"
]

# Date formats for parsing
DATE_FORMATS = [
    "%d/%m/%Y",      # 04/04/2024
    "%d-%m-%Y",      # 04-04-2024
    "%Y-%m-%d",      # 2024-04-04
    "%d %b %Y",      # 04 Apr 2024
    "%d-%b-%Y",      # 04-Apr-2024
    "%d/%m/%y",      # 04/04/24
    "%d-%m-%y",      # 04-04-24
    "%Y/%m/%d",      # 2024/04/04
    "%m/%d/%Y",      # 04/04/2024 (US format - fallback)
]

# Database schema
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS bank_transactions_intel (
    uid TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    bank_name TEXT NOT NULL,
    txn_date DATE NOT NULL,
    value_date DATE,
    remarks TEXT,
    base_string TEXT NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    txn_type TEXT CHECK(txn_type IN ('CREDIT', 'DEBIT')),
    balance DECIMAL(15,2),
    category TEXT,
    sub_category TEXT,
    fiscal_year TEXT,
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_intel_user ON bank_transactions_intel(user_name);
CREATE INDEX IF NOT EXISTS idx_intel_bank ON bank_transactions_intel(bank_name);
CREATE INDEX IF NOT EXISTS idx_intel_date ON bank_transactions_intel(txn_date);
CREATE INDEX IF NOT EXISTS idx_intel_category ON bank_transactions_intel(category);
CREATE INDEX IF NOT EXISTS idx_intel_fy ON bank_transactions_intel(fiscal_year);

CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    parent_category TEXT,
    keywords TEXT,
    is_income BOOLEAN DEFAULT FALSE,
    asset_class TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL,
    user_name TEXT NOT NULL,
    bank_name TEXT NOT NULL,
    transactions_processed INTEGER DEFAULT 0,
    transactions_inserted INTEGER DEFAULT 0,
    transactions_skipped INTEGER DEFAULT 0,
    status TEXT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ingestion_file ON ingestion_log(source_file);
CREATE INDEX IF NOT EXISTS idx_ingestion_user ON ingestion_log(user_name);
"""


class BankIntelligenceAnalyzer:
    """
    Intelligent analyzer for bank statements.

    Provides:
    - Recursive directory scanning
    - Fuzzy header detection
    - Automatic category classification
    - Deduplication via UID
    - SQLite storage with full metadata
    """

    def __init__(self, db_path: str, data_root: str = ""):
        """
        Initialize analyzer.

        Args:
            db_path: Path to SQLite database (money_movement.db)
            data_root: Root directory for user data. When empty, uses PathResolver
                       config via config/paths.json. For standalone usage, pass
                       explicit path like "Data/Users".

        Note:
            When integrating with PFAS, use PathResolver to get the correct path:
            >>> resolver = PathResolver(project_root, user_name)
            >>> data_root = str(resolver.user_dir.parent)  # Gets Users directory
        """
        if not data_root:
            # Try to load from config
            from pfas.core.paths import PathResolver
            try:
                resolver = PathResolver(Path.cwd(), "")
                data_root = resolver.config.get("users_base", "Users")
            except Exception:
                data_root = "Users"  # Fallback default
        self.db_path = db_path
        self.data_root = Path(data_root)
        self.conn: Optional[sqlite3.Connection] = None
        self.classifier = CategoryClassifier()

    def connect(self) -> None:
        """Connect to database and initialize schema."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self) -> "BankIntelligenceAnalyzer":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    def scan_and_ingest_all(self) -> IngestionResult:
        """
        Recursively scan Data/Users directory and ingest all bank statements.

        Supports two directory structures:
        1. Legacy: Data/Users/{user}/Bank/{bank_name}/
        2. New: Data/Users/{user}/inbox/Bank/{bank_name}/ and
                Data/Users/{user}/archive/Bank/{bank_name}/

        Returns:
            IngestionResult with summary of all ingestions
        """
        if not self.conn:
            self.connect()

        result = IngestionResult(success=True)

        if not self.data_root.exists():
            result.success = False
            result.add_error(f"Data root not found: {self.data_root}")
            return result

        # Scan for users
        for user_dir in self.data_root.iterdir():
            if not user_dir.is_dir():
                continue

            user_name = user_dir.name

            # Find all bank directories (supports both legacy and new structure)
            bank_dirs_to_scan = []

            # New structure: inbox/Bank and archive/Bank
            for subdir in ["inbox", "archive"]:
                bank_base = user_dir / subdir / "Bank"
                if bank_base.exists():
                    bank_dirs_to_scan.append(bank_base)

            # Legacy structure: direct Bank folder
            legacy_bank = user_dir / "Bank"
            if legacy_bank.exists() and legacy_bank not in bank_dirs_to_scan:
                # Only add if it's not a symlink to inbox/archive
                if not any(legacy_bank.resolve() == bd.resolve() for bd in bank_dirs_to_scan):
                    bank_dirs_to_scan.append(legacy_bank)

            # Scan all found bank directories
            for bank_dir in bank_dirs_to_scan:
                for bank_subdir in bank_dir.iterdir():
                    if not bank_subdir.is_dir():
                        continue

                    bank_name = bank_subdir.name
                    bank_result = self._ingest_bank_directory(
                        bank_subdir, user_name, bank_name
                    )

                    # Aggregate results
                    result.transactions_processed += bank_result.transactions_processed
                    result.transactions_inserted += bank_result.transactions_inserted
                    result.transactions_skipped += bank_result.transactions_skipped
                    result.errors.extend(bank_result.errors)
                    result.warnings.extend(bank_result.warnings)
                    result.source_files.extend(bank_result.source_files)

                    if not bank_result.success:
                        result.success = False

        return result

    def _find_bank_config(
        self, bank_dir: Path, user_name: str, bank_name: str
    ) -> Optional[Path]:
        """
        Find bank config file in multiple locations.

        Search order:
        1. bank_dir/user_bank_config.json (alongside statements)
        2. user_dir/config/user_bank_config.json (user config dir)
        3. user_dir/config/{bank_name.lower()}_bank_config.json

        Returns:
            Path to config file if found, None otherwise
        """
        # Derive user directory from bank_dir
        # bank_dir is like: Data/Users/{user}/inbox/Bank/{bank} or Data/Users/{user}/Bank/{bank}
        parts = bank_dir.parts
        try:
            # Find 'Bank' in path and get user_dir
            bank_idx = parts.index('Bank')
            user_dir = Path(*parts[:bank_idx - 1]) if parts[bank_idx - 1] in ('inbox', 'archive') else Path(*parts[:bank_idx])
        except (ValueError, IndexError):
            user_dir = bank_dir.parent.parent

        # Search locations
        candidates = [
            bank_dir / "user_bank_config.json",
            user_dir / "config" / "user_bank_config.json",
            user_dir / "config" / f"{bank_name.lower()}_bank_config.json",
        ]

        for candidate in candidates:
            if candidate.exists():
                return candidate

        return None

    def _ingest_bank_directory(
        self, bank_dir: Path, user_name: str, bank_name: str
    ) -> IngestionResult:
        """
        Ingest all statements from a bank directory.

        Args:
            bank_dir: Path to bank directory
            user_name: User name
            bank_name: Bank name

        Returns:
            IngestionResult for this bank
        """
        result = IngestionResult(success=True)

        # Load or create config
        config_path = self._find_bank_config(bank_dir, user_name, bank_name)
        if config_path:
            try:
                config = UserBankConfig.from_json(str(config_path))
            except Exception as e:
                result.add_warning(f"Failed to load config from {config_path}: {e}, using defaults")
                config = UserBankConfig.default_for_bank(user_name, bank_name)
        else:
            config = UserBankConfig.default_for_bank(user_name, bank_name)
            # Optionally create default config in bank directory
            default_config_path = bank_dir / "user_bank_config.json"
            try:
                config.to_json(str(default_config_path))
            except Exception:
                pass  # Ignore if we can't write config

        # Apply custom overrides to classifier
        if config.category_overrides:
            self.classifier = CategoryClassifier(config.category_overrides)

        # Find statement files
        statement_files = list(bank_dir.glob("*.xls")) + \
                         list(bank_dir.glob("*.xlsx")) + \
                         list(bank_dir.glob("*.csv"))

        for file_path in statement_files:
            file_result = self.ingest_statement(
                str(file_path), user_name, bank_name, config
            )
            result.transactions_processed += file_result.transactions_processed
            result.transactions_inserted += file_result.transactions_inserted
            result.transactions_skipped += file_result.transactions_skipped
            result.errors.extend(file_result.errors)
            result.warnings.extend(file_result.warnings)
            result.source_files.append(str(file_path))

            if not file_result.success:
                result.success = False

        return result

    def ingest_statement(
        self,
        file_path: str,
        user_name: str,
        bank_name: str,
        config: Optional[UserBankConfig] = None
    ) -> IngestionResult:
        """
        Ingest a single bank statement file.

        Args:
            file_path: Path to statement file
            user_name: User name
            bank_name: Bank name
            config: Optional user bank configuration

        Returns:
            IngestionResult for this file
        """
        if not self.conn:
            self.connect()

        result = IngestionResult(success=True)
        result.source_files.append(file_path)

        try:
            # Read file
            df = self._read_statement_file(file_path)

            if df is None or df.empty:
                result.add_error(f"Empty or unreadable file: {file_path}")
                result.success = False
                return result

            # Find header row
            header_row = self._find_header_row(df, config)
            if header_row < 0:
                result.add_error(f"Could not detect header row in: {file_path}")
                result.success = False
                return result

            # Set header and filter data
            df = self._set_header_and_filter(df, header_row)

            # Map columns
            column_map = self._map_columns(df, config)
            if not column_map:
                result.add_error(f"Could not map required columns in: {file_path}")
                result.success = False
                return result

            # Parse transactions
            transactions = self._parse_transactions(
                df, column_map, user_name, bank_name, file_path, config
            )

            result.transactions_processed = len(transactions)

            # Insert transactions
            for txn in transactions:
                if self._insert_transaction(txn):
                    result.transactions_inserted += 1
                else:
                    result.transactions_skipped += 1  # Duplicate

            # Log ingestion
            self._log_ingestion(result, user_name, bank_name, file_path)

        except Exception as e:
            result.success = False
            result.add_error(f"Error processing {file_path}: {str(e)}")

        return result

    def _read_statement_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Read statement file into DataFrame."""
        file_path = Path(file_path)
        suffix = file_path.suffix.lower()

        try:
            if suffix in ['.xls', '.xlsx']:
                # Try reading without header first to detect header row
                df = pd.read_excel(file_path, header=None)
            elif suffix == '.csv':
                df = pd.read_csv(file_path, header=None)
            else:
                return None

            return df
        except Exception:
            return None

    def _find_header_row(
        self, df: pd.DataFrame, config: Optional[UserBankConfig] = None
    ) -> int:
        """
        Find header row using fuzzy matching.

        Args:
            df: DataFrame without header set
            config: Optional configuration with header keywords

        Returns:
            Row index of header, or -1 if not found
        """
        keywords = HEADER_KEYWORDS
        if config and config.header_search_keywords:
            keywords = config.header_search_keywords + HEADER_KEYWORDS

        best_score = 0
        best_row = -1

        # Only check first 20 rows
        for idx in range(min(20, len(df))):
            row_values = df.iloc[idx].astype(str).tolist()
            row_text = " ".join(row_values).upper()

            # Count keyword matches
            score = 0
            for keyword in keywords:
                if keyword.upper() in row_text:
                    score += 1

            # Also use fuzzy matching if available
            if HAS_RAPIDFUZZ:
                for val in row_values:
                    val_upper = str(val).upper()
                    for keyword in keywords:
                        fuzzy_score = fuzz.ratio(val_upper, keyword.upper())
                        if fuzzy_score >= 80:
                            score += 0.5

            if score > best_score:
                best_score = score
                best_row = idx

        # Require at least 2 keyword matches
        if best_score >= 2:
            return best_row

        return -1

    def _set_header_and_filter(
        self, df: pd.DataFrame, header_row: int
    ) -> pd.DataFrame:
        """Set header row and filter data rows."""
        # Set column names from header row
        df.columns = df.iloc[header_row].astype(str)

        # Keep only rows after header
        df = df.iloc[header_row + 1:].reset_index(drop=True)

        # Drop empty rows
        df = df.dropna(how='all')

        return df

    def _map_columns(
        self, df: pd.DataFrame, config: Optional[UserBankConfig] = None
    ) -> Optional[Dict[str, str]]:
        """
        Map DataFrame columns to required fields.

        Returns:
            Dictionary mapping field names to column names, or None if failed
        """
        columns = [str(c).upper() for c in df.columns]
        column_map = {}

        # Find date column
        date_names = ["DATE", "TXN DATE", "TRANSACTION DATE", "VALUE DATE",
                      "TRAN DATE", "TRANS DATE", "TXN. DATE"]
        if config and config.date_column_names:
            date_names = config.date_column_names + date_names

        for name in date_names:
            for i, col in enumerate(columns):
                if name.upper() in col:
                    column_map["date"] = df.columns[i]
                    break
            if "date" in column_map:
                break

        # Find description/remarks column
        desc_names = ["REMARK", "REMARKS", "NARRATION", "DESCRIPTION",
                      "PARTICULARS", "TRANSACTION PARTICULARS", "DETAILS"]
        for name in desc_names:
            for i, col in enumerate(columns):
                if name.upper() in col:
                    column_map["description"] = df.columns[i]
                    break
            if "description" in column_map:
                break

        # Find debit column
        debit_names = ["WITHDRAWAL", "DR", "DEBIT", "WITHDRAWAL/DR",
                       "WITHDRAWAL AMT", "DEBIT AMOUNT", "DR AMOUNT"]
        if config and config.amount_column_patterns.get("debit"):
            debit_names = config.amount_column_patterns["debit"] + debit_names

        for name in debit_names:
            for i, col in enumerate(columns):
                if name.upper() in col or col == name.upper():
                    column_map["debit"] = df.columns[i]
                    break
            if "debit" in column_map:
                break

        # Find credit column
        credit_names = ["DEPOSIT", "CR", "CREDIT", "DEPOSIT/CR",
                        "DEPOSIT AMT", "CREDIT AMOUNT", "CR AMOUNT"]
        if config and config.amount_column_patterns.get("credit"):
            credit_names = config.amount_column_patterns["credit"] + credit_names

        for name in credit_names:
            for i, col in enumerate(columns):
                if name.upper() in col or col == name.upper():
                    column_map["credit"] = df.columns[i]
                    break
            if "credit" in column_map:
                break

        # Find balance column (optional)
        balance_names = ["BALANCE", "CLOSING BALANCE", "BAL", "AVAILABLE BALANCE"]
        for name in balance_names:
            for i, col in enumerate(columns):
                if name.upper() in col:
                    column_map["balance"] = df.columns[i]
                    break
            if "balance" in column_map:
                break

        # Require at least date and description and one of debit/credit
        if "date" not in column_map or "description" not in column_map:
            return None
        if "debit" not in column_map and "credit" not in column_map:
            return None

        return column_map

    def _parse_transactions(
        self,
        df: pd.DataFrame,
        column_map: Dict[str, str],
        user_name: str,
        bank_name: str,
        source_file: str,
        config: Optional[UserBankConfig] = None
    ) -> List[BankTransactionIntel]:
        """Parse DataFrame rows into transactions."""
        transactions = []

        for _, row in df.iterrows():
            try:
                # Parse date
                date_val = row.get(column_map["date"])
                txn_date = self._parse_date(date_val, config)
                if txn_date is None:
                    continue

                # Get description
                description = str(row.get(column_map["description"], ""))
                if not description or description.lower() in ['nan', 'none', '']:
                    continue

                # Parse amounts
                debit = Decimal("0")
                credit = Decimal("0")

                if "debit" in column_map:
                    debit = self._parse_amount(row.get(column_map["debit"]))
                if "credit" in column_map:
                    credit = self._parse_amount(row.get(column_map["credit"]))

                # Skip if no amount
                if debit == 0 and credit == 0:
                    continue

                # Determine transaction type
                if debit > 0:
                    txn_type = TransactionType.DEBIT
                    amount = debit
                else:
                    txn_type = TransactionType.CREDIT
                    amount = credit

                # Parse balance
                balance = None
                if "balance" in column_map:
                    balance = self._parse_amount(row.get(column_map["balance"]))
                    if balance == 0:
                        balance = None

                # Classify category
                category, sub_category, is_income = self.classifier.classify(description)

                # Create transaction
                txn = BankTransactionIntel(
                    user_name=user_name,
                    bank_name=bank_name,
                    txn_date=txn_date,
                    base_string=description,
                    amount=amount,
                    txn_type=txn_type,
                    balance=balance,
                    category=category,
                    sub_category=sub_category,
                    source_file=source_file
                )

                transactions.append(txn)

            except Exception:
                continue  # Skip problematic rows

        return transactions

    def _parse_date(
        self, date_val: Any, config: Optional[UserBankConfig] = None
    ) -> Optional[date]:
        """Parse date value with multiple format fallbacks."""
        if date_val is None or pd.isna(date_val):
            return None

        # If already a date/datetime
        if isinstance(date_val, datetime):
            return date_val.date()
        if isinstance(date_val, date):
            return date_val

        # Convert to string
        date_str = str(date_val).strip()
        if not date_str or date_str.lower() in ['nan', 'none', 'nat']:
            return None

        # Try config format first
        formats = DATE_FORMATS
        if config and config.date_format:
            formats = [config.date_format] + DATE_FORMATS

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        # Try pandas as last resort (dayfirst=True for Indian format)
        try:
            return pd.to_datetime(date_str, dayfirst=True).date()
        except Exception:
            return None

    def _parse_amount(self, amount_val: Any) -> Decimal:
        """Parse amount value to Decimal."""
        if amount_val is None or pd.isna(amount_val):
            return Decimal("0")

        # Convert to string and clean
        amount_str = str(amount_val).strip()

        # Remove currency symbols and commas
        amount_str = amount_str.replace(",", "").replace("₹", "").replace("$", "")
        amount_str = amount_str.replace("(", "-").replace(")", "")
        amount_str = amount_str.strip()

        if not amount_str or amount_str.lower() in ['nan', 'none', '', '-']:
            return Decimal("0")

        try:
            return Decimal(amount_str)
        except InvalidOperation:
            return Decimal("0")

    def _insert_transaction(self, txn: BankTransactionIntel) -> bool:
        """
        Insert transaction with deduplication via UID.

        Returns:
            True if inserted, False if duplicate
        """
        try:
            data = txn.to_dict()
            self.conn.execute(
                """
                INSERT INTO bank_transactions_intel
                (uid, user_name, bank_name, txn_date, value_date, remarks,
                 base_string, amount, txn_type, balance, category, sub_category,
                 fiscal_year, source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    data["uid"],
                    data["user_name"],
                    data["bank_name"],
                    data["txn_date"],
                    data["value_date"],
                    data["remarks"],
                    data["base_string"],
                    data["amount"],
                    data["txn_type"],
                    data["balance"],
                    data["category"],
                    data["sub_category"],
                    data["fiscal_year"],
                    data["source_file"],
                )
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Duplicate UID
            return False

    def _log_ingestion(
        self,
        result: IngestionResult,
        user_name: str,
        bank_name: str,
        source_file: str
    ) -> None:
        """Log ingestion result to database."""
        try:
            status = "SUCCESS" if result.success else "FAILED"
            error_msg = "; ".join(result.errors) if result.errors else None

            self.conn.execute(
                """
                INSERT INTO ingestion_log
                (source_file, user_name, bank_name, transactions_processed,
                 transactions_inserted, transactions_skipped, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_file,
                    user_name,
                    bank_name,
                    result.transactions_processed,
                    result.transactions_inserted,
                    result.transactions_skipped,
                    status,
                    error_msg,
                )
            )
            self.conn.commit()
        except Exception:
            pass  # Don't fail on logging errors


def main():
    """CLI entry point for testing."""
    import sys

    # Default paths - try PathResolver config first, fallback to legacy
    try:
        from pfas.core.paths import PathResolver
        resolver = PathResolver(Path.cwd(), "")
        data_root = resolver.config.get("users_base", "Users")
    except Exception:
        data_root = "Users"  # Fallback for standalone usage
    db_path = "Data/Reports/Bank_Intelligence/money_movement.db"

    if len(sys.argv) > 1:
        data_root = sys.argv[1]
    if len(sys.argv) > 2:
        db_path = sys.argv[2]

    print(f"Scanning: {data_root}")
    print(f"Database: {db_path}")

    # Ensure directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    with BankIntelligenceAnalyzer(db_path, data_root) as analyzer:
        result = analyzer.scan_and_ingest_all()
        print(f"\n{result}")

        if result.errors:
            print("\nErrors:")
            for error in result.errors:
                print(f"  - {error}")

        if result.warnings:
            print("\nWarnings:")
            for warning in result.warnings:
                print(f"  - {warning}")


if __name__ == "__main__":
    main()
