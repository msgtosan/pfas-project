"""
Database Integrity Auditor for Bank Intelligence.

Provides standalone scripts to query SQLite databases,
print recent records with full metadata, and verify
data integrity before reporting.
"""

import sqlite3
from pathlib import Path
from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any
import json


class DatabaseAuditor:
    """
    Audits bank intelligence database for integrity.

    Provides:
    - Recent records display with full metadata
    - Data validation checks
    - Statistics and summaries
    - Ingestion log review
    """

    def __init__(self, db_path: str):
        """
        Initialize auditor.

        Args:
            db_path: Path to money_movement.db
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        """Connect to database."""
        if not Path(self.db_path).exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")

        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self) -> "DatabaseAuditor":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    def audit_recent_records(self, count: int = 10) -> None:
        """
        Print most recent records with full metadata.

        Args:
            count: Number of records to display
        """
        if not self.conn:
            self.connect()

        print(f"\n{'='*60}")
        print(f"=== Recent Bank Transactions (Last {count}) ===")
        print(f"{'='*60}")

        cursor = self.conn.execute(
            """
            SELECT uid, user_name, bank_name, txn_date, base_string,
                   amount, txn_type, category, fiscal_year, source_file,
                   created_at
            FROM bank_transactions_intel
            ORDER BY created_at DESC, txn_date DESC
            LIMIT ?
            """,
            (count,)
        )

        rows = cursor.fetchall()

        if not rows:
            print("\nNo transactions found in database.")
            return

        for row in rows:
            print(f"\nUID: {row['uid'][:16]}...")
            print(f"User: {row['user_name']} | Bank: {row['bank_name']} | Date: {row['txn_date']}")
            print(f"Base String: {row['base_string'][:60]}{'...' if len(row['base_string'] or '') > 60 else ''}")

            amount = Decimal(row['amount']) if row['amount'] else Decimal(0)
            sign = "+" if amount >= 0 else ""
            print(f"Amount: {sign}{amount:,.2f} ({row['txn_type']})")
            print(f"Category: {row['category']} | FY: {row['fiscal_year']}")

            if row['source_file']:
                print(f"Source: {Path(row['source_file']).name}")

            print(f"Ingested: {row['created_at']}")
            print("-" * 40)

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get database statistics.

        Returns:
            Dictionary with various statistics
        """
        if not self.conn:
            self.connect()

        stats = {}

        # Total transactions
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM bank_transactions_intel"
        )
        stats["total_transactions"] = cursor.fetchone()[0]

        # Transactions by user
        cursor = self.conn.execute(
            """
            SELECT user_name, COUNT(*) as count
            FROM bank_transactions_intel
            GROUP BY user_name
            """
        )
        stats["by_user"] = {row[0]: row[1] for row in cursor.fetchall()}

        # Transactions by bank
        cursor = self.conn.execute(
            """
            SELECT bank_name, COUNT(*) as count
            FROM bank_transactions_intel
            GROUP BY bank_name
            """
        )
        stats["by_bank"] = {row[0]: row[1] for row in cursor.fetchall()}

        # Transactions by fiscal year
        cursor = self.conn.execute(
            """
            SELECT fiscal_year, COUNT(*) as count
            FROM bank_transactions_intel
            GROUP BY fiscal_year
            ORDER BY fiscal_year DESC
            """
        )
        stats["by_fiscal_year"] = {row[0]: row[1] for row in cursor.fetchall()}

        # Transactions by category
        cursor = self.conn.execute(
            """
            SELECT category, COUNT(*) as count,
                   SUM(CAST(amount AS REAL)) as total
            FROM bank_transactions_intel
            GROUP BY category
            ORDER BY count DESC
            """
        )
        stats["by_category"] = {
            row[0]: {"count": row[1], "total": row[2]}
            for row in cursor.fetchall()
        }

        # Date range
        cursor = self.conn.execute(
            """
            SELECT MIN(txn_date), MAX(txn_date)
            FROM bank_transactions_intel
            """
        )
        row = cursor.fetchone()
        stats["date_range"] = {
            "earliest": row[0],
            "latest": row[1]
        }

        # Total credits and debits
        cursor = self.conn.execute(
            """
            SELECT txn_type, SUM(ABS(CAST(amount AS REAL))) as total
            FROM bank_transactions_intel
            GROUP BY txn_type
            """
        )
        for row in cursor.fetchall():
            if row[0] == "CREDIT":
                stats["total_credits"] = row[1]
            else:
                stats["total_debits"] = row[1]

        return stats

    def print_statistics(self) -> None:
        """Print formatted statistics."""
        stats = self.get_statistics()

        print(f"\n{'='*60}")
        print("=== Database Statistics ===")
        print(f"{'='*60}")

        print(f"\nTotal Transactions: {stats['total_transactions']:,}")

        if stats.get('date_range'):
            print(f"Date Range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")

        if stats.get('total_credits'):
            print(f"Total Credits: +{stats['total_credits']:,.2f}")
        if stats.get('total_debits'):
            print(f"Total Debits: -{stats['total_debits']:,.2f}")

        print("\n--- By User ---")
        for user, count in stats.get('by_user', {}).items():
            print(f"  {user}: {count:,}")

        print("\n--- By Bank ---")
        for bank, count in stats.get('by_bank', {}).items():
            print(f"  {bank}: {count:,}")

        print("\n--- By Fiscal Year ---")
        for fy, count in stats.get('by_fiscal_year', {}).items():
            print(f"  {fy}: {count:,}")

        print("\n--- By Category (Top 10) ---")
        sorted_cats = sorted(
            stats.get('by_category', {}).items(),
            key=lambda x: x[1]['count'],
            reverse=True
        )[:10]
        for cat, data in sorted_cats:
            total = data.get('total', 0) or 0
            sign = "+" if total >= 0 else ""
            print(f"  {cat}: {data['count']:,} txns ({sign}{total:,.2f})")

    def validate_data(self) -> List[str]:
        """
        Validate data integrity.

        Returns:
            List of validation issues found
        """
        if not self.conn:
            self.connect()

        issues = []

        # Check for missing UIDs
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM bank_transactions_intel WHERE uid IS NULL OR uid = ''"
        )
        null_uids = cursor.fetchone()[0]
        if null_uids > 0:
            issues.append(f"Found {null_uids} transactions with missing UID")

        # Check for missing dates
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM bank_transactions_intel WHERE txn_date IS NULL"
        )
        null_dates = cursor.fetchone()[0]
        if null_dates > 0:
            issues.append(f"Found {null_dates} transactions with missing date")

        # Check for missing base_string
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM bank_transactions_intel WHERE base_string IS NULL OR base_string = ''"
        )
        null_base = cursor.fetchone()[0]
        if null_base > 0:
            issues.append(f"Found {null_base} transactions with missing base_string")

        # Check for invalid amounts
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM bank_transactions_intel WHERE amount IS NULL OR amount = 0"
        )
        zero_amounts = cursor.fetchone()[0]
        if zero_amounts > 0:
            issues.append(f"Found {zero_amounts} transactions with zero/null amount")

        # Check for duplicate UIDs (shouldn't happen with PRIMARY KEY)
        cursor = self.conn.execute(
            """
            SELECT uid, COUNT(*) as cnt
            FROM bank_transactions_intel
            GROUP BY uid
            HAVING cnt > 1
            """
        )
        duplicates = cursor.fetchall()
        if duplicates:
            issues.append(f"Found {len(duplicates)} duplicate UIDs (database integrity issue!)")

        # Check for future dates
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM bank_transactions_intel WHERE txn_date > date('now')"
        )
        future_dates = cursor.fetchone()[0]
        if future_dates > 0:
            issues.append(f"Found {future_dates} transactions with future dates")

        # Check fiscal year consistency
        cursor = self.conn.execute(
            """
            SELECT COUNT(*) FROM bank_transactions_intel
            WHERE fiscal_year IS NULL OR fiscal_year = ''
            """
        )
        null_fy = cursor.fetchone()[0]
        if null_fy > 0:
            issues.append(f"Found {null_fy} transactions with missing fiscal year")

        return issues

    def print_validation_report(self) -> None:
        """Print validation report."""
        issues = self.validate_data()

        print(f"\n{'='*60}")
        print("=== Data Validation Report ===")
        print(f"{'='*60}")

        if not issues:
            print("\n[OK] No data integrity issues found.")
        else:
            print(f"\n[WARNING] Found {len(issues)} issue(s):")
            for issue in issues:
                print(f"  - {issue}")

    def review_ingestion_log(self, count: int = 10) -> None:
        """
        Review recent ingestion log entries.

        Args:
            count: Number of entries to display
        """
        if not self.conn:
            self.connect()

        print(f"\n{'='*60}")
        print(f"=== Recent Ingestion Log (Last {count}) ===")
        print(f"{'='*60}")

        cursor = self.conn.execute(
            """
            SELECT source_file, user_name, bank_name,
                   transactions_processed, transactions_inserted,
                   transactions_skipped, status, error_message, created_at
            FROM ingestion_log
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (count,)
        )

        rows = cursor.fetchall()

        if not rows:
            print("\nNo ingestion log entries found.")
            return

        for row in rows:
            status_icon = "[OK]" if row['status'] == "SUCCESS" else "[FAIL]"
            print(f"\n{status_icon} {Path(row['source_file']).name}")
            print(f"  User: {row['user_name']} | Bank: {row['bank_name']}")
            print(f"  Processed: {row['transactions_processed']} | Inserted: {row['transactions_inserted']} | Skipped: {row['transactions_skipped']}")
            print(f"  Time: {row['created_at']}")

            if row['error_message']:
                print(f"  Error: {row['error_message']}")

    def get_income_summary_for_fy(self, fiscal_year: str) -> Dict[str, Decimal]:
        """
        Get income summary for a fiscal year (for PFAS integration).

        Args:
            fiscal_year: Fiscal year (e.g., "FY 2024-25")

        Returns:
            Dictionary of category -> total income
        """
        if not self.conn:
            self.connect()

        income_categories = [
            "RENT_INCOME", "SGB_INTEREST", "DIVIDEND", "SAVINGS_INTEREST"
        ]

        placeholders = ",".join("?" * len(income_categories))
        cursor = self.conn.execute(
            f"""
            SELECT category, SUM(CAST(amount AS REAL)) as total
            FROM bank_transactions_intel
            WHERE fiscal_year = ?
              AND category IN ({placeholders})
              AND txn_type = 'CREDIT'
            GROUP BY category
            """,
            [fiscal_year] + income_categories
        )

        return {
            row[0]: Decimal(str(row[1])) if row[1] else Decimal(0)
            for row in cursor.fetchall()
        }

    def print_income_summary(self, fiscal_year: Optional[str] = None) -> None:
        """
        Print income summary for PFAS asset classes.

        Args:
            fiscal_year: Optional fiscal year filter
        """
        if not self.conn:
            self.connect()

        # Get available fiscal years if not specified
        if not fiscal_year:
            cursor = self.conn.execute(
                "SELECT DISTINCT fiscal_year FROM bank_transactions_intel ORDER BY fiscal_year DESC LIMIT 1"
            )
            row = cursor.fetchone()
            if row:
                fiscal_year = row[0]
            else:
                print("\nNo transactions found.")
                return

        print(f"\n{'='*60}")
        print(f"=== Income Summary for {fiscal_year} ===")
        print(f"{'='*60}")

        summary = self.get_income_summary_for_fy(fiscal_year)

        category_display = {
            "RENT_INCOME": "Rental Income",
            "SGB_INTEREST": "SGB Interest",
            "DIVIDEND": "Stock Dividends",
            "SAVINGS_INTEREST": "Savings Bank Interest"
        }

        total = Decimal(0)
        for category, display_name in category_display.items():
            amount = summary.get(category, Decimal(0))
            total += amount
            print(f"  {display_name}: {amount:>15,.2f}")

        print(f"  {'-'*35}")
        print(f"  {'Total Income'}: {total:>15,.2f}")

        print("\n[INFO] These amounts can be used to update PFAS asset tables.")


def main():
    """CLI entry point for auditing."""
    import sys
    import argparse

    parser = argparse.ArgumentParser(description="Bank Intelligence Database Auditor")
    parser.add_argument(
        "--db", default="Data/Reports/Bank_Intelligence/money_movement.db",
        help="Path to database file"
    )
    parser.add_argument(
        "--recent", type=int, default=10,
        help="Number of recent records to show"
    )
    parser.add_argument(
        "--stats", action="store_true",
        help="Show database statistics"
    )
    parser.add_argument(
        "--validate", action="store_true",
        help="Run data validation"
    )
    parser.add_argument(
        "--ingestion-log", action="store_true",
        help="Review ingestion log"
    )
    parser.add_argument(
        "--income", action="store_true",
        help="Show income summary for PFAS"
    )
    parser.add_argument(
        "--fy", type=str, default=None,
        help="Fiscal year filter (e.g., 'FY 2024-25')"
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Run all audit checks"
    )

    args = parser.parse_args()

    try:
        with DatabaseAuditor(args.db) as auditor:
            if args.all or not any([args.stats, args.validate, args.ingestion_log, args.income]):
                auditor.audit_recent_records(args.recent)
                auditor.print_statistics()
                auditor.print_validation_report()
                auditor.review_ingestion_log(5)
                auditor.print_income_summary(args.fy)
            else:
                if args.stats:
                    auditor.print_statistics()
                if args.validate:
                    auditor.print_validation_report()
                if args.ingestion_log:
                    auditor.review_ingestion_log(args.recent)
                if args.income:
                    auditor.print_income_summary(args.fy)
                if not any([args.stats, args.validate, args.ingestion_log, args.income]):
                    auditor.audit_recent_records(args.recent)

    except FileNotFoundError as e:
        print(f"\n[ERROR] {e}")
        print("Run the intelligent analyzer first to create the database.")
        sys.exit(1)


if __name__ == "__main__":
    main()
