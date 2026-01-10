"""
Bank interest calculation and 80TTA/80TTB deduction.

Calculates bank interest income and eligible tax deductions under sections 80TTA and 80TTB.
"""

from decimal import Decimal
from datetime import date
from typing import Tuple, Dict
import sqlite3


class InterestCalculator:
    """Calculate bank interest and 80TTA/80TTB deduction."""

    # Section 80TTA limit (non-senior citizens)
    MAX_80TTA = Decimal("10000")

    # Section 80TTB limit (senior citizens, 60+ years)
    MAX_80TTB = Decimal("50000")

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize interest calculator.

        Args:
            db_connection: Database connection
        """
        self.conn = db_connection

    def calculate_for_fy(
        self,
        bank_account_id: int,
        fy: str,
        is_senior_citizen: bool = False
    ) -> Tuple[Decimal, Decimal]:
        """
        Calculate total interest and 80TTA/80TTB eligible amount for FY.

        Args:
            bank_account_id: Bank account ID
            fy: Financial year (e.g., '2024-25')
            is_senior_citizen: True for 80TTB (higher limit)

        Returns:
            Tuple of (total_interest, eligible_deduction)
        """
        # Get FY date range
        start_date, end_date = self._get_fy_dates(fy)

        # Query interest transactions
        cursor = self.conn.execute(
            """
            SELECT SUM(credit) as total_interest, SUM(debit) as tds_deducted
            FROM bank_transactions
            WHERE bank_account_id = ?
              AND date BETWEEN ? AND ?
              AND is_interest = 1
            """,
            (bank_account_id, start_date.isoformat(), end_date.isoformat())
        )

        row = cursor.fetchone()
        total_interest = Decimal(str(row["total_interest"] or 0))
        tds_deducted = Decimal(str(row["tds_deducted"] or 0))

        # Calculate eligible deduction
        max_deduction = self.MAX_80TTB if is_senior_citizen else self.MAX_80TTA
        eligible = min(total_interest, max_deduction)

        # Save or update summary
        self._save_summary(bank_account_id, fy, total_interest, tds_deducted, eligible)

        return total_interest, eligible

    def calculate_all_accounts(
        self,
        user_id: int,
        fy: str,
        is_senior_citizen: bool = False
    ) -> Tuple[Decimal, Decimal]:
        """
        Calculate combined interest from all bank accounts.

        Args:
            user_id: User ID
            fy: Financial year
            is_senior_citizen: True for 80TTB

        Returns:
            Tuple of (total_interest, eligible_deduction)
        """
        cursor = self.conn.execute(
            "SELECT id FROM bank_accounts WHERE user_id = ?",
            (user_id,)
        )

        total_interest = Decimal("0")
        for row in cursor.fetchall():
            interest, _ = self.calculate_for_fy(row["id"], fy, is_senior_citizen)
            total_interest += interest

        # 80TTA/80TTB applies to combined interest across all savings accounts
        max_deduction = self.MAX_80TTB if is_senior_citizen else self.MAX_80TTA
        eligible = min(total_interest, max_deduction)

        return total_interest, eligible

    def get_interest_summary(
        self,
        bank_account_id: int,
        fy: str
    ) -> Dict[str, Decimal]:
        """
        Get saved interest summary for a bank account and FY.

        Args:
            bank_account_id: Bank account ID
            fy: Financial year

        Returns:
            Dictionary with total_interest, tds_deducted, eligible_deduction
        """
        cursor = self.conn.execute(
            """
            SELECT total_interest, tds_deducted, section_80tta_eligible
            FROM bank_interest_summary
            WHERE bank_account_id = ? AND financial_year = ?
            """,
            (bank_account_id, fy)
        )

        row = cursor.fetchone()
        if not row:
            return {
                "total_interest": Decimal("0"),
                "tds_deducted": Decimal("0"),
                "eligible_deduction": Decimal("0")
            }

        return {
            "total_interest": Decimal(str(row["total_interest"])),
            "tds_deducted": Decimal(str(row["tds_deducted"])),
            "eligible_deduction": Decimal(str(row["section_80tta_eligible"]))
        }

    def _get_fy_dates(self, fy: str) -> Tuple[date, date]:
        """
        Get start and end dates for a financial year.

        Args:
            fy: Financial year string like '2024-25'

        Returns:
            Tuple of (start_date, end_date)
        """
        start_year = int(fy.split('-')[0])
        start_date = date(start_year, 4, 1)
        end_date = date(start_year + 1, 3, 31)

        return start_date, end_date

    def _save_summary(
        self,
        bank_account_id: int,
        fy: str,
        total_interest: Decimal,
        tds_deducted: Decimal,
        eligible: Decimal
    ) -> None:
        """Save or update interest summary."""
        self.conn.execute(
            """
            INSERT OR REPLACE INTO bank_interest_summary
            (bank_account_id, financial_year, total_interest, tds_deducted, section_80tta_eligible)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                bank_account_id,
                fy,
                float(total_interest),
                float(tds_deducted),
                float(eligible)
            )
        )
        self.conn.commit()

    def get_monthly_interest_breakdown(
        self,
        bank_account_id: int,
        fy: str
    ) -> Dict[str, Decimal]:
        """
        Get month-wise interest breakdown for a financial year.

        Args:
            bank_account_id: Bank account ID
            fy: Financial year

        Returns:
            Dictionary with month names as keys and interest amounts as values
        """
        start_date, end_date = self._get_fy_dates(fy)

        cursor = self.conn.execute(
            """
            SELECT strftime('%Y-%m', date) as month, SUM(credit) as interest
            FROM bank_transactions
            WHERE bank_account_id = ?
              AND date BETWEEN ? AND ?
              AND is_interest = 1
            GROUP BY month
            ORDER BY month
            """,
            (bank_account_id, start_date.isoformat(), end_date.isoformat())
        )

        breakdown = {}
        for row in cursor.fetchall():
            month = row["month"]
            interest = Decimal(str(row["interest"] or 0))
            breakdown[month] = interest

        return breakdown
