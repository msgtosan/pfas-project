"""RSU Tax Credit Correlation Module.

Correlates RSU tax credits from payslips with vest events.

When RSUs vest:
1. Company withholds shares to cover tax on perquisite value
2. Payslip shows NEGATIVE "RSU Tax" deduction (credit back)
3. Form 12BA shows RSU perquisite value
4. Form 16 Part B includes in perquisites under 17(2)

This module helps match payslip credits with vest events for
accurate tax reporting and audit trail.
"""

from datetime import date, timedelta
from decimal import Decimal
from typing import Optional, List
import sqlite3

from .models import RSUTaxCredit, CorrelationStatus


class RSUTaxCreditCorrelator:
    """
    Correlate RSU tax credits with vest events.

    The RSU tax credit in payslip should approximately match the
    tax on RSU perquisite (typically 30-35% of perquisite value).

    Correlation logic:
    1. Find unmatched RSU tax credits from salary records
    2. Look for vest events in same month
    3. Verify tax amount is within expected range
    4. Link credit to vest event
    """

    # Expected tax rate range on RSU perquisites
    MIN_TAX_RATE = Decimal("0.25")  # 25% minimum (lower slab)
    MAX_TAX_RATE = Decimal("0.45")  # 45% maximum (highest slab + cess)

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize correlator.

        Args:
            db_connection: Database connection
        """
        self.conn = db_connection

    def correlate_all_pending(self, user_id: int) -> dict:
        """
        Correlate all pending RSU tax credits for a user.

        Args:
            user_id: User ID

        Returns:
            Dictionary with correlation statistics
        """
        stats = {
            'total': 0,
            'matched': 0,
            'unmatched': 0,
            'errors': []
        }

        # Get pending credits
        cursor = self.conn.execute(
            """SELECT rtc.id, rtc.salary_record_id, rtc.credit_amount, rtc.credit_date
            FROM rsu_tax_credits rtc
            JOIN salary_records sr ON rtc.salary_record_id = sr.id
            WHERE sr.user_id = ? AND rtc.correlation_status = ?""",
            (user_id, CorrelationStatus.PENDING.value)
        )

        pending_credits = cursor.fetchall()
        stats['total'] = len(pending_credits)

        for credit in pending_credits:
            try:
                matched = self._correlate_credit(
                    credit['id'],
                    credit['credit_amount'],
                    credit['credit_date'],
                    user_id
                )
                if matched:
                    stats['matched'] += 1
                else:
                    stats['unmatched'] += 1
            except Exception as e:
                stats['errors'].append(str(e))

        return stats

    def _correlate_credit(
        self,
        credit_id: int,
        credit_amount: Decimal,
        credit_date: date,
        user_id: int
    ) -> bool:
        """
        Try to correlate a single RSU tax credit with vest event.

        Args:
            credit_id: RSU tax credit record ID
            credit_amount: Credit amount
            credit_date: Date of credit
            user_id: User ID

        Returns:
            True if correlated successfully
        """
        credit_amount = Decimal(str(credit_amount))

        # Calculate expected perquisite range
        min_perquisite = credit_amount / self.MAX_TAX_RATE
        max_perquisite = credit_amount / self.MIN_TAX_RATE

        # Look for matching perquisites in Form 12BA
        # (Phase 2 will add RSU vest table to correlate with)
        cursor = self.conn.execute(
            """SELECT p.id, p.taxable_value, f.assessment_year
            FROM perquisites p
            JOIN form16_records f ON p.form16_id = f.id
            WHERE f.user_id = ?
                AND p.perquisite_type = 'RSU'
                AND CAST(p.taxable_value AS REAL) BETWEEN ? AND ?""",
            (user_id, float(min_perquisite), float(max_perquisite))
        )

        matching_perquisites = cursor.fetchall()

        if matching_perquisites:
            # Found potential match - update status
            self.conn.execute(
                """UPDATE rsu_tax_credits
                SET correlation_status = ?
                WHERE id = ?""",
                (CorrelationStatus.MATCHED.value, credit_id)
            )
            self.conn.commit()
            return True

        # No match found - mark as unmatched
        self.conn.execute(
            """UPDATE rsu_tax_credits
            SET correlation_status = ?
            WHERE id = ?""",
            (CorrelationStatus.UNMATCHED.value, credit_id)
        )
        self.conn.commit()
        return False

    def correlate_with_vest(
        self,
        credit_id: int,
        vest_perquisite: Decimal,
        vest_date: date
    ) -> bool:
        """
        Correlate RSU tax credit with a specific vest event.

        Args:
            credit_id: RSU tax credit record ID
            vest_perquisite: Perquisite value from vest
            vest_date: Date of vest

        Returns:
            True if correlation is valid
        """
        # Get credit details
        cursor = self.conn.execute(
            "SELECT credit_amount, credit_date FROM rsu_tax_credits WHERE id = ?",
            (credit_id,)
        )
        row = cursor.fetchone()

        if not row:
            return False

        credit_amount = Decimal(str(row['credit_amount']))
        credit_date_val = row['credit_date']

        if isinstance(credit_date_val, str):
            credit_date_val = date.fromisoformat(credit_date_val)

        # Verify tax credit is within expected range
        expected_tax_min = vest_perquisite * self.MIN_TAX_RATE
        expected_tax_max = vest_perquisite * self.MAX_TAX_RATE

        if not (expected_tax_min <= credit_amount <= expected_tax_max):
            return False

        # Verify dates are within same month (credit appears after vest)
        date_diff = (credit_date_val - vest_date).days
        if date_diff < 0 or date_diff > 45:  # Allow up to 45 days lag
            return False

        # Update correlation status
        self.conn.execute(
            """UPDATE rsu_tax_credits
            SET correlation_status = ?, vest_id = NULL
            WHERE id = ?""",
            (CorrelationStatus.MATCHED.value, credit_id)
        )
        self.conn.commit()
        return True

    def get_unmatched_credits(self, user_id: int) -> List[dict]:
        """
        Get all unmatched RSU tax credits for a user.

        Args:
            user_id: User ID

        Returns:
            List of unmatched credit details
        """
        cursor = self.conn.execute(
            """SELECT rtc.id, rtc.credit_amount, rtc.credit_date,
                      sr.pay_period, rtc.correlation_status
            FROM rsu_tax_credits rtc
            JOIN salary_records sr ON rtc.salary_record_id = sr.id
            WHERE sr.user_id = ?
                AND rtc.correlation_status IN (?, ?)
            ORDER BY rtc.credit_date""",
            (user_id, CorrelationStatus.PENDING.value, CorrelationStatus.UNMATCHED.value)
        )

        return [dict(row) for row in cursor.fetchall()]

    def get_annual_rsu_summary(self, user_id: int, financial_year: str) -> dict:
        """
        Get annual RSU tax credit summary.

        Args:
            user_id: User ID
            financial_year: FY in format '2024-25'

        Returns:
            Summary dictionary
        """
        # Parse FY dates
        start_year = int(financial_year.split('-')[0])
        fy_start = date(start_year, 4, 1)
        fy_end = date(start_year + 1, 3, 31)

        cursor = self.conn.execute(
            """SELECT
                COUNT(*) as credit_count,
                COALESCE(SUM(CAST(rtc.credit_amount AS REAL)), 0) as total_credits,
                SUM(CASE WHEN rtc.correlation_status = 'MATCHED' THEN 1 ELSE 0 END) as matched,
                SUM(CASE WHEN rtc.correlation_status = 'PENDING' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN rtc.correlation_status = 'UNMATCHED' THEN 1 ELSE 0 END) as unmatched
            FROM rsu_tax_credits rtc
            JOIN salary_records sr ON rtc.salary_record_id = sr.id
            WHERE sr.user_id = ?
                AND rtc.credit_date >= ?
                AND rtc.credit_date <= ?""",
            (user_id, fy_start.isoformat(), fy_end.isoformat())
        )

        row = cursor.fetchone()

        return {
            'financial_year': financial_year,
            'credit_count': row['credit_count'] or 0,
            'total_credits': Decimal(str(row['total_credits'] or 0)),
            'matched': row['matched'] or 0,
            'pending': row['pending'] or 0,
            'unmatched': row['unmatched'] or 0
        }
