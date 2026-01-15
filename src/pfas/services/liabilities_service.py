"""
Liabilities Service.

Provides:
1. Loan tracking (Home, Car, Personal, Education)
2. Credit card balance tracking
3. EMI and prepayment recording
4. Outstanding balance calculation
5. Loan amortization schedule
"""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional, Dict, Any
import sqlite3

from pfas.core.models import Liability, LiabilityType, LiabilityTransaction


@dataclass
class LoanSummary:
    """Summary of all loans for a user."""
    total_principal: Decimal = Decimal("0")
    total_outstanding: Decimal = Decimal("0")
    total_paid: Decimal = Decimal("0")
    monthly_emi_total: Decimal = Decimal("0")
    loan_count: int = 0
    loans: List[Liability] = field(default_factory=list)

    @property
    def payment_progress_percent(self) -> Optional[Decimal]:
        """Percentage of total principal paid."""
        if self.total_principal > 0:
            return ((self.total_paid / self.total_principal) * 100).quantize(Decimal("0.01"))
        return None


@dataclass
class AmortizationEntry:
    """Single entry in amortization schedule."""
    month: int
    emi_date: date
    opening_balance: Decimal
    emi_amount: Decimal
    principal_component: Decimal
    interest_component: Decimal
    closing_balance: Decimal


class LiabilitiesService:
    """
    Service for managing liabilities (loans and credit cards).

    Example:
        service = LiabilitiesService(conn)

        # Add a loan
        loan_id = service.add_liability(
            user_id=1,
            liability_type=LiabilityType.HOME_LOAN,
            lender_name="HDFC Bank",
            principal_amount=Decimal("5000000"),
            interest_rate=Decimal("8.5"),
            tenure_months=240,
            start_date=date(2023, 1, 1)
        )

        # Record EMI payment
        service.record_emi_payment(loan_id, date.today(), Decimal("50000"))

        # Get summary
        summary = service.get_loan_summary(user_id=1)
    """

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize with database connection.

        Args:
            db_connection: SQLite connection object
        """
        self.conn = db_connection

    def add_liability(
        self,
        user_id: int,
        liability_type: LiabilityType,
        lender_name: str,
        principal_amount: Decimal,
        interest_rate: Decimal,
        start_date: date,
        tenure_months: Optional[int] = None,
        emi_amount: Optional[Decimal] = None,
        account_number: str = ""
    ) -> int:
        """
        Add a new liability (loan/credit card).

        Args:
            user_id: User ID
            liability_type: Type of liability
            lender_name: Name of lender
            principal_amount: Original loan amount
            interest_rate: Annual interest rate (e.g., 8.5 for 8.5%)
            start_date: Loan start date
            tenure_months: Loan tenure in months
            emi_amount: Monthly EMI (calculated if not provided)
            account_number: Account/loan number

        Returns:
            ID of created liability
        """
        # Calculate EMI if not provided
        if emi_amount is None and tenure_months and interest_rate > 0:
            emi_amount = self._calculate_emi(principal_amount, interest_rate, tenure_months)

        # Calculate end date
        end_date = None
        if tenure_months:
            end_year = start_date.year + (start_date.month + tenure_months - 1) // 12
            end_month = (start_date.month + tenure_months - 1) % 12 + 1
            end_date = date(end_year, end_month, start_date.day)

        cursor = self.conn.execute("""
            INSERT INTO liabilities (
                user_id, liability_type, lender_name, account_number,
                principal_amount, outstanding_amount, interest_rate,
                emi_amount, start_date, end_date, tenure_months, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
        """, (
            user_id,
            liability_type.value,
            lender_name,
            account_number,
            float(principal_amount),
            float(principal_amount),  # Outstanding starts at principal
            float(interest_rate),
            float(emi_amount) if emi_amount else None,
            start_date.isoformat(),
            end_date.isoformat() if end_date else None,
            tenure_months,
        ))

        self.conn.commit()
        return cursor.lastrowid

    def record_emi_payment(
        self,
        liability_id: int,
        payment_date: date,
        amount: Decimal,
        user_id: int,
        reference_number: str = ""
    ) -> int:
        """
        Record an EMI payment.

        Args:
            liability_id: Liability ID
            payment_date: Payment date
            amount: Payment amount
            user_id: User ID
            reference_number: Transaction reference

        Returns:
            ID of created transaction
        """
        # Get liability details for interest calculation
        liability = self.get_liability(liability_id)
        if not liability:
            raise ValueError(f"Liability {liability_id} not found")

        # Calculate interest and principal components
        monthly_rate = liability.interest_rate / Decimal("12") / Decimal("100")
        interest_component = (liability.outstanding_amount * monthly_rate).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        principal_component = amount - interest_component
        new_outstanding = liability.outstanding_amount - principal_component

        cursor = self.conn.execute("""
            INSERT INTO liability_transactions (
                liability_id, transaction_date, transaction_type, amount,
                principal_component, interest_component, outstanding_after,
                reference_number, user_id
            ) VALUES (?, ?, 'EMI', ?, ?, ?, ?, ?, ?)
        """, (
            liability_id,
            payment_date.isoformat(),
            float(amount),
            float(principal_component),
            float(interest_component),
            float(new_outstanding),
            reference_number,
            user_id,
        ))

        # Update outstanding in liability table
        self.conn.execute("""
            UPDATE liabilities SET outstanding_amount = ? WHERE id = ?
        """, (float(new_outstanding), liability_id))

        self.conn.commit()
        return cursor.lastrowid

    def record_prepayment(
        self,
        liability_id: int,
        payment_date: date,
        amount: Decimal,
        user_id: int,
        reference_number: str = ""
    ) -> int:
        """
        Record a loan prepayment.

        Args:
            liability_id: Liability ID
            payment_date: Payment date
            amount: Prepayment amount
            user_id: User ID
            reference_number: Transaction reference

        Returns:
            ID of created transaction
        """
        liability = self.get_liability(liability_id)
        if not liability:
            raise ValueError(f"Liability {liability_id} not found")

        new_outstanding = liability.outstanding_amount - amount

        cursor = self.conn.execute("""
            INSERT INTO liability_transactions (
                liability_id, transaction_date, transaction_type, amount,
                principal_component, interest_component, outstanding_after,
                reference_number, user_id
            ) VALUES (?, ?, 'PREPAYMENT', ?, ?, 0, ?, ?, ?)
        """, (
            liability_id,
            payment_date.isoformat(),
            float(amount),
            float(amount),  # All prepayment goes to principal
            float(new_outstanding),
            reference_number,
            user_id,
        ))

        # Update outstanding
        self.conn.execute("""
            UPDATE liabilities SET outstanding_amount = ? WHERE id = ?
        """, (float(new_outstanding), liability_id))

        # Mark as inactive if fully paid
        if new_outstanding <= 0:
            self.conn.execute("""
                UPDATE liabilities SET is_active = FALSE WHERE id = ?
            """, (liability_id,))

        self.conn.commit()
        return cursor.lastrowid

    def record_disbursement(
        self,
        liability_id: int,
        disbursement_date: date,
        amount: Decimal,
        user_id: int,
        reference_number: str = ""
    ) -> int:
        """
        Record a loan disbursement (for top-up or tranche disbursement).

        Args:
            liability_id: Liability ID
            disbursement_date: Disbursement date
            amount: Disbursement amount
            user_id: User ID
            reference_number: Transaction reference

        Returns:
            ID of created transaction
        """
        liability = self.get_liability(liability_id)
        if not liability:
            raise ValueError(f"Liability {liability_id} not found")

        new_outstanding = liability.outstanding_amount + amount

        cursor = self.conn.execute("""
            INSERT INTO liability_transactions (
                liability_id, transaction_date, transaction_type, amount,
                principal_component, interest_component, outstanding_after,
                reference_number, user_id
            ) VALUES (?, ?, 'DISBURSEMENT', ?, ?, 0, ?, ?, ?)
        """, (
            liability_id,
            disbursement_date.isoformat(),
            float(amount),
            float(amount),
            float(new_outstanding),
            reference_number,
            user_id,
        ))

        # Update outstanding and principal
        self.conn.execute("""
            UPDATE liabilities
            SET outstanding_amount = ?, principal_amount = principal_amount + ?
            WHERE id = ?
        """, (float(new_outstanding), float(amount), liability_id))

        self.conn.commit()
        return cursor.lastrowid

    def get_liability(self, liability_id: int) -> Optional[Liability]:
        """
        Get a single liability by ID.

        Args:
            liability_id: Liability ID

        Returns:
            Liability object or None
        """
        cursor = self.conn.execute("""
            SELECT id, liability_type, lender_name, account_number,
                   principal_amount, outstanding_amount, interest_rate,
                   emi_amount, start_date, end_date, tenure_months, is_active
            FROM liabilities WHERE id = ?
        """, (liability_id,))

        row = cursor.fetchone()
        if not row:
            return None

        return Liability(
            id=row[0],
            liability_type=LiabilityType(row[1]),
            lender_name=row[2],
            account_number=row[3] or "",
            principal_amount=Decimal(str(row[4])),
            outstanding_amount=Decimal(str(row[5])),
            interest_rate=Decimal(str(row[6])),
            emi_amount=Decimal(str(row[7])) if row[7] else None,
            start_date=date.fromisoformat(row[8]) if row[8] else None,
            end_date=date.fromisoformat(row[9]) if row[9] else None,
            tenure_months=row[10],
            is_active=bool(row[11]),
        )

    def get_user_liabilities(
        self,
        user_id: int,
        include_inactive: bool = False
    ) -> List[Liability]:
        """
        Get all liabilities for a user.

        Args:
            user_id: User ID
            include_inactive: Include closed loans

        Returns:
            List of Liability objects
        """
        query = """
            SELECT id, liability_type, lender_name, account_number,
                   principal_amount, outstanding_amount, interest_rate,
                   emi_amount, start_date, end_date, tenure_months, is_active
            FROM liabilities WHERE user_id = ?
        """
        if not include_inactive:
            query += " AND is_active = TRUE"
        query += " ORDER BY start_date DESC"

        cursor = self.conn.execute(query, (user_id,))

        liabilities = []
        for row in cursor.fetchall():
            liabilities.append(Liability(
                id=row[0],
                liability_type=LiabilityType(row[1]),
                lender_name=row[2],
                account_number=row[3] or "",
                principal_amount=Decimal(str(row[4])),
                outstanding_amount=Decimal(str(row[5])),
                interest_rate=Decimal(str(row[6])),
                emi_amount=Decimal(str(row[7])) if row[7] else None,
                start_date=date.fromisoformat(row[8]) if row[8] else None,
                end_date=date.fromisoformat(row[9]) if row[9] else None,
                tenure_months=row[10],
                is_active=bool(row[11]),
            ))

        return liabilities

    def get_loan_summary(self, user_id: int) -> LoanSummary:
        """
        Get summary of all loans for a user.

        Args:
            user_id: User ID

        Returns:
            LoanSummary object
        """
        summary = LoanSummary()
        liabilities = self.get_user_liabilities(user_id, include_inactive=False)

        for liability in liabilities:
            summary.loans.append(liability)
            summary.total_principal += liability.principal_amount
            summary.total_outstanding += liability.outstanding_amount
            if liability.emi_amount:
                summary.monthly_emi_total += liability.emi_amount
            summary.loan_count += 1

        summary.total_paid = summary.total_principal - summary.total_outstanding

        return summary

    def get_transaction_history(
        self,
        liability_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[LiabilityTransaction]:
        """
        Get transaction history for a liability.

        Args:
            liability_id: Liability ID
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            List of LiabilityTransaction objects
        """
        query = """
            SELECT liability_id, transaction_date, transaction_type, amount,
                   principal_component, interest_component, outstanding_after,
                   reference_number
            FROM liability_transactions
            WHERE liability_id = ?
        """
        params = [liability_id]

        if start_date:
            query += " AND transaction_date >= ?"
            params.append(start_date.isoformat())
        if end_date:
            query += " AND transaction_date <= ?"
            params.append(end_date.isoformat())

        query += " ORDER BY transaction_date, id"

        cursor = self.conn.execute(query, params)

        transactions = []
        for row in cursor.fetchall():
            transactions.append(LiabilityTransaction(
                liability_id=row[0],
                transaction_date=date.fromisoformat(row[1]) if isinstance(row[1], str) else row[1],
                transaction_type=row[2],
                amount=Decimal(str(row[3])),
                principal_component=Decimal(str(row[4])) if row[4] else None,
                interest_component=Decimal(str(row[5])) if row[5] else None,
                outstanding_after=Decimal(str(row[6])) if row[6] else None,
                reference_number=row[7] or "",
            ))

        return transactions

    def generate_amortization_schedule(
        self,
        liability_id: int
    ) -> List[AmortizationEntry]:
        """
        Generate amortization schedule for a loan.

        Args:
            liability_id: Liability ID

        Returns:
            List of AmortizationEntry objects
        """
        liability = self.get_liability(liability_id)
        if not liability or not liability.emi_amount or not liability.tenure_months:
            return []

        schedule = []
        balance = liability.principal_amount
        monthly_rate = liability.interest_rate / Decimal("12") / Decimal("100")
        emi = liability.emi_amount
        current_date = liability.start_date

        for month in range(1, liability.tenure_months + 1):
            interest = (balance * monthly_rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            principal = emi - interest
            closing_balance = balance - principal

            # Move to next month
            if current_date.month == 12:
                emi_date = date(current_date.year + 1, 1, current_date.day)
            else:
                try:
                    emi_date = date(current_date.year, current_date.month + 1, current_date.day)
                except ValueError:
                    # Handle months with fewer days
                    emi_date = date(current_date.year, current_date.month + 1, 28)

            schedule.append(AmortizationEntry(
                month=month,
                emi_date=emi_date,
                opening_balance=balance,
                emi_amount=emi,
                principal_component=principal,
                interest_component=interest,
                closing_balance=max(Decimal("0"), closing_balance),
            ))

            balance = closing_balance
            current_date = emi_date

            if balance <= 0:
                break

        return schedule

    def _calculate_emi(
        self,
        principal: Decimal,
        annual_rate: Decimal,
        tenure_months: int
    ) -> Decimal:
        """
        Calculate EMI using standard formula.

        EMI = P × r × (1+r)^n / ((1+r)^n - 1)

        Args:
            principal: Loan principal
            annual_rate: Annual interest rate (e.g., 8.5 for 8.5%)
            tenure_months: Tenure in months

        Returns:
            EMI amount
        """
        if annual_rate == 0:
            return (principal / Decimal(str(tenure_months))).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )

        r = float(annual_rate) / 12 / 100
        n = tenure_months
        p = float(principal)

        emi = p * r * ((1 + r) ** n) / (((1 + r) ** n) - 1)

        return Decimal(str(emi)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def close_liability(self, liability_id: int) -> bool:
        """
        Mark a liability as closed/inactive.

        Args:
            liability_id: Liability ID

        Returns:
            True if successful
        """
        self.conn.execute("""
            UPDATE liabilities SET is_active = FALSE WHERE id = ?
        """, (liability_id,))
        self.conn.commit()
        return True
