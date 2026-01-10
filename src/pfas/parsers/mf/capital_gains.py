"""Mutual Fund Capital Gains Calculation Engine."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import List, Tuple
import sqlite3

from .models import MFTransaction, AssetClass, TransactionType


@dataclass
class CapitalGainsSummary:
    """
    Summary of capital gains for a financial year and asset class.

    Attributes:
        financial_year: FY in format '2024-25'
        asset_class: EQUITY or DEBT
        stcg_amount: Gross short-term capital gains
        ltcg_amount: Gross long-term capital gains
        ltcg_exemption: LTCG exemption (₹1.25L for equity)
        taxable_stcg: Taxable STCG amount
        taxable_ltcg: Taxable LTCG amount (after exemption)
        stcg_tax_rate: Tax rate for STCG (%)
        ltcg_tax_rate: Tax rate for LTCG (%)
    """
    financial_year: str
    asset_class: AssetClass

    # Gross amounts
    stcg_amount: Decimal = Decimal("0")
    ltcg_amount: Decimal = Decimal("0")

    # Exemptions (₹1.25L for equity LTCG from FY 2024-25)
    ltcg_exemption: Decimal = Decimal("0")

    # Taxable amounts
    taxable_stcg: Decimal = Decimal("0")
    taxable_ltcg: Decimal = Decimal("0")

    # Tax rates
    stcg_tax_rate: Decimal = Decimal("0")
    ltcg_tax_rate: Decimal = Decimal("0")


class CapitalGainsCalculator:
    """
    Calculate capital gains for mutual fund transactions.

    Tax rates as per Budget 2024:
    - Equity STCG: 20%
    - Equity LTCG: 12.5% (₹1.25 lakh exemption)
    - Debt STCG/LTCG: Taxed at slab rate (no special rate)

    Grandfathering: For pre-31-Jan-2018 purchases, use higher of:
    - Actual cost of acquisition
    - Fair Market Value on 31-Jan-2018
    """

    # Tax rates (Budget 2024)
    EQUITY_STCG_RATE = Decimal("20")  # 20% for equity STCG
    EQUITY_LTCG_RATE = Decimal("12.5")  # 12.5% for equity LTCG
    EQUITY_LTCG_EXEMPTION = Decimal("125000")  # ₹1.25 lakh exemption

    # Debt funds - taxed at slab rate (no special rate)
    DEBT_STCG_RATE = Decimal("0")  # Slab rate
    DEBT_LTCG_RATE = Decimal("0")  # Slab rate (no indexation from April 2023)

    GRANDFATHERING_DATE = date(2018, 1, 31)

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize capital gains calculator.

        Args:
            db_connection: Database connection
        """
        self.conn = db_connection

    def calculate_for_transaction(self, txn: MFTransaction) -> Tuple[Decimal, Decimal]:
        """
        Calculate capital gain for a single redemption transaction.

        Args:
            txn: MF transaction (must be REDEMPTION type)

        Returns:
            Tuple of (short_term_gain, long_term_gain)

        Examples:
            >>> calc = CapitalGainsCalculator(conn)
            >>> txn = MFTransaction(...)  # Redemption transaction
            >>> stcg, ltcg = calc.calculate_for_transaction(txn)
        """
        if txn.transaction_type != TransactionType.REDEMPTION:
            return Decimal("0"), Decimal("0")

        # Get cost of acquisition (with grandfathering if applicable)
        cost_of_acquisition = self._get_cost_of_acquisition(txn)

        # Sale value
        sale_value = txn.amount

        # Capital gain = Sale value - Cost - STT
        gain = sale_value - cost_of_acquisition - txn.stt

        if txn.is_long_term:
            return Decimal("0"), gain
        else:
            return gain, Decimal("0")

    def _get_cost_of_acquisition(self, txn: MFTransaction) -> Decimal:
        """
        Get cost of acquisition, applying grandfathering if eligible.

        Grandfathering rules (for pre-31-Jan-2018 purchases):
        - Cost = Higher of (actual cost, FMV on 31-Jan-2018)
        - But FMV is capped at sale price (to avoid artificial loss)

        Args:
            txn: MF transaction

        Returns:
            Cost of acquisition

        Examples:
            >>> # Case 1: Post-grandfathering purchase
            >>> cost = calc._get_cost_of_acquisition(txn)  # Uses actual cost

            >>> # Case 2: Pre-grandfathering, FMV > cost
            >>> cost = calc._get_cost_of_acquisition(txn)  # Uses FMV
        """
        # Actual cost of purchase
        if txn.purchase_nav and txn.purchase_units:
            actual_cost = txn.purchase_nav * txn.purchase_units
        elif txn.purchase_amount:
            actual_cost = txn.purchase_amount
        else:
            actual_cost = Decimal("0")

        # Check if grandfathering applies
        if txn.purchase_date and txn.purchase_date <= self.GRANDFATHERING_DATE:
            if txn.grandfathered_value and txn.grandfathered_value > 0:
                # FMV on 31-Jan-2018
                fmv = txn.grandfathered_value

                # Cap FMV at sale price (to prevent artificial loss)
                sale_price = txn.amount
                fmv_capped = min(fmv, sale_price)

                # Use higher of actual cost or capped FMV
                return max(actual_cost, fmv_capped)

        return actual_cost

    def calculate_summary(self, user_id: int, fy: str) -> List[CapitalGainsSummary]:
        """
        Calculate capital gains summary for a financial year.

        Args:
            user_id: User ID
            fy: Financial year (e.g., '2024-25')

        Returns:
            List of CapitalGainsSummary for EQUITY and DEBT

        Examples:
            >>> summaries = calc.calculate_summary(user_id=1, fy='2024-25')
            >>> for summary in summaries:
            ...     print(f"{summary.asset_class}: STCG={summary.stcg_amount}, LTCG={summary.ltcg_amount}")
        """
        summaries = []

        for asset_class in [AssetClass.EQUITY, AssetClass.DEBT]:
            summary = self._calculate_for_asset_class(user_id, fy, asset_class)
            summaries.append(summary)

        return summaries

    def _calculate_for_asset_class(
        self, user_id: int, fy: str, asset_class: AssetClass
    ) -> CapitalGainsSummary:
        """
        Calculate CG summary for specific asset class.

        Args:
            user_id: User ID
            fy: Financial year (e.g., '2024-25')
            asset_class: EQUITY or DEBT

        Returns:
            CapitalGainsSummary for the asset class
        """
        # Parse FY to get date range
        start_year = int(fy.split('-')[0])
        fy_start = date(start_year, 4, 1)
        fy_end = date(start_year + 1, 3, 31)

        # Query redemption transactions for this FY and asset class
        cursor = self.conn.execute(
            """
            SELECT mt.*, ms.asset_class
            FROM mf_transactions mt
            JOIN mf_folios mf ON mt.folio_id = mf.id
            JOIN mf_schemes ms ON mf.scheme_id = ms.id
            WHERE mt.user_id = ?
                AND mt.transaction_type = 'REDEMPTION'
                AND mt.date >= ?
                AND mt.date <= ?
                AND ms.asset_class = ?
            ORDER BY mt.date
            """,
            (user_id, fy_start.isoformat(), fy_end.isoformat(), asset_class.value)
        )

        stcg_total = Decimal("0")
        ltcg_total = Decimal("0")

        for row in cursor.fetchall():
            stcg = Decimal(str(row['short_term_gain'] or 0))
            ltcg = Decimal(str(row['long_term_gain'] or 0))

            stcg_total += stcg
            ltcg_total += ltcg

        # Calculate exemption for equity LTCG
        ltcg_exemption = Decimal("0")
        if asset_class == AssetClass.EQUITY:
            ltcg_exemption = min(ltcg_total, self.EQUITY_LTCG_EXEMPTION)

        taxable_ltcg = max(Decimal("0"), ltcg_total - ltcg_exemption)

        # Set tax rates
        if asset_class == AssetClass.EQUITY:
            stcg_rate = self.EQUITY_STCG_RATE
            ltcg_rate = self.EQUITY_LTCG_RATE
        else:
            stcg_rate = self.DEBT_STCG_RATE  # Slab rate
            ltcg_rate = self.DEBT_LTCG_RATE  # Slab rate

        return CapitalGainsSummary(
            financial_year=fy,
            asset_class=asset_class,
            stcg_amount=stcg_total,
            ltcg_amount=ltcg_total,
            ltcg_exemption=ltcg_exemption,
            taxable_stcg=stcg_total,  # STCG fully taxable
            taxable_ltcg=taxable_ltcg,
            stcg_tax_rate=stcg_rate,
            ltcg_tax_rate=ltcg_rate
        )

    def save_summary_to_db(self, summary: CapitalGainsSummary, user_id: int) -> None:
        """
        Save capital gains summary to database.

        Args:
            summary: CapitalGainsSummary to save
            user_id: User ID
        """
        self.conn.execute(
            """
            INSERT OR REPLACE INTO mf_capital_gains
            (user_id, financial_year, asset_class, stcg_amount, ltcg_amount,
             ltcg_exemption, taxable_stcg, taxable_ltcg, stcg_tax_rate, ltcg_tax_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                summary.financial_year,
                summary.asset_class.value,
                str(summary.stcg_amount),
                str(summary.ltcg_amount),
                str(summary.ltcg_exemption),
                str(summary.taxable_stcg),
                str(summary.taxable_ltcg),
                str(summary.stcg_tax_rate),
                str(summary.ltcg_tax_rate)
            )
        )
        self.conn.commit()
