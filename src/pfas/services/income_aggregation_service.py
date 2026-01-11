"""Income Aggregation Service - Aggregates income from parsed database records.

Eliminates need to re-parse statement files.
All income data is fetched from pre-parsed database tables.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


@dataclass
class IncomeRecord:
    """Aggregated income record from database."""
    income_type: str
    sub_classification: str
    income_sub_grouping: str
    gross_amount: Decimal
    deductions: Decimal
    taxable_amount: Decimal
    tds_deducted: Decimal
    applicable_tax_rate_type: str
    source_table: str


class IncomeAggregationService:
    """
    Service to aggregate income from parsed database records.
    Eliminates need to re-parse statement files.
    """

    def __init__(self, db_connection):
        """
        Initialize with database connection.

        Args:
            db_connection: SQLite connection object
        """
        self.conn = db_connection

    def get_user_income_summary(
        self,
        user_id: int,
        financial_year: str
    ) -> list[IncomeRecord]:
        """
        Fetch all income records for a user from the database.
        Uses pre-aggregated data - no file parsing needed.

        Args:
            user_id: User ID
            financial_year: e.g., '2024-25'

        Returns:
            List of IncomeRecord
        """
        # First try pre-computed summary table
        records = self._get_from_summary_table(user_id, financial_year)

        if not records:
            # Fall back to real-time aggregation from source tables
            records = self._aggregate_from_source_tables(user_id, financial_year)

        return records

    def _get_from_summary_table(
        self,
        user_id: int,
        financial_year: str
    ) -> list[IncomeRecord]:
        """Fetch from pre-computed summary table."""
        cursor = self.conn.execute("""
            SELECT income_type, sub_classification, income_sub_grouping,
                   gross_amount, deductions, taxable_amount, tds_deducted,
                   applicable_tax_rate_type, source_table
            FROM user_income_summary
            WHERE user_id = ? AND financial_year = ?
            ORDER BY income_type, sub_classification
        """, (user_id, financial_year))

        return [self._row_to_record(row) for row in cursor.fetchall()]

    def _aggregate_from_source_tables(
        self,
        user_id: int,
        financial_year: str
    ) -> list[IncomeRecord]:
        """Real-time aggregation from source database tables."""
        records = []

        # Aggregate salary income
        records.extend(self._aggregate_salary(user_id, financial_year))

        # Aggregate MF capital gains
        records.extend(self._aggregate_mf_capital_gains(user_id, financial_year))

        # Aggregate stock capital gains
        records.extend(self._aggregate_stock_capital_gains(user_id, financial_year))

        # Aggregate stock dividends
        records.extend(self._aggregate_stock_dividends(user_id, financial_year))

        # Aggregate foreign income (RSU sales, foreign dividends)
        records.extend(self._aggregate_foreign_income(user_id, financial_year))

        # Aggregate bank interest
        records.extend(self._aggregate_bank_interest(user_id, financial_year))

        return records

    def _aggregate_salary(
        self,
        user_id: int,
        financial_year: str
    ) -> list[IncomeRecord]:
        """Aggregate salary from salary_records and form16_records."""
        records = []

        # Try Form16 first (more accurate annual summary)
        ay = f"20{financial_year.split('-')[1]}-{int(financial_year.split('-')[1]) + 1}"
        cursor = self.conn.execute("""
            SELECT gross_salary, standard_deduction, professional_tax, total_tds
            FROM form16_records
            WHERE user_id = ? AND assessment_year = ?
        """, (user_id, ay))

        row = cursor.fetchone()
        if row and row[0]:
            gross = Decimal(str(row[0] or 0))
            std_ded = Decimal(str(row[1] or 0))
            prof_tax = Decimal(str(row[2] or 0))
            tds = Decimal(str(row[3] or 0))

            records.append(IncomeRecord(
                income_type='SALARY',
                sub_classification='N/A',
                income_sub_grouping='Employer Salary (Form 16)',
                gross_amount=gross,
                deductions=std_ded + prof_tax,
                taxable_amount=gross - std_ded - prof_tax,
                tds_deducted=tds,
                applicable_tax_rate_type='SLAB',
                source_table='form16_records'
            ))
            return records

        # Fall back to monthly salary records
        fy_start, fy_end = self._get_fy_dates(financial_year)
        cursor = self.conn.execute("""
            SELECT SUM(gross_salary), SUM(professional_tax), SUM(income_tax_deducted)
            FROM salary_records
            WHERE user_id = ? AND pay_date BETWEEN ? AND ?
        """, (user_id, fy_start, fy_end))

        row = cursor.fetchone()
        if row and row[0]:
            gross = Decimal(str(row[0] or 0))
            prof_tax = Decimal(str(row[1] or 0))
            tds = Decimal(str(row[2] or 0))

            records.append(IncomeRecord(
                income_type='SALARY',
                sub_classification='N/A',
                income_sub_grouping='Employer Salary (Payslips)',
                gross_amount=gross,
                deductions=prof_tax,
                taxable_amount=gross - prof_tax,
                tds_deducted=tds,
                applicable_tax_rate_type='SLAB',
                source_table='salary_records'
            ))

        return records

    def _aggregate_mf_capital_gains(
        self,
        user_id: int,
        financial_year: str
    ) -> list[IncomeRecord]:
        """Aggregate mutual fund capital gains."""
        records = []

        cursor = self.conn.execute("""
            SELECT asset_class, stcg_amount, ltcg_amount, ltcg_exemption
            FROM mf_capital_gains
            WHERE user_id = ? AND financial_year = ?
        """, (user_id, financial_year))

        for row in cursor.fetchall():
            asset_class = row[0]
            stcg = Decimal(str(row[1] or 0))
            ltcg = Decimal(str(row[2] or 0))
            exemption = Decimal(str(row[3] or 0))

            if asset_class == 'EQUITY':
                rate_type_stcg = 'FLAT_15'
                rate_type_ltcg = 'FLAT_10'
                grouping = 'Equity Mutual Funds'
            else:
                rate_type_stcg = 'SLAB'
                rate_type_ltcg = 'SLAB'
                grouping = f'{asset_class} Mutual Funds'

            if stcg != 0:
                records.append(IncomeRecord(
                    income_type='CAPITAL_GAINS',
                    sub_classification='STCG',
                    income_sub_grouping=grouping,
                    gross_amount=stcg,
                    deductions=Decimal('0'),
                    taxable_amount=stcg,
                    tds_deducted=Decimal('0'),
                    applicable_tax_rate_type=rate_type_stcg,
                    source_table='mf_capital_gains'
                ))

            if ltcg != 0:
                records.append(IncomeRecord(
                    income_type='CAPITAL_GAINS',
                    sub_classification='LTCG',
                    income_sub_grouping=grouping,
                    gross_amount=ltcg,
                    deductions=exemption,
                    taxable_amount=max(Decimal('0'), ltcg - exemption),
                    tds_deducted=Decimal('0'),
                    applicable_tax_rate_type=rate_type_ltcg,
                    source_table='mf_capital_gains'
                ))

        return records

    def _aggregate_stock_capital_gains(
        self,
        user_id: int,
        financial_year: str
    ) -> list[IncomeRecord]:
        """Aggregate stock capital gains."""
        records = []

        cursor = self.conn.execute("""
            SELECT trade_category, stcg_amount, ltcg_amount, ltcg_exemption, speculative_income
            FROM stock_capital_gains
            WHERE user_id = ? AND financial_year = ?
        """, (user_id, financial_year))

        for row in cursor.fetchall():
            category = row[0]
            stcg = Decimal(str(row[1] or 0))
            ltcg = Decimal(str(row[2] or 0))
            exemption = Decimal(str(row[3] or 0))
            speculative = Decimal(str(row[4] or 0))

            if category == 'DELIVERY':
                if stcg != 0:
                    records.append(IncomeRecord(
                        income_type='CAPITAL_GAINS',
                        sub_classification='STCG',
                        income_sub_grouping='Indian Listed Equity',
                        gross_amount=stcg,
                        deductions=Decimal('0'),
                        taxable_amount=stcg,
                        tds_deducted=Decimal('0'),
                        applicable_tax_rate_type='FLAT_15',
                        source_table='stock_capital_gains'
                    ))

                if ltcg != 0:
                    records.append(IncomeRecord(
                        income_type='CAPITAL_GAINS',
                        sub_classification='LTCG',
                        income_sub_grouping='Indian Listed Equity',
                        gross_amount=ltcg,
                        deductions=exemption,
                        taxable_amount=max(Decimal('0'), ltcg - exemption),
                        tds_deducted=Decimal('0'),
                        applicable_tax_rate_type='FLAT_10',
                        source_table='stock_capital_gains'
                    ))

            elif category == 'INTRADAY':
                if speculative != 0:
                    records.append(IncomeRecord(
                        income_type='CAPITAL_GAINS',
                        sub_classification='SPECULATIVE',
                        income_sub_grouping='Intraday Trading',
                        gross_amount=speculative,
                        deductions=Decimal('0'),
                        taxable_amount=speculative,
                        tds_deducted=Decimal('0'),
                        applicable_tax_rate_type='SLAB',
                        source_table='stock_capital_gains'
                    ))

            elif category == 'FNO':
                total_fno = stcg + ltcg
                if total_fno != 0:
                    records.append(IncomeRecord(
                        income_type='BUSINESS',
                        sub_classification='F&O',
                        income_sub_grouping='Futures & Options',
                        gross_amount=total_fno,
                        deductions=Decimal('0'),
                        taxable_amount=total_fno,
                        tds_deducted=Decimal('0'),
                        applicable_tax_rate_type='SLAB',
                        source_table='stock_capital_gains'
                    ))

        return records

    def _aggregate_stock_dividends(
        self,
        user_id: int,
        financial_year: str
    ) -> list[IncomeRecord]:
        """Aggregate stock dividends."""
        records = []

        cursor = self.conn.execute("""
            SELECT total_dividend, total_tds
            FROM stock_dividend_summary
            WHERE user_id = ? AND financial_year = ?
        """, (user_id, financial_year))

        row = cursor.fetchone()
        if row and row[0]:
            dividend = Decimal(str(row[0] or 0))
            tds = Decimal(str(row[1] or 0))

            if dividend != 0:
                records.append(IncomeRecord(
                    income_type='OTHER_SOURCES',
                    sub_classification='DIVIDENDS',
                    income_sub_grouping='Indian Equity Dividends',
                    gross_amount=dividend,
                    deductions=Decimal('0'),
                    taxable_amount=dividend,
                    tds_deducted=tds,
                    applicable_tax_rate_type='SLAB',
                    source_table='stock_dividend_summary'
                ))

        return records

    def _aggregate_foreign_income(
        self,
        user_id: int,
        financial_year: str
    ) -> list[IncomeRecord]:
        """Aggregate foreign income (RSU sales, foreign dividends)."""
        records = []
        fy_start, fy_end = self._get_fy_dates(financial_year)

        # RSU Sales
        cursor = self.conn.execute("""
            SELECT is_ltcg, SUM(gain_inr)
            FROM rsu_sales
            WHERE user_id = ? AND sale_date BETWEEN ? AND ?
            GROUP BY is_ltcg
        """, (user_id, fy_start, fy_end))

        for row in cursor.fetchall():
            is_ltcg = row[0]
            gain = Decimal(str(row[1] or 0))

            if gain != 0:
                records.append(IncomeRecord(
                    income_type='CAPITAL_GAINS',
                    sub_classification='LTCG' if is_ltcg else 'STCG',
                    income_sub_grouping='USA Stocks (RSU)',
                    gross_amount=gain,
                    deductions=Decimal('0'),
                    taxable_amount=gain,
                    tds_deducted=Decimal('0'),
                    applicable_tax_rate_type='SLAB',  # Foreign CG at slab
                    source_table='rsu_sales'
                ))

        # ESPP Sales
        cursor = self.conn.execute("""
            SELECT is_ltcg, SUM(gain_inr)
            FROM espp_sales
            WHERE user_id = ? AND sale_date BETWEEN ? AND ?
            GROUP BY is_ltcg
        """, (user_id, fy_start, fy_end))

        for row in cursor.fetchall():
            is_ltcg = row[0]
            gain = Decimal(str(row[1] or 0))

            if gain != 0:
                records.append(IncomeRecord(
                    income_type='CAPITAL_GAINS',
                    sub_classification='LTCG' if is_ltcg else 'STCG',
                    income_sub_grouping='USA Stocks (ESPP)',
                    gross_amount=gain,
                    deductions=Decimal('0'),
                    taxable_amount=gain,
                    tds_deducted=Decimal('0'),
                    applicable_tax_rate_type='SLAB',
                    source_table='espp_sales'
                ))

        # Foreign Dividends
        cursor = self.conn.execute("""
            SELECT SUM(gross_dividend_inr), SUM(withholding_tax_inr)
            FROM foreign_dividends
            WHERE user_id = ? AND dividend_date BETWEEN ? AND ?
        """, (user_id, fy_start, fy_end))

        row = cursor.fetchone()
        if row and row[0]:
            dividend = Decimal(str(row[0] or 0))
            withholding = Decimal(str(row[1] or 0))

            if dividend != 0:
                records.append(IncomeRecord(
                    income_type='OTHER_SOURCES',
                    sub_classification='DIVIDENDS',
                    income_sub_grouping='Foreign Dividends (USA)',
                    gross_amount=dividend,
                    deductions=Decimal('0'),
                    taxable_amount=dividend,
                    tds_deducted=withholding,  # FTC eligible
                    applicable_tax_rate_type='SLAB',
                    source_table='foreign_dividends'
                ))

        return records

    def _aggregate_bank_interest(
        self,
        user_id: int,
        financial_year: str
    ) -> list[IncomeRecord]:
        """Aggregate bank interest."""
        records = []

        cursor = self.conn.execute("""
            SELECT SUM(total_interest), SUM(tds_deducted), SUM(section_80tta_eligible)
            FROM bank_interest_summary
            WHERE user_id = ? AND financial_year = ?
        """, (user_id, financial_year))

        row = cursor.fetchone()
        if row and row[0]:
            interest = Decimal(str(row[0] or 0))
            tds = Decimal(str(row[1] or 0))
            tta_eligible = Decimal(str(row[2] or 0))

            # 80TTA deduction max 10000
            deduction = min(tta_eligible, Decimal('10000'))

            if interest != 0:
                records.append(IncomeRecord(
                    income_type='OTHER_SOURCES',
                    sub_classification='INTEREST',
                    income_sub_grouping='Savings Bank Interest',
                    gross_amount=interest,
                    deductions=deduction,
                    taxable_amount=interest - deduction,
                    tds_deducted=tds,
                    applicable_tax_rate_type='SLAB',
                    source_table='bank_interest_summary'
                ))

        return records

    def refresh_summary(self, user_id: int, financial_year: str) -> int:
        """
        Refresh the user_income_summary table from source tables.
        Called after new statements are parsed.

        Returns:
            Number of records updated
        """
        # Delete existing summary records
        self.conn.execute("""
            DELETE FROM user_income_summary
            WHERE user_id = ? AND financial_year = ?
        """, (user_id, financial_year))

        # Re-aggregate from source tables
        records = self._aggregate_from_source_tables(user_id, financial_year)

        # Insert new records
        for r in records:
            self.conn.execute("""
                INSERT INTO user_income_summary (
                    user_id, financial_year, income_type, sub_classification,
                    income_sub_grouping, gross_amount, deductions, taxable_amount,
                    tds_deducted, applicable_tax_rate_type, source_table, last_synced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """, (
                user_id, financial_year, r.income_type, r.sub_classification,
                r.income_sub_grouping, float(r.gross_amount), float(r.deductions),
                float(r.taxable_amount), float(r.tds_deducted),
                r.applicable_tax_rate_type, r.source_table
            ))

        self.conn.commit()
        return len(records)

    def _row_to_record(self, row) -> IncomeRecord:
        """Convert database row to IncomeRecord."""
        return IncomeRecord(
            income_type=row[0],
            sub_classification=row[1] or 'N/A',
            income_sub_grouping=row[2] or 'N/A',
            gross_amount=Decimal(str(row[3] or 0)),
            deductions=Decimal(str(row[4] or 0)),
            taxable_amount=Decimal(str(row[5] or 0)),
            tds_deducted=Decimal(str(row[6] or 0)),
            applicable_tax_rate_type=row[7] or 'SLAB',
            source_table=row[8]
        )

    def _get_fy_dates(self, financial_year: str) -> tuple[str, str]:
        """Get start and end dates for a financial year."""
        start_year = int(financial_year.split('-')[0])
        return (f"{start_year}-04-01", f"{start_year + 1}-03-31")
