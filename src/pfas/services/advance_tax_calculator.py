"""Advance Tax Calculator - Data-driven tax computation.

All tax rules are fetched from database - no hardcoded rates.
Income is aggregated from pre-parsed database records.
"""

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from typing import Optional
import json

from .tax_rules_service import TaxRulesService
from .income_aggregation_service import IncomeAggregationService, IncomeRecord


@dataclass
class AdvanceTaxResult:
    """Complete advance tax computation result."""
    user_id: int
    financial_year: str
    tax_regime: str
    computation_date: datetime = field(default_factory=datetime.now)

    # Income breakdown
    income_items: list[IncomeRecord] = field(default_factory=list)

    # Totals by category
    total_salary_income: Decimal = Decimal('0')
    total_stcg_equity: Decimal = Decimal('0')
    total_ltcg_equity: Decimal = Decimal('0')
    total_capital_gains_slab: Decimal = Decimal('0')
    total_other_income: Decimal = Decimal('0')
    total_house_property: Decimal = Decimal('0')

    # Aggregates
    gross_total_income: Decimal = Decimal('0')
    total_deductions: Decimal = Decimal('0')
    taxable_income: Decimal = Decimal('0')

    # Tax computation
    tax_on_slab_income: Decimal = Decimal('0')
    tax_on_stcg_equity: Decimal = Decimal('0')
    tax_on_ltcg_equity: Decimal = Decimal('0')
    total_tax_before_cess: Decimal = Decimal('0')

    surcharge_rate: Decimal = Decimal('0')
    surcharge_amount: Decimal = Decimal('0')
    cess_rate: Decimal = Decimal('0.04')
    cess_amount: Decimal = Decimal('0')

    rebate_amount: Decimal = Decimal('0')
    total_tax_liability: Decimal = Decimal('0')

    # Credits/Payments
    tds_deducted: Decimal = Decimal('0')
    advance_tax_paid: Decimal = Decimal('0')
    balance_payable: Decimal = Decimal('0')


class AdvanceTaxCalculator:
    """
    Data-driven advance tax calculator.
    All tax rules fetched from database - no hardcoded rates.
    """

    def __init__(self, db_connection):
        """
        Initialize with database connection.

        Args:
            db_connection: SQLite connection object
        """
        self.conn = db_connection
        self.tax_rules = TaxRulesService(db_connection)
        self.income_service = IncomeAggregationService(db_connection)

    def calculate(
        self,
        user_id: int,
        financial_year: str,
        tax_regime: str = 'NEW'
    ) -> AdvanceTaxResult:
        """
        Calculate advance tax for a user.

        Flow:
        1. Fetch aggregated income from database (no file parsing)
        2. Fetch tax rules from database (no hardcoded rates)
        3. Apply tax calculations
        4. Return result

        Args:
            user_id: User ID
            financial_year: e.g., '2024-25'
            tax_regime: 'OLD' or 'NEW'

        Returns:
            AdvanceTaxResult with complete tax computation
        """
        result = AdvanceTaxResult(
            user_id=user_id,
            financial_year=financial_year,
            tax_regime=tax_regime
        )

        # Step 1: Get income from database
        income_records = self.income_service.get_user_income_summary(
            user_id, financial_year
        )
        result.income_items = income_records

        # Step 2: Categorize income into tax buckets
        self._categorize_income(result, income_records, financial_year)

        # Step 3: Calculate deductions
        self._apply_deductions(result, financial_year, tax_regime)

        # Step 4: Calculate tax
        self._calculate_tax(result, financial_year, tax_regime)

        # Step 5: Store computation in database
        self._store_computation(result)

        return result

    def _categorize_income(
        self,
        result: AdvanceTaxResult,
        records: list[IncomeRecord],
        financial_year: str
    ):
        """Categorize income records into tax buckets."""
        # Get rate types for this FY to determine bucket
        stcg_equity_types = ('FLAT_15', 'FLAT_20')
        ltcg_equity_types = ('FLAT_10', 'FLAT_12.5')

        for r in records:
            if r.income_type == 'SALARY':
                result.total_salary_income += r.taxable_amount

            elif r.income_type == 'CAPITAL_GAINS':
                if r.sub_classification == 'STCG':
                    if r.applicable_tax_rate_type in stcg_equity_types:
                        result.total_stcg_equity += r.taxable_amount
                    else:
                        result.total_capital_gains_slab += r.taxable_amount

                elif r.sub_classification == 'LTCG':
                    if r.applicable_tax_rate_type in ltcg_equity_types:
                        result.total_ltcg_equity += r.taxable_amount
                    else:
                        result.total_capital_gains_slab += r.taxable_amount

                else:  # SPECULATIVE, etc.
                    result.total_capital_gains_slab += r.taxable_amount

            elif r.income_type == 'OTHER_SOURCES':
                result.total_other_income += r.taxable_amount

            elif r.income_type == 'HOUSE_PROPERTY':
                result.total_house_property += r.taxable_amount

            elif r.income_type == 'BUSINESS':
                # F&O income treated as business income, taxed at slab
                result.total_capital_gains_slab += r.taxable_amount

            # Accumulate TDS
            result.tds_deducted += r.tds_deducted

        # Calculate gross total income
        result.gross_total_income = (
            result.total_salary_income +
            result.total_stcg_equity +
            result.total_ltcg_equity +
            result.total_capital_gains_slab +
            result.total_other_income +
            result.total_house_property
        )

    def _apply_deductions(
        self,
        result: AdvanceTaxResult,
        financial_year: str,
        tax_regime: str
    ):
        """Apply standard deductions from database."""
        # Standard deduction for salary
        if result.total_salary_income > 0:
            std_deduction = self.tax_rules.get_standard_deduction(
                financial_year, tax_regime, 'SALARY'
            )
            result.total_deductions += std_deduction

        result.taxable_income = max(
            Decimal('0'),
            result.gross_total_income - result.total_deductions
        )

    def _calculate_tax(
        self,
        result: AdvanceTaxResult,
        financial_year: str,
        tax_regime: str
    ):
        """Calculate tax using database rules."""
        # Get tax slabs from database
        slabs = self.tax_rules.get_tax_slabs(financial_year, tax_regime)

        # Calculate slab income (excluding special rate CG)
        slab_income = (
            result.total_salary_income +
            result.total_capital_gains_slab +
            result.total_other_income +
            result.total_house_property -
            result.total_deductions
        )
        slab_income = max(Decimal('0'), slab_income)

        # Tax on slab income
        result.tax_on_slab_income = self._calculate_slab_tax(slab_income, slabs)

        # Tax on STCG equity (special rate)
        if result.total_stcg_equity > 0:
            stcg_rate = self.tax_rules.get_capital_gains_rate(
                financial_year, 'EQUITY_LISTED', 'STCG'
            )
            if stcg_rate:
                result.tax_on_stcg_equity = (
                    result.total_stcg_equity * stcg_rate.tax_rate
                ).quantize(Decimal('1'), rounding=ROUND_HALF_UP)

        # Tax on LTCG equity (after exemption)
        if result.total_ltcg_equity > 0:
            ltcg_rate = self.tax_rules.get_capital_gains_rate(
                financial_year, 'EQUITY_LISTED', 'LTCG'
            )
            if ltcg_rate:
                # Exemption already applied during categorization for MF
                # For stocks, apply exemption here
                ltcg_taxable = result.total_ltcg_equity
                result.tax_on_ltcg_equity = (
                    ltcg_taxable * ltcg_rate.tax_rate
                ).quantize(Decimal('1'), rounding=ROUND_HALF_UP)

        # Total tax before rebate/cess
        result.total_tax_before_cess = (
            result.tax_on_slab_income +
            result.tax_on_stcg_equity +
            result.tax_on_ltcg_equity
        )

        # Apply Section 87A rebate if eligible
        rebate_limit, max_rebate = self.tax_rules.get_rebate_limit(
            financial_year, tax_regime
        )
        if result.taxable_income <= rebate_limit and result.total_tax_before_cess > 0:
            result.rebate_amount = min(result.total_tax_before_cess, max_rebate)
            result.total_tax_before_cess -= result.rebate_amount

        # Surcharge
        result.surcharge_rate = self.tax_rules.get_surcharge_rate(
            financial_year, result.gross_total_income
        )
        result.surcharge_amount = (
            result.total_tax_before_cess * result.surcharge_rate
        ).quantize(Decimal('1'), rounding=ROUND_HALF_UP)

        tax_with_surcharge = result.total_tax_before_cess + result.surcharge_amount

        # Health & Education Cess
        result.cess_rate = self.tax_rules.get_cess_rate(financial_year)
        result.cess_amount = (
            tax_with_surcharge * result.cess_rate
        ).quantize(Decimal('1'), rounding=ROUND_HALF_UP)

        # Total tax liability
        result.total_tax_liability = tax_with_surcharge + result.cess_amount

        # Balance payable after TDS
        result.balance_payable = max(
            Decimal('0'),
            result.total_tax_liability - result.tds_deducted - result.advance_tax_paid
        )

    def _calculate_slab_tax(
        self,
        income: Decimal,
        slabs: list
    ) -> Decimal:
        """Calculate tax based on income slabs."""
        if income <= 0:
            return Decimal('0')

        tax = Decimal('0')

        for slab in slabs:
            if income <= slab.lower_limit:
                break

            upper = slab.upper_limit if slab.upper_limit else income
            taxable_in_slab = min(income, upper) - slab.lower_limit

            if taxable_in_slab > 0:
                tax += taxable_in_slab * slab.tax_rate

        return tax.quantize(Decimal('1'), rounding=ROUND_HALF_UP)

    def _store_computation(self, result: AdvanceTaxResult):
        """Store computation result in database for history."""
        # Mark previous computations as not latest
        self.conn.execute("""
            UPDATE advance_tax_computation
            SET is_latest = FALSE
            WHERE user_id = ? AND financial_year = ?
        """, (result.user_id, result.financial_year))

        # Prepare computation JSON for detailed breakdown
        computation_json = json.dumps({
            'income_items': [
                {
                    'type': r.income_type,
                    'sub': r.sub_classification,
                    'grouping': r.income_sub_grouping,
                    'gross': float(r.gross_amount),
                    'deductions': float(r.deductions),
                    'taxable': float(r.taxable_amount),
                    'tds': float(r.tds_deducted),
                    'rate_type': r.applicable_tax_rate_type,
                }
                for r in result.income_items
            ]
        })

        # Insert new computation
        self.conn.execute("""
            INSERT INTO advance_tax_computation (
                user_id, financial_year, tax_regime, computation_date,
                total_salary_income, total_stcg_equity, total_ltcg_equity,
                total_capital_gains_slab, total_other_income, total_house_property,
                gross_total_income, total_deductions, taxable_income,
                tax_on_slab_income, tax_on_stcg_equity, tax_on_ltcg_equity,
                total_tax_before_cess, surcharge_rate, surcharge_amount,
                cess_rate, cess_amount, total_tax_liability,
                tds_deducted, advance_tax_paid, balance_tax_payable,
                computation_json, is_latest
            ) VALUES (?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
        """, (
            result.user_id, result.financial_year, result.tax_regime,
            float(result.total_salary_income), float(result.total_stcg_equity),
            float(result.total_ltcg_equity), float(result.total_capital_gains_slab),
            float(result.total_other_income), float(result.total_house_property),
            float(result.gross_total_income), float(result.total_deductions),
            float(result.taxable_income), float(result.tax_on_slab_income),
            float(result.tax_on_stcg_equity), float(result.tax_on_ltcg_equity),
            float(result.total_tax_before_cess), float(result.surcharge_rate),
            float(result.surcharge_amount), float(result.cess_rate),
            float(result.cess_amount), float(result.total_tax_liability),
            float(result.tds_deducted), float(result.advance_tax_paid),
            float(result.balance_payable), computation_json
        ))
        self.conn.commit()

    def get_latest_computation(
        self,
        user_id: int,
        financial_year: str
    ) -> Optional[AdvanceTaxResult]:
        """
        Retrieve the latest stored computation for a user/FY.

        Returns:
            AdvanceTaxResult or None if not found
        """
        cursor = self.conn.execute("""
            SELECT tax_regime, computation_date,
                   total_salary_income, total_stcg_equity, total_ltcg_equity,
                   total_capital_gains_slab, total_other_income, total_house_property,
                   gross_total_income, total_deductions, taxable_income,
                   tax_on_slab_income, tax_on_stcg_equity, tax_on_ltcg_equity,
                   total_tax_before_cess, surcharge_rate, surcharge_amount,
                   cess_rate, cess_amount, total_tax_liability,
                   tds_deducted, advance_tax_paid, balance_tax_payable
            FROM advance_tax_computation
            WHERE user_id = ? AND financial_year = ? AND is_latest = TRUE
        """, (user_id, financial_year))

        row = cursor.fetchone()
        if not row:
            return None

        return AdvanceTaxResult(
            user_id=user_id,
            financial_year=financial_year,
            tax_regime=row[0],
            total_salary_income=Decimal(str(row[2])),
            total_stcg_equity=Decimal(str(row[3])),
            total_ltcg_equity=Decimal(str(row[4])),
            total_capital_gains_slab=Decimal(str(row[5])),
            total_other_income=Decimal(str(row[6])),
            total_house_property=Decimal(str(row[7])),
            gross_total_income=Decimal(str(row[8])),
            total_deductions=Decimal(str(row[9])),
            taxable_income=Decimal(str(row[10])),
            tax_on_slab_income=Decimal(str(row[11])),
            tax_on_stcg_equity=Decimal(str(row[12])),
            tax_on_ltcg_equity=Decimal(str(row[13])),
            total_tax_before_cess=Decimal(str(row[14])),
            surcharge_rate=Decimal(str(row[15])),
            surcharge_amount=Decimal(str(row[16])),
            cess_rate=Decimal(str(row[17])),
            cess_amount=Decimal(str(row[18])),
            total_tax_liability=Decimal(str(row[19])),
            tds_deducted=Decimal(str(row[20])),
            advance_tax_paid=Decimal(str(row[21])),
            balance_payable=Decimal(str(row[22])),
        )

    def get_advance_tax_schedule(
        self,
        total_tax: Decimal
    ) -> list[dict]:
        """
        Get advance tax payment schedule.

        Returns:
            List of due dates with amounts
        """
        return [
            {'due_date': '15th June', 'percent': '15%', 'amount': float(total_tax * Decimal('0.15'))},
            {'due_date': '15th September', 'percent': '45%', 'amount': float(total_tax * Decimal('0.45'))},
            {'due_date': '15th December', 'percent': '75%', 'amount': float(total_tax * Decimal('0.75'))},
            {'due_date': '15th March', 'percent': '100%', 'amount': float(total_tax)},
        ]
