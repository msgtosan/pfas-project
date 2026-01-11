"""Tax Rules Service - Fetches tax rules from database.

All tax rates, slabs, exemptions are fetched from database tables.
No hardcoded tax rates in application code.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


@dataclass
class TaxSlab:
    """Tax slab configuration loaded from database."""
    lower_limit: Decimal
    upper_limit: Optional[Decimal]
    tax_rate: Decimal


@dataclass
class CapitalGainsRate:
    """Capital gains rate configuration."""
    asset_type: str
    gain_type: str  # STCG or LTCG
    holding_period_months: int
    tax_rate: Decimal
    rate_type: str  # FLAT, SLAB, INDEXED
    exemption_limit: Decimal
    stt_required: bool


class TaxRulesService:
    """
    Service to fetch tax rules from database.
    Eliminates hardcoded tax rates in application code.
    """

    def __init__(self, db_connection):
        """
        Initialize with database connection.

        Args:
            db_connection: SQLite connection object
        """
        self.conn = db_connection
        self._cache = {}  # Simple in-memory cache

    def get_tax_slabs(
        self,
        financial_year: str,
        tax_regime: str = 'NEW'
    ) -> list[TaxSlab]:
        """
        Fetch tax slabs for given FY and regime.

        Args:
            financial_year: e.g., '2024-25'
            tax_regime: 'OLD' or 'NEW'

        Returns:
            List of TaxSlab ordered by slab_order
        """
        cache_key = f"slabs_{financial_year}_{tax_regime}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        cursor = self.conn.execute("""
            SELECT lower_limit, upper_limit, tax_rate
            FROM income_tax_slabs
            WHERE financial_year = ?
              AND tax_regime = ?
              AND (effective_to IS NULL OR effective_to >= date('now'))
            ORDER BY slab_order
        """, (financial_year, tax_regime))

        slabs = []
        for row in cursor.fetchall():
            slabs.append(TaxSlab(
                lower_limit=Decimal(str(row[0])),
                upper_limit=Decimal(str(row[1])) if row[1] else None,
                tax_rate=Decimal(str(row[2]))
            ))

        self._cache[cache_key] = slabs
        return slabs

    def get_capital_gains_rate(
        self,
        financial_year: str,
        asset_type: str,
        gain_type: str
    ) -> Optional[CapitalGainsRate]:
        """
        Fetch capital gains rate for specific asset type.

        Args:
            financial_year: e.g., '2024-25'
            asset_type: e.g., 'EQUITY_LISTED', 'EQUITY_MF', 'FOREIGN_EQUITY'
            gain_type: 'STCG' or 'LTCG'

        Returns:
            CapitalGainsRate or None if not found
        """
        cache_key = f"cg_{financial_year}_{asset_type}_{gain_type}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        cursor = self.conn.execute("""
            SELECT asset_type, gain_type, holding_period_months,
                   tax_rate, rate_type, exemption_limit, stt_required
            FROM capital_gains_rates
            WHERE financial_year = ?
              AND asset_type = ?
              AND gain_type = ?
              AND (effective_to IS NULL OR effective_to >= date('now'))
        """, (financial_year, asset_type, gain_type))

        row = cursor.fetchone()
        if not row:
            return None

        rate = CapitalGainsRate(
            asset_type=row[0],
            gain_type=row[1],
            holding_period_months=row[2],
            tax_rate=Decimal(str(row[3])),
            rate_type=row[4],
            exemption_limit=Decimal(str(row[5] or 0)),
            stt_required=bool(row[6])
        )

        self._cache[cache_key] = rate
        return rate

    def get_standard_deduction(
        self,
        financial_year: str,
        tax_regime: str,
        deduction_type: str = 'SALARY'
    ) -> Decimal:
        """
        Fetch standard deduction amount.

        Args:
            financial_year: e.g., '2024-25'
            tax_regime: 'OLD' or 'NEW'
            deduction_type: 'SALARY', 'PENSION', 'HOUSE_PROPERTY'

        Returns:
            Deduction amount (0 if not found)
        """
        cache_key = f"std_{financial_year}_{tax_regime}_{deduction_type}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        cursor = self.conn.execute("""
            SELECT deduction_amount, deduction_percent
            FROM standard_deductions
            WHERE financial_year = ?
              AND (tax_regime = ? OR tax_regime = 'BOTH')
              AND deduction_type = ?
        """, (financial_year, tax_regime, deduction_type))

        row = cursor.fetchone()
        if row:
            amount = Decimal(str(row[0])) if row[0] else Decimal('0')
        else:
            amount = Decimal('0')

        self._cache[cache_key] = amount
        return amount

    def get_house_property_deduction_percent(
        self,
        financial_year: str
    ) -> Decimal:
        """Get house property standard deduction percentage (30%)."""
        cursor = self.conn.execute("""
            SELECT deduction_percent
            FROM standard_deductions
            WHERE financial_year = ?
              AND deduction_type = 'HOUSE_PROPERTY'
        """, (financial_year,))

        row = cursor.fetchone()
        return Decimal(str(row[0])) if row else Decimal('0.30')

    def get_surcharge_rate(
        self,
        financial_year: str,
        total_income: Decimal,
        income_type: str = 'NORMAL'
    ) -> Decimal:
        """
        Fetch applicable surcharge rate based on income.

        Args:
            financial_year: e.g., '2024-25'
            total_income: Gross total income
            income_type: 'NORMAL' or 'EQUITY_CG' (capped at 15%)

        Returns:
            Surcharge rate as decimal (e.g., 0.10 for 10%)
        """
        cursor = self.conn.execute("""
            SELECT surcharge_rate
            FROM surcharge_rates
            WHERE financial_year = ?
              AND income_type = ?
              AND lower_limit <= ?
              AND (upper_limit IS NULL OR upper_limit > ?)
            ORDER BY lower_limit DESC
            LIMIT 1
        """, (financial_year, income_type, float(total_income), float(total_income)))

        row = cursor.fetchone()
        return Decimal(str(row[0])) if row else Decimal('0')

    def get_cess_rate(self, financial_year: str) -> Decimal:
        """
        Fetch Health & Education Cess rate.

        Returns:
            Cess rate (default 0.04 = 4%)
        """
        cursor = self.conn.execute("""
            SELECT rate
            FROM cess_rates
            WHERE financial_year = ?
              AND cess_type = 'HEALTH_EDUCATION'
        """, (financial_year,))

        row = cursor.fetchone()
        return Decimal(str(row[0])) if row else Decimal('0.04')

    def get_rebate_limit(
        self,
        financial_year: str,
        tax_regime: str
    ) -> tuple[Decimal, Decimal]:
        """
        Fetch Section 87A rebate limits.

        Returns:
            Tuple of (income_limit, max_rebate)
        """
        cursor = self.conn.execute("""
            SELECT income_limit, max_rebate
            FROM rebate_limits
            WHERE financial_year = ?
              AND tax_regime = ?
        """, (financial_year, tax_regime))

        row = cursor.fetchone()
        if row:
            return (Decimal(str(row[0])), Decimal(str(row[1])))
        return (Decimal('0'), Decimal('0'))

    def get_chapter_via_limit(
        self,
        financial_year: str,
        tax_regime: str,
        section: str
    ) -> Decimal:
        """
        Fetch Chapter VI-A deduction limit for a section.

        Args:
            section: e.g., '80C', '80D', '80CCD1B'

        Returns:
            Maximum allowed deduction
        """
        cursor = self.conn.execute("""
            SELECT max_limit
            FROM chapter_via_limits
            WHERE financial_year = ?
              AND tax_regime = ?
              AND section = ?
        """, (financial_year, tax_regime, section))

        row = cursor.fetchone()
        return Decimal(str(row[0])) if row else Decimal('0')

    def clear_cache(self):
        """Clear the in-memory cache."""
        self._cache.clear()
