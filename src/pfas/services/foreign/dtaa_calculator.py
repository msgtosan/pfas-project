"""DTAA (Double Taxation Avoidance Agreement) credit calculator.

Calculates foreign tax credits under Indo-US DTAA for:
- Dividend withholding (25% US tax)
- Section 90/91 relief
- Form 67 requirements
"""

import sqlite3
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional, List, Dict

from pfas.services.currency import SBITTRateProvider


@dataclass
class DTAACredit:
    """DTAA credit record for foreign tax paid."""

    income_type: str  # 'DIVIDEND', 'INTEREST', 'ROYALTY'
    income_country: str  # 'US', 'UK', etc.
    income_date: date
    gross_income_usd: Decimal
    tax_withheld_usd: Decimal
    gross_income_inr: Decimal
    tax_withheld_inr: Decimal
    dtaa_article: str  # e.g., 'Article 10(2)(b)' for dividends
    max_dtaa_rate: Decimal  # Treaty rate limit
    indian_tax_on_income: Decimal = Decimal("0")
    credit_allowed: Decimal = Decimal("0")  # Lower of foreign tax or Indian tax


@dataclass
class DTAASummary:
    """Annual DTAA summary for Form 67."""

    financial_year: str
    country: str

    # Income totals
    dividend_income_inr: Decimal = Decimal("0")
    interest_income_inr: Decimal = Decimal("0")
    other_income_inr: Decimal = Decimal("0")
    total_income_inr: Decimal = Decimal("0")

    # Foreign tax totals
    dividend_tax_withheld_inr: Decimal = Decimal("0")
    interest_tax_withheld_inr: Decimal = Decimal("0")
    other_tax_withheld_inr: Decimal = Decimal("0")
    total_tax_withheld_inr: Decimal = Decimal("0")

    # Credits
    dtaa_credit_allowed: Decimal = Decimal("0")
    section_91_credit: Decimal = Decimal("0")
    total_credit: Decimal = Decimal("0")

    # Details for Form 67
    tax_identification_number: str = ""
    dtaa_articles_used: List[str] = field(default_factory=list)


@dataclass
class Form67Data:
    """Data required for Form 67 filing."""

    financial_year: str
    name: str
    pan: str
    assessment_year: str

    # Foreign country details
    countries: List[Dict] = field(default_factory=list)

    # Income details by country
    income_details: List[Dict] = field(default_factory=list)

    # Relief claimed
    section_90_relief: Decimal = Decimal("0")  # With DTAA
    section_91_relief: Decimal = Decimal("0")  # Without DTAA

    total_foreign_tax_paid: Decimal = Decimal("0")
    total_relief_claimed: Decimal = Decimal("0")


class DTAACalculator:
    """
    Calculates DTAA credits for foreign income.

    Indo-US DTAA Key Provisions:
    - Article 10: Dividends - Max 25% withholding
    - Article 11: Interest - Max 15% withholding
    - Article 13: Capital Gains - Taxable in residence country

    Relief Method:
    - Credit method: Foreign tax credit against Indian tax
    - Credit limited to Indian tax on that income
    """

    # DTAA treaty rates (max withholding)
    TREATY_RATES = {
        'US': {
            'DIVIDEND': Decimal("0.25"),  # 25%
            'INTEREST': Decimal("0.15"),  # 15%
            'ROYALTY': Decimal("0.15"),   # 15%
        },
        'UK': {
            'DIVIDEND': Decimal("0.15"),
            'INTEREST': Decimal("0.15"),
            'ROYALTY': Decimal("0.15"),
        },
    }

    # DTAA article references
    DTAA_ARTICLES = {
        'US': {
            'DIVIDEND': 'Article 10(2)(b)',
            'INTEREST': 'Article 11(2)',
            'ROYALTY': 'Article 12(2)',
            'CAPITAL_GAINS': 'Article 13',
        },
    }

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize DTAA calculator.

        Args:
            db_connection: Database connection
        """
        self.conn = db_connection
        self.rate_provider = SBITTRateProvider(db_connection)

    def calculate_dividend_credit(
        self,
        dividend_date: date,
        gross_dividend_usd: Decimal,
        tax_withheld_usd: Decimal,
        country: str = "US",
        indian_tax_rate: Decimal = Decimal("0.30")
    ) -> DTAACredit:
        """
        Calculate DTAA credit for foreign dividend.

        Args:
            dividend_date: Date dividend received
            gross_dividend_usd: Gross dividend in USD
            tax_withheld_usd: Tax withheld by foreign country
            country: Foreign country code
            indian_tax_rate: Indian tax rate on this income

        Returns:
            DTAACredit with calculated credit
        """
        # Convert to INR
        tt_rate = self.rate_provider.get_rate(dividend_date)
        gross_inr = gross_dividend_usd * tt_rate
        tax_inr = tax_withheld_usd * tt_rate

        # Get treaty rate
        treaty_rate = self.TREATY_RATES.get(country, {}).get('DIVIDEND', Decimal("0.25"))

        # Calculate Indian tax on this income
        indian_tax = gross_inr * indian_tax_rate

        # Credit allowed = Lower of foreign tax or Indian tax
        credit_allowed = min(tax_inr, indian_tax)

        return DTAACredit(
            income_type='DIVIDEND',
            income_country=country,
            income_date=dividend_date,
            gross_income_usd=gross_dividend_usd,
            tax_withheld_usd=tax_withheld_usd,
            gross_income_inr=gross_inr,
            tax_withheld_inr=tax_inr,
            dtaa_article=self.DTAA_ARTICLES.get(country, {}).get('DIVIDEND', ''),
            max_dtaa_rate=treaty_rate,
            indian_tax_on_income=indian_tax,
            credit_allowed=credit_allowed,
        )

    def calculate_interest_credit(
        self,
        interest_date: date,
        gross_interest_usd: Decimal,
        tax_withheld_usd: Decimal,
        country: str = "US",
        indian_tax_rate: Decimal = Decimal("0.30")
    ) -> DTAACredit:
        """
        Calculate DTAA credit for foreign interest income.

        Args:
            interest_date: Date interest received
            gross_interest_usd: Gross interest in USD
            tax_withheld_usd: Tax withheld by foreign country
            country: Foreign country code
            indian_tax_rate: Indian tax rate on this income

        Returns:
            DTAACredit with calculated credit
        """
        tt_rate = self.rate_provider.get_rate(interest_date)
        gross_inr = gross_interest_usd * tt_rate
        tax_inr = tax_withheld_usd * tt_rate

        treaty_rate = self.TREATY_RATES.get(country, {}).get('INTEREST', Decimal("0.15"))
        indian_tax = gross_inr * indian_tax_rate
        credit_allowed = min(tax_inr, indian_tax)

        return DTAACredit(
            income_type='INTEREST',
            income_country=country,
            income_date=interest_date,
            gross_income_usd=gross_interest_usd,
            tax_withheld_usd=tax_withheld_usd,
            gross_income_inr=gross_inr,
            tax_withheld_inr=tax_inr,
            dtaa_article=self.DTAA_ARTICLES.get(country, {}).get('INTEREST', ''),
            max_dtaa_rate=treaty_rate,
            indian_tax_on_income=indian_tax,
            credit_allowed=credit_allowed,
        )

    def calculate_annual_credits(
        self,
        user_id: int,
        financial_year: str,
        indian_tax_rate: Decimal = Decimal("0.30")
    ) -> Dict[str, DTAASummary]:
        """
        Calculate all DTAA credits for a financial year.

        Args:
            user_id: User ID
            financial_year: FY in format '2024-25'
            indian_tax_rate: User's Indian tax rate

        Returns:
            Dict of country -> DTAASummary
        """
        start_year = int(financial_year.split('-')[0])
        fy_start = date(start_year, 4, 1)
        fy_end = date(start_year + 1, 3, 31)

        summaries: Dict[str, DTAASummary] = {}

        # Get foreign dividends
        cursor = self.conn.execute(
            """SELECT dividend_date, symbol, gross_dividend_usd, withholding_tax_usd,
                      gross_dividend_inr, withholding_tax_inr
            FROM foreign_dividends
            WHERE user_id = ?
                AND dividend_date >= ?
                AND dividend_date <= ?""",
            (user_id, fy_start.isoformat(), fy_end.isoformat())
        )

        for row in cursor.fetchall():
            country = "US"  # Assume US for now, could be stored in record

            if country not in summaries:
                summaries[country] = DTAASummary(
                    financial_year=financial_year,
                    country=country,
                )

            gross_inr = Decimal(str(row['gross_dividend_inr'])) if row['gross_dividend_inr'] else Decimal("0")
            tax_inr = Decimal(str(row['withholding_tax_inr'])) if row['withholding_tax_inr'] else Decimal("0")

            summaries[country].dividend_income_inr += gross_inr
            summaries[country].dividend_tax_withheld_inr += tax_inr
            summaries[country].total_income_inr += gross_inr
            summaries[country].total_tax_withheld_inr += tax_inr

        # Calculate credits for each country
        for country, summary in summaries.items():
            # Indian tax on dividend income
            indian_tax = summary.dividend_income_inr * indian_tax_rate

            # DTAA credit = lower of foreign tax or Indian tax
            summary.dtaa_credit_allowed = min(summary.dividend_tax_withheld_inr, indian_tax)
            summary.total_credit = summary.dtaa_credit_allowed

            # Add article references
            if summary.dividend_income_inr > 0:
                article = self.DTAA_ARTICLES.get(country, {}).get('DIVIDEND', '')
                if article:
                    summary.dtaa_articles_used.append(article)

        return summaries

    def generate_form_67_data(
        self,
        user_id: int,
        financial_year: str,
        pan: str,
        name: str
    ) -> Form67Data:
        """
        Generate data for Form 67 filing.

        Form 67 is required to claim foreign tax credit.

        Args:
            user_id: User ID
            financial_year: FY in format '2024-25'
            pan: PAN number
            name: Taxpayer name

        Returns:
            Form67Data for filing
        """
        start_year = int(financial_year.split('-')[0])
        assessment_year = f"{start_year + 1}-{str(start_year + 2)[2:]}"

        summaries = self.calculate_annual_credits(user_id, financial_year)

        form_data = Form67Data(
            financial_year=financial_year,
            name=name,
            pan=pan,
            assessment_year=assessment_year,
        )

        for country, summary in summaries.items():
            # Country details
            form_data.countries.append({
                'country_code': country,
                'country_name': self._get_country_name(country),
                'dtaa_status': 'YES',
            })

            # Income details
            if summary.dividend_income_inr > 0:
                form_data.income_details.append({
                    'country': country,
                    'income_type': 'Dividend',
                    'income_amount_inr': summary.dividend_income_inr,
                    'foreign_tax_paid_inr': summary.dividend_tax_withheld_inr,
                    'dtaa_article': self.DTAA_ARTICLES.get(country, {}).get('DIVIDEND', ''),
                    'relief_claimed': summary.dtaa_credit_allowed,
                })

            form_data.section_90_relief += summary.dtaa_credit_allowed
            form_data.total_foreign_tax_paid += summary.total_tax_withheld_inr

        form_data.total_relief_claimed = form_data.section_90_relief + form_data.section_91_relief

        return form_data

    def save_dtaa_credit(self, credit: DTAACredit, user_id: int) -> int:
        """Save DTAA credit to database."""
        cursor = self.conn.execute(
            """INSERT INTO dtaa_credits
            (user_id, income_type, income_country, income_date,
             gross_income_usd, tax_withheld_usd, gross_income_inr,
             tax_withheld_inr, dtaa_article, max_dtaa_rate,
             indian_tax_on_income, credit_allowed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                user_id,
                credit.income_type,
                credit.income_country,
                credit.income_date.isoformat(),
                str(credit.gross_income_usd),
                str(credit.tax_withheld_usd),
                str(credit.gross_income_inr),
                str(credit.tax_withheld_inr),
                credit.dtaa_article,
                str(credit.max_dtaa_rate),
                str(credit.indian_tax_on_income),
                str(credit.credit_allowed),
            )
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_credits_for_year(
        self,
        user_id: int,
        financial_year: str
    ) -> List[DTAACredit]:
        """Get all DTAA credits for a financial year."""
        start_year = int(financial_year.split('-')[0])
        fy_start = date(start_year, 4, 1)
        fy_end = date(start_year + 1, 3, 31)

        cursor = self.conn.execute(
            """SELECT income_type, income_country, income_date,
                      gross_income_usd, tax_withheld_usd, gross_income_inr,
                      tax_withheld_inr, dtaa_article, max_dtaa_rate,
                      indian_tax_on_income, credit_allowed
            FROM dtaa_credits
            WHERE user_id = ?
                AND income_date >= ?
                AND income_date <= ?
            ORDER BY income_date""",
            (user_id, fy_start.isoformat(), fy_end.isoformat())
        )

        credits = []
        for row in cursor.fetchall():
            credits.append(DTAACredit(
                income_type=row['income_type'],
                income_country=row['income_country'],
                income_date=date.fromisoformat(row['income_date'])
                if isinstance(row['income_date'], str) else row['income_date'],
                gross_income_usd=Decimal(str(row['gross_income_usd'])),
                tax_withheld_usd=Decimal(str(row['tax_withheld_usd'])),
                gross_income_inr=Decimal(str(row['gross_income_inr'])),
                tax_withheld_inr=Decimal(str(row['tax_withheld_inr'])),
                dtaa_article=row['dtaa_article'],
                max_dtaa_rate=Decimal(str(row['max_dtaa_rate'])),
                indian_tax_on_income=Decimal(str(row['indian_tax_on_income'])),
                credit_allowed=Decimal(str(row['credit_allowed'])),
            ))

        return credits

    def _get_country_name(self, code: str) -> str:
        """Get full country name from code."""
        countries = {
            'US': 'United States of America',
            'UK': 'United Kingdom',
            'SG': 'Singapore',
            'UAE': 'United Arab Emirates',
        }
        return countries.get(code, code)
