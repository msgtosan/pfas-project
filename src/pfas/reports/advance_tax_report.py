"""Advance Tax Report Generator for Indian Tax Residents.

Generates advance tax reports based on New Tax Regime for FY 2024-25 and FY 2025-26.
Supports multiple income types: Salary, Capital Gains, Dividends, Rental Income, etc.
"""

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from pathlib import Path
from typing import Optional
import pandas as pd
from datetime import datetime


class FinancialYear(Enum):
    """Supported financial years."""
    FY_2024_25 = "2024-25"
    FY_2025_26 = "2025-26"


class IncomeType(Enum):
    """Income type classifications."""
    SALARY = "Salary Income"
    CAPITAL_GAINS = "Capital Gains"
    OTHER_SOURCES = "Income from Other Sources"
    HOUSE_PROPERTY = "Income from House Property"
    PERQUISITES = "Perquisites"


class CapitalGainsSubType(Enum):
    """Capital gains sub-classifications."""
    STCG_EQUITY = "STCG - Equity (Listed Indian)"
    STCG_DEBT = "STCG - Debt Funds"
    STCG_GOLD = "STCG - Gold"
    STCG_REAL_ESTATE = "STCG - Real Estate"
    STCG_USA_STOCKS = "STCG - USA Stocks/RSU"
    LTCG_EQUITY = "LTCG - Equity (Listed Indian)"
    LTCG_DEBT = "LTCG - Debt Funds"
    LTCG_GOLD = "LTCG - Gold"
    LTCG_REAL_ESTATE = "LTCG - Real Estate"
    LTCG_USA_STOCKS = "LTCG - USA Stocks/RSU"
    INTRADAY = "Intraday/Speculative"


@dataclass
class TaxSlabs:
    """Tax slab configuration for New Tax Regime."""
    slabs: list[tuple[Decimal, Decimal, Decimal]]  # (lower, upper, rate)
    standard_deduction: Decimal
    ltcg_exemption: Decimal
    stcg_equity_rate: Decimal
    ltcg_equity_rate: Decimal
    health_cess_rate: Decimal = Decimal("0.04")

    @classmethod
    def for_fy_2024_25(cls) -> "TaxSlabs":
        """Tax slabs for FY 2024-25 New Tax Regime."""
        return cls(
            slabs=[
                (Decimal("0"), Decimal("300000"), Decimal("0")),
                (Decimal("300000"), Decimal("700000"), Decimal("0.05")),
                (Decimal("700000"), Decimal("1000000"), Decimal("0.10")),
                (Decimal("1000000"), Decimal("1200000"), Decimal("0.15")),
                (Decimal("1200000"), Decimal("1500000"), Decimal("0.20")),
                (Decimal("1500000"), Decimal("99999999999"), Decimal("0.30")),
            ],
            standard_deduction=Decimal("50000"),
            ltcg_exemption=Decimal("100000"),
            stcg_equity_rate=Decimal("0.15"),
            ltcg_equity_rate=Decimal("0.10"),
        )

    @classmethod
    def for_fy_2025_26(cls) -> "TaxSlabs":
        """Tax slabs for FY 2025-26 New Tax Regime."""
        return cls(
            slabs=[
                (Decimal("0"), Decimal("400000"), Decimal("0")),
                (Decimal("400000"), Decimal("800000"), Decimal("0.05")),
                (Decimal("800000"), Decimal("1200000"), Decimal("0.10")),
                (Decimal("1200000"), Decimal("1600000"), Decimal("0.15")),
                (Decimal("1600000"), Decimal("2000000"), Decimal("0.20")),
                (Decimal("2000000"), Decimal("2400000"), Decimal("0.25")),
                (Decimal("2400000"), Decimal("99999999999"), Decimal("0.30")),
            ],
            standard_deduction=Decimal("75000"),
            ltcg_exemption=Decimal("125000"),
            stcg_equity_rate=Decimal("0.20"),
            ltcg_equity_rate=Decimal("0.125"),
        )


@dataclass
class IncomeItem:
    """Individual income item."""
    income_type: IncomeType
    sub_classification: str
    income_sub_grouping: str
    gross_amount: Decimal
    deductions: Decimal = Decimal("0")
    taxable_amount: Decimal = field(init=False)
    tax_rate: str = ""
    tax_amount: Decimal = Decimal("0")
    source: str = ""

    def __post_init__(self):
        self.taxable_amount = self.gross_amount - self.deductions


@dataclass
class AdvanceTaxSummary:
    """Advance tax calculation summary."""
    financial_year: FinancialYear
    user_name: str
    income_items: list[IncomeItem] = field(default_factory=list)

    # Computed fields
    total_salary_income: Decimal = Decimal("0")
    total_capital_gains_slab: Decimal = Decimal("0")
    total_stcg_equity: Decimal = Decimal("0")
    total_ltcg_equity: Decimal = Decimal("0")
    total_other_income: Decimal = Decimal("0")
    total_house_property: Decimal = Decimal("0")

    gross_total_income: Decimal = Decimal("0")
    total_deductions: Decimal = Decimal("0")
    taxable_income: Decimal = Decimal("0")

    tax_on_slab_income: Decimal = Decimal("0")
    tax_on_stcg_equity: Decimal = Decimal("0")
    tax_on_ltcg_equity: Decimal = Decimal("0")
    total_tax_before_cess: Decimal = Decimal("0")
    health_education_cess: Decimal = Decimal("0")
    total_tax_liability: Decimal = Decimal("0")

    surcharge: Decimal = Decimal("0")
    surcharge_rate: Decimal = Decimal("0")


class AdvanceTaxCalculator:
    """Calculator for advance tax computation."""

    def __init__(self, financial_year: FinancialYear):
        self.financial_year = financial_year
        self.tax_slabs = (
            TaxSlabs.for_fy_2024_25()
            if financial_year == FinancialYear.FY_2024_25
            else TaxSlabs.for_fy_2025_26()
        )

    def calculate_slab_tax(self, taxable_income: Decimal) -> Decimal:
        """Calculate tax based on income slabs."""
        tax = Decimal("0")
        remaining = taxable_income

        for lower, upper, rate in self.tax_slabs.slabs:
            if remaining <= 0:
                break
            slab_income = min(remaining, upper - lower)
            if taxable_income > lower:
                taxable_in_slab = min(slab_income, taxable_income - lower)
                if taxable_in_slab > 0:
                    tax += taxable_in_slab * rate
                remaining -= (upper - lower)

        return tax.quantize(Decimal("1"), rounding=ROUND_HALF_UP)

    def calculate_surcharge(self, total_income: Decimal, base_tax: Decimal) -> tuple[Decimal, Decimal]:
        """Calculate surcharge based on total income."""
        if total_income <= Decimal("5000000"):
            return Decimal("0"), Decimal("0")
        elif total_income <= Decimal("10000000"):
            rate = Decimal("0.10")
        elif total_income <= Decimal("20000000"):
            rate = Decimal("0.15")
        elif total_income <= Decimal("50000000"):
            rate = Decimal("0.25")
        else:
            rate = Decimal("0.37")

        surcharge = (base_tax * rate).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        return surcharge, rate

    def calculate_tax(self, summary: AdvanceTaxSummary) -> AdvanceTaxSummary:
        """Calculate complete tax liability."""
        # Calculate slab-based tax on regular income
        slab_income = (
            summary.total_salary_income +
            summary.total_capital_gains_slab +
            summary.total_other_income +
            summary.total_house_property -
            summary.total_deductions
        )
        slab_income = max(Decimal("0"), slab_income)

        summary.tax_on_slab_income = self.calculate_slab_tax(slab_income)

        # STCG on Equity
        if summary.total_stcg_equity > 0:
            summary.tax_on_stcg_equity = (
                summary.total_stcg_equity * self.tax_slabs.stcg_equity_rate
            ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)

        # LTCG on Equity (after exemption)
        ltcg_taxable = max(
            Decimal("0"),
            summary.total_ltcg_equity - self.tax_slabs.ltcg_exemption
        )
        if ltcg_taxable > 0:
            summary.tax_on_ltcg_equity = (
                ltcg_taxable * self.tax_slabs.ltcg_equity_rate
            ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)

        # Total tax before cess
        summary.total_tax_before_cess = (
            summary.tax_on_slab_income +
            summary.tax_on_stcg_equity +
            summary.tax_on_ltcg_equity
        )

        # Calculate surcharge
        summary.surcharge, summary.surcharge_rate = self.calculate_surcharge(
            summary.gross_total_income, summary.total_tax_before_cess
        )

        tax_with_surcharge = summary.total_tax_before_cess + summary.surcharge

        # Health & Education Cess
        summary.health_education_cess = (
            tax_with_surcharge * self.tax_slabs.health_cess_rate
        ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)

        # Total tax liability
        summary.total_tax_liability = tax_with_surcharge + summary.health_education_cess

        return summary


class UserIncomeDataLoader:
    """Loads income data from user's data folder."""

    def __init__(self, base_path: Path, user_name: str):
        self.base_path = base_path
        self.user_name = user_name
        self.user_path = base_path / "Users" / user_name

    def load_zerodha_equity_data(self, fy: FinancialYear) -> dict:
        """Load Zerodha equity capital gains data."""
        result = {
            "intraday": Decimal("0"),
            "stcg": Decimal("0"),
            "ltcg": Decimal("0"),
            "dividends": Decimal("0"),
        }

        # Find Zerodha file
        zerodha_folder = self.user_path / "Indian-Stocks" / "Zerodha"
        if not zerodha_folder.exists():
            return result

        # Look for tax pnl file
        for f in zerodha_folder.glob("taxpnl-*.xlsx"):
            try:
                # Read Equity sheet
                df = pd.read_excel(f, sheet_name="Equity")

                # Extract profit values
                for idx, row in df.iterrows():
                    val = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
                    amount = row.iloc[2] if len(row) > 2 and pd.notna(row.iloc[2]) else 0

                    if "Intraday/Speculative profit" in val:
                        result["intraday"] = Decimal(str(amount))
                    elif "Short Term profit" in val:
                        result["stcg"] = Decimal(str(amount))
                    elif "Long Term profit" in val:
                        result["ltcg"] = Decimal(str(amount))

                # Read Dividends
                try:
                    div_df = pd.read_excel(f, sheet_name="Equity Dividends")
                    for idx, row in div_df.iterrows():
                        val = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
                        if "Total Dividend Amount" in val:
                            result["dividends"] = Decimal(str(row.iloc[6] or 0))
                            break
                except Exception:
                    pass

            except Exception as e:
                print(f"Error reading Zerodha file: {e}")

        return result

    def load_karvy_mf_data(self, fy: FinancialYear) -> dict:
        """Load Karvy mutual fund capital gains data."""
        result = {"stcg": Decimal("0"), "ltcg": Decimal("0")}

        karvy_folder = self.user_path / "Mutual-Fund" / "KARVY"
        if not karvy_folder.exists():
            return result

        for f in karvy_folder.glob("*CG*.xlsx"):
            try:
                df = pd.read_excel(f)
                for idx, row in df.iterrows():
                    val = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
                    if "Short Term Capital Gain/Loss" in val:
                        total_col = row.iloc[7] if len(row) > 7 else 0
                        result["stcg"] = Decimal(str(total_col or 0))
            except Exception as e:
                print(f"Error reading Karvy file: {e}")

        return result

    def load_other_income(self, fy: FinancialYear) -> dict:
        """Load other income data (rental, etc.)."""
        result = {"rental": Decimal("0"), "municipal_tax": Decimal("0")}

        other_folder = self.user_path / "Other-Income"
        if not other_folder.exists():
            return result

        for f in other_folder.glob("*FY*.xlsx"):
            try:
                df = pd.read_excel(f)
                for idx, row in df.iterrows():
                    val = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
                    amount = row.iloc[2] if len(row) > 2 and pd.notna(row.iloc[2]) else 0

                    if "Apr'24 to Mar'25" in val or "rental" in val.lower():
                        if pd.notna(amount) and isinstance(amount, (int, float)):
                            result["rental"] = Decimal(str(amount))
                    elif "Muncipal tax" in val or "municipal" in val.lower():
                        if pd.notna(amount) and isinstance(amount, (int, float)):
                            result["municipal_tax"] = Decimal(str(amount))
            except Exception as e:
                print(f"Error reading Other Income file: {e}")

        return result

    def load_usa_stock_data(self, fy: FinancialYear) -> dict:
        """Load USA stock (RSU/ESPP) data."""
        result = {
            "rsu_vesting_income": Decimal("0"),
            "stcg": Decimal("0"),
            "ltcg": Decimal("0"),
            "dividends": Decimal("0"),
        }

        usa_folder = self.user_path / "USA-Stocks" / "ETrade"
        if not usa_folder.exists():
            return result

        # Read G&L file for capital gains
        for f in usa_folder.glob("G&L*.xlsx"):
            try:
                df = pd.read_excel(f, sheet_name="G&L_Collapsed")

                for idx, row in df.iterrows():
                    record_type = str(row.get("Record Type", ""))
                    # Use Adjusted Gain/Loss (actual profit/loss after cost basis)
                    gain_loss = row.get("Adjusted Gain/Loss", 0) or 0
                    tax_status = str(row.get("Capital Gains Status", ""))

                    if record_type == "Sell":
                        # Convert USD to INR (approximate SBI TT Buying Rate)
                        inr_amount = Decimal(str(gain_loss)) * Decimal("83.5")
                        if "Short Term" in tax_status:
                            result["stcg"] += inr_amount
                        elif "Long Term" in tax_status:
                            result["ltcg"] += inr_amount

            except Exception as e:
                print(f"Error reading G&L file: {e}")

        return result


class AdvanceTaxReportGenerator:
    """Generates advance tax reports in Excel format."""

    def __init__(self, base_data_path: Path, output_path: Path):
        self.base_data_path = base_data_path
        self.output_path = output_path
        self.output_path.mkdir(parents=True, exist_ok=True)

    def generate_report(
        self,
        user_name: str,
        financial_years: list[FinancialYear] | None = None
    ) -> list[Path]:
        """Generate advance tax reports for specified user and financial years."""
        if financial_years is None:
            financial_years = [FinancialYear.FY_2024_25, FinancialYear.FY_2025_26]

        generated_files = []
        loader = UserIncomeDataLoader(self.base_data_path, user_name)

        for fy in financial_years:
            summary = self._build_income_summary(user_name, fy, loader)
            calculator = AdvanceTaxCalculator(fy)
            summary = calculator.calculate_tax(summary)

            output_file = self._generate_excel_report(summary)
            generated_files.append(output_file)

        return generated_files

    def _build_income_summary(
        self,
        user_name: str,
        fy: FinancialYear,
        loader: UserIncomeDataLoader
    ) -> AdvanceTaxSummary:
        """Build income summary from loaded data."""
        summary = AdvanceTaxSummary(
            financial_year=fy,
            user_name=user_name,
        )

        # Load data from various sources
        zerodha_data = loader.load_zerodha_equity_data(fy)
        karvy_data = loader.load_karvy_mf_data(fy)
        other_income = loader.load_other_income(fy)
        usa_data = loader.load_usa_stock_data(fy)

        # Add Zerodha equity capital gains
        if zerodha_data["stcg"] < 0:
            # Loss - can be carried forward
            summary.income_items.append(IncomeItem(
                income_type=IncomeType.CAPITAL_GAINS,
                sub_classification="STCG",
                income_sub_grouping="Equity (Listed Indian - Zerodha)",
                gross_amount=zerodha_data["stcg"],
                tax_rate="Loss (can offset LTCG)",
                source="Zerodha",
            ))
        else:
            summary.income_items.append(IncomeItem(
                income_type=IncomeType.CAPITAL_GAINS,
                sub_classification="STCG",
                income_sub_grouping="Equity (Listed Indian - Zerodha)",
                gross_amount=zerodha_data["stcg"],
                tax_rate="15% (FY24-25) / 20% (FY25-26)",
                source="Zerodha",
            ))
            summary.total_stcg_equity += max(Decimal("0"), zerodha_data["stcg"])

        # LTCG from Zerodha
        if zerodha_data["ltcg"] > 0:
            summary.income_items.append(IncomeItem(
                income_type=IncomeType.CAPITAL_GAINS,
                sub_classification="LTCG",
                income_sub_grouping="Equity (Listed Indian - Zerodha)",
                gross_amount=zerodha_data["ltcg"],
                tax_rate="10% (exempt ₹1L) FY24-25 / 12.5% (exempt ₹1.25L) FY25-26",
                source="Zerodha",
            ))
            summary.total_ltcg_equity += zerodha_data["ltcg"]

        # Intraday from Zerodha (taxed at slab rates)
        if zerodha_data["intraday"] != 0:
            summary.income_items.append(IncomeItem(
                income_type=IncomeType.CAPITAL_GAINS,
                sub_classification="Speculative",
                income_sub_grouping="Intraday Trading",
                gross_amount=zerodha_data["intraday"],
                tax_rate="Slab rates (Business Income)",
                source="Zerodha",
            ))
            summary.total_capital_gains_slab += zerodha_data["intraday"]

        # Karvy MF capital gains (STCG on MF)
        if karvy_data["stcg"] > 0:
            summary.income_items.append(IncomeItem(
                income_type=IncomeType.CAPITAL_GAINS,
                sub_classification="STCG",
                income_sub_grouping="Equity Mutual Funds (Karvy)",
                gross_amount=karvy_data["stcg"],
                tax_rate="15% (FY24-25) / 20% (FY25-26)",
                source="Karvy",
            ))
            summary.total_stcg_equity += karvy_data["stcg"]

        # Dividends from Zerodha
        if zerodha_data["dividends"] > 0:
            summary.income_items.append(IncomeItem(
                income_type=IncomeType.OTHER_SOURCES,
                sub_classification="Dividends",
                income_sub_grouping="Indian Equity Dividends",
                gross_amount=zerodha_data["dividends"],
                tax_rate="Slab rates",
                source="Zerodha",
            ))
            summary.total_other_income += zerodha_data["dividends"]

        # USA stock capital gains
        if usa_data["stcg"] != 0:
            summary.income_items.append(IncomeItem(
                income_type=IncomeType.CAPITAL_GAINS,
                sub_classification="STCG",
                income_sub_grouping="USA Stocks (RSU/ESPP)",
                gross_amount=usa_data["stcg"],
                tax_rate="Slab rates",
                source="E*Trade",
            ))
            summary.total_capital_gains_slab += usa_data["stcg"]

        if usa_data["ltcg"] != 0:
            summary.income_items.append(IncomeItem(
                income_type=IncomeType.CAPITAL_GAINS,
                sub_classification="LTCG",
                income_sub_grouping="USA Stocks (RSU/ESPP)",
                gross_amount=usa_data["ltcg"],
                tax_rate="12.5% flat (FY25-26) / 20% with indexation (FY24-25)",
                source="E*Trade",
            ))
            # USA LTCG is taxed at different rates (not like Indian equity)
            summary.total_capital_gains_slab += usa_data["ltcg"]

        # Rental Income (House Property)
        rental_net = other_income["rental"] - other_income["municipal_tax"]
        if rental_net > 0:
            # Standard deduction for house property = 30% of NAV
            std_deduction_hp = (rental_net * Decimal("0.30")).quantize(
                Decimal("1"), rounding=ROUND_HALF_UP
            )
            summary.income_items.append(IncomeItem(
                income_type=IncomeType.HOUSE_PROPERTY,
                sub_classification="Rental Income",
                income_sub_grouping="Let-out Property",
                gross_amount=other_income["rental"],
                deductions=other_income["municipal_tax"] + std_deduction_hp,
                tax_rate="Slab rates (after 30% std deduction)",
                source="Other Income File",
            ))
            taxable_rental = rental_net - std_deduction_hp
            summary.total_house_property += taxable_rental

        # Calculate totals
        summary.gross_total_income = (
            summary.total_salary_income +
            summary.total_stcg_equity +
            summary.total_ltcg_equity +
            summary.total_capital_gains_slab +
            summary.total_other_income +
            summary.total_house_property
        )

        # Standard deduction for salary
        tax_slabs = (
            TaxSlabs.for_fy_2024_25()
            if fy == FinancialYear.FY_2024_25
            else TaxSlabs.for_fy_2025_26()
        )
        if summary.total_salary_income > 0:
            summary.total_deductions += tax_slabs.standard_deduction

        summary.taxable_income = summary.gross_total_income - summary.total_deductions

        return summary

    def _generate_excel_report(self, summary: AdvanceTaxSummary) -> Path:
        """Generate Excel report from summary."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"AdvanceTax_{summary.user_name}_{summary.financial_year.value}_{timestamp}.xlsx"
        output_file = self.output_path / filename

        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            # Income Details Sheet
            income_data = []
            for item in summary.income_items:
                income_data.append({
                    "Income Type": item.income_type.value,
                    "Sub-Classification": item.sub_classification,
                    "Income Sub-Grouping": item.income_sub_grouping,
                    "Gross Amount (₹)": float(item.gross_amount),
                    "Deductions (₹)": float(item.deductions),
                    "Taxable Amount (₹)": float(item.taxable_amount),
                    "Tax Rate": item.tax_rate,
                    "Source": item.source,
                })

            df_income = pd.DataFrame(income_data)
            df_income.to_excel(writer, sheet_name="Income Details", index=False)

            # Tax Summary Sheet
            tax_slabs = (
                TaxSlabs.for_fy_2024_25()
                if summary.financial_year == FinancialYear.FY_2024_25
                else TaxSlabs.for_fy_2025_26()
            )

            summary_data = [
                {"Particulars": "Financial Year", "Amount (₹)": summary.financial_year.value},
                {"Particulars": "User Name", "Amount (₹)": summary.user_name},
                {"Particulars": "", "Amount (₹)": ""},
                {"Particulars": "INCOME SUMMARY", "Amount (₹)": ""},
                {"Particulars": "Salary Income", "Amount (₹)": float(summary.total_salary_income)},
                {"Particulars": "STCG - Equity (15%/20%)", "Amount (₹)": float(summary.total_stcg_equity)},
                {"Particulars": "LTCG - Equity (10%/12.5%)", "Amount (₹)": float(summary.total_ltcg_equity)},
                {"Particulars": "Capital Gains (Slab Rate)", "Amount (₹)": float(summary.total_capital_gains_slab)},
                {"Particulars": "Other Income (Dividends etc.)", "Amount (₹)": float(summary.total_other_income)},
                {"Particulars": "Income from House Property", "Amount (₹)": float(summary.total_house_property)},
                {"Particulars": "", "Amount (₹)": ""},
                {"Particulars": "GROSS TOTAL INCOME", "Amount (₹)": float(summary.gross_total_income)},
                {"Particulars": f"Less: Standard Deduction", "Amount (₹)": float(tax_slabs.standard_deduction)},
                {"Particulars": "TAXABLE INCOME", "Amount (₹)": float(summary.taxable_income)},
                {"Particulars": "", "Amount (₹)": ""},
                {"Particulars": "TAX COMPUTATION", "Amount (₹)": ""},
                {"Particulars": "Tax on Slab Income", "Amount (₹)": float(summary.tax_on_slab_income)},
                {"Particulars": f"Tax on STCG Equity ({float(tax_slabs.stcg_equity_rate)*100}%)", "Amount (₹)": float(summary.tax_on_stcg_equity)},
                {"Particulars": f"Tax on LTCG Equity ({float(tax_slabs.ltcg_equity_rate)*100}%)", "Amount (₹)": float(summary.tax_on_ltcg_equity)},
                {"Particulars": f"(LTCG Exemption: ₹{float(tax_slabs.ltcg_exemption):,.0f})", "Amount (₹)": ""},
                {"Particulars": "", "Amount (₹)": ""},
                {"Particulars": "Total Tax Before Cess", "Amount (₹)": float(summary.total_tax_before_cess)},
                {"Particulars": f"Add: Surcharge ({float(summary.surcharge_rate)*100}%)", "Amount (₹)": float(summary.surcharge)},
                {"Particulars": "Add: Health & Education Cess (4%)", "Amount (₹)": float(summary.health_education_cess)},
                {"Particulars": "", "Amount (₹)": ""},
                {"Particulars": "TOTAL TAX LIABILITY", "Amount (₹)": float(summary.total_tax_liability)},
            ]

            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name="Tax Summary", index=False)

            # Advance Tax Schedule Sheet
            advance_schedule = [
                {"Due Date": "15th June", "% of Tax": "15%", "Amount (₹)": float(summary.total_tax_liability * Decimal("0.15"))},
                {"Due Date": "15th September", "% of Tax": "45%", "Amount (₹)": float(summary.total_tax_liability * Decimal("0.45"))},
                {"Due Date": "15th December", "% of Tax": "75%", "Amount (₹)": float(summary.total_tax_liability * Decimal("0.75"))},
                {"Due Date": "15th March", "% of Tax": "100%", "Amount (₹)": float(summary.total_tax_liability)},
            ]

            df_schedule = pd.DataFrame(advance_schedule)
            df_schedule.to_excel(writer, sheet_name="Advance Tax Schedule", index=False)

            # Tax Rates Reference Sheet
            fy_label = summary.financial_year.value
            rates_data = [
                {"Income Slab": f"Up to ₹{3 if summary.financial_year == FinancialYear.FY_2024_25 else 4}L", "Tax Rate": "Nil"},
            ]

            if summary.financial_year == FinancialYear.FY_2024_25:
                rates_data.extend([
                    {"Income Slab": "₹3L - ₹7L", "Tax Rate": "5%"},
                    {"Income Slab": "₹7L - ₹10L", "Tax Rate": "10%"},
                    {"Income Slab": "₹10L - ₹12L", "Tax Rate": "15%"},
                    {"Income Slab": "₹12L - ₹15L", "Tax Rate": "20%"},
                    {"Income Slab": "Above ₹15L", "Tax Rate": "30%"},
                ])
            else:
                rates_data.extend([
                    {"Income Slab": "₹4L - ₹8L", "Tax Rate": "5%"},
                    {"Income Slab": "₹8L - ₹12L", "Tax Rate": "10%"},
                    {"Income Slab": "₹12L - ₹16L", "Tax Rate": "15%"},
                    {"Income Slab": "₹16L - ₹20L", "Tax Rate": "20%"},
                    {"Income Slab": "₹20L - ₹24L", "Tax Rate": "25%"},
                    {"Income Slab": "Above ₹24L", "Tax Rate": "30%"},
                ])

            rates_data.extend([
                {"Income Slab": "", "Tax Rate": ""},
                {"Income Slab": "SPECIAL RATES", "Tax Rate": ""},
                {"Income Slab": f"STCG (Equity with STT)", "Tax Rate": f"{float(tax_slabs.stcg_equity_rate)*100}%"},
                {"Income Slab": f"LTCG (Equity with STT)", "Tax Rate": f"{float(tax_slabs.ltcg_equity_rate)*100}% (exempt: ₹{float(tax_slabs.ltcg_exemption):,.0f})"},
                {"Income Slab": f"Standard Deduction (Salary)", "Tax Rate": f"₹{float(tax_slabs.standard_deduction):,.0f}"},
            ])

            df_rates = pd.DataFrame(rates_data)
            df_rates.to_excel(writer, sheet_name=f"Tax Rates {fy_label}", index=False)

        return output_file


def generate_advance_tax_report(
    user_name: str,
    financial_years: list[str] | None = None,
    data_path: str = "Data",
    output_path: str = "Data/Reports"
) -> list[str]:
    """
    Generate advance tax report for a user.

    Args:
        user_name: Name of the user (folder name under Data/Users/)
        financial_years: List of FY strings like ["2024-25", "2025-26"].
                        If None, generates for all available years.
        data_path: Base path to Data folder
        output_path: Path for output reports

    Returns:
        List of generated report file paths
    """
    base_path = Path(data_path)
    output_dir = Path(output_path)

    # Parse financial years
    fy_list = []
    if financial_years:
        for fy in financial_years:
            if fy == "2024-25":
                fy_list.append(FinancialYear.FY_2024_25)
            elif fy == "2025-26":
                fy_list.append(FinancialYear.FY_2025_26)
    else:
        fy_list = [FinancialYear.FY_2024_25, FinancialYear.FY_2025_26]

    generator = AdvanceTaxReportGenerator(base_path, output_dir)
    generated_files = generator.generate_report(user_name, fy_list)

    return [str(f) for f in generated_files]


if __name__ == "__main__":
    import sys

    # Default values
    user_name = "Sanjay"
    fy_list = None

    if len(sys.argv) > 1:
        user_name = sys.argv[1]
    if len(sys.argv) > 2:
        fy_list = sys.argv[2].split(",")

    reports = generate_advance_tax_report(
        user_name=user_name,
        financial_years=fy_list,
        data_path="Data",
        output_path="Data/Reports"
    )

    print(f"Generated reports:")
    for r in reports:
        print(f"  - {r}")
