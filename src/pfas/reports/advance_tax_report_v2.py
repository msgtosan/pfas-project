"""Advance Tax Report Generator v2 - Database-driven.

Generates Excel reports from database-computed tax results.
No file parsing - all data from database.
"""

from pathlib import Path
from datetime import datetime
from decimal import Decimal
import pandas as pd

from pfas.services.advance_tax_calculator import AdvanceTaxCalculator, AdvanceTaxResult
from pfas.services.tax_rules_service import TaxRulesService


class AdvanceTaxReportGeneratorV2:
    """Database-driven advance tax report generator."""

    def __init__(self, db_connection, output_path: Path):
        self.conn = db_connection
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.calculator = AdvanceTaxCalculator(db_connection)
        self.tax_rules = TaxRulesService(db_connection)

    def generate_report(
        self,
        user_id: int,
        user_name: str,
        financial_year: str,
        tax_regime: str = 'NEW'
    ) -> Path:
        """Generate Excel report for a user."""
        # Calculate tax using database
        result = self.calculator.calculate(user_id, financial_year, tax_regime)

        # Generate Excel
        return self._generate_excel(result, user_name)

    def _generate_excel(self, result: AdvanceTaxResult, user_name: str) -> Path:
        """Generate Excel report from computation result."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"AdvanceTax_{user_name}_{result.financial_year}_{timestamp}.xlsx"
        output_file = self.output_path / filename

        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            # Sheet 1: Income Details
            income_data = []
            for item in result.income_items:
                income_data.append({
                    "Income Type": item.income_type,
                    "Sub-Classification": item.sub_classification,
                    "Income Sub-Grouping": item.income_sub_grouping,
                    "Gross Amount (₹)": float(item.gross_amount),
                    "Deductions (₹)": float(item.deductions),
                    "Taxable Amount (₹)": float(item.taxable_amount),
                    "TDS Deducted (₹)": float(item.tds_deducted),
                    "Tax Rate Type": item.applicable_tax_rate_type,
                    "Source": item.source_table,
                })

            if income_data:
                df_income = pd.DataFrame(income_data)
                df_income.to_excel(writer, sheet_name="Income Details", index=False)

            # Sheet 2: Tax Summary
            summary_data = [
                {"Particulars": "Financial Year", "Amount (₹)": result.financial_year},
                {"Particulars": "Tax Regime", "Amount (₹)": result.tax_regime},
                {"Particulars": "User", "Amount (₹)": user_name},
                {"Particulars": "", "Amount (₹)": ""},
                {"Particulars": "INCOME SUMMARY", "Amount (₹)": ""},
                {"Particulars": "Salary Income", "Amount (₹)": float(result.total_salary_income)},
                {"Particulars": "STCG - Equity (Special Rate)", "Amount (₹)": float(result.total_stcg_equity)},
                {"Particulars": "LTCG - Equity (Special Rate)", "Amount (₹)": float(result.total_ltcg_equity)},
                {"Particulars": "Capital Gains (Slab Rate)", "Amount (₹)": float(result.total_capital_gains_slab)},
                {"Particulars": "Other Income", "Amount (₹)": float(result.total_other_income)},
                {"Particulars": "House Property", "Amount (₹)": float(result.total_house_property)},
                {"Particulars": "", "Amount (₹)": ""},
                {"Particulars": "GROSS TOTAL INCOME", "Amount (₹)": float(result.gross_total_income)},
                {"Particulars": "Less: Deductions", "Amount (₹)": float(result.total_deductions)},
                {"Particulars": "TAXABLE INCOME", "Amount (₹)": float(result.taxable_income)},
                {"Particulars": "", "Amount (₹)": ""},
                {"Particulars": "TAX COMPUTATION", "Amount (₹)": ""},
                {"Particulars": "Tax on Slab Income", "Amount (₹)": float(result.tax_on_slab_income)},
                {"Particulars": "Tax on STCG Equity", "Amount (₹)": float(result.tax_on_stcg_equity)},
                {"Particulars": "Tax on LTCG Equity", "Amount (₹)": float(result.tax_on_ltcg_equity)},
                {"Particulars": "", "Amount (₹)": ""},
                {"Particulars": "Tax Before Cess", "Amount (₹)": float(result.total_tax_before_cess)},
                {"Particulars": f"Surcharge ({float(result.surcharge_rate)*100}%)", "Amount (₹)": float(result.surcharge_amount)},
                {"Particulars": f"Health & Education Cess ({float(result.cess_rate)*100}%)", "Amount (₹)": float(result.cess_amount)},
                {"Particulars": "", "Amount (₹)": ""},
                {"Particulars": "TOTAL TAX LIABILITY", "Amount (₹)": float(result.total_tax_liability)},
                {"Particulars": "Less: TDS Deducted", "Amount (₹)": float(result.tds_deducted)},
                {"Particulars": "Less: Advance Tax Paid", "Amount (₹)": float(result.advance_tax_paid)},
                {"Particulars": "BALANCE TAX PAYABLE", "Amount (₹)": float(result.balance_payable)},
            ]
            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name="Tax Summary", index=False)

            # Sheet 3: Advance Tax Schedule
            schedule = self.calculator.get_advance_tax_schedule(result.total_tax_liability)
            df_schedule = pd.DataFrame(schedule)
            df_schedule.columns = ["Due Date", "% of Tax", "Amount (₹)"]
            df_schedule.to_excel(writer, sheet_name="Advance Tax Schedule", index=False)

            # Sheet 4: Tax Rates Reference
            slabs = self.tax_rules.get_tax_slabs(result.financial_year, result.tax_regime)
            rates_data = []
            for i, slab in enumerate(slabs):
                upper = f"₹{int(slab.upper_limit/100000)}L" if slab.upper_limit else "Above"
                lower = f"₹{int(slab.lower_limit/100000)}L" if slab.lower_limit > 0 else "Up to"
                if slab.lower_limit == 0:
                    range_str = f"Up to ₹{int(slab.upper_limit/100000)}L"
                elif slab.upper_limit:
                    range_str = f"₹{int(slab.lower_limit/100000)}L - ₹{int(slab.upper_limit/100000)}L"
                else:
                    range_str = f"Above ₹{int(slab.lower_limit/100000)}L"
                rates_data.append({
                    "Income Slab": range_str,
                    "Tax Rate": f"{float(slab.tax_rate)*100}%" if slab.tax_rate > 0 else "Nil"
                })

            # Add special rates
            stcg_rate = self.tax_rules.get_capital_gains_rate(result.financial_year, 'EQUITY_LISTED', 'STCG')
            ltcg_rate = self.tax_rules.get_capital_gains_rate(result.financial_year, 'EQUITY_LISTED', 'LTCG')
            std_ded = self.tax_rules.get_standard_deduction(result.financial_year, result.tax_regime, 'SALARY')

            rates_data.extend([
                {"Income Slab": "", "Tax Rate": ""},
                {"Income Slab": "SPECIAL RATES", "Tax Rate": ""},
                {"Income Slab": "STCG (Listed Equity with STT)", "Tax Rate": f"{float(stcg_rate.tax_rate)*100}%" if stcg_rate else "N/A"},
                {"Income Slab": "LTCG (Listed Equity with STT)", "Tax Rate": f"{float(ltcg_rate.tax_rate)*100}% (exempt ₹{int(ltcg_rate.exemption_limit/1000)}K)" if ltcg_rate else "N/A"},
                {"Income Slab": "Standard Deduction (Salary)", "Tax Rate": f"₹{int(std_ded):,}"},
            ])

            df_rates = pd.DataFrame(rates_data)
            df_rates.to_excel(writer, sheet_name=f"Tax Rates {result.financial_year}", index=False)

        return output_file


def generate_advance_tax_report_v2(
    db_connection,
    user_id: int,
    user_name: str,
    financial_year: str,
    tax_regime: str = 'NEW',
    output_path: str = "Data/Reports"
) -> str:
    """
    Convenience function to generate advance tax report.

    Args:
        db_connection: Database connection
        user_id: User ID in database
        user_name: Display name for report
        financial_year: e.g., '2024-25'
        tax_regime: 'OLD' or 'NEW'
        output_path: Output directory

    Returns:
        Path to generated report
    """
    generator = AdvanceTaxReportGeneratorV2(db_connection, Path(output_path))
    return str(generator.generate_report(user_id, user_name, financial_year, tax_regime))
