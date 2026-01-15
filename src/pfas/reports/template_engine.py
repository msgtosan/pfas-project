"""
Report Templating Engine.

Provides a unified framework for generating reports:
1. Balance Sheet Report
2. Cash Flow Statement Report
3. Portfolio Report
4. Net Worth Report

All reports can be exported to Excel (xlsx) with consistent formatting.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import sqlite3

import pandas as pd

from pfas.core.models import (
    BalanceSheetSnapshot,
    CashFlowStatement,
    get_financial_year,
)
from pfas.services.balance_sheet_service import BalanceSheetService
from pfas.services.cash_flow_service import CashFlowStatementService
from pfas.services.portfolio_valuation_service import PortfolioValuationService


@dataclass
class ReportConfig:
    """Configuration for report generation."""
    title: str
    user_name: str
    user_id: int
    financial_year: str = ""
    as_of_date: Optional[date] = None
    output_format: str = "xlsx"
    include_details: bool = True
    custom_params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReportMetadata:
    """Metadata for generated report."""
    report_type: str
    generated_at: str
    user_name: str
    period: str
    file_path: str


class ReportGenerator(ABC):
    """
    Abstract base class for report generators.

    Subclasses implement specific report types while
    inheriting common Excel formatting and export logic.
    """

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize with database connection.

        Args:
            db_connection: SQLite connection object
        """
        self.conn = db_connection

    @abstractmethod
    def get_report_data(self, config: ReportConfig) -> Dict[str, Any]:
        """
        Fetch data for the report.

        Args:
            config: Report configuration

        Returns:
            Dict with report data
        """
        pass

    @abstractmethod
    def generate(self, config: ReportConfig, output_path: Path) -> ReportMetadata:
        """
        Generate the report file.

        Args:
            config: Report configuration
            output_path: Directory for output file

        Returns:
            ReportMetadata with file location
        """
        pass

    def _create_excel_report(
        self,
        sheets: Dict[str, pd.DataFrame],
        config: ReportConfig,
        output_path: Path,
        report_type: str
    ) -> Path:
        """
        Create Excel report with standard formatting.

        Args:
            sheets: Dict of sheet_name -> DataFrame
            config: Report configuration
            output_path: Output directory
            report_type: Type of report for filename

        Returns:
            Path to created file
        """
        from datetime import datetime

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{report_type}_{config.user_name}_{config.financial_year or config.as_of_date}_{timestamp}.xlsx"
        file_path = output_path / filename

        output_path.mkdir(parents=True, exist_ok=True)

        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            for sheet_name, df in sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

                # Auto-adjust column widths
                worksheet = writer.sheets[sheet_name]
                for idx, col in enumerate(df.columns):
                    max_length = max(
                        df[col].astype(str).str.len().max(),
                        len(str(col))
                    ) + 2
                    worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)

        return file_path

    def _format_currency(self, value: Decimal, symbol: str = "Rs") -> str:
        """Format decimal as currency string."""
        if value >= 10000000:  # 1 Crore
            return f"{symbol} {float(value / 10000000):.2f} Cr"
        elif value >= 100000:  # 1 Lakh
            return f"{symbol} {float(value / 100000):.2f} L"
        else:
            return f"{symbol} {float(value):,.2f}"


class BalanceSheetReportGenerator(ReportGenerator):
    """Generate balance sheet reports."""

    def get_report_data(self, config: ReportConfig) -> Dict[str, Any]:
        """Fetch balance sheet data."""
        service = BalanceSheetService(self.conn)
        as_of = config.as_of_date or date.today()
        snapshot = service.get_balance_sheet(config.user_id, as_of)

        return {
            "snapshot": snapshot,
            "as_of": as_of,
        }

    def generate(self, config: ReportConfig, output_path: Path) -> ReportMetadata:
        """Generate balance sheet report."""
        from datetime import datetime

        data = self.get_report_data(config)
        snapshot: BalanceSheetSnapshot = data["snapshot"]

        # Summary sheet
        summary_data = [
            {"Category": "ASSETS", "Amount (Rs)": ""},
            {"Category": "Bank & Cash", "Amount (Rs)": float(snapshot.total_bank_balances)},
            {"Category": "  Bank Savings", "Amount (Rs)": float(snapshot.bank_savings)},
            {"Category": "  Bank FD", "Amount (Rs)": float(snapshot.bank_fd)},
            {"Category": "", "Amount (Rs)": ""},
            {"Category": "Investments", "Amount (Rs)": float(snapshot.total_investments)},
            {"Category": "  Mutual Funds - Equity", "Amount (Rs)": float(snapshot.mutual_funds_equity)},
            {"Category": "  Mutual Funds - Debt", "Amount (Rs)": float(snapshot.mutual_funds_debt)},
            {"Category": "  Indian Stocks", "Amount (Rs)": float(snapshot.stocks_indian)},
            {"Category": "  Foreign Stocks", "Amount (Rs)": float(snapshot.stocks_foreign)},
            {"Category": "", "Amount (Rs)": ""},
            {"Category": "Retirement Funds", "Amount (Rs)": float(snapshot.total_retirement_funds)},
            {"Category": "  EPF", "Amount (Rs)": float(snapshot.epf_balance)},
            {"Category": "  PPF", "Amount (Rs)": float(snapshot.ppf_balance)},
            {"Category": "  NPS", "Amount (Rs)": float(snapshot.nps_tier1 + snapshot.nps_tier2)},
            {"Category": "", "Amount (Rs)": ""},
            {"Category": "TOTAL ASSETS", "Amount (Rs)": float(snapshot.total_assets)},
            {"Category": "", "Amount (Rs)": ""},
            {"Category": "LIABILITIES", "Amount (Rs)": ""},
            {"Category": "  Home Loans", "Amount (Rs)": float(snapshot.home_loans)},
            {"Category": "  Car Loans", "Amount (Rs)": float(snapshot.car_loans)},
            {"Category": "  Personal Loans", "Amount (Rs)": float(snapshot.personal_loans)},
            {"Category": "  Credit Cards", "Amount (Rs)": float(snapshot.credit_cards)},
            {"Category": "TOTAL LIABILITIES", "Amount (Rs)": float(snapshot.total_liabilities)},
            {"Category": "", "Amount (Rs)": ""},
            {"Category": "NET WORTH", "Amount (Rs)": float(snapshot.net_worth)},
        ]
        df_summary = pd.DataFrame(summary_data)

        # Holdings detail sheet
        holdings_data = []
        for h in snapshot.asset_holdings:
            holdings_data.append({
                "Asset Type": h.asset_type.value,
                "Name": h.asset_name,
                "Identifier": h.asset_identifier,
                "Quantity": float(h.quantity),
                "Unit Price": float(h.unit_price),
                "Total Value": float(h.total_value),
                "Cost Basis": float(h.cost_basis),
                "Unrealized Gain": float(h.unrealized_gain),
                "Return %": float(h.return_percentage) if h.return_percentage else 0,
            })
        df_holdings = pd.DataFrame(holdings_data) if holdings_data else pd.DataFrame()

        # Liabilities detail sheet
        liability_data = []
        for l in snapshot.liability_details:
            liability_data.append({
                "Type": l.liability_type.value,
                "Lender": l.lender_name,
                "Principal": float(l.principal_amount),
                "Outstanding": float(l.outstanding_amount),
                "Interest Rate": float(l.interest_rate),
                "EMI": float(l.emi_amount) if l.emi_amount else 0,
            })
        df_liabilities = pd.DataFrame(liability_data) if liability_data else pd.DataFrame()

        sheets = {
            "Balance Sheet": df_summary,
            "Asset Holdings": df_holdings,
            "Liabilities": df_liabilities,
        }

        file_path = self._create_excel_report(
            sheets, config, output_path, "BalanceSheet"
        )

        return ReportMetadata(
            report_type="Balance Sheet",
            generated_at=datetime.now().isoformat(),
            user_name=config.user_name,
            period=str(config.as_of_date or date.today()),
            file_path=str(file_path),
        )


class CashFlowReportGenerator(ReportGenerator):
    """Generate cash flow statement reports."""

    def get_report_data(self, config: ReportConfig) -> Dict[str, Any]:
        """Fetch cash flow data."""
        service = CashFlowStatementService(self.conn)
        statement = service.get_cash_flow_statement(config.user_id, config.financial_year)

        return {
            "statement": statement,
        }

    def generate(self, config: ReportConfig, output_path: Path) -> ReportMetadata:
        """Generate cash flow statement report."""
        from datetime import datetime

        data = self.get_report_data(config)
        statement: CashFlowStatement = data["statement"]

        # Summary sheet
        summary_data = [
            {"Category": f"Cash Flow Statement - {config.financial_year}", "Amount (Rs)": ""},
            {"Category": f"Period: {statement.period_start} to {statement.period_end}", "Amount (Rs)": ""},
            {"Category": "", "Amount (Rs)": ""},
            {"Category": "A. OPERATING ACTIVITIES", "Amount (Rs)": ""},
            {"Category": "  Salary Received", "Amount (Rs)": float(statement.salary_received)},
            {"Category": "  Dividends Received", "Amount (Rs)": float(statement.dividends_received)},
            {"Category": "  Interest Received", "Amount (Rs)": float(statement.interest_received)},
            {"Category": "  Rent Received", "Amount (Rs)": float(statement.rent_received)},
            {"Category": "  Taxes Paid", "Amount (Rs)": float(-statement.taxes_paid)},
            {"Category": "  Insurance Paid", "Amount (Rs)": float(-statement.insurance_paid)},
            {"Category": "Net Cash from Operating", "Amount (Rs)": float(statement.net_operating)},
            {"Category": "", "Amount (Rs)": ""},
            {"Category": "B. INVESTING ACTIVITIES", "Amount (Rs)": ""},
            {"Category": "  MF Purchases", "Amount (Rs)": float(-statement.mf_purchases)},
            {"Category": "  MF Redemptions", "Amount (Rs)": float(statement.mf_redemptions)},
            {"Category": "  Stock Buys", "Amount (Rs)": float(-statement.stock_buys)},
            {"Category": "  Stock Sells", "Amount (Rs)": float(statement.stock_sells)},
            {"Category": "  PPF Deposits", "Amount (Rs)": float(-statement.ppf_deposits)},
            {"Category": "  NPS Contributions", "Amount (Rs)": float(-statement.nps_contributions)},
            {"Category": "Net Cash from Investing", "Amount (Rs)": float(statement.net_investing)},
            {"Category": "", "Amount (Rs)": ""},
            {"Category": "C. FINANCING ACTIVITIES", "Amount (Rs)": ""},
            {"Category": "  Loan Proceeds", "Amount (Rs)": float(statement.loan_proceeds)},
            {"Category": "  Loan Repayments", "Amount (Rs)": float(-statement.loan_repayments)},
            {"Category": "  Credit Card Payments", "Amount (Rs)": float(-statement.credit_card_payments)},
            {"Category": "Net Cash from Financing", "Amount (Rs)": float(statement.net_financing)},
            {"Category": "", "Amount (Rs)": ""},
            {"Category": "NET CHANGE IN CASH", "Amount (Rs)": float(statement.net_change_in_cash)},
            {"Category": "Opening Cash Balance", "Amount (Rs)": float(statement.opening_cash)},
            {"Category": "Closing Cash Balance", "Amount (Rs)": float(statement.closing_cash)},
        ]
        df_summary = pd.DataFrame(summary_data)

        # Details sheets
        sheets = {"Cash Flow Summary": df_summary}

        if config.include_details and statement.operating_details:
            ops_data = [{
                "Date": str(f.flow_date),
                "Category": f.category,
                "Description": f.description,
                "Direction": f.flow_direction.value,
                "Amount": float(f.amount),
            } for f in statement.operating_details]
            sheets["Operating Details"] = pd.DataFrame(ops_data)

        if config.include_details and statement.investing_details:
            inv_data = [{
                "Date": str(f.flow_date),
                "Category": f.category,
                "Description": f.description,
                "Direction": f.flow_direction.value,
                "Amount": float(f.amount),
            } for f in statement.investing_details]
            sheets["Investing Details"] = pd.DataFrame(inv_data)

        file_path = self._create_excel_report(
            sheets, config, output_path, "CashFlow"
        )

        return ReportMetadata(
            report_type="Cash Flow Statement",
            generated_at=datetime.now().isoformat(),
            user_name=config.user_name,
            period=config.financial_year,
            file_path=str(file_path),
        )


class PortfolioReportGenerator(ReportGenerator):
    """Generate portfolio reports with holdings and performance."""

    def get_report_data(self, config: ReportConfig) -> Dict[str, Any]:
        """Fetch portfolio data."""
        service = PortfolioValuationService(self.conn)
        summary = service.get_portfolio_summary(config.user_id, config.as_of_date)
        xirr = service.calculate_xirr(config.user_id)

        return {
            "summary": summary,
            "xirr": xirr,
        }

    def generate(self, config: ReportConfig, output_path: Path) -> ReportMetadata:
        """Generate portfolio report."""
        from datetime import datetime

        data = self.get_report_data(config)
        summary = data["summary"]
        xirr = data["xirr"]

        # Summary sheet
        summary_data = [
            {"Metric": "Portfolio Summary", "Value": ""},
            {"Metric": f"As of: {summary.as_of_date}", "Value": ""},
            {"Metric": "", "Value": ""},
            {"Metric": "Total Invested", "Value": float(summary.total_invested)},
            {"Metric": "Current Value", "Value": float(summary.total_current_value)},
            {"Metric": "Unrealized Gain", "Value": float(summary.total_unrealized_gain)},
            {"Metric": "Overall Return %", "Value": float(summary.overall_return_percent or 0)},
            {"Metric": "XIRR %", "Value": float(xirr.xirr_percent or 0)},
            {"Metric": "", "Value": ""},
            {"Metric": "By Asset Class", "Value": ""},
            {"Metric": "Mutual Funds - Invested", "Value": float(summary.mutual_funds_invested)},
            {"Metric": "Mutual Funds - Current", "Value": float(summary.mutual_funds_current)},
            {"Metric": "Stocks - Invested", "Value": float(summary.stocks_invested)},
            {"Metric": "Stocks - Current", "Value": float(summary.stocks_current)},
        ]
        df_summary = pd.DataFrame(summary_data)

        # Holdings detail
        holdings_data = [{
            "Asset Type": h.asset_type.value,
            "Name": h.asset_name,
            "Quantity": float(h.quantity),
            "Avg Cost": float(h.cost_basis / h.quantity) if h.quantity else 0,
            "Current Price": float(h.unit_price),
            "Cost Basis": float(h.cost_basis),
            "Current Value": float(h.total_value),
            "Gain/Loss": float(h.unrealized_gain),
            "Return %": float(h.return_percentage) if h.return_percentage else 0,
        } for h in summary.holdings]
        df_holdings = pd.DataFrame(holdings_data)

        sheets = {
            "Portfolio Summary": df_summary,
            "Holdings": df_holdings,
        }

        file_path = self._create_excel_report(
            sheets, config, output_path, "Portfolio"
        )

        return ReportMetadata(
            report_type="Portfolio Report",
            generated_at=datetime.now().isoformat(),
            user_name=config.user_name,
            period=str(config.as_of_date or date.today()),
            file_path=str(file_path),
        )


class NetWorthReportGenerator(ReportGenerator):
    """Generate net worth trend report."""

    def get_report_data(self, config: ReportConfig) -> Dict[str, Any]:
        """Fetch net worth history data."""
        service = BalanceSheetService(self.conn)
        history = service.get_net_worth_history(config.user_id)

        return {
            "history": history,
        }

    def generate(self, config: ReportConfig, output_path: Path) -> ReportMetadata:
        """Generate net worth trend report."""
        from datetime import datetime

        data = self.get_report_data(config)
        history = data["history"]

        # History sheet
        history_data = [{
            "Date": str(h["date"]),
            "Total Assets": float(h["total_assets"]),
            "Total Liabilities": float(h["total_liabilities"]),
            "Net Worth": float(h["net_worth"]),
        } for h in history]
        df_history = pd.DataFrame(history_data) if history_data else pd.DataFrame()

        sheets = {"Net Worth History": df_history}

        file_path = self._create_excel_report(
            sheets, config, output_path, "NetWorth"
        )

        return ReportMetadata(
            report_type="Net Worth Report",
            generated_at=datetime.now().isoformat(),
            user_name=config.user_name,
            period="Historical",
            file_path=str(file_path),
        )


# Factory function for getting report generators
def get_report_generator(
    report_type: str,
    db_connection: sqlite3.Connection
) -> ReportGenerator:
    """
    Get appropriate report generator for report type.

    Args:
        report_type: Type of report ('balance_sheet', 'cash_flow', 'portfolio', 'net_worth')
        db_connection: Database connection

    Returns:
        ReportGenerator instance
    """
    generators = {
        "balance_sheet": BalanceSheetReportGenerator,
        "cash_flow": CashFlowReportGenerator,
        "portfolio": PortfolioReportGenerator,
        "net_worth": NetWorthReportGenerator,
    }

    generator_class = generators.get(report_type.lower())
    if not generator_class:
        raise ValueError(f"Unknown report type: {report_type}")

    return generator_class(db_connection)
