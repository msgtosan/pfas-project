#!/usr/bin/env python3
"""PFAS Financial Statement Report Generator CLI.

Usage (from project root):
    ./pfas-report --user Sanjay --fy 2024-25
    ./pfas-report --user Sanjay --fy 2024-25 --report balance-sheet
    ./pfas-report --help

After installation (pip install -e .):
    pfas-report --user Sanjay --fy 2024-25
"""

import argparse
import sys
import json
from pathlib import Path
from datetime import date
from decimal import Decimal
from typing import Optional, List

# Handle imports for both installed package and direct execution
try:
    from pfas.core.database import DatabaseManager
    from pfas.services import BalanceSheetService, CashFlowStatementService, PortfolioValuationService
    from pfas.parsers.mf import CapitalGainsCalculator
    from pfas.core.models import get_fy_dates
    from pfas.core.accounts import setup_chart_of_accounts
except ImportError:
    # If running directly, add src to path
    import os
    src_path = Path(__file__).parent.parent.parent.parent / "src"
    if src_path.exists() and str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    from pfas.core.database import DatabaseManager
    from pfas.services import BalanceSheetService, CashFlowStatementService, PortfolioValuationService
    from pfas.parsers.mf import CapitalGainsCalculator
    from pfas.core.models import get_fy_dates
    from pfas.core.accounts import setup_chart_of_accounts

try:
    import sqlcipher3 as sqlite3
    HAS_SQLCIPHER = True
except ImportError:
    import sqlite3
    HAS_SQLCIPHER = False


# Default paths - relative to project root
def get_project_root() -> Path:
    """Get project root directory."""
    # Check if running from installed package or direct execution
    cli_path = Path(__file__).resolve()
    # Go up from src/pfas/cli/reports_cli.py to project root
    root = cli_path.parent.parent.parent.parent
    if (root / "Data").exists() or (root / "src").exists():
        return root
    # Fallback to current working directory
    return Path.cwd()


PROJECT_ROOT = get_project_root()
DEFAULT_PASSWORD = "pfas_secure_2024"


def get_user_data_dir(user_name: str) -> Path:
    """Get user-specific data directory."""
    user_dir = PROJECT_ROOT / "Data" / "Users" / user_name
    user_dir.mkdir(parents=True, exist_ok=True)
    return user_dir


def get_user_db_path(user_name: str) -> str:
    """Get user-specific database path."""
    user_dir = get_user_data_dir(user_name)
    return str(user_dir / "pfas.db")


def get_user_reports_dir(user_name: str) -> Path:
    """Get user-specific reports directory."""
    user_dir = get_user_data_dir(user_name)
    reports_dir = user_dir / "Reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    return reports_dir


def get_db_connection(db_path: str, password: str):
    """Get database connection."""
    db = DatabaseManager()
    conn = db.init(db_path, password)
    return conn


def init_database(db_path: str, password: str):
    """Initialize database with schema."""
    print(f"Initializing database at: {db_path}")

    # Create parent directory if needed
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    db = DatabaseManager()
    conn = db.init(db_path, password)
    setup_chart_of_accounts(conn)

    print("Database initialized successfully.")
    return conn


def get_or_create_user(conn, user_name: str) -> int:
    """Get user ID or create if not exists."""
    cursor = conn.execute(
        "SELECT id FROM users WHERE name = ?",
        (user_name,)
    )
    row = cursor.fetchone()

    if row:
        return row[0]

    # Create user with dummy encrypted PAN
    cursor = conn.execute("""
        INSERT INTO users (pan_encrypted, pan_salt, name)
        VALUES (?, ?, ?)
    """, (b'encrypted', b'salt', user_name))
    conn.commit()
    return cursor.lastrowid


def list_users(conn):
    """List all users in database."""
    cursor = conn.execute("SELECT id, name, email FROM users")
    users = cursor.fetchall()

    if not users:
        print("No users found in database.")
        return

    print("\nAvailable Users:")
    print("-" * 40)
    for user in users:
        email = user[2] or "N/A"
        print(f"  ID: {user[0]}, Name: {user[1]}, Email: {email}")
    print()


def get_available_fys(conn, user_id: int) -> List[str]:
    """Get list of financial years with data for user."""
    fys = set()

    # Check various tables for financial year data
    tables_with_fy = [
        ("mf_transactions", "financial_year"),
        ("stock_trades", "financial_year"),
        ("bank_transactions", "financial_year"),
        ("salary_records", "financial_year"),
    ]

    for table, col in tables_with_fy:
        try:
            cursor = conn.execute(f"""
                SELECT DISTINCT {col} FROM {table}
                WHERE user_id = ? AND {col} IS NOT NULL
            """, (user_id,))
            for row in cursor.fetchall():
                if row[0]:
                    fys.add(row[0])
        except:
            pass

    return sorted(list(fys))


def format_currency(amount) -> str:
    """Format amount as Indian currency."""
    if amount is None:
        return "Rs.0.00"
    if isinstance(amount, str):
        amount = Decimal(amount)
    return f"Rs.{amount:,.2f}"


def generate_balance_sheet(conn, user_id: int, as_of: date, output_format: str = "text"):
    """Generate Balance Sheet report."""
    service = BalanceSheetService(conn)
    snapshot = service.get_balance_sheet(user_id=user_id, as_of=as_of)

    if output_format == "json":
        return {
            "snapshot_date": str(snapshot.snapshot_date),
            "total_assets": float(snapshot.total_assets),
            "total_liabilities": float(snapshot.total_liabilities),
            "net_worth": float(snapshot.net_worth),
            "assets": {
                "bank_savings": float(snapshot.bank_savings),
                "mutual_funds_equity": float(snapshot.mutual_funds_equity),
                "mutual_funds_debt": float(snapshot.mutual_funds_debt),
                "stocks_indian": float(snapshot.stocks_indian),
                "stocks_foreign": float(snapshot.stocks_foreign),
                "epf_balance": float(snapshot.epf_balance),
                "ppf_balance": float(snapshot.ppf_balance),
                "nps_tier1": float(snapshot.nps_tier1),
                "nps_tier2": float(snapshot.nps_tier2),
                "sgb_holdings": float(snapshot.sgb_holdings),
                "reit_holdings": float(snapshot.reit_holdings),
                "real_estate": float(snapshot.real_estate),
                "other_assets": float(snapshot.other_assets),
            },
            "liabilities": {
                "home_loans": float(snapshot.home_loans),
                "car_loans": float(snapshot.car_loans),
                "personal_loans": float(snapshot.personal_loans),
                "credit_cards": float(snapshot.credit_cards),
                "other_liabilities": float(snapshot.other_liabilities),
            }
        }

    # Text format
    report = []
    report.append("=" * 60)
    report.append(f"BALANCE SHEET as of {snapshot.snapshot_date}")
    report.append("=" * 60)
    report.append("")
    report.append("ASSETS")
    report.append("-" * 40)
    report.append(f"  Bank Savings:          {format_currency(snapshot.bank_savings)}")
    report.append(f"  Mutual Funds (Equity): {format_currency(snapshot.mutual_funds_equity)}")
    report.append(f"  Mutual Funds (Debt):   {format_currency(snapshot.mutual_funds_debt)}")
    report.append(f"  Stocks (Indian):       {format_currency(snapshot.stocks_indian)}")
    report.append(f"  Stocks (Foreign):      {format_currency(snapshot.stocks_foreign)}")
    report.append(f"  EPF Balance:           {format_currency(snapshot.epf_balance)}")
    report.append(f"  PPF Balance:           {format_currency(snapshot.ppf_balance)}")
    report.append(f"  NPS Tier I:            {format_currency(snapshot.nps_tier1)}")
    report.append(f"  NPS Tier II:           {format_currency(snapshot.nps_tier2)}")
    report.append(f"  SGB Holdings:          {format_currency(snapshot.sgb_holdings)}")
    report.append(f"  REIT Holdings:         {format_currency(snapshot.reit_holdings)}")
    report.append(f"  Real Estate:           {format_currency(snapshot.real_estate)}")
    report.append(f"  Other Assets:          {format_currency(snapshot.other_assets)}")
    report.append("-" * 40)
    report.append(f"  TOTAL ASSETS:          {format_currency(snapshot.total_assets)}")
    report.append("")
    report.append("LIABILITIES")
    report.append("-" * 40)
    report.append(f"  Home Loans:            {format_currency(snapshot.home_loans)}")
    report.append(f"  Car Loans:             {format_currency(snapshot.car_loans)}")
    report.append(f"  Personal Loans:        {format_currency(snapshot.personal_loans)}")
    report.append(f"  Credit Cards:          {format_currency(snapshot.credit_cards)}")
    report.append(f"  Other Liabilities:     {format_currency(snapshot.other_liabilities)}")
    report.append("-" * 40)
    report.append(f"  TOTAL LIABILITIES:     {format_currency(snapshot.total_liabilities)}")
    report.append("")
    report.append("=" * 60)
    report.append(f"  NET WORTH:             {format_currency(snapshot.net_worth)}")
    report.append("=" * 60)

    return "\n".join(report)


def generate_cash_flow_statement(conn, user_id: int, fy: str, output_format: str = "text"):
    """Generate Cash Flow Statement report."""
    service = CashFlowStatementService(conn)
    statement = service.get_cash_flow_statement(user_id=user_id, financial_year=fy)

    if output_format == "json":
        return {
            "financial_year": statement.financial_year,
            "period_start": str(statement.period_start),
            "period_end": str(statement.period_end),
            "operating": {
                "salary_received": float(statement.salary_received),
                "dividends_received": float(statement.dividends_received),
                "interest_received": float(statement.interest_received),
                "rent_received": float(statement.rent_received),
                "taxes_paid": float(statement.taxes_paid),
                "net_operating": float(statement.net_operating),
            },
            "investing": {
                "mf_purchases": float(statement.mf_purchases),
                "mf_redemptions": float(statement.mf_redemptions),
                "stock_buys": float(statement.stock_buys),
                "stock_sells": float(statement.stock_sells),
                "ppf_deposits": float(statement.ppf_deposits),
                "nps_contributions": float(statement.nps_contributions),
                "net_investing": float(statement.net_investing),
            },
            "financing": {
                "loan_proceeds": float(statement.loan_proceeds),
                "loan_repayments": float(statement.loan_repayments),
                "net_financing": float(statement.net_financing),
            },
            "net_change_in_cash": float(statement.net_change_in_cash),
        }

    # Text format
    report = []
    report.append("=" * 60)
    report.append(f"CASH FLOW STATEMENT - FY {statement.financial_year}")
    report.append(f"Period: {statement.period_start} to {statement.period_end}")
    report.append("=" * 60)
    report.append("")
    report.append("OPERATING ACTIVITIES")
    report.append("-" * 40)
    report.append(f"  Salary Received:       {format_currency(statement.salary_received)}")
    report.append(f"  Dividends Received:    {format_currency(statement.dividends_received)}")
    report.append(f"  Interest Received:     {format_currency(statement.interest_received)}")
    report.append(f"  Rent Received:         {format_currency(statement.rent_received)}")
    report.append(f"  Taxes Paid:           ({format_currency(statement.taxes_paid)})")
    report.append("-" * 40)
    report.append(f"  NET OPERATING:         {format_currency(statement.net_operating)}")
    report.append("")
    report.append("INVESTING ACTIVITIES")
    report.append("-" * 40)
    report.append(f"  MF Purchases:         ({format_currency(statement.mf_purchases)})")
    report.append(f"  MF Redemptions:        {format_currency(statement.mf_redemptions)}")
    report.append(f"  Stock Buys:           ({format_currency(statement.stock_buys)})")
    report.append(f"  Stock Sells:           {format_currency(statement.stock_sells)}")
    report.append(f"  PPF Deposits:         ({format_currency(statement.ppf_deposits)})")
    report.append(f"  NPS Contributions:    ({format_currency(statement.nps_contributions)})")
    report.append("-" * 40)
    report.append(f"  NET INVESTING:         {format_currency(statement.net_investing)}")
    report.append("")
    report.append("FINANCING ACTIVITIES")
    report.append("-" * 40)
    report.append(f"  Loan Proceeds:         {format_currency(statement.loan_proceeds)}")
    report.append(f"  Loan Repayments:      ({format_currency(statement.loan_repayments)})")
    report.append("-" * 40)
    report.append(f"  NET FINANCING:         {format_currency(statement.net_financing)}")
    report.append("")
    report.append("=" * 60)
    report.append(f"  NET CHANGE IN CASH:    {format_currency(statement.net_change_in_cash)}")
    report.append("=" * 60)

    return "\n".join(report)


def generate_income_statement(conn, user_id: int, fy: str, output_format: str = "text"):
    """Generate Income Statement (Capital Gains Summary)."""
    calculator = CapitalGainsCalculator(conn)
    summaries = calculator.calculate_summary(user_id=user_id, fy=fy)

    if output_format == "json":
        return {
            "financial_year": fy,
            "capital_gains": [
                {
                    "asset_class": s.asset_class.value,
                    "stcg": float(s.stcg_amount),
                    "ltcg": float(s.ltcg_amount),
                    "ltcg_exemption": float(s.ltcg_exemption),
                    "taxable_stcg": float(s.taxable_stcg),
                    "taxable_ltcg": float(s.taxable_ltcg),
                }
                for s in summaries
            ]
        }

    # Text format
    report = []
    report.append("=" * 60)
    report.append(f"INCOME STATEMENT (CAPITAL GAINS) - FY {fy}")
    report.append("=" * 60)
    report.append("")

    total_stcg = Decimal("0")
    total_ltcg = Decimal("0")
    total_taxable_stcg = Decimal("0")
    total_taxable_ltcg = Decimal("0")

    for summary in summaries:
        report.append(f"{summary.asset_class.value} FUNDS")
        report.append("-" * 40)
        report.append(f"  Short Term CG:         {format_currency(summary.stcg_amount)}")
        report.append(f"  Long Term CG:          {format_currency(summary.ltcg_amount)}")
        report.append(f"  LTCG Exemption:       ({format_currency(summary.ltcg_exemption)})")
        report.append(f"  Taxable STCG:          {format_currency(summary.taxable_stcg)}")
        report.append(f"  Taxable LTCG:          {format_currency(summary.taxable_ltcg)}")
        report.append("")

        total_stcg += summary.stcg_amount
        total_ltcg += summary.ltcg_amount
        total_taxable_stcg += summary.taxable_stcg
        total_taxable_ltcg += summary.taxable_ltcg

    report.append("=" * 60)
    report.append("SUMMARY")
    report.append("-" * 40)
    report.append(f"  Total STCG:            {format_currency(total_stcg)}")
    report.append(f"  Total LTCG:            {format_currency(total_ltcg)}")
    report.append(f"  Taxable STCG:          {format_currency(total_taxable_stcg)}")
    report.append(f"  Taxable LTCG:          {format_currency(total_taxable_ltcg)}")
    report.append("=" * 60)

    return "\n".join(report)


def generate_portfolio_summary(conn, user_id: int, output_format: str = "text"):
    """Generate Portfolio Valuation Summary."""
    service = PortfolioValuationService(conn)
    summary = service.get_portfolio_summary(user_id=user_id)
    xirr_result = service.calculate_xirr(user_id=user_id)

    if output_format == "json":
        return {
            "total_invested": float(summary.total_invested),
            "total_current_value": float(summary.total_current_value),
            "total_unrealized_gain": float(summary.total_unrealized_gain),
            "holdings_count": len(summary.holdings),
            "xirr_percent": float(xirr_result.xirr_percent) if xirr_result.xirr_percent else None,
        }

    # Text format
    report = []
    report.append("=" * 60)
    report.append("PORTFOLIO VALUATION SUMMARY")
    report.append("=" * 60)
    report.append("")
    report.append(f"  Total Invested:        {format_currency(summary.total_invested)}")
    report.append(f"  Current Value:         {format_currency(summary.total_current_value)}")
    report.append(f"  Unrealized Gain/Loss:  {format_currency(summary.total_unrealized_gain)}")
    report.append(f"  Holdings Count:        {len(summary.holdings)}")

    if xirr_result.xirr_percent is not None:
        report.append(f"  XIRR:                  {xirr_result.xirr_percent:.2f}%")
    else:
        report.append(f"  XIRR:                  {xirr_result.error or 'N/A'}")

    report.append("=" * 60)

    return "\n".join(report)


def save_text_report(content: str, output_path: Path):
    """Save text report to file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content)
    print(f"Report saved to: {output_path}")


def export_to_excel(reports: dict, output_path: Path, user_name: str, fy: str):
    """Export reports to Excel file."""
    try:
        import pandas as pd
        from openpyxl import Workbook
        from openpyxl.styles import Font, Alignment
    except ImportError:
        print("Error: pandas and openpyxl required for Excel export")
        print("Install with: pip install pandas openpyxl")
        return False

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Summary sheet
        summary_data = {
            "Report": ["User", "Financial Year", "Generated On"],
            "Value": [user_name, fy, str(date.today())]
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name="Summary", index=False)

        # Balance Sheet
        if "balance_sheet" in reports:
            bs = reports["balance_sheet"]
            if isinstance(bs, dict):
                bs_data = []
                for key, value in bs.items():
                    if isinstance(value, dict):
                        for k, v in value.items():
                            bs_data.append({"Item": f"{key}.{k}", "Value": v})
                    else:
                        bs_data.append({"Item": key, "Value": value})
                pd.DataFrame(bs_data).to_excel(writer, sheet_name="Balance Sheet", index=False)

        # Cash Flow
        if "cash_flow" in reports:
            cf = reports["cash_flow"]
            if isinstance(cf, dict):
                cf_data = []
                for key, value in cf.items():
                    if isinstance(value, dict):
                        for k, v in value.items():
                            cf_data.append({"Item": f"{key}.{k}", "Value": v})
                    else:
                        cf_data.append({"Item": key, "Value": value})
                pd.DataFrame(cf_data).to_excel(writer, sheet_name="Cash Flow", index=False)

        # Income Statement
        if "income" in reports:
            inc = reports["income"]
            if isinstance(inc, dict) and "capital_gains" in inc:
                pd.DataFrame(inc["capital_gains"]).to_excel(
                    writer, sheet_name="Capital Gains", index=False
                )

    print(f"Excel report saved to: {output_path}")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="PFAS Financial Statement Report Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --user Sanjay --fy 2024-25
  %(prog)s --user Sanjay --fy 2024-25 --report balance-sheet
  %(prog)s --user Sanjay --fy 2024-25 --report cash-flow
  %(prog)s --user Sanjay --fy 2024-25 --report income
  %(prog)s --user Sanjay --fy 2024-25 --format xlsx
  %(prog)s --user Sanjay --fy all

Data Structure:
  Each user has separate database and reports:
    Data/Users/{user}/pfas.db      - User's database
    Data/Users/{user}/Reports/     - User's reports
        """
    )

    parser.add_argument(
        "--user", "-u",
        required=True,
        help="User name (required) - determines database and report location"
    )
    parser.add_argument(
        "--fy", "-y",
        help="Financial year (e.g., 2024-25) or 'all' for all years"
    )
    parser.add_argument(
        "--report", "-r",
        choices=["all", "balance-sheet", "cash-flow", "income", "portfolio"],
        default="all",
        help="Report type to generate (default: all)"
    )
    parser.add_argument(
        "--output", "-o",
        help="Custom output file path (optional)"
    )
    parser.add_argument(
        "--format", "-f",
        choices=["text", "json", "xlsx"],
        default="text",
        help="Output format: text, json, or xlsx (default: text)"
    )
    parser.add_argument(
        "--db",
        help="Custom database path (default: Data/Users/{user}/pfas.db)"
    )
    parser.add_argument(
        "--password",
        default=DEFAULT_PASSWORD,
        help="Database password"
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize user's database"
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save reports to files, only print to console"
    )

    args = parser.parse_args()

    # Determine database path (user-specific or custom)
    if args.db:
        db_path = args.db
    else:
        db_path = get_user_db_path(args.user)

    # Handle init-db
    if args.init_db:
        init_database(db_path, args.password)
        return 0

    # Get database connection
    try:
        conn = get_db_connection(db_path, args.password)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print(f"Database path: {db_path}")
        print(f"Run with --init-db to initialize the database for user '{args.user}'.")
        return 1

    # Validate required arguments for report generation
    if not args.fy:
        parser.error("--fy is required for report generation")

    # Get user ID (user_id=1 for single-user databases)
    user_id = get_or_create_user(conn, args.user)
    print(f"\nUser: {args.user}")
    print(f"Database: {db_path}")

    # Determine financial years
    if args.fy.lower() == "all":
        fys = get_available_fys(conn, user_id)
        if not fys:
            fys = ["2024-25"]  # Default
            print("No data found, using default FY 2024-25")
    else:
        fys = [fy.strip() for fy in args.fy.split(",")]

    print(f"Financial Year(s): {', '.join(fys)}")

    # Get user reports directory
    reports_dir = get_user_reports_dir(args.user)
    print(f"Reports directory: {reports_dir}")
    print()

    # Generate reports
    all_reports = {}

    for fy in fys:
        print(f"{'=' * 60}")
        print(f"FINANCIAL YEAR: {fy}")
        print(f"{'=' * 60}")

        fy_reports = {}
        fy_suffix = fy.replace("-", "")

        # Balance Sheet (as of FY end date)
        if args.report in ["all", "balance-sheet"]:
            try:
                _, end_date = get_fy_dates(fy)
                report = generate_balance_sheet(conn, user_id, end_date,
                                               "json" if args.format in ["json", "xlsx"] else "text")

                if args.format == "text":
                    print(report)
                    if not args.no_save:
                        save_text_report(report, reports_dir / f"balance_sheet_FY{fy_suffix}.txt")
                else:
                    fy_reports["balance_sheet"] = report
            except Exception as e:
                print(f"Error generating Balance Sheet: {e}")

        # Cash Flow Statement
        if args.report in ["all", "cash-flow"]:
            try:
                report = generate_cash_flow_statement(conn, user_id, fy,
                                                     "json" if args.format in ["json", "xlsx"] else "text")

                if args.format == "text":
                    print(report)
                    if not args.no_save:
                        save_text_report(report, reports_dir / f"cash_flow_FY{fy_suffix}.txt")
                else:
                    fy_reports["cash_flow"] = report
            except Exception as e:
                print(f"Error generating Cash Flow Statement: {e}")

        # Income Statement
        if args.report in ["all", "income"]:
            try:
                report = generate_income_statement(conn, user_id, fy,
                                                  "json" if args.format in ["json", "xlsx"] else "text")

                if args.format == "text":
                    print(report)
                    if not args.no_save:
                        save_text_report(report, reports_dir / f"income_statement_FY{fy_suffix}.txt")
                else:
                    fy_reports["income"] = report
            except Exception as e:
                print(f"Error generating Income Statement: {e}")

        # Portfolio Summary
        if args.report in ["all", "portfolio"]:
            try:
                report = generate_portfolio_summary(conn, user_id,
                                                   "json" if args.format in ["json", "xlsx"] else "text")

                if args.format == "text":
                    print(report)
                    if not args.no_save:
                        save_text_report(report, reports_dir / f"portfolio_summary_FY{fy_suffix}.txt")
                else:
                    fy_reports["portfolio"] = report
            except Exception as e:
                print(f"Error generating Portfolio Summary: {e}")

        all_reports[fy] = fy_reports

    # JSON output
    if args.format == "json":
        json_output = json.dumps(all_reports, indent=2)
        print(json_output)
        if not args.no_save:
            json_path = reports_dir / f"financial_reports_FY{fys[0].replace('-', '')}.json"
            json_path.write_text(json_output)
            print(f"\nJSON report saved to: {json_path}")

    # Excel export
    if args.format == "xlsx":
        # Use custom output path or default
        if args.output:
            output_path = Path(args.output)
        else:
            output_path = reports_dir / f"financial_reports_FY{fys[0].replace('-', '')}.xlsx"

        # Merge all FY reports for Excel
        merged_reports = {}
        for fy, fy_reports in all_reports.items():
            for report_type, data in fy_reports.items():
                merged_reports[report_type] = data

        export_to_excel(merged_reports, output_path, args.user, fys[0])

    return 0


if __name__ == "__main__":
    sys.exit(main())
