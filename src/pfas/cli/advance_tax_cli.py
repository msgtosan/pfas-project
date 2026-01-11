#!/usr/bin/env python3
"""CLI tool for Advance Tax Report Generation.

Usage:
    python -m pfas.cli.advance_tax_cli --user Sanjay --fy 2024-25 --regime NEW
    python -m pfas.cli.advance_tax_cli --user Sanjay --fy 2024-25,2025-26
    python -m pfas.cli.advance_tax_cli --init-db  # Initialize database with tax rules
"""

import argparse
import sys
from pathlib import Path
from decimal import Decimal

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

try:
    import sqlcipher3 as sqlite3
    HAS_SQLCIPHER = True
except ImportError:
    import sqlite3
    HAS_SQLCIPHER = False


def init_database(db_path: str, password: str = "pfas_secure_2024"):
    """Initialize database with tax rules schema and seed data."""
    from pfas.core.database import DatabaseManager
    from pfas.core.tax_schema import init_tax_schema

    print(f"Initializing database at: {db_path}")

    db = DatabaseManager()
    conn = db.init(db_path, password)

    # Initialize tax schema
    init_tax_schema(conn)

    print("Database initialized with tax rules schema and seed data.")
    print("Tax rules loaded for FY 2024-25 and FY 2025-26")

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


def import_income_from_files(conn, user_id: int, user_name: str, financial_year: str):
    """
    Import income data from user's files into database.
    This bridges the gap between file-based data and database.
    """
    from pfas.services.income_aggregation_service import IncomeAggregationService
    import pandas as pd

    base_path = Path("Data/Users") / user_name
    if not base_path.exists():
        print(f"Warning: User data folder not found: {base_path}")
        return

    print(f"Importing income data for {user_name} FY {financial_year}...")

    # Import Zerodha equity data
    zerodha_folder = base_path / "Indian-Stocks" / "Zerodha"
    if zerodha_folder.exists():
        for f in zerodha_folder.glob("taxpnl-*.xlsx"):
            try:
                _import_zerodha_data(conn, user_id, f, financial_year)
                print(f"  Imported: {f.name}")
            except Exception as e:
                print(f"  Error importing {f.name}: {e}")

    # Import Karvy MF data
    karvy_folder = base_path / "Mutual-Fund" / "KARVY"
    if karvy_folder.exists():
        for f in karvy_folder.glob("*CG*.xlsx"):
            try:
                _import_karvy_data(conn, user_id, f, financial_year)
                print(f"  Imported: {f.name}")
            except Exception as e:
                print(f"  Error importing {f.name}: {e}")

    # Import USA stock G&L data
    usa_folder = base_path / "USA-Stocks" / "ETrade"
    if usa_folder.exists():
        for f in usa_folder.glob("G&L*.xlsx"):
            try:
                _import_usa_gl_data(conn, user_id, f, financial_year)
                print(f"  Imported: {f.name}")
            except Exception as e:
                print(f"  Error importing {f.name}: {e}")

    # Import Other Income (rental)
    other_folder = base_path / "Other-Income"
    if other_folder.exists():
        for f in other_folder.glob("*FY*.xlsx"):
            try:
                _import_other_income(conn, user_id, f, financial_year)
                print(f"  Imported: {f.name}")
            except Exception as e:
                print(f"  Error importing {f.name}: {e}")

    # Refresh income summary
    service = IncomeAggregationService(conn)
    count = service.refresh_summary(user_id, financial_year)
    print(f"  Refreshed income summary: {count} records")


def _import_zerodha_data(conn, user_id: int, file_path: Path, financial_year: str):
    """Import Zerodha tax PnL data."""
    import pandas as pd

    # Read Equity sheet
    df = pd.read_excel(file_path, sheet_name='Equity')

    intraday = Decimal('0')
    stcg = Decimal('0')
    ltcg = Decimal('0')

    for idx, row in df.iterrows():
        val = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
        amount = row.iloc[2] if len(row) > 2 and pd.notna(row.iloc[2]) else 0

        if "Intraday/Speculative profit" in val:
            intraday = Decimal(str(amount))
        elif "Short Term profit" in val:
            stcg = Decimal(str(amount))
        elif "Long Term profit" in val:
            ltcg = Decimal(str(amount))

    # Read Dividends
    try:
        div_df = pd.read_excel(file_path, sheet_name='Equity Dividends')
        dividends = Decimal('0')
        for idx, row in div_df.iterrows():
            val = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
            if "Total Dividend Amount" in val:
                dividends = Decimal(str(row.iloc[6] or 0))
                break
    except Exception:
        dividends = Decimal('0')

    # Store in stock_capital_gains
    # DELIVERY
    conn.execute("""
        INSERT OR REPLACE INTO stock_capital_gains
        (user_id, financial_year, trade_category, stcg_amount, ltcg_amount, ltcg_exemption)
        VALUES (?, ?, 'DELIVERY', ?, ?, 100000)
    """, (user_id, financial_year, float(stcg), float(ltcg)))

    # INTRADAY
    if intraday != 0:
        conn.execute("""
            INSERT OR REPLACE INTO stock_capital_gains
            (user_id, financial_year, trade_category, speculative_income)
            VALUES (?, ?, 'INTRADAY', ?)
        """, (user_id, financial_year, float(intraday)))

    # Dividends
    if dividends != 0:
        conn.execute("""
            INSERT OR REPLACE INTO stock_dividend_summary
            (user_id, financial_year, total_dividend, total_tds, net_dividend)
            VALUES (?, ?, ?, 0, ?)
        """, (user_id, financial_year, float(dividends), float(dividends)))

    conn.commit()


def _import_karvy_data(conn, user_id: int, file_path: Path, financial_year: str):
    """Import Karvy MF capital gains data."""
    import pandas as pd

    df = pd.read_excel(file_path)
    stcg = Decimal('0')

    for idx, row in df.iterrows():
        val = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
        if "Short Term Capital Gain/Loss" in val:
            total_col = row.iloc[7] if len(row) > 7 else 0
            stcg = Decimal(str(total_col or 0))

    if stcg != 0:
        conn.execute("""
            INSERT OR REPLACE INTO mf_capital_gains
            (user_id, financial_year, asset_class, stcg_amount, ltcg_amount, ltcg_exemption)
            VALUES (?, ?, 'EQUITY', ?, 0, 100000)
        """, (user_id, financial_year, float(stcg)))
        conn.commit()


def _import_usa_gl_data(conn, user_id: int, file_path: Path, financial_year: str):
    """Import USA stock G&L data."""
    import pandas as pd

    try:
        df = pd.read_excel(file_path, sheet_name='G&L_Collapsed')
    except Exception:
        return

    stcg_usd = Decimal('0')
    ltcg_usd = Decimal('0')

    for idx, row in df.iterrows():
        record_type = str(row.get('Record Type', ''))
        gain_loss = row.get('Adjusted Gain/Loss', 0) or 0
        tax_status = str(row.get('Capital Gains Status', ''))

        if record_type == 'Sell':
            if 'Short Term' in tax_status:
                stcg_usd += Decimal(str(gain_loss))
            elif 'Long Term' in tax_status:
                ltcg_usd += Decimal(str(gain_loss))

    # Convert to INR
    tt_rate = Decimal('83.5')
    stcg_inr = stcg_usd * tt_rate
    ltcg_inr = ltcg_usd * tt_rate

    if stcg_inr != 0:
        conn.execute("""
            INSERT INTO rsu_sales
            (user_id, sale_date, shares_sold, sell_price_usd, sell_value_usd,
             cost_basis_usd, gain_inr, is_ltcg)
            VALUES (?, date('now'), 1, 0, 0, 0, ?, FALSE)
        """, (user_id, float(stcg_inr)))

    if ltcg_inr != 0:
        conn.execute("""
            INSERT INTO rsu_sales
            (user_id, sale_date, shares_sold, sell_price_usd, sell_value_usd,
             cost_basis_usd, gain_inr, is_ltcg)
            VALUES (?, date('now'), 1, 0, 0, 0, ?, TRUE)
        """, (user_id, float(ltcg_inr)))

    conn.commit()


def _import_other_income(conn, user_id: int, file_path: Path, financial_year: str):
    """Import other income (rental, etc.)."""
    import pandas as pd

    df = pd.read_excel(file_path)
    rental = Decimal('0')
    municipal_tax = Decimal('0')

    for idx, row in df.iterrows():
        val = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
        amount = row.iloc[2] if len(row) > 2 and pd.notna(row.iloc[2]) else 0

        if "Apr'24 to Mar'25" in val:
            if pd.notna(amount) and isinstance(amount, (int, float)):
                rental = Decimal(str(amount))
        elif "Muncipal tax" in val:
            if pd.notna(amount) and isinstance(amount, (int, float)):
                municipal_tax = Decimal(str(amount))

    # Store rental income in user_income_summary directly
    if rental > 0:
        # Calculate house property income
        nav = rental - municipal_tax
        std_deduction = nav * Decimal('0.30')
        taxable = nav - std_deduction

        conn.execute("""
            INSERT OR REPLACE INTO user_income_summary
            (user_id, financial_year, income_type, sub_classification,
             income_sub_grouping, gross_amount, deductions, taxable_amount,
             applicable_tax_rate_type, source_table, last_synced_at)
            VALUES (?, ?, 'HOUSE_PROPERTY', 'RENTAL', 'Let-out Property',
                    ?, ?, ?, 'SLAB', 'manual_import', datetime('now'))
        """, (user_id, financial_year, float(rental),
              float(municipal_tax + std_deduction), float(taxable)))
        conn.commit()


def generate_reports(
    conn,
    user_name: str,
    financial_years: list[str],
    tax_regime: str,
    output_path: str
):
    """Generate advance tax reports."""
    from pfas.reports.advance_tax_report_v2 import AdvanceTaxReportGeneratorV2

    user_id = get_or_create_user(conn, user_name)
    generator = AdvanceTaxReportGeneratorV2(conn, Path(output_path))

    reports = []
    for fy in financial_years:
        # Import data from files if needed
        import_income_from_files(conn, user_id, user_name, fy)

        # Generate report
        report_path = generator.generate_report(user_id, user_name, fy, tax_regime)
        reports.append(report_path)
        print(f"Generated: {report_path}")

    return reports


def main():
    parser = argparse.ArgumentParser(
        description="Generate Advance Tax Reports (Data-Driven)"
    )
    parser.add_argument(
        "--user", "-u",
        help="User name (folder name under Data/Users/)"
    )
    parser.add_argument(
        "--fy", "-f",
        help="Financial year(s), comma-separated (e.g., 2024-25,2025-26)"
    )
    parser.add_argument(
        "--regime", "-r",
        choices=["OLD", "NEW"],
        default="NEW",
        help="Tax regime (default: NEW)"
    )
    parser.add_argument(
        "--output", "-o",
        default="Data/Reports",
        help="Output directory (default: Data/Reports)"
    )
    parser.add_argument(
        "--db", "-d",
        default="Data/pfas.db",
        help="Database path (default: Data/pfas.db)"
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize database with tax rules"
    )

    args = parser.parse_args()

    # Initialize or connect to database
    db_path = args.db
    password = "pfas_secure_2024"

    if args.init_db:
        init_database(db_path, password)
        if not args.user:
            return 0

    # Require user for report generation
    if not args.user:
        parser.error("--user is required for report generation")

    # Connect to existing database
    from pfas.core.database import DatabaseManager
    from pfas.core.tax_schema import init_tax_schema

    db = DatabaseManager()
    conn = db.init(db_path, password)

    # Ensure tax schema exists
    init_tax_schema(conn)

    # Parse financial years
    if args.fy:
        financial_years = [fy.strip() for fy in args.fy.split(",")]
    else:
        financial_years = ["2024-25", "2025-26"]

    # Generate reports
    reports = generate_reports(
        conn,
        args.user,
        financial_years,
        args.regime,
        args.output
    )

    print(f"\nGenerated {len(reports)} report(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
