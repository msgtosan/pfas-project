#!/usr/bin/env python3
"""
Capital Gains CLI - Calculate and reconcile MF capital gains.

Usage:
    python -m pfas.cli.capital_gains_cli calculate --user Sanjay --fy 2024-25
    python -m pfas.cli.capital_gains_cli reconcile --user Sanjay --fy 2024-25
    python -m pfas.cli.capital_gains_cli report --user Sanjay --fy 2024-25 -o report.txt
"""

import argparse
import sys
import json
from pathlib import Path
from datetime import date
from decimal import Decimal

# Add src to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pfas.core.database import DatabaseManager
from pfas.parsers.mf.cas_pdf_parser import CASPDFParser
from pfas.analyzers.capital_gains_reconciler import CapitalGainsReconciler

# Default paths
DEFAULT_DB_PASSWORD = "pfas_secure_2024"
DATA_ROOT = PROJECT_ROOT / "Data"


def load_user_passwords(user: str) -> dict:
    """Load user passwords from config."""
    passwords_file = DATA_ROOT / "Users" / user / "config" / "passwords.json"
    if passwords_file.exists():
        try:
            with open(passwords_file) as f:
                return json.load(f)
        except:
            pass
    return {}


def find_cas_files(user: str) -> list:
    """Find CAS PDF files for a user."""
    inbox = DATA_ROOT / "Users" / user / "inbox" / "Mutual-Fund"
    cas_files = []

    if inbox.exists():
        for pdf in inbox.glob("*.pdf"):
            name_lower = pdf.name.lower()
            if "cas" in name_lower or "consolidated" in name_lower:
                cas_files.append(pdf)

    return sorted(cas_files, key=lambda x: x.stat().st_mtime, reverse=True)


def get_pdf_password(pdf_path: Path, passwords: dict) -> str:
    """Get password for a PDF file."""
    filename = pdf_path.name

    # Check exact file match
    if "files" in passwords and filename in passwords["files"]:
        return passwords["files"][filename]

    # Check patterns
    if "patterns" in passwords:
        for pattern, pwd in passwords["patterns"].items():
            if pattern in filename:
                return pwd

    # Try common patterns
    return ""


def get_user_id(conn, user_name: str) -> int:
    """Get user ID from database."""
    cursor = conn.execute("SELECT id FROM users WHERE name = ?", (user_name,))
    row = cursor.fetchone()
    if row:
        return row[0]

    # Create user
    cursor = conn.execute(
        "INSERT INTO users (pan_encrypted, pan_salt, name) VALUES (?, ?, ?)",
        (b"encrypted", b"salt", user_name)
    )
    conn.commit()
    return cursor.lastrowid


def cmd_calculate(args):
    """Calculate capital gains from CAS files."""
    print(f"=" * 80)
    print(f"CAPITAL GAINS CALCULATION - {args.user}")
    print(f"=" * 80)

    # Load passwords
    passwords = load_user_passwords(args.user)
    db_password = passwords.get("database", {}).get("password", DEFAULT_DB_PASSWORD)

    # Database
    db_path = DATA_ROOT / "Users" / args.user / "db" / "finance.db"
    db_manager = DatabaseManager()
    conn = db_manager.init(str(db_path), db_password)

    user_id = get_user_id(conn, args.user)
    print(f"User: {args.user} (ID: {user_id})")

    # Find CAS files
    cas_files = find_cas_files(args.user)
    if not cas_files:
        print("No CAS files found!")
        return 1

    print(f"\nFound {len(cas_files)} CAS file(s)")

    # Use the most recent CAS file
    cas_file = cas_files[0]
    pdf_password = get_pdf_password(cas_file, passwords)

    print(f"\nProcessing: {cas_file.name}")

    # Parse CAS
    parser = CASPDFParser()
    try:
        cas_data = parser.parse(str(cas_file), pdf_password)
    except Exception as e:
        print(f"Error parsing CAS: {e}")
        return 1

    print(f"  Investor: {cas_data.investor_info.name}")
    print(f"  Period: {cas_data.statement_period}")
    print(f"  Folios: {len(cas_data.folios)}")
    print(f"  Transactions: {cas_data.total_transactions}")

    # Calculate capital gains
    print(f"\nCalculating capital gains...")
    reconciler = CapitalGainsReconciler(conn)

    if args.fy:
        fy_gains = reconciler.calculate_from_cas(cas_data, user_id, args.fy)
    else:
        fy_gains = reconciler.calculate_from_cas(cas_data, user_id)

    # Display results
    print(f"\n{'=' * 80}")
    print(f"CAPITAL GAINS SUMMARY")
    print(f"{'=' * 80}")

    for fy in sorted(fy_gains.keys()):
        gains = fy_gains[fy]
        total_ltcg = gains.fifo_equity_ltcg + gains.fifo_debt_ltcg + gains.fifo_hybrid_ltcg
        total_stcg = gains.fifo_equity_stcg + gains.fifo_debt_stcg + gains.fifo_hybrid_stcg

        if total_ltcg == 0 and total_stcg == 0:
            continue

        print(f"\nFY {fy}:")
        print(f"  Equity LTCG: Rs. {gains.fifo_equity_ltcg:>15,.2f}  STCG: Rs. {gains.fifo_equity_stcg:>15,.2f}")
        print(f"  Debt   LTCG: Rs. {gains.fifo_debt_ltcg:>15,.2f}  STCG: Rs. {gains.fifo_debt_stcg:>15,.2f}")
        print(f"  Hybrid LTCG: Rs. {gains.fifo_hybrid_ltcg:>15,.2f}  STCG: Rs. {gains.fifo_hybrid_stcg:>15,.2f}")
        print(f"  {'─' * 60}")
        print(f"  TOTAL  LTCG: Rs. {total_ltcg:>15,.2f}  STCG: Rs. {total_stcg:>15,.2f}")

        # Save to database
        reconciler.save_to_database(user_id, fy)

    print(f"\n{'=' * 80}")
    print("COMPLETED - Results saved to database")
    print(f"{'=' * 80}")

    return 0


def cmd_report(args):
    """Generate capital gains report."""
    print(f"Generating Capital Gains Report for {args.user} FY {args.fy}")

    # Load passwords
    passwords = load_user_passwords(args.user)
    db_password = passwords.get("database", {}).get("password", DEFAULT_DB_PASSWORD)

    # Database
    db_path = DATA_ROOT / "Users" / args.user / "db" / "finance.db"
    db_manager = DatabaseManager()
    conn = db_manager.init(str(db_path), db_password)

    user_id = get_user_id(conn, args.user)

    # Find and parse CAS
    cas_files = find_cas_files(args.user)
    if not cas_files:
        print("No CAS files found!")
        return 1

    cas_file = cas_files[0]
    pdf_password = get_pdf_password(cas_file, passwords)

    parser = CASPDFParser()
    cas_data = parser.parse(str(cas_file), pdf_password)

    # Calculate
    reconciler = CapitalGainsReconciler(conn)
    reconciler.calculate_from_cas(cas_data, user_id, args.fy)
    reconciler.reconcile(user_id, args.fy)

    # Generate report
    report = reconciler.generate_report(user_id, args.fy)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
        print(f"Report saved to: {args.output}")
    else:
        print(report)

    return 0


def cmd_compare(args):
    """Compare with casparser results."""
    print(f"Comparing PFAS vs casparser for {args.user} FY {args.fy}")

    # Load passwords
    passwords = load_user_passwords(args.user)
    db_password = passwords.get("database", {}).get("password", DEFAULT_DB_PASSWORD)

    # Database
    db_path = DATA_ROOT / "Users" / args.user / "db" / "finance.db"
    db_manager = DatabaseManager()
    conn = db_manager.init(str(db_path), db_password)

    user_id = get_user_id(conn, args.user)

    # Find and parse CAS
    cas_files = find_cas_files(args.user)
    if not cas_files:
        print("No CAS files found!")
        return 1

    cas_file = cas_files[0]
    pdf_password = get_pdf_password(cas_file, passwords)

    print(f"Processing: {cas_file.name}")

    parser = CASPDFParser()
    cas_data = parser.parse(str(cas_file), pdf_password)

    # PFAS calculation
    print("\nCalculating with PFAS FIFO engine...")
    reconciler = CapitalGainsReconciler(conn)
    pfas_gains = reconciler.calculate_from_cas(cas_data, user_id, args.fy)

    if args.fy not in pfas_gains:
        print(f"No gains found for FY {args.fy}")
        return 1

    pfas = pfas_gains[args.fy]

    # Try casparser if available
    casparser_ltcg = Decimal("0")
    casparser_stcg = Decimal("0")

    try:
        # Import casparser
        sys.path.insert(0, str(Path("/home/sshankar/CASTest/venv/lib/python3.12/site-packages")))
        from casparser import read_cas_pdf
        from casparser.analysis import CapitalGainsReport

        print("Calculating with casparser...")
        cp_data = read_cas_pdf(str(cas_file), pdf_password)
        cg_report = CapitalGainsReport(cp_data)

        # Get summary - casparser API
        summary = cg_report.get_summary()

        # Parse the summary for the target FY
        fy_key = f"FY{args.fy}"
        for line in str(summary).split('\n'):
            if f"FY{args.fy}" in line and "Total" in line:
                # Parse the totals
                pass

        print("casparser calculation complete")

    except Exception as e:
        print(f"casparser not available or error: {e}")

    # Display comparison
    print(f"\n{'=' * 80}")
    print(f"COMPARISON - FY {args.fy}")
    print(f"{'=' * 80}")

    total_pfas_ltcg = pfas.fifo_equity_ltcg + pfas.fifo_debt_ltcg + pfas.fifo_hybrid_ltcg
    total_pfas_stcg = pfas.fifo_equity_stcg + pfas.fifo_debt_stcg + pfas.fifo_hybrid_stcg

    print(f"\nPFAS FIFO Calculation:")
    print(f"  Equity: LTCG Rs. {pfas.fifo_equity_ltcg:,.2f}, STCG Rs. {pfas.fifo_equity_stcg:,.2f}")
    print(f"  Debt:   LTCG Rs. {pfas.fifo_debt_ltcg:,.2f}, STCG Rs. {pfas.fifo_debt_stcg:,.2f}")
    print(f"  Hybrid: LTCG Rs. {pfas.fifo_hybrid_ltcg:,.2f}, STCG Rs. {pfas.fifo_hybrid_stcg:,.2f}")
    print(f"  TOTAL:  LTCG Rs. {total_pfas_ltcg:,.2f}, STCG Rs. {total_pfas_stcg:,.2f}")

    if casparser_ltcg or casparser_stcg:
        print(f"\ncasparser Calculation:")
        print(f"  LTCG: Rs. {casparser_ltcg:,.2f}")
        print(f"  STCG: Rs. {casparser_stcg:,.2f}")

        print(f"\nDifference:")
        print(f"  LTCG: Rs. {total_pfas_ltcg - casparser_ltcg:,.2f}")
        print(f"  STCG: Rs. {total_pfas_stcg - casparser_stcg:,.2f}")

    # Tax computation
    print(f"\n{'=' * 80}")
    print("TAX COMPUTATION (Based on PFAS FIFO)")
    print(f"{'=' * 80}")

    # Equity LTCG
    equity_ltcg_exemption = min(pfas.fifo_equity_ltcg, Decimal("125000"))
    taxable_equity_ltcg = max(Decimal("0"), pfas.fifo_equity_ltcg - equity_ltcg_exemption)

    print(f"\nEquity:")
    print(f"  LTCG (Gross):     Rs. {pfas.fifo_equity_ltcg:>15,.2f}")
    print(f"  LTCG Exemption:   Rs. {equity_ltcg_exemption:>15,.2f}")
    print(f"  LTCG (Taxable):   Rs. {taxable_equity_ltcg:>15,.2f} @ 12.5%")
    print(f"  STCG:             Rs. {pfas.fifo_equity_stcg:>15,.2f} @ 20%")

    print(f"\nDebt (at slab rate post April 2023):")
    print(f"  Total Gains:      Rs. {pfas.fifo_debt_stcg:>15,.2f}")

    print(f"\nHybrid:")
    print(f"  LTCG:             Rs. {pfas.fifo_hybrid_ltcg:>15,.2f}")
    print(f"  STCG:             Rs. {pfas.fifo_hybrid_stcg:>15,.2f}")

    # Estimated tax
    equity_ltcg_tax = taxable_equity_ltcg * Decimal("0.125")
    equity_stcg_tax = pfas.fifo_equity_stcg * Decimal("0.20")

    print(f"\n{'=' * 80}")
    print("ESTIMATED TAX LIABILITY")
    print(f"{'=' * 80}")
    print(f"  Equity LTCG Tax (12.5%): Rs. {equity_ltcg_tax:>15,.2f}")
    print(f"  Equity STCG Tax (20%):   Rs. {equity_stcg_tax:>15,.2f}")
    print(f"  Debt gains: Taxed at your slab rate")
    print(f"  Hybrid: Check fund-specific rules")

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Capital Gains Calculator and Reconciler"
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Calculate command
    calc_parser = subparsers.add_parser("calculate", help="Calculate capital gains from CAS")
    calc_parser.add_argument("--user", "-u", required=True, help="User name")
    calc_parser.add_argument("--fy", help="Financial year (e.g., 2024-25)")
    calc_parser.set_defaults(func=cmd_calculate)

    # Report command
    report_parser = subparsers.add_parser("report", help="Generate capital gains report")
    report_parser.add_argument("--user", "-u", required=True, help="User name")
    report_parser.add_argument("--fy", required=True, help="Financial year")
    report_parser.add_argument("--output", "-o", help="Output file path")
    report_parser.set_defaults(func=cmd_report)

    # Compare command
    compare_parser = subparsers.add_parser("compare", help="Compare with casparser")
    compare_parser.add_argument("--user", "-u", required=True, help="User name")
    compare_parser.add_argument("--fy", required=True, help="Financial year")
    compare_parser.set_defaults(func=cmd_compare)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
