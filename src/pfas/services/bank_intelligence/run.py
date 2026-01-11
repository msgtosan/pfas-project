#!/usr/bin/env python3
"""
Bank Intelligence CLI - Data-Driven Bank Statement Processing.

Usage:
    python -m src.pfas.services.bank_intelligence.run ingest [options]
    python -m src.pfas.services.bank_intelligence.run report [options]
    python -m src.pfas.services.bank_intelligence.run audit [options]

No code changes required for:
- Adding new statement files
- Adding new users/banks
- Modifying category rules (via JSON config)
"""

import argparse
import sys
from pathlib import Path

# Default paths
DEFAULT_DATA_ROOT = "Data/Users"
DEFAULT_DB_PATH = "Data/Reports/Bank_Intelligence/money_movement.db"
DEFAULT_REPORT_PATH = "Data/Reports/Bank_Intelligence/Master_Report.xlsx"


def cmd_ingest(args):
    """Ingest bank statements from Data/Users directory."""
    from .intelligent_analyzer import BankIntelligenceAnalyzer

    print("=" * 60)
    print("Bank Intelligence - Statement Ingestion")
    print("=" * 60)
    print(f"Data Root: {args.data_root}")
    print(f"Database: {args.db_path}")
    print()

    # Ensure output directory exists
    Path(args.db_path).parent.mkdir(parents=True, exist_ok=True)

    with BankIntelligenceAnalyzer(args.db_path, args.data_root) as analyzer:
        result = analyzer.scan_and_ingest_all()

    print("\n" + "=" * 60)
    print("INGESTION RESULTS")
    print("=" * 60)
    print(f"Status: {'SUCCESS' if result.success else 'FAILED'}")
    print(f"Transactions Processed: {result.transactions_processed:,}")
    print(f"Transactions Inserted: {result.transactions_inserted:,}")
    print(f"Transactions Skipped (duplicates): {result.transactions_skipped:,}")
    print(f"Source Files: {len(result.source_files)}")

    if result.source_files:
        print("\nFiles Processed:")
        for f in result.source_files:
            print(f"  - {Path(f).name}")

    if result.errors:
        print("\nErrors:")
        for err in result.errors:
            print(f"  [ERROR] {err}")

    if result.warnings:
        print("\nWarnings:")
        for warn in result.warnings:
            print(f"  [WARN] {warn}")

    return 0 if result.success else 1


def cmd_report(args):
    """Generate Excel Master Report."""
    from .report_generation import FiscalReportGenerator

    print("=" * 60)
    print("Bank Intelligence - Report Generation")
    print("=" * 60)
    print(f"Database: {args.db_path}")
    print(f"Output: {args.output}")

    if args.fiscal_year:
        print(f"Fiscal Year Filter: {args.fiscal_year}")
    if args.user:
        print(f"User Filter: {args.user}")
    print()

    # Ensure output directory exists
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)

    try:
        with FiscalReportGenerator(args.db_path) as generator:
            # Show available data
            fiscal_years = generator.get_fiscal_years()
            users = generator.get_users()

            print(f"Available Fiscal Years: {fiscal_years}")
            print(f"Available Users: {users}")
            print()

            # Generate report
            output = generator.generate_master_report(
                args.output,
                fiscal_year=args.fiscal_year,
                user_name=args.user
            )

            print(f"\n[SUCCESS] Report generated: {output}")
            print(f"File size: {Path(output).stat().st_size:,} bytes")

            # Show income summary
            print("\n" + "-" * 40)
            print("PFAS Income Summary:")
            print("-" * 40)
            for fy in fiscal_years:
                income = generator.get_income_for_pfas(fy, args.user)
                if income:
                    total = sum(income.values())
                    print(f"\n{fy}: Total = {total:,.2f}")
                    for cat, amt in sorted(income.items()):
                        print(f"  {cat}: {amt:,.2f}")

        return 0

    except FileNotFoundError as e:
        print(f"\n[ERROR] {e}")
        print("Run 'ingest' command first to create the database.")
        return 1


def cmd_audit(args):
    """Audit database integrity and show statistics."""
    from .db_audit import DatabaseAuditor

    print("=" * 60)
    print("Bank Intelligence - Database Audit")
    print("=" * 60)
    print(f"Database: {args.db_path}")
    print()

    try:
        with DatabaseAuditor(args.db_path) as auditor:
            if args.all or not any([args.stats, args.validate, args.ingestion_log, args.income]):
                # Show everything
                auditor.audit_recent_records(args.recent)
                auditor.print_statistics()
                auditor.print_validation_report()
                auditor.review_ingestion_log(5)
                auditor.print_income_summary(args.fy)
            else:
                if args.recent and not any([args.stats, args.validate, args.ingestion_log, args.income]):
                    auditor.audit_recent_records(args.recent)
                if args.stats:
                    auditor.print_statistics()
                if args.validate:
                    auditor.print_validation_report()
                if args.ingestion_log:
                    auditor.review_ingestion_log(args.recent)
                if args.income:
                    auditor.print_income_summary(args.fy)

        return 0

    except FileNotFoundError as e:
        print(f"\n[ERROR] {e}")
        print("Run 'ingest' command first to create the database.")
        return 1


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Bank Intelligence Suite - Data-Driven Bank Statement Processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest all bank statements
  python -m src.pfas.services.bank_intelligence.run ingest

  # Generate Excel report
  python -m src.pfas.services.bank_intelligence.run report

  # Audit database
  python -m src.pfas.services.bank_intelligence.run audit --stats

  # Filter report by fiscal year
  python -m src.pfas.services.bank_intelligence.run report --fiscal-year "FY 2024-25"

No code changes needed for:
  - Adding new statement files (just drop into folder)
  - Adding new users/banks (create folder + config.json)
  - Modifying categories (edit user_bank_config.json)
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Ingest command
    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Ingest bank statements into database"
    )
    ingest_parser.add_argument(
        "--data-root", default=DEFAULT_DATA_ROOT,
        help=f"Root directory for user data (default: {DEFAULT_DATA_ROOT})"
    )
    ingest_parser.add_argument(
        "--db-path", default=DEFAULT_DB_PATH,
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})"
    )

    # Report command
    report_parser = subparsers.add_parser(
        "report",
        help="Generate Excel Master Report"
    )
    report_parser.add_argument(
        "--db-path", default=DEFAULT_DB_PATH,
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})"
    )
    report_parser.add_argument(
        "--output", "-o", default=DEFAULT_REPORT_PATH,
        help=f"Output Excel file path (default: {DEFAULT_REPORT_PATH})"
    )
    report_parser.add_argument(
        "--fiscal-year", "--fy",
        help="Filter by fiscal year (e.g., 'FY 2024-25')"
    )
    report_parser.add_argument(
        "--user", "-u",
        help="Filter by user name"
    )

    # Audit command
    audit_parser = subparsers.add_parser(
        "audit",
        help="Audit database integrity and statistics"
    )
    audit_parser.add_argument(
        "--db-path", default=DEFAULT_DB_PATH,
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})"
    )
    audit_parser.add_argument(
        "--recent", type=int, default=10,
        help="Number of recent records to show (default: 10)"
    )
    audit_parser.add_argument(
        "--stats", action="store_true",
        help="Show database statistics"
    )
    audit_parser.add_argument(
        "--validate", action="store_true",
        help="Run data validation checks"
    )
    audit_parser.add_argument(
        "--ingestion-log", action="store_true",
        help="Show ingestion log"
    )
    audit_parser.add_argument(
        "--income", action="store_true",
        help="Show income summary for PFAS"
    )
    audit_parser.add_argument(
        "--fy",
        help="Fiscal year for income summary (e.g., 'FY 2024-25')"
    )
    audit_parser.add_argument(
        "--all", action="store_true",
        help="Show all audit information"
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 1

    if args.command == "ingest":
        return cmd_ingest(args)
    elif args.command == "report":
        return cmd_report(args)
    elif args.command == "audit":
        return cmd_audit(args)

    return 0


if __name__ == "__main__":
    sys.exit(main())
