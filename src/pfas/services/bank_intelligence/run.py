#!/usr/bin/env python3
"""
Bank Intelligence CLI - Data-Driven Bank Statement Processing.

Usage:
    python -m src.pfas.services.bank_intelligence.run ingest --user Sanjay [options]
    python -m src.pfas.services.bank_intelligence.run report --user Sanjay [options]
    python -m src.pfas.services.bank_intelligence.run audit --user Sanjay [options]

No code changes required for:
- Adding new statement files
- Adding new users/banks
- Modifying category rules (via JSON config)
"""

import argparse
import sys
import os
from pathlib import Path
from typing import Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from pfas.core.paths import PathResolver


def _get_data_root() -> Path:
    """Get data root from environment or relative to working directory."""
    if 'PFAS_DATA_ROOT' in os.environ:
        return Path(os.environ['PFAS_DATA_ROOT'])
    # Try project-relative path
    for candidate in ['Data', '../Data', '../../Data']:
        if Path(candidate).exists():
            return Path(candidate)
    return Path('Data')


def _get_resolver(user_name: Optional[str]) -> Optional[PathResolver]:
    """Get PathResolver for a user, or None if no user specified."""
    if not user_name:
        return None
    data_root = _get_data_root()
    return PathResolver(data_root, user_name)


def _get_db_path(args) -> str:
    """Get database path from args or user's folder."""
    if hasattr(args, 'db_path') and args.db_path:
        return args.db_path

    resolver = _get_resolver(getattr(args, 'user', None))
    if resolver:
        # User-specific database
        db_dir = resolver.user_dir / "db"
        db_dir.mkdir(parents=True, exist_ok=True)
        return str(db_dir / "bank_intelligence.db")
    else:
        # Global fallback (for cross-user analysis)
        data_root = _get_data_root()
        db_path = data_root / "Reports" / "Bank_Intelligence" / "money_movement.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return str(db_path)


def _get_report_path(args, report_type: str = "bank_intelligence") -> str:
    """Get report output path from args or user's folder."""
    if hasattr(args, 'output') and args.output:
        return args.output

    resolver = _get_resolver(getattr(args, 'user', None))
    if resolver:
        # User-specific reports folder
        prefs = resolver.get_preferences()
        fmt = getattr(args, 'format', None) or prefs.reports.default_format
        return str(resolver.get_report_path(report_type, prefs.default_fy, fmt))
    else:
        # Global fallback
        data_root = _get_data_root()
        report_path = data_root / "Reports" / "Bank_Intelligence" / "Master_Report.xlsx"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        return str(report_path)


def cmd_ingest(args):
    """Ingest bank statements from user's directory."""
    from .intelligent_analyzer import BankIntelligenceAnalyzer

    # Get paths using PathResolver
    resolver = _get_resolver(args.user)
    db_path = _get_db_path(args)

    # Determine data root for scanning
    if resolver:
        # User-specific: scan only that user's data
        data_root = str(resolver.user_dir.parent)  # Data/Users
    else:
        # Global: scan all users
        data_root = str(_get_data_root() / "Users")

    print("=" * 60)
    print("Bank Intelligence - Statement Ingestion")
    print("=" * 60)
    if args.user:
        print(f"User: {args.user}")
    print(f"Data Root: {data_root}")
    print(f"Database: {db_path}")
    print()

    # Ensure output directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    with BankIntelligenceAnalyzer(db_path, data_root) as analyzer:
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

    # Get paths using PathResolver
    db_path = _get_db_path(args)
    output_path = _get_report_path(args)

    print("=" * 60)
    print("Bank Intelligence - Report Generation")
    print("=" * 60)
    if args.user:
        print(f"User: {args.user}")
    print(f"Database: {db_path}")
    print(f"Output: {output_path}")

    if args.fiscal_year:
        print(f"Fiscal Year Filter: {args.fiscal_year}")
    print()

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    try:
        with FiscalReportGenerator(db_path) as generator:
            # Show available data
            fiscal_years = generator.get_fiscal_years()
            users = generator.get_users()

            print(f"Available Fiscal Years: {fiscal_years}")
            print(f"Available Users: {users}")
            print()

            # Generate report
            output = generator.generate_master_report(
                output_path,
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

    # Get database path using PathResolver
    db_path = _get_db_path(args)

    print("=" * 60)
    print("Bank Intelligence - Database Audit")
    print("=" * 60)
    if args.user:
        print(f"User: {args.user}")
    print(f"Database: {db_path}")
    print()

    try:
        with DatabaseAuditor(db_path) as auditor:
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
  # Ingest bank statements for a user
  python -m src.pfas.services.bank_intelligence.run ingest --user Sanjay

  # Generate Excel report for a user
  python -m src.pfas.services.bank_intelligence.run report --user Sanjay

  # Audit user's database
  python -m src.pfas.services.bank_intelligence.run audit --user Sanjay --stats

  # Filter report by fiscal year
  python -m src.pfas.services.bank_intelligence.run report --user Sanjay --fy "FY 2024-25"

No code changes needed for:
  - Adding new statement files (just drop into user's Bank/ folder)
  - Adding new users (create folder under Data/Users/)
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
        "--user", "-u",
        help="User name (uses user-specific db and data paths)"
    )
    ingest_parser.add_argument(
        "--db-path",
        help="Override database path (default: user's db/bank_intelligence.db)"
    )

    # Report command
    report_parser = subparsers.add_parser(
        "report",
        help="Generate Excel Master Report"
    )
    report_parser.add_argument(
        "--user", "-u",
        help="User name (uses user-specific db and output paths)"
    )
    report_parser.add_argument(
        "--db-path",
        help="Override database path"
    )
    report_parser.add_argument(
        "--output", "-o",
        help="Override output file path (default: user's reports/bank_intelligence/)"
    )
    report_parser.add_argument(
        "--format", "-f",
        choices=["xlsx", "pdf", "json", "csv"],
        help="Output format (default: from user preferences)"
    )
    report_parser.add_argument(
        "--fiscal-year", "--fy",
        help="Filter by fiscal year (e.g., 'FY 2024-25')"
    )

    # Audit command
    audit_parser = subparsers.add_parser(
        "audit",
        help="Audit database integrity and statistics"
    )
    audit_parser.add_argument(
        "--user", "-u",
        help="User name (uses user-specific database)"
    )
    audit_parser.add_argument(
        "--db-path",
        help="Override database path"
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
