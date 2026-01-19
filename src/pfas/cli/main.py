#!/usr/bin/env python3
"""
PFAS CLI - Personal Financial Accounting System Command Line Interface.

Main entry point for PFAS operations including ingestion, audit, and reporting.

Usage:
    pfas ingest --user Sanjay --asset Mutual-Fund
    pfas scan --user Sanjay --asset Mutual-Fund
    pfas audit --user Sanjay --asset Mutual-Fund --file holdings.xlsx
    pfas report --user Sanjay --asset Mutual-Fund --type transactions
    pfas archive --user Sanjay --asset Mutual-Fund
"""

import argparse
import getpass
import logging
import sys
from pathlib import Path
from typing import Optional

# Handle imports for both installed package and direct execution
try:
    from pfas.core.database import DatabaseManager
    from pfas.core.paths import PathResolver
except ImportError:
    src_path = Path(__file__).parent.parent.parent.parent / "src"
    if src_path.exists() and str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    from pfas.core.database import DatabaseManager
    from pfas.core.paths import PathResolver


# Constants
DEFAULT_PASSWORD = "pfas_secure_2024"
SUPPORTED_ASSETS = [
    "Mutual-Fund",
    "Indian-Stocks",
    "USA-Stocks",
    "Bank",
    "EPF",
    "FD-Bonds",
    "NPS",
    "Other-Income",
    "PPF",
    "SGB",
    "Salary",
]


def get_data_root() -> Path:
    """Get PFAS data root directory."""
    # Try environment variable first
    import os
    if 'PFAS_DATA_ROOT' in os.environ:
        return Path(os.environ['PFAS_DATA_ROOT'])

    # Try project Data directory
    cli_path = Path(__file__).resolve()
    project_root = cli_path.parent.parent.parent.parent
    data_path = project_root / "Data"

    if data_path.exists():
        return data_path

    # Fallback to current directory
    return Path.cwd() / "Data"


def setup_logging(verbose: bool = False, debug: bool = False):
    """Configure logging."""
    if debug:
        level = logging.DEBUG
    elif verbose:
        level = logging.INFO
    else:
        level = logging.WARNING

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )


def get_or_create_user(conn, user_name: str) -> int:
    """Get user ID or create user if not exists."""
    cursor = conn.execute(
        "SELECT id FROM users WHERE name = ?",
        (user_name,)
    )
    row = cursor.fetchone()

    if row:
        return row[0] if isinstance(row, tuple) else row['id']

    # Create user with placeholder PAN
    import os
    cursor = conn.execute(
        """
        INSERT INTO users (name, pan_encrypted, pan_salt)
        VALUES (?, ?, ?)
        """,
        (user_name, os.urandom(32), os.urandom(16))
    )
    conn.commit()
    return cursor.lastrowid


def password_prompt(file_path: Path) -> Optional[str]:
    """Prompt for PDF password."""
    print(f"\nPassword required for: {file_path.name}")
    print("(Common: PAN number in uppercase, e.g., ABCDE1234F)")
    return getpass.getpass("Enter password: ")


# ============================================================================
# Command Handlers
# ============================================================================

def cmd_scan(args, resolver: PathResolver, conn):
    """Handle scan command - scan inbox for files."""
    from pfas.parsers.mf.scanner import MFStatementScanner

    print(f"\nScanning inbox for {args.asset} files...")

    inbox_path = resolver.inbox() / args.asset
    if not inbox_path.exists():
        print(f"Inbox path does not exist: {inbox_path}")
        return 1

    scanner = MFStatementScanner(inbox_path)
    result = scanner.scan()

    print(f"\nScan Results:")
    print(f"  Total files scanned: {result.total_scanned}")
    print(f"  Valid files found:   {len(result.files)}")

    if result.files:
        print(f"\nFiles found:")
        for f in result.files:
            protected = " [PASSWORD]" if f.password_protected else ""
            print(f"  [{f.rta.value}] {f.path.name}{protected}")

    if result.warnings:
        print(f"\nWarnings:")
        for w in result.warnings[:5]:
            print(f"  - {w}")

    return 0


def cmd_ingest(args, resolver: PathResolver, conn, user_id: int):
    """Handle ingest command - ingest statement files."""
    from pfas.services.archiver import FileArchiver
    from pfas.services.encrypted_file_handler import create_encrypted_file_handler

    print(f"\nIngesting {args.asset} statements...")

    inbox_path = resolver.inbox() / args.asset

    # Create encrypted file handler for password management
    encrypted_handler = None
    if not args.no_prompt:
        encrypted_handler = create_encrypted_file_handler(resolver, interactive=True)

    # Get appropriate ingester
    ingester = None
    result = None

    try:
        if args.asset == "Mutual-Fund":
            from pfas.parsers.mf.ingester import MFIngester
            ingester = MFIngester(
                conn, user_id, inbox_path,
                password_callback=password_prompt if not args.no_prompt else None,
                encrypted_file_handler=encrypted_handler
            )
        elif args.asset == "Bank":
            from pfas.parsers.bank.ingester import BankIngester
            ingester = BankIngester(conn, user_id, inbox_path)
        elif args.asset == "Indian-Stocks":
            from pfas.parsers.stock.ingester import IndianStockIngester
            ingester = IndianStockIngester(conn, user_id, inbox_path)
        elif args.asset == "USA-Stocks":
            from pfas.parsers.assets.ingester import USAStockIngester
            ingester = USAStockIngester(conn, user_id, inbox_path)
        elif args.asset == "Salary":
            from pfas.parsers.salary.ingester import SalaryIngester
            ingester = SalaryIngester(conn, user_id, inbox_path)
        elif args.asset == "EPF":
            from pfas.parsers.assets.ingester import EPFIngester
            ingester = EPFIngester(conn, user_id, inbox_path)
        elif args.asset == "NPS":
            from pfas.parsers.assets.ingester import NPSIngester
            ingester = NPSIngester(conn, user_id, inbox_path)
        elif args.asset == "PPF":
            from pfas.parsers.assets.ingester import PPFIngester
            ingester = PPFIngester(conn, user_id, inbox_path)
        elif args.asset == "SGB":
            from pfas.parsers.assets.ingester import SGBIngester
            ingester = SGBIngester(conn, user_id, inbox_path)
        elif args.asset == "FD-Bonds":
            from pfas.parsers.assets.ingester import FDBondsIngester
            ingester = FDBondsIngester(conn, user_id, inbox_path)
        else:
            print(f"Ingestion not yet implemented for {args.asset}")
            return 1

        # Run ingestion with failure-safe handling
        result = ingester.ingest(force=args.force, move_failed=True)

        # Print detailed summary
        result.print_summary()

        # Archive ONLY successfully processed files
        if args.archive and result.succeeded_files:
            print(f"Archiving successfully processed files...")
            archiver = FileArchiver(
                resolver.inbox(),
                resolver.archive(),
                args.user,
                conn
            )
            # CRITICAL: Only archive succeeded_files, NOT all processed_files!
            archive_result = archiver.archive_files(result.succeeded_files)

            if archive_result.files_archived > 0:
                print(f"  ✓ Archived: {archive_result.files_archived} files")
            if archive_result.files_failed > 0:
                print(f"  ✗ Failed to archive: {archive_result.files_failed} files")
                for error in archive_result.errors:
                    print(f"     {error}")
        elif args.archive and not result.succeeded_files:
            print(f"\n  No files to archive (all failed or skipped)")

        # Generate reports if requested (MF only for now)
        if args.report and args.asset == "Mutual-Fund":
            cmd_report(args, resolver, conn, user_id)

        return 0 if result.success else 1

    except Exception as e:
        print(f"\nIngestion failed: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        return 1


def cmd_audit(args, resolver: PathResolver, conn, user_id: int):
    """Handle audit command - reconcile with audit file."""
    if args.asset == "Mutual-Fund":
        from pfas.audit.reconciler import Reconciler
        from pfas.audit.mf_audit_parser import MFAuditParser

        if not args.file:
            print("Error: --file is required for audit command")
            return 1

        audit_file = Path(args.file)
        if not audit_file.exists():
            # Try relative to inbox
            audit_file = resolver.inbox() / args.asset / args.file
            if not audit_file.exists():
                print(f"Audit file not found: {args.file}")
                return 1

        print(f"\nReconciling with: {audit_file.name}")

        parser = MFAuditParser()
        audit_data = parser.parse(audit_file)

        if not audit_data.success:
            print(f"Failed to parse audit file:")
            for e in audit_data.errors:
                print(f"  - {e}")
            return 1

        print(f"  Holdings in file: {len(audit_data.holdings)}")
        print(f"  Total value: Rs. {audit_data.total_value:,.2f}")

        from decimal import Decimal
        threshold = Decimal(str(args.threshold)) if args.threshold else Decimal("100")
        reconciler = Reconciler(conn, user_id, threshold)
        result = reconciler.reconcile_holdings(audit_data)

        print(f"\nReconciliation Results:")
        print(f"  Records compared: {result.records_compared}")
        print(f"  Records matched:  {result.records_matched}")
        print(f"  Match rate:       {result.match_rate:.1f}%")
        print(f"  Mismatches:       {result.mismatch_count}")

        if result.mismatches:
            print(f"\nTop Mismatches:")
            for m in result.mismatches[:10]:
                print(f"  {m.scheme_name[:40]}")
                print(f"    {m.field_name}: DB={m.db_value}, File={m.file_value}")

        if result.missing_in_db:
            print(f"\nMissing in DB ({len(result.missing_in_db)}):")
            for m in result.missing_in_db[:5]:
                print(f"  - {m}")

        return 0

    else:
        print(f"Audit not yet implemented for {args.asset}")
        return 1


def cmd_report(args, resolver: PathResolver, conn, user_id: int):
    """Handle report command - generate reports."""
    # Load user preferences for format settings
    prefs = resolver.get_preferences()

    # Get format from args or user preferences
    fmt = getattr(args, 'format', None) or prefs.reports.default_format

    if args.asset == "Mutual-Fund":
        from pfas.reports.mf_ingestion_report import generate_mf_reports

        output_dir = resolver.reports() / args.asset
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"\nGenerating reports in: {output_dir}")
        print(f"Format: {fmt} (from {'CLI' if getattr(args, 'format', None) else 'user preferences'})")

        report_type = getattr(args, 'type', None)
        report_types = [report_type] if report_type else None
        reports = generate_mf_reports(
            conn, user_id, args.user, output_dir, report_types
        )

        print(f"\nReports generated:")
        for rt, path in reports.items():
            print(f"  {rt}: {path.name}")

        return 0

    else:
        print(f"Reports not yet implemented for {args.asset}")
        return 1


def cmd_archive(args, resolver: PathResolver, conn, user_id: int):
    """Handle archive command - archive processed files."""
    from pfas.services.archiver import FileArchiver

    print(f"\nArchiving {args.asset} files...")

    # Get list of completed ingestions that haven't been archived
    cursor = conn.execute(
        """
        SELECT source_file, file_hash FROM ingestion_log
        WHERE user_id = ? AND asset_type = ? AND status = 'COMPLETED'
        """,
        (user_id, args.asset)
    )

    files_to_archive = []
    for row in cursor.fetchall():
        source_path = Path(row['source_file'])
        if source_path.exists():
            files_to_archive.append(source_path)

    if not files_to_archive:
        print("No files to archive")
        return 0

    archiver = FileArchiver(
        resolver.inbox(),
        resolver.archive(),
        args.user,
        conn
    )

    result = archiver.archive_files(files_to_archive)

    print(f"\nArchive Results:")
    print(f"  Files archived: {result.files_archived}")
    print(f"  Files failed:   {result.files_failed}")

    if result.archived_paths:
        print(f"\nArchived files:")
        for p in result.archived_paths[:10]:
            print(f"  - {p.name}")

    return 0 if result.success else 1


def cmd_status(args, resolver: PathResolver, conn, user_id: int):
    """Handle status command - show current status."""
    print(f"\nPFAS Status for user: {args.user}")
    print(f"Data root: {resolver.root}")
    print(f"User dir:  {resolver.user_dir}")

    # Show user preferences
    prefs = resolver.get_preferences()
    print(f"\nUser Preferences:")
    print(f"  Default format: {prefs.reports.default_format}")
    print(f"  Currency:       {prefs.display.currency_symbol}")
    print(f"  Auto-archive:   {prefs.parsers.auto_archive}")
    print(f"  Default FY:     {prefs.default_fy}")

    # Count records
    tables = [
        ('mf_transactions', 'MF Transactions'),
        ('mf_holdings', 'MF Holdings'),
        ('mf_folios', 'MF Folios'),
        ('ingestion_log', 'Ingestion Log'),
        ('reconciliation_audit', 'Audit Mismatches'),
    ]

    print(f"\nDatabase Summary:")
    for table, label in tables:
        try:
            cursor = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE user_id = ?",
                (user_id,)
            )
            count = cursor.fetchone()[0]
            print(f"  {label}: {count}")
        except Exception:
            pass

    # Inbox status
    print(f"\nInbox Status:")
    for asset in SUPPORTED_ASSETS:
        inbox_path = resolver.inbox() / asset
        if inbox_path.exists():
            files = list(inbox_path.rglob("*"))
            files = [f for f in files if f.is_file()]
            if files:
                print(f"  {asset}: {len(files)} files")

    return 0


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        prog='pfas',
        description='PFAS - Personal Financial Accounting System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  pfas scan --user Sanjay --asset Mutual-Fund
  pfas ingest --user Sanjay --asset Mutual-Fund --audit --report
  pfas audit --user Sanjay --asset Mutual-Fund --file holdings.xlsx
  pfas report --user Sanjay --asset Mutual-Fund --type transactions
  pfas archive --user Sanjay --asset Mutual-Fund
  pfas status --user Sanjay
        """
    )

    # Global arguments
    parser.add_argument('--user', '-u', required=True, help='User name')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--debug', action='store_true', help='Debug output')
    parser.add_argument('--db-password', default=DEFAULT_PASSWORD, help='Database password')
    parser.add_argument('--data-root', help='Data root directory')

    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Command')

    # scan command
    scan_parser = subparsers.add_parser('scan', help='Scan inbox for statement files')
    scan_parser.add_argument('--asset', '-a', default='Mutual-Fund',
                            choices=SUPPORTED_ASSETS, help='Asset type')

    # ingest command
    ingest_parser = subparsers.add_parser('ingest', help='Ingest statement files')
    ingest_parser.add_argument('--asset', '-a', default='Mutual-Fund',
                              choices=SUPPORTED_ASSETS, help='Asset type')
    ingest_parser.add_argument('--force', '-f', action='store_true',
                              help='Reprocess already ingested files')
    ingest_parser.add_argument('--archive', action='store_true',
                              help='Archive processed files')
    ingest_parser.add_argument('--report', action='store_true',
                              help='Generate reports after ingestion')
    ingest_parser.add_argument('--no-prompt', action='store_true',
                              help='Skip password prompts')

    # audit command
    audit_parser = subparsers.add_parser('audit', help='Reconcile with audit file')
    audit_parser.add_argument('--asset', '-a', default='Mutual-Fund',
                             choices=SUPPORTED_ASSETS, help='Asset type')
    audit_parser.add_argument('--file', '-f', help='Audit file (Excel)')
    audit_parser.add_argument('--threshold', '-t', type=float, default=100,
                             help='Mismatch threshold in rupees')

    # report command
    report_parser = subparsers.add_parser('report', help='Generate reports')
    report_parser.add_argument('--asset', '-a', default='Mutual-Fund',
                              choices=SUPPORTED_ASSETS, help='Asset type')
    report_parser.add_argument('--type', '-t',
                              choices=['transactions', 'holdings', 'audit', 'ingestion'],
                              help='Report type (default: all)')
    report_parser.add_argument('--format', '-f',
                              choices=['xlsx', 'pdf', 'json', 'csv', 'html'],
                              help='Output format (default: from user preferences)')
    report_parser.add_argument('--fy', help='Financial year (e.g., 2024-25)')

    # archive command
    archive_parser = subparsers.add_parser('archive', help='Archive processed files')
    archive_parser.add_argument('--asset', '-a', default='Mutual-Fund',
                               choices=SUPPORTED_ASSETS, help='Asset type')

    # status command
    status_parser = subparsers.add_parser('status', help='Show current status')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Setup
    setup_logging(args.verbose, args.debug)

    # Get data root
    data_root = Path(args.data_root) if args.data_root else get_data_root()

    # Initialize path resolver
    resolver = PathResolver(data_root, args.user)

    print(f"PFAS - {args.command.upper()}")
    print(f"User: {args.user}")
    print(f"Data: {resolver.user_dir}")

    # Initialize database
    try:
        db = DatabaseManager()
        conn = db.init(str(resolver.db_path()), args.db_password)
    except Exception as e:
        print(f"Database error: {e}")
        return 1

    # Get or create user
    user_id = get_or_create_user(conn, args.user)

    # Route to command handler
    try:
        if args.command == 'scan':
            return cmd_scan(args, resolver, conn)
        elif args.command == 'ingest':
            return cmd_ingest(args, resolver, conn, user_id)
        elif args.command == 'audit':
            return cmd_audit(args, resolver, conn, user_id)
        elif args.command == 'report':
            return cmd_report(args, resolver, conn, user_id)
        elif args.command == 'archive':
            return cmd_archive(args, resolver, conn, user_id)
        elif args.command == 'status':
            return cmd_status(args, resolver, conn, user_id)
        else:
            parser.print_help()
            return 1

    except KeyboardInterrupt:
        print("\nOperation cancelled")
        return 130
    except Exception as e:
        print(f"\nError: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
