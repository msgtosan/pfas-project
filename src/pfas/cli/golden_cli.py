"""
Golden Reference CLI - PFAS

Command-line interface for managing golden reference data and reconciliation.

Usage:
    python -m pfas.cli.golden_cli --ingest --user Sanjay --file nsdl_cas.pdf
    python -m pfas.cli.golden_cli --reconcile --user Sanjay --asset MF
    python -m pfas.cli.golden_cli --status --user Sanjay
    python -m pfas.cli.golden_cli --suspense --user Sanjay

Examples:
    # Ingest NSDL CAS with password from config
    python -m pfas.cli.golden_cli -i -u Sanjay -f golden/nsdl/NSDLe-CAS_100980467_DEC_2025.PDF

    # Ingest with explicit password
    python -m pfas.cli.golden_cli -i -u Sanjay -f cas.pdf --password SECRET

    # Reconcile all MF holdings
    python -m pfas.cli.golden_cli -r -u Sanjay -a MUTUAL_FUND

    # View reconciliation status
    python -m pfas.cli.golden_cli -s -u Sanjay

    # View open suspense items
    python -m pfas.cli.golden_cli --suspense -u Sanjay
"""

import argparse
import json
import logging
import sys
from datetime import date
from getpass import getpass
from pathlib import Path
from typing import Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pfas.core.paths import PathResolver
from pfas.services.golden_reference import (
    NSDLCASParser,
    GoldenReferenceIngester,
    CrossCorrelator,
    TruthResolver,
    AssetClass,
    MetricType,
    ReconciliationConfig,
    UserConfigLoader,
    UserReconciliationSettings,
    ReconciliationMode,
    ReconciliationReporter,
)

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False, debug: bool = False):
    """Configure logging based on verbosity."""
    level = logging.DEBUG if debug else (logging.INFO if verbose else logging.WARNING)
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


def get_db_connection(resolver: PathResolver, password: Optional[str] = None):
    """Get database connection with optional encryption."""
    db_path = resolver.db_path()

    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    try:
        import sqlcipher3
        conn = sqlcipher3.connect(str(db_path))

        if password:
            conn.execute(f"PRAGMA key='{password}'")
            conn.execute("PRAGMA cipher_compatibility=4")
        else:
            pwd_file = resolver.password_config_file()
            if pwd_file.exists():
                with open(pwd_file) as f:
                    pwd_data = json.load(f)
                    db_password = pwd_data.get("database", {}).get("password")
                    if db_password:
                        conn.execute(f"PRAGMA key='{db_password}'")
                        conn.execute("PRAGMA cipher_compatibility=4")

        conn.execute("SELECT 1").fetchone()
        return conn

    except ImportError:
        import sqlite3
        return sqlite3.connect(str(db_path))


def get_user_id(conn, user_name: str) -> int:
    """Get user ID from database."""
    cursor = conn.execute("SELECT id FROM users WHERE name = ?", [user_name])
    row = cursor.fetchone()
    if row:
        return row[0]
    raise ValueError(f"User not found: {user_name}")


def get_golden_password(resolver: PathResolver, file_path: Path) -> Optional[str]:
    """Get password for golden reference file."""
    pwd_file = resolver.password_config_file()

    if pwd_file.exists():
        try:
            with open(pwd_file, encoding='utf-8') as f:
                data = json.load(f)

            # Check golden-specific passwords
            golden_passwords = data.get("golden", {})

            # Exact filename match
            if file_path.name in golden_passwords:
                return golden_passwords[file_path.name]

            # Pattern match for NSDL
            if "nsdl" in file_path.name.lower():
                if "nsdl" in golden_passwords:
                    return golden_passwords["nsdl"]
                if "NSDL" in golden_passwords:
                    return golden_passwords["NSDL"]

            # Check patterns
            patterns = data.get("patterns", {})
            for pattern, pwd in patterns.items():
                if pattern in file_path.name:
                    return pwd

        except Exception as e:
            logger.warning(f"Failed to read password config: {e}")

    return None


def cmd_ingest(args, resolver: PathResolver, conn, user_id: int):
    """Handle ingest command."""
    file_path = Path(args.file)

    # Resolve relative paths against user's golden directory
    if not file_path.is_absolute():
        golden_dir = resolver.user_dir / "golden"
        file_path = golden_dir / file_path

    if not file_path.exists():
        print(f"Error: File not found: {file_path}")
        return 1

    # Get password
    password = args.password
    if not password:
        password = get_golden_password(resolver, file_path)

    if not password and not args.no_password:
        # Prompt for password
        password = getpass(f"Enter password for {file_path.name}: ")

    print(f"\nIngesting golden reference: {file_path.name}")
    print(f"  User: {args.user}")

    try:
        # Parse the file
        parser = NSDLCASParser()
        cas_data = parser.parse(file_path, password=password)

        print(f"  Statement date: {cas_data.statement_date}")
        print(f"  Period: {cas_data.period_start} to {cas_data.period_end}")
        print(f"  Investor: {cas_data.investor_info.name}")
        print(f"  Holdings found:")
        print(f"    Equity: {len(cas_data.equity_holdings)}")
        print(f"    Mutual Funds: {len(cas_data.mf_holdings)}")
        print(f"    NPS: {len(cas_data.nps_holdings)}")
        print(f"    Bonds: {len(cas_data.bond_holdings)}")
        print(f"    SGB: {len(cas_data.sgb_holdings)}")
        print(f"  Total value: ₹{cas_data.total_value:,.2f}")

        if args.dry_run:
            print("\n  [DRY RUN - Not saving to database]")
            return 0

        # Ingest into database
        file_hash = parser.calculate_file_hash(file_path)
        ingester = GoldenReferenceIngester(conn, user_id)
        ref_id = ingester.ingest_nsdl_cas(cas_data, file_path=file_path, file_hash=file_hash)

        print(f"\n  Golden reference created: ID {ref_id}")

        # Auto-reconcile if requested
        if args.auto_reconcile:
            print("\n  Running reconciliation...")
            correlator = CrossCorrelator(conn, user_id)

            for asset_class in [AssetClass.MUTUAL_FUND, AssetClass.STOCKS, AssetClass.NPS]:
                holdings = [h for h in cas_data.all_holdings
                           if asset_class.value == _map_nsdl_type(h.asset_type)]
                if holdings:
                    summary = correlator.reconcile_holdings(asset_class, ref_id)
                    print(f"    {asset_class.value}: {summary.match_rate:.1f}% match rate")

        return 0

    except Exception as e:
        print(f"\nError: {e}")
        logger.exception("Ingestion failed")
        return 1


def cmd_reconcile(args, resolver: PathResolver, conn, user_id: int):
    """Handle reconcile command."""
    print(f"\nReconciling for user: {args.user}")

    # Load user reconciliation settings
    config_loader = UserConfigLoader(resolver.user_config_dir())
    user_settings = config_loader.load_reconciliation_settings()

    print(f"  Mode: {user_settings.mode.value}")
    print(f"  Tolerance: {user_settings.absolute_tolerance} absolute, {user_settings.percentage_tolerance}% relative")

    # Get latest golden reference
    cursor = conn.execute("""
        SELECT id, source_type, statement_date
        FROM golden_reference
        WHERE user_id = ? AND status = 'ACTIVE'
        ORDER BY statement_date DESC
        LIMIT 1
    """, (user_id,))
    row = cursor.fetchone()

    if not row:
        print("Error: No active golden reference found. Run --ingest first.")
        return 1

    ref_id, source_type, stmt_date = row
    print(f"  Using golden reference: ID {ref_id} ({source_type}, {stmt_date})")

    # Create reconciliation config from user settings
    recon_config = ReconciliationConfig(
        absolute_tolerance=user_settings.absolute_tolerance,
        percentage_tolerance=user_settings.percentage_tolerance / 100,  # Convert to decimal
        warning_threshold=user_settings.warning_threshold,
        error_threshold=user_settings.error_threshold,
        critical_threshold=user_settings.critical_threshold,
        create_suspense_on_mismatch=user_settings.create_suspense_on_mismatch,
        auto_resolve_within_tolerance=user_settings.auto_resolve_within_tolerance,
    )

    # Parse asset class
    asset_class = AssetClass(args.asset) if args.asset else None

    correlator = CrossCorrelator(conn, user_id, config=recon_config)

    if asset_class:
        # Reconcile single asset class
        summary = correlator.reconcile_holdings(asset_class, ref_id)
        _print_reconciliation_summary(summary)
    else:
        # Reconcile enabled asset classes from user config
        enabled = user_settings.enabled_asset_classes
        for ac_name in enabled:
            try:
                ac = AssetClass(ac_name)
                summary = correlator.reconcile_holdings(ac, ref_id)
                _print_reconciliation_summary(summary)
            except ValueError:
                print(f"\n  Unknown asset class: {ac_name}")
            except Exception as e:
                print(f"\n  {ac_name}: Error - {e}")

    return 0


def cmd_status(args, resolver: PathResolver, conn, user_id: int):
    """Handle status command."""
    print(f"\nGolden Reference Status for: {args.user}")
    print("=" * 60)

    # Get golden references
    cursor = conn.execute("""
        SELECT id, source_type, statement_date, status,
               (SELECT COUNT(*) FROM golden_holdings WHERE golden_ref_id = gr.id) as holdings
        FROM golden_reference gr
        WHERE user_id = ?
        ORDER BY statement_date DESC
        LIMIT 10
    """, (user_id,))

    print("\nGolden References:")
    print("-" * 60)
    for row in cursor.fetchall():
        status_icon = "✓" if row[3] == "ACTIVE" else "○"
        print(f"  {status_icon} ID {row[0]}: {row[1]} ({row[2]}) - {row[4]} holdings")

    # Get reconciliation summary
    cursor = conn.execute("""
        SELECT
            asset_class,
            COUNT(*) as events,
            SUM(CASE WHEN match_result IN ('EXACT', 'WITHIN_TOLERANCE') THEN 1 ELSE 0 END) as matched,
            SUM(CASE WHEN match_result = 'MISMATCH' THEN 1 ELSE 0 END) as mismatched,
            MAX(reconciliation_date) as last_recon
        FROM reconciliation_events
        WHERE user_id = ?
        GROUP BY asset_class
    """, (user_id,))

    print("\nReconciliation Summary:")
    print("-" * 60)
    for row in cursor.fetchall():
        match_rate = (row[2] / row[1] * 100) if row[1] > 0 else 0
        print(f"  {row[0]}: {row[2]}/{row[1]} matched ({match_rate:.1f}%), "
              f"last: {row[4]}")

    # Get open suspense count
    cursor = conn.execute("""
        SELECT COUNT(*) FROM reconciliation_suspense
        WHERE user_id = ? AND status IN ('OPEN', 'IN_PROGRESS')
    """, (user_id,))
    suspense_count = cursor.fetchone()[0]

    if suspense_count > 0:
        print(f"\n⚠️  Open suspense items: {suspense_count}")

    return 0


def cmd_suspense(args, resolver: PathResolver, conn, user_id: int):
    """Handle suspense command."""
    print(f"\nOpen Suspense Items for: {args.user}")
    print("=" * 60)

    correlator = CrossCorrelator(conn, user_id)
    items = correlator.get_open_suspense()

    if not items:
        print("No open suspense items.")
        return 0

    for item in items:
        print(f"\n  [{item.priority}] {item.asset_type.value}")
        print(f"    ISIN/Folio: {item.isin or item.folio_number or 'N/A'}")
        print(f"    Name: {item.name or 'Unknown'}")
        print(f"    Suspense Value: ₹{item.suspense_value:,.2f}" if item.suspense_value else "    Suspense Value: N/A")
        print(f"    Reason: {item.suspense_reason}")
        print(f"    Opened: {item.opened_date}")

    print(f"\nTotal: {len(items)} open items")
    return 0


def cmd_export(args, resolver: PathResolver, conn, user_id: int):
    """Handle export command."""
    print(f"\nExporting Reconciliation Report for: {args.user}")

    # Determine output directory
    output_dir = args.output
    if not output_dir:
        output_dir = resolver.user_dir / "reports" / "reconciliation"

    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"  Output directory: {output_dir}")

    # Get asset class
    asset_class = AssetClass(args.asset) if args.asset else None

    # Get latest reconciliation data
    query = """
        SELECT DISTINCT asset_class, golden_ref_id, reconciliation_date
        FROM reconciliation_events
        WHERE user_id = ?
    """
    params = [user_id]

    if asset_class:
        query += " AND asset_class = ?"
        params.append(asset_class.value)

    query += " ORDER BY reconciliation_date DESC LIMIT 10"

    cursor = conn.execute(query, params)
    rows = cursor.fetchall()

    if not rows:
        print("No reconciliation data found. Run --reconcile first.")
        return 1

    reporter = ReconciliationReporter(output_dir=output_dir, user_name=args.user)

    for asset_val, ref_id, recon_date in rows:
        print(f"\n  Exporting {asset_val} ({recon_date})...")

        # Load events
        cursor = conn.execute("""
            SELECT * FROM reconciliation_events
            WHERE user_id = ? AND asset_class = ? AND reconciliation_date = ?
        """, (user_id, asset_val, recon_date))

        events = []
        cols = [desc[0] for desc in cursor.description]
        for row in cursor.fetchall():
            row_dict = dict(zip(cols, row))
            from pfas.services.golden_reference.models import (
                ReconciliationEvent, MatchResult, ReconciliationStatus, Severity, SourceType
            )
            from decimal import Decimal

            event = ReconciliationEvent(
                id=row_dict.get("id"),
                user_id=row_dict.get("user_id"),
                reconciliation_date=date.fromisoformat(row_dict["reconciliation_date"]) if row_dict.get("reconciliation_date") else date.today(),
                metric_type=MetricType(row_dict["metric_type"]) if row_dict.get("metric_type") else MetricType.NET_WORTH,
                asset_class=AssetClass(row_dict["asset_class"]) if row_dict.get("asset_class") else AssetClass.MUTUAL_FUND,
                source_type=SourceType(row_dict["source_type"]) if row_dict.get("source_type") else SourceType.NSDL_CAS,
                golden_ref_id=row_dict.get("golden_ref_id"),
                isin=row_dict.get("isin"),
                folio_number=row_dict.get("folio_number"),
                symbol=row_dict.get("symbol"),
                system_value=Decimal(str(row_dict["system_value"])) if row_dict.get("system_value") else None,
                golden_value=Decimal(str(row_dict["golden_value"])) if row_dict.get("golden_value") else None,
                difference=Decimal(str(row_dict["difference"])) if row_dict.get("difference") else None,
                difference_pct=Decimal(str(row_dict["difference_pct"])) if row_dict.get("difference_pct") else None,
                tolerance_used=Decimal(str(row_dict["tolerance_used"])) if row_dict.get("tolerance_used") else Decimal("0.01"),
                status=ReconciliationStatus(row_dict["status"]) if row_dict.get("status") else ReconciliationStatus.PENDING,
                match_result=MatchResult(row_dict["match_result"]) if row_dict.get("match_result") else MatchResult.NOT_APPLICABLE,
                severity=Severity(row_dict["severity"]) if row_dict.get("severity") else Severity.INFO,
            )
            events.append(event)

        if not events:
            continue

        # Build summary
        from pfas.services.golden_reference.models import ReconciliationSummary, SourceType

        summary = ReconciliationSummary(
            user_id=user_id,
            reconciliation_date=date.fromisoformat(recon_date) if isinstance(recon_date, str) else recon_date,
            asset_class=AssetClass(asset_val),
            source_type=events[0].source_type if events else SourceType.NSDL_CAS,
            golden_ref_id=ref_id,
            total_items=len(events),
            matched_exact=sum(1 for e in events if e.match_result == MatchResult.EXACT),
            matched_tolerance=sum(1 for e in events if e.match_result == MatchResult.WITHIN_TOLERANCE),
            mismatches=sum(1 for e in events if e.match_result == MatchResult.MISMATCH),
            missing_system=sum(1 for e in events if e.match_result == MatchResult.MISSING_SYSTEM),
            missing_golden=sum(1 for e in events if e.match_result == MatchResult.MISSING_GOLDEN),
            total_system_value=sum(e.system_value or Decimal("0") for e in events),
            total_golden_value=sum(e.golden_value or Decimal("0") for e in events),
            total_difference=sum(abs(e.difference or Decimal("0")) for e in events),
            events=events,
        )

        # Generate report
        if args.format == "excel":
            path = reporter.generate_excel(summary, events=events)
            if path:
                print(f"    Excel: {path}")
            else:
                print("    Excel generation failed (xlsxwriter not available)")
        elif args.format == "csv":
            path = reporter.generate_csv(events)
            print(f"    CSV: {path}")
        else:
            reporter.print_summary(summary)

        # Only export first asset class if specific one was requested
        if asset_class:
            break

    print(f"\nExport complete.")
    return 0


def cmd_config(args, resolver: PathResolver, conn, user_id: int):
    """Handle config command."""
    print(f"\nReconciliation Configuration for: {args.user}")
    print("=" * 60)

    config_loader = UserConfigLoader(resolver.user_config_dir())
    settings = config_loader.load_reconciliation_settings()

    print(f"\nExecution Mode:")
    print(f"  Mode: {settings.mode.value}")
    print(f"  Frequency: {settings.frequency.value} (if scheduled)")
    print(f"  Auto-reconcile on ingest: {settings.auto_reconcile_on_ingest}")

    print(f"\nTolerance Settings:")
    print(f"  Absolute tolerance: {settings.absolute_tolerance}")
    print(f"  Percentage tolerance: {settings.percentage_tolerance}%")

    print(f"\nSeverity Thresholds:")
    print(f"  Warning: ₹{settings.warning_threshold:,.2f}")
    print(f"  Error: ₹{settings.error_threshold:,.2f}")
    print(f"  Critical: ₹{settings.critical_threshold:,.2f}")

    print(f"\nNotifications:")
    print(f"  On mismatch: {settings.notify_on_mismatch}")
    print(f"  On critical: {settings.notify_on_critical}")
    print(f"  Email: {settings.email_notifications}")

    print(f"\nEnabled Asset Classes:")
    for ac in settings.enabled_asset_classes:
        print(f"  - {ac}")

    print(f"\nDefault Sources:")
    for asset, source in settings.default_sources.items():
        print(f"  {asset}: {source}")

    print(f"\nSuspense Behavior:")
    print(f"  Create on mismatch: {settings.create_suspense_on_mismatch}")
    print(f"  Auto-resolve within tolerance: {settings.auto_resolve_within_tolerance}")

    print(f"\nConfig file: {resolver.user_config_dir() / 'reconciliation.json'}")
    print("Edit this file to change settings.")

    return 0


def _print_reconciliation_summary(summary):
    """Print reconciliation summary."""
    print(f"\n  {summary.asset_class.value} Reconciliation:")
    print(f"    Total items: {summary.total_items}")
    print(f"    Exact matches: {summary.matched_exact}")
    print(f"    Within tolerance: {summary.matched_tolerance}")
    print(f"    Mismatches: {summary.mismatches}")
    print(f"    Missing in system: {summary.missing_system}")
    print(f"    Missing in golden: {summary.missing_golden}")
    print(f"    Match rate: {summary.match_rate:.1f}%")
    print(f"    System total: ₹{summary.total_system_value:,.2f}")
    print(f"    Golden total: ₹{summary.total_golden_value:,.2f}")
    print(f"    Difference: ₹{summary.total_difference:,.2f}")


def _map_nsdl_type(nsdl_type: str) -> str:
    """Map NSDL asset type to AssetClass value."""
    mapping = {
        "EQUITY": "STOCKS",
        "MF": "MUTUAL_FUND",
        "NPS": "NPS",
        "BOND": "BONDS",
        "SGB": "SGB",
    }
    return mapping.get(nsdl_type, "STOCKS")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="golden_cli",
        description="Manage golden reference data and reconciliation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -i -u Sanjay -f nsdl_cas.pdf        # Ingest NSDL CAS
  %(prog)s -r -u Sanjay -a MUTUAL_FUND         # Reconcile MF holdings
  %(prog)s -s -u Sanjay                        # View status
  %(prog)s --suspense -u Sanjay                # View suspense items
        """
    )

    # User (required)
    parser.add_argument(
        "-u", "--user",
        required=True,
        help="User name"
    )

    # Commands
    cmd_group = parser.add_mutually_exclusive_group(required=True)
    cmd_group.add_argument(
        "-i", "--ingest",
        action="store_true",
        help="Ingest golden reference file"
    )
    cmd_group.add_argument(
        "-r", "--reconcile",
        action="store_true",
        help="Run reconciliation"
    )
    cmd_group.add_argument(
        "-s", "--status",
        action="store_true",
        help="Show status"
    )
    cmd_group.add_argument(
        "--suspense",
        action="store_true",
        help="Show open suspense items"
    )
    cmd_group.add_argument(
        "-c", "--config",
        action="store_true",
        help="Show/configure reconciliation settings"
    )
    cmd_group.add_argument(
        "-e", "--export",
        action="store_true",
        help="Export reconciliation report"
    )

    # Ingest options
    parser.add_argument(
        "-f", "--file",
        type=str,
        help="File to ingest (relative to golden/ or absolute)"
    )
    parser.add_argument(
        "-p", "--password",
        help="Password for encrypted file"
    )
    parser.add_argument(
        "--no-password",
        action="store_true",
        help="File is not password protected"
    )
    parser.add_argument(
        "--auto-reconcile",
        action="store_true",
        help="Auto-reconcile after ingestion"
    )

    # Reconcile options
    parser.add_argument(
        "-a", "--asset",
        choices=["MUTUAL_FUND", "STOCKS", "NPS", "EPF", "PPF", "SGB"],
        help="Asset class to reconcile (default: all)"
    )

    # Export options
    parser.add_argument(
        "--format",
        choices=["excel", "csv", "text"],
        default="excel",
        help="Export format (default: excel)"
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output directory for exports (default: user reports dir)"
    )

    # Database options
    parser.add_argument(
        "--data-root",
        type=Path,
        default=PROJECT_ROOT / "Data",
        help="Root data directory"
    )
    parser.add_argument(
        "--db-password",
        help="Database encryption password"
    )

    # Output options
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Debug output"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse file without saving"
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    setup_logging(args.verbose, args.debug)

    try:
        # Initialize PathResolver
        resolver = PathResolver(args.data_root, args.user)
        logger.info(f"User directory: {resolver.user_dir}")

        if not resolver.user_dir.exists():
            print(f"Error: User directory not found: {resolver.user_dir}")
            return 1

        # Connect to database
        conn = get_db_connection(resolver, args.db_password)
        logger.info("Connected to database")

        # Get user ID
        user_id = get_user_id(conn, args.user)
        logger.info(f"User: {args.user} (ID: {user_id})")

        # Dispatch to command handler
        if args.ingest:
            if not args.file:
                print("Error: --file required for --ingest")
                return 1
            return cmd_ingest(args, resolver, conn, user_id)
        elif args.reconcile:
            return cmd_reconcile(args, resolver, conn, user_id)
        elif args.status:
            return cmd_status(args, resolver, conn, user_id)
        elif args.suspense:
            return cmd_suspense(args, resolver, conn, user_id)
        elif args.config:
            return cmd_config(args, resolver, conn, user_id)
        elif args.export:
            return cmd_export(args, resolver, conn, user_id)

    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1
    except ValueError as e:
        print(f"Error: {e}")
        return 1
    except Exception as e:
        logger.exception("Unexpected error")
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
