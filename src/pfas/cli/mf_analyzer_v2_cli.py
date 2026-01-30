#!/usr/bin/env python3
"""
Enhanced MF Analyzer CLI - Command-line interface for MF analysis.

Usage:
    mf-analyzer --user Sanjay
    mf-analyzer --user Sanjay --config config/mf_analyzer_config_v2.json
    mf-analyzer --user Sanjay --fy 2024-25 --reconcile
    mf-analyzer --user Sanjay --report-only --output-dir ./reports
    mf-analyzer --user Sanjay --snapshot FY_END
"""

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Optional

# Add src to path for development
src_path = Path(__file__).parent.parent.parent
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from pfas.core.database import DatabaseManager
from pfas.analyzers.mf_analyzer import MFAnalyzer

# Default password - used only if not specified in config or CLI
DEFAULT_DB_PASSWORD = "pfas_secure_2024"

# Default config paths relative to project root
DEFAULT_GLOBAL_CONFIG = "config/mf_analyzer_config_v2.json"
# Note: User config paths are now resolved via PathResolver (no hardcoding)
USER_PASSWORDS_FILE = "passwords.json"
USER_PREFERENCES_FILE = "preferences.json"
from pfas.analyzers.mf_reconciler import MFReconciler
from pfas.analyzers.mf_fy_analyzer import MFFYAnalyzer
from pfas.reports.mf_enhanced_report import MFEnhancedReportGenerator
from pfas.parsers.mf.ingester import MFIngester
from pfas.core.paths import PathResolver

logger = logging.getLogger(__name__)


def setup_logging(level: str = "INFO", log_file: Optional[str] = None):
    """Configure logging."""
    log_level = getattr(logging, level.upper(), logging.INFO)

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers
    )


def deep_merge(base: dict, overlay: dict) -> dict:
    """Deep merge overlay into base dict. Overlay values take precedence."""
    result = base.copy()
    for key, value in overlay.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_json_file(path: Path) -> dict:
    """Load JSON file, return empty dict if not found or invalid."""
    if path.exists():
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load {path}: {e}")
    return {}


def load_config(
    config_path: Optional[str],
    user_name: str,
    root_path: Path
) -> dict:
    """
    Load configuration with hierarchical precedence:
    1. Default built-in config (lowest)
    2. Global project config (config/mf_analyzer_config_v2.json)
    3. User preferences (via PathResolver: {user_dir}/config/preferences.json)
    4. User passwords (via PathResolver: {user_dir}/config/passwords.json)
    5. Command-line specified config file (highest for paths/processing)

    Args:
        config_path: Optional path to config file (CLI override)
        user_name: User name for user-specific configs
        root_path: Project root path

    Returns:
        Merged configuration dictionary
    """
    # Use PathResolver for user-specific paths (centralized, config-driven)
    resolver = PathResolver(root_path, user_name)

    # 1. Default built-in config (paths are relative to user dir via PathResolver)
    config = {
        "paths": {
            "inbox": "inbox/Mutual-Fund",
            "archive": "archive/Mutual-Fund",
            "reports_output": "reports/Mutual-Fund",
            "database": str(resolver.db_path())
        },
        "processing": {
            "archive_processed_files": True,
            "generate_json_output": False
        },
        "database": {
            "password": DEFAULT_DB_PASSWORD
        }
    }

    # 2. Load global project config
    global_config_path = root_path / DEFAULT_GLOBAL_CONFIG
    if global_config_path.exists():
        global_config = load_json_file(global_config_path)
        config = deep_merge(config, global_config)
        logger.info(f"Loaded global config: {global_config_path}")

    # 3. Load user preferences (via PathResolver)
    user_config_dir = resolver.user_config_dir()
    user_prefs_path = user_config_dir / USER_PREFERENCES_FILE
    if user_prefs_path.exists():
        user_prefs = load_json_file(user_prefs_path)
        config = deep_merge(config, {"user_preferences": user_prefs})
        logger.info(f"Loaded user preferences: {user_prefs_path}")

    # 4. Load user passwords (includes database password)
    user_passwords_path = user_config_dir / USER_PASSWORDS_FILE
    if user_passwords_path.exists():
        passwords = load_json_file(user_passwords_path)
        # Merge database password if present
        if "database" in passwords and "password" in passwords["database"]:
            config["database"]["password"] = passwords["database"]["password"]
        # Store file passwords for use by parsers
        config["passwords"] = passwords
        logger.info(f"Loaded user passwords: {user_passwords_path}")

    # 5. Load CLI-specified config file (overrides paths/processing)
    if config_path:
        cli_config_path = Path(config_path)
        if cli_config_path.exists():
            cli_config = load_json_file(cli_config_path)
            config = deep_merge(config, cli_config)
            logger.info(f"Loaded CLI config: {cli_config_path}")
        elif not cli_config_path.is_absolute():
            # Try relative to root
            cli_config_path = root_path / config_path
            if cli_config_path.exists():
                cli_config = load_json_file(cli_config_path)
                config = deep_merge(config, cli_config)
                logger.info(f"Loaded CLI config: {cli_config_path}")

    return config


def get_user_id(conn, user_name: str) -> int:
    """Get or create user ID."""
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


def run_ingest(
    conn,
    user_id: int,
    user_name: str,
    config: dict,
    root_path: Path,
    force: bool = False
):
    """Run ingestion pipeline."""
    print(f"\n{'='*60}")
    print(f"INGESTING MF STATEMENTS FOR {user_name}")
    print(f"{'='*60}")

    inbox_path = root_path / config["paths"]["data_root"].replace("{user}", user_name) / config["paths"]["inbox"]

    if not inbox_path.exists():
        print(f"[WARN] Inbox path does not exist: {inbox_path}")
        return

    ingester = MFIngester(conn, user_id, inbox_path)
    result = ingester.ingest(force=force)

    print(f"\nIngestion Results:")
    print(f"  Files processed: {result.files_processed}")
    print(f"  Files skipped:   {result.files_skipped}")
    print(f"  Records inserted: {result.records_inserted}")
    print(f"  Duplicates skipped: {result.records_skipped}")

    if result.errors:
        print(f"\nErrors:")
        for error in result.errors:
            print(f"  - {error}")

    if result.warnings:
        print(f"\nWarnings:")
        for warning in result.warnings[:5]:  # Show first 5
            print(f"  - {warning}")


def run_analyze(
    conn,
    user_id: int,
    user_name: str,
    config: dict,
    root_path: Path
):
    """Run MF analysis."""
    print(f"\n{'='*60}")
    print(f"ANALYZING MF HOLDINGS FOR {user_name}")
    print(f"{'='*60}")

    analyzer = MFAnalyzer(config=config, conn=conn)

    inbox_path = root_path / config["paths"]["data_root"].replace("{user}", user_name) / config["paths"]["inbox"]

    result = analyzer.analyze(
        user_name=user_name,
        user_id=user_id,
        mf_folder=inbox_path
    )

    print(f"\nAnalysis Results:")
    print(f"  Files scanned:      {result.files_scanned}")
    print(f"  Holdings processed: {result.holdings_processed}")
    print(f"  Duplicates skipped: {result.duplicates_skipped}")
    print(f"  Total value:        Rs. {result.total_current_value:,.2f}")
    print(f"  Total appreciation: Rs. {result.total_appreciation:,.2f}")

    if result.errors:
        print(f"\nErrors:")
        for error in result.errors[:5]:
            print(f"  - {error}")

    return result


def run_fy_analysis(
    conn,
    user_id: int,
    financial_year: str,
    config: dict
):
    """Run FY-specific analysis."""
    print(f"\n{'='*60}")
    print(f"FY {financial_year} ANALYSIS")
    print(f"{'='*60}")

    fy_analyzer = MFFYAnalyzer(conn, config)

    # Generate FY summary
    summaries = fy_analyzer.generate_fy_summary(user_id, financial_year)

    print(f"\nFY Summary by Category:")
    for summary in summaries:
        if summary.scheme_type == "ALL":
            continue
        print(f"\n  {summary.scheme_type}:")
        print(f"    Purchases:   Rs. {summary.purchase_amount:,.2f} ({summary.purchase_count} txns)")
        print(f"    Redemptions: Rs. {summary.redemption_amount:,.2f} ({summary.redemption_count} txns)")
        print(f"    STCG:        Rs. {summary.stcg_realized:,.2f}")
        print(f"    LTCG:        Rs. {summary.ltcg_realized:,.2f}")

        # Save to DB
        fy_analyzer.save_fy_summary(summary)

    return summaries


def run_reconciliation(
    conn,
    user_id: int,
    financial_year: str,
    config: dict,
    cg_file: Optional[Path] = None
):
    """Run capital gains reconciliation."""
    print(f"\n{'='*60}")
    print(f"CAPITAL GAINS RECONCILIATION - FY {financial_year}")
    print(f"{'='*60}")

    reconciler = MFReconciler(conn, config)

    for rta in ["CAMS", "KFINTECH"]:
        result = reconciler.reconcile(
            user_id=user_id,
            financial_year=financial_year,
            rta=rta,
            reported_cg_file=cg_file if cg_file else None
        )

        reconciler.save_result(result)

        status = "RECONCILED" if result.is_reconciled else "MISMATCH"
        print(f"\n  {rta}:")
        print(f"    Calculated:  STCG={result.calc_stcg:,.2f}, LTCG={result.calc_ltcg:,.2f}")
        print(f"    Reported:    STCG={result.reported_stcg:,.2f}, LTCG={result.reported_ltcg:,.2f}")
        print(f"    Difference:  {result.total_difference:,.2f}")
        print(f"    Status:      {status}")


def run_snapshot(
    conn,
    user_id: int,
    snapshot_type: str,
    config: dict,
    financial_year: Optional[str] = None
):
    """Take holdings snapshot."""
    print(f"\n{'='*60}")
    print(f"TAKING HOLDINGS SNAPSHOT ({snapshot_type})")
    print(f"{'='*60}")

    fy_analyzer = MFFYAnalyzer(conn, config)

    snapshot = fy_analyzer.take_holdings_snapshot(
        user_id=user_id,
        snapshot_date=date.today(),
        snapshot_type=snapshot_type,
        financial_year=financial_year
    )

    fy_analyzer.save_holdings_snapshot(snapshot)

    print(f"\nSnapshot saved:")
    print(f"  Date:          {snapshot.snapshot_date}")
    print(f"  Total Value:   Rs. {snapshot.total_value:,.2f}")
    print(f"  Total Cost:    Rs. {snapshot.total_cost:,.2f}")
    print(f"  Appreciation:  Rs. {snapshot.total_appreciation:,.2f}")
    print(f"  Schemes:       {snapshot.total_schemes}")
    print(f"  Folios:        {snapshot.total_folios}")


def run_report(
    conn,
    user_id: int,
    user_name: str,
    config: dict,
    root_path: Path,
    financial_year: Optional[str] = None,
    output_dir: Optional[Path] = None
):
    """Generate reports."""
    print(f"\n{'='*60}")
    print(f"GENERATING REPORTS FOR {user_name}")
    print(f"{'='*60}")

    if output_dir is None:
        output_dir = root_path / config["paths"]["data_root"].replace("{user}", user_name) / config["paths"]["reports_output"]

    generator = MFEnhancedReportGenerator(conn, output_dir, config)

    report_path = generator.generate(
        user_id=user_id,
        user_name=user_name,
        financial_year=financial_year,
        include_json=config.get("processing", {}).get("generate_json_output", False)
    )

    print(f"\nReport generated: {report_path}")
    return report_path


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="MF Analyzer - Mutual Fund Statement Analysis Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --user Sanjay
  %(prog)s --user Sanjay --fy 2024-25 --reconcile
  %(prog)s --user Sanjay --report-only --output-dir ./reports
  %(prog)s --user Sanjay --snapshot FY_END --fy 2024-25
        """
    )

    parser.add_argument(
        "--user", "-u",
        required=True,
        help="User name (e.g., Sanjay)"
    )

    parser.add_argument(
        "--config", "-c",
        default="config/mf_analyzer_config_v2.json",
        help="Path to JSON config file"
    )

    parser.add_argument(
        "--fy",
        help="Financial year (e.g., 2024-25)"
    )

    parser.add_argument(
        "--ingest-only",
        action="store_true",
        help="Only run ingestion, skip analysis and reports"
    )

    parser.add_argument(
        "--analyze-only",
        action="store_true",
        help="Only run analysis, skip ingestion and reports"
    )

    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Only generate reports, skip ingestion and analysis"
    )

    parser.add_argument(
        "--reconcile",
        action="store_true",
        help="Run capital gains reconciliation"
    )

    parser.add_argument(
        "--reconcile-file",
        type=Path,
        help="Path to RTA capital gains statement for reconciliation"
    )

    parser.add_argument(
        "--snapshot",
        choices=["FY_START", "FY_END", "QUARTERLY", "MONTHLY", "ADHOC"],
        help="Take holdings snapshot of specified type"
    )

    parser.add_argument(
        "--output-dir", "-o",
        type=Path,
        help="Output directory for reports"
    )

    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Force reprocess already ingested files"
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    parser.add_argument(
        "--log-file",
        help="Log file path"
    )

    parser.add_argument(
        "--db",
        help="Database path (default: from config)"
    )

    parser.add_argument(
        "--db-password",
        default=None,
        help="Database password (default: from user config or built-in)"
    )

    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Root path for data (default: current directory)"
    )

    args = parser.parse_args()

    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    setup_logging(log_level, args.log_file)

    # Load hierarchical config: built-in -> global -> user prefs -> user passwords -> CLI
    config = load_config(args.config, args.user, args.root)

    # Show config sources
    config_sources = []
    global_config = args.root / DEFAULT_GLOBAL_CONFIG
    if global_config.exists():
        config_sources.append(f"global: {global_config}")
    user_config_dir = args.root / USER_CONFIG_DIR.format(user=args.user)
    if (user_config_dir / USER_PREFERENCES_FILE).exists():
        config_sources.append(f"user prefs: {user_config_dir / USER_PREFERENCES_FILE}")
    if (user_config_dir / USER_PASSWORDS_FILE).exists():
        config_sources.append(f"user passwords: {user_config_dir / USER_PASSWORDS_FILE}")
    if args.config and Path(args.config).exists():
        config_sources.append(f"CLI override: {args.config}")

    print(f"Config sources: {', '.join(config_sources) if config_sources else 'defaults only'}")

    # Database path: CLI arg > config > default
    db_path = args.db
    if not db_path:
        data_root = args.root / config["paths"]["data_root"].replace("{user}", args.user)
        db_name = config["paths"].get("database", "db/finance.db")
        if "/" in db_name:
            db_path = data_root / db_name
        else:
            db_path = data_root / "db" / db_name

    print(f"Database: {db_path}")

    # Database password: CLI arg > user config > default
    db_password = args.db_password
    if db_password is None:
        db_password = config.get("database", {}).get("password", DEFAULT_DB_PASSWORD)

    # Initialize database
    db_manager = DatabaseManager()
    conn = db_manager.init(str(db_path), db_password)

    try:
        user_id = get_user_id(conn, args.user)
        print(f"User: {args.user} (ID: {user_id})")

        # Run requested operations
        if args.ingest_only:
            run_ingest(conn, user_id, args.user, config, args.root, args.force)

        elif args.analyze_only:
            run_analyze(conn, user_id, args.user, config, args.root)

        elif args.report_only:
            run_report(conn, user_id, args.user, config, args.root, args.fy, args.output_dir)

        elif args.snapshot:
            run_snapshot(conn, user_id, args.snapshot, config, args.fy)

        elif args.reconcile:
            if not args.fy:
                print("[ERROR] --fy required for reconciliation")
                sys.exit(1)
            run_reconciliation(conn, user_id, args.fy, config, args.reconcile_file)

        else:
            # Full pipeline
            run_ingest(conn, user_id, args.user, config, args.root, args.force)
            run_analyze(conn, user_id, args.user, config, args.root)

            if args.fy:
                run_fy_analysis(conn, user_id, args.fy, config)
                if args.reconcile:
                    run_reconciliation(conn, user_id, args.fy, config, args.reconcile_file)

            run_report(conn, user_id, args.user, config, args.root, args.fy, args.output_dir)

        print(f"\n{'='*60}")
        print("COMPLETED SUCCESSFULLY")
        print(f"{'='*60}")

    except Exception as e:
        logger.exception("Error during MF analysis")
        print(f"\n[ERROR] {e}")
        sys.exit(1)

    finally:
        db_manager.close()


if __name__ == "__main__":
    main()
