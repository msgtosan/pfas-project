#!/usr/bin/env python3
"""
Stock Analyzer CLI - PFAS

Multi-broker stock statement scanning, analysis, and reporting.

Usage:
    python stock_analyzer_cli.py --user Sanjay --fy 2025-26
    python stock_analyzer_cli.py --user Sanjay --fy 2025-26 --output custom_report.xlsx
    python stock_analyzer_cli.py --user Sanjay --fy 2025-26 --scan-only
    python stock_analyzer_cli.py --user Sanjay --fy 2025-26 --broker zerodha

Examples:
    # Full analysis with default settings
    python stock_analyzer_cli.py --user Sanjay --fy 2025-26

    # Scan only (no database ingestion)
    python stock_analyzer_cli.py --user Sanjay --fy 2025-26 --scan-only

    # Specific broker only
    python stock_analyzer_cli.py --user Sanjay --fy 2025-26 --broker icicidirect

    # With verbose logging
    python stock_analyzer_cli.py --user Sanjay --fy 2025-26 -v

    # With custom config
    python stock_analyzer_cli.py --user Sanjay --fy 2025-26 --config my_config.json
"""

import argparse
import logging
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Optional

# Handle imports for both installed package and direct execution
try:
    from pfas.analyzers.stock_analyzer import (
        StockAnalyzer,
        StockStatementScanner,
        BrokerDetector,
        AnalysisResult,
        BrokerType,
        StatementType
    )
    from pfas.core.database import DatabaseManager
    from pfas.core.paths import PathResolver
except ImportError:
    # Fallback for direct execution
    src_path = Path(__file__).resolve().parent.parent.parent
    if src_path.exists() and str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    from pfas.analyzers.stock_analyzer import (
        StockAnalyzer,
        StockStatementScanner,
        BrokerDetector,
        AnalysisResult,
        BrokerType,
        StatementType
    )
    from pfas.core.database import DatabaseManager
    from pfas.core.paths import PathResolver


# Default database password (for development)
DEFAULT_PASSWORD = "pfas_dev_2024"


def get_project_root() -> Path:
    """Auto-detect project root."""
    cli_path = Path(__file__).resolve()

    # Try walking up to find project root
    for parent in cli_path.parents:
        if (parent / "Data").exists() or (parent / "config").exists():
            return parent

    # Fallback to cwd
    return Path.cwd()


def setup_logging(verbose: bool = False, debug: bool = False):
    """Configure logging based on verbosity flags."""
    if debug:
        level = logging.DEBUG
    elif verbose:
        level = logging.INFO
    else:
        level = logging.WARNING

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


def get_financial_year(fy_input: Optional[str] = None) -> str:
    """
    Get financial year string.

    Args:
        fy_input: User-provided FY (e.g., "2025-26", "2025", "25-26")

    Returns:
        Normalized FY string (e.g., "2025-26")
    """
    if not fy_input:
        # Default to current FY
        today = date.today()
        if today.month >= 4:
            return f"{today.year}-{str(today.year + 1)[-2:]}"
        else:
            return f"{today.year - 1}-{str(today.year)[-2:]}"

    # Normalize input
    fy_input = fy_input.strip()

    # Handle "2025-26" format
    if "-" in fy_input and len(fy_input) >= 7:
        return fy_input

    # Handle "25-26" format
    if "-" in fy_input and len(fy_input) == 5:
        parts = fy_input.split("-")
        return f"20{parts[0]}-{parts[1]}"

    # Handle "2025" format
    if fy_input.isdigit() and len(fy_input) == 4:
        year = int(fy_input)
        return f"{year}-{str(year + 1)[-2:]}"

    return fy_input


def format_currency(value: Decimal) -> str:
    """Format decimal as Indian currency."""
    return f"₹{value:,.2f}"


def format_percentage(value: Decimal) -> str:
    """Format decimal as percentage."""
    return f"{value:.2f}%"


def print_scan_results(scanned_files, config):
    """Print scan results in a formatted table."""
    print("\n" + "=" * 80)
    print("SCAN RESULTS")
    print("=" * 80)

    print(f"\nTotal files found: {len(scanned_files)}\n")

    if not scanned_files:
        print("No statement files found.")
        return

    # Group by broker
    by_broker = {}
    for sf in scanned_files:
        broker = sf.broker.value
        if broker not in by_broker:
            by_broker[broker] = {"holdings": [], "transactions": [], "unknown": []}

        if sf.statement_type == StatementType.HOLDINGS:
            by_broker[broker]["holdings"].append(sf)
        elif sf.statement_type == StatementType.TRANSACTIONS:
            by_broker[broker]["transactions"].append(sf)
        else:
            by_broker[broker]["unknown"].append(sf)

    for broker, files in by_broker.items():
        broker_name = config.get("brokers", {}).get(broker, {}).get("name", broker.upper())
        print(f"\n{broker_name}:")
        print("-" * 40)

        if files["holdings"]:
            print(f"  Holdings ({len(files['holdings'])} files):")
            for sf in files["holdings"]:
                print(f"    - {sf.path.name}")

        if files["transactions"]:
            print(f"  Transactions ({len(files['transactions'])} files):")
            for sf in files["transactions"]:
                print(f"    - {sf.path.name}")

        if files["unknown"]:
            print(f"  Unknown ({len(files['unknown'])} files):")
            for sf in files["unknown"]:
                print(f"    - {sf.path.name} [detection: {sf.detection_method}]")


def print_analysis_result(result: AnalysisResult, financial_year: str):
    """Print analysis results in a formatted report."""
    print("\n" + "=" * 80)
    print(f"STOCK ANALYSIS REPORT - FY {financial_year}")
    print("=" * 80)

    # Processing stats
    print("\n📊 PROCESSING SUMMARY")
    print("-" * 40)
    print(f"  Files scanned:          {result.files_scanned}")
    print(f"  Holdings processed:     {result.holdings_processed}")
    print(f"  Transactions processed: {result.transactions_processed}")
    print(f"  Duplicates skipped:     {result.duplicates_skipped}")

    # Portfolio summary
    print("\n💰 PORTFOLIO SUMMARY")
    print("-" * 40)
    print(f"  Total Market Value:     {format_currency(result.total_market_value)}")
    print(f"  Total Cost Basis:       {format_currency(result.total_cost_basis)}")
    print(f"  Unrealized P&L:         {format_currency(result.total_unrealized_pnl)}")

    if result.total_cost_basis > 0:
        pnl_pct = (result.total_unrealized_pnl / result.total_cost_basis) * 100
        print(f"  Unrealized P&L %:       {format_percentage(pnl_pct)}")

    # By broker breakdown
    if result.by_broker:
        print("\n📈 BY BROKER")
        print("-" * 40)
        for broker, stats in result.by_broker.items():
            print(f"  {broker}:")
            print(f"    Market Value:   {format_currency(stats['market_value'])}")
            print(f"    Unrealized P&L: {format_currency(stats['unrealized_pnl'])}")

    # Capital gains summary
    print("\n📋 CAPITAL GAINS SUMMARY")
    print("-" * 40)
    print(f"  STCG (Short-term):      {format_currency(result.total_stcg)}")
    print(f"  LTCG (Long-term):       {format_currency(result.total_ltcg)}")
    print(f"  LTCG Exemption Used:    {format_currency(result.ltcg_exemption_used)}")

    taxable_ltcg = max(Decimal("0"), result.total_ltcg - result.ltcg_exemption_used)
    print(f"  Taxable LTCG:           {format_currency(taxable_ltcg)}")

    # Quarterly breakdown
    if result.by_quarter:
        print("\n📅 QUARTERLY BREAKDOWN")
        print("-" * 40)
        for qtr, stats in sorted(result.by_quarter.items()):
            print(f"  {qtr}: {format_currency(stats.get('profit', Decimal('0')))}")

    # XIRR
    if result.xirr_overall is not None:
        print("\n📊 XIRR PERFORMANCE")
        print("-" * 40)
        print(f"  Overall XIRR:           {result.xirr_overall * 100:.2f}%")

        if result.xirr_by_stock:
            top_performers = sorted(
                [(s, x) for s, x in result.xirr_by_stock.items() if x is not None],
                key=lambda t: t[1],
                reverse=True
            )[:5]

            if top_performers:
                print("\n  Top 5 Performers:")
                for symbol, xirr in top_performers:
                    print(f"    {symbol}: {xirr * 100:.2f}%")

    # Warnings and errors
    if result.warnings:
        print("\n⚠️  WARNINGS")
        print("-" * 40)
        for warning in result.warnings[:10]:  # Limit to first 10
            print(f"  - {warning}")
        if len(result.warnings) > 10:
            print(f"  ... and {len(result.warnings) - 10} more")

    if result.errors:
        print("\n❌ ERRORS")
        print("-" * 40)
        for error in result.errors:
            print(f"  - {error}")

    # Status
    print("\n" + "=" * 80)
    if result.success:
        print("✅ Analysis completed successfully")
    else:
        print("❌ Analysis completed with errors")
    print("=" * 80)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Stock Statement Analyzer - PFAS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --user Sanjay --fy 2025-26
  %(prog)s --user Sanjay --fy 2025-26 --scan-only
  %(prog)s --user Sanjay --fy 2025-26 --broker zerodha
  %(prog)s --user Sanjay --fy 2025-26 --output my_report.xlsx -v
        """
    )

    # Required arguments
    parser.add_argument(
        "--user", "-u",
        required=True,
        help="User name (e.g., Sanjay)"
    )

    parser.add_argument(
        "--fy", "-f",
        help="Financial year (e.g., 2025-26). Defaults to current FY."
    )

    # Optional arguments
    parser.add_argument(
        "--config", "-c",
        type=Path,
        help="Path to config JSON file"
    )

    parser.add_argument(
        "--output", "-o",
        type=Path,
        help="Output report file path"
    )

    parser.add_argument(
        "--broker", "-b",
        choices=["icicidirect", "zerodha", "groww", "all"],
        default="all",
        help="Filter by broker (default: all)"
    )

    parser.add_argument(
        "--scan-only",
        action="store_true",
        help="Only scan files, don't ingest to database"
    )

    parser.add_argument(
        "--no-report",
        action="store_true",
        help="Skip report generation"
    )

    parser.add_argument(
        "--include-archive",
        action="store_true",
        help="Include archive folder in scan"
    )

    # Logging
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose, args.debug)
    logger = logging.getLogger(__name__)

    # Get project root and initialize PathResolver
    project_root = get_project_root()
    logger.info(f"Project root: {project_root}")

    resolver = PathResolver(project_root, args.user)
    resolver.ensure_user_structure()

    # Normalize financial year
    financial_year = get_financial_year(args.fy)
    logger.info(f"Financial year: {financial_year}")

    # Determine config path
    config_path = args.config
    if not config_path:
        config_path = project_root / "config" / "stock_analyzer_config.json"

    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)

    # Load config
    import json
    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)

    # Update config with CLI options
    if args.include_archive:
        config.setdefault("processing", {})["include_archive"] = True

    # Base path for scanning
    base_path = resolver.inbox() / "Indian-Stocks"

    # Scan-only mode
    if args.scan_only:
        print(f"\n🔍 Scanning {base_path}...")
        scanner = StockStatementScanner(config, resolver)
        scanned_files = scanner.scan(
            base_path,
            recursive=config.get("processing", {}).get("scan_recursive", True),
            include_archive=args.include_archive
        )

        # Filter by broker if specified
        if args.broker != "all":
            scanned_files = [
                sf for sf in scanned_files
                if sf.broker.value == args.broker
            ]

        print_scan_results(scanned_files, config)
        sys.exit(0)

    # Full analysis mode
    print(f"\n🚀 Starting stock analysis for {args.user}, FY {financial_year}")

    # Initialize database
    db_path = resolver.db_path()
    logger.info(f"Database: {db_path}")

    try:
        db_manager = DatabaseManager(str(db_path))
        conn = db_manager.conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

    # Run analysis
    try:
        analyzer = StockAnalyzer(
            conn=conn,
            config_path=config_path,
            path_resolver=resolver
        )

        result = analyzer.analyze(
            user_name=args.user,
            financial_year=financial_year,
            base_path=base_path
        )

        # Print results
        print_analysis_result(result, financial_year)

        # Generate report
        if not args.no_report and result.success:
            output_path = args.output
            if not output_path:
                output_path = resolver.reports() / f"stock_report_{args.user}_{financial_year}.xlsx"

            report_path = analyzer.generate_reports(output_path)
            print(f"\n📄 Report saved to: {report_path}")

        # Exit code based on success
        sys.exit(0 if result.success else 1)

    except Exception as e:
        logger.exception("Analysis failed")
        print(f"\n❌ Error: {e}")
        sys.exit(1)

    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()
