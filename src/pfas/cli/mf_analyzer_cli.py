#!/usr/bin/env python3
"""
MF Analyzer CLI - Mutual Fund Statement Analysis and Reporting.

Usage:
    ./mf-analyzer --user Sanjay
    ./mf-analyzer --user Sanjay --config config/mf_analyzer_config.json
    ./mf-analyzer --user Sanjay --report-only
    ./mf-analyzer --help
"""

import argparse
import json
import logging
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Optional

# Handle imports for both installed package and direct execution
try:
    from pfas.analyzers import MFAnalyzer, AnalysisResult
    from pfas.core.database import DatabaseManager
    from pfas.core.paths import PathResolver
except ImportError:
    src_path = Path(__file__).parent.parent.parent.parent / "src"
    if src_path.exists() and str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    from pfas.analyzers import MFAnalyzer, AnalysisResult
    from pfas.core.database import DatabaseManager
    from pfas.core.paths import PathResolver


def get_project_root() -> Path:
    """Get project root directory."""
    cli_path = Path(__file__).resolve()
    root = cli_path.parent.parent.parent.parent
    if (root / "Data").exists() or (root / "src").exists():
        return root
    return Path.cwd()


PROJECT_ROOT = get_project_root()
DEFAULT_PASSWORD = "pfas_secure_2024"


def format_currency(amount: Decimal) -> str:
    """Format amount as Indian currency."""
    return f"Rs. {amount:,.2f}"


def print_result(result: AnalysisResult, user_name: str):
    """Print analysis result to console."""
    print("\n" + "=" * 60)
    print(f"MF ANALYSIS RESULT - {user_name}")
    print("=" * 60)

    print(f"\nFiles Scanned:          {result.files_scanned}")
    print(f"Holdings Processed:     {result.holdings_processed}")
    print(f"Transactions Processed: {result.transactions_processed}")
    print(f"Duplicates Skipped:     {result.duplicates_skipped}")

    if result.holdings:
        print("\n" + "-" * 40)
        print("PORTFOLIO SUMMARY")
        print("-" * 40)
        print(f"Total Current Value:    {format_currency(result.total_current_value)}")
        print(f"Total Cost Value:       {format_currency(result.total_cost_value)}")
        print(f"Total Appreciation:     {format_currency(result.total_appreciation)}")

        if result.total_cost_value > 0:
            pct = (result.total_appreciation / result.total_cost_value) * 100
            print(f"Appreciation %:         {pct:.2f}%")

        if result.weighted_xirr:
            print(f"Weighted XIRR:          {result.weighted_xirr:.2f}%")

        print("\n" + "-" * 40)
        print("ALLOCATION")
        print("-" * 40)
        total = result.total_current_value
        if total > 0:
            print(f"Equity:   {format_currency(result.equity_value)} ({result.equity_value/total*100:.1f}%)")
            print(f"Debt:     {format_currency(result.debt_value)} ({result.debt_value/total*100:.1f}%)")
            print(f"Hybrid:   {format_currency(result.hybrid_value)} ({result.hybrid_value/total*100:.1f}%)")

        print(f"\nUnique Schemes: {len(set(h.scheme_name for h in result.holdings))}")
        print(f"Unique Folios:  {len(set(h.folio_number for h in result.holdings))}")

    if result.errors:
        print("\n" + "-" * 40)
        print("ERRORS")
        print("-" * 40)
        for error in result.errors:
            print(f"  - {error}")

    if result.warnings:
        print("\n" + "-" * 40)
        print("WARNINGS")
        print("-" * 40)
        for warning in result.warnings[:5]:  # Show first 5
            print(f"  - {warning}")
        if len(result.warnings) > 5:
            print(f"  ... and {len(result.warnings) - 5} more")

    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="PFAS Mutual Fund Statement Analyzer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --user Sanjay
  %(prog)s --user Sanjay --config config/mf_analyzer_config.json
  %(prog)s --user Sanjay --report-only
  %(prog)s --user Sanjay --mf-folder /path/to/mf/files

Data Structure:
  Input:  Data/Users/{user}/Mutual-Fund/
  Output: Data/Users/{user}/Reports/Mutual-Fund/
  DB:     Data/Users/{user}/pfas.db
        """
    )

    parser.add_argument(
        "--user", "-u",
        required=True,
        help="User name (required)"
    )
    parser.add_argument(
        "--config", "-c",
        help="Path to JSON config file (optional)"
    )
    parser.add_argument(
        "--mf-folder",
        help="Custom MF folder path (default: Data/Users/{user}/Mutual-Fund/)"
    )
    parser.add_argument(
        "--output-dir",
        help="Custom output directory for reports"
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Only generate reports (skip analysis)"
    )
    parser.add_argument(
        "--no-report",
        action="store_true",
        help="Skip report generation"
    )
    parser.add_argument(
        "--db",
        help="Custom database path"
    )
    parser.add_argument(
        "--password",
        default=DEFAULT_PASSWORD,
        help="Database password"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--json-output",
        action="store_true",
        help="Output result as JSON"
    )
    parser.add_argument(
        "--diagnose",
        action="store_true",
        help="Run database audit and diagnostics"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug-level logging (more verbose than -v)"
    )

    args = parser.parse_args()

    # Setup logging
    if args.debug:
        log_level = logging.DEBUG
    elif args.verbose:
        log_level = logging.INFO
    else:
        log_level = logging.WARNING
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Determine paths using PathResolver (centralized, config-driven)
    resolver = PathResolver(PROJECT_ROOT, args.user)
    db_path = args.db or str(resolver.db_path())
    mf_folder = Path(args.mf_folder) if args.mf_folder else resolver.inbox() / "Mutual-Fund"
    output_dir = Path(args.output_dir) if args.output_dir else resolver.reports() / "Mutual-Funds"

    print(f"User: {args.user}")
    print(f"Database: {db_path}")
    print(f"MF Folder: {mf_folder}")
    print(f"Reports Output: {output_dir}")

    # Load config
    config = {}
    if args.config:
        try:
            with open(args.config, "r") as f:
                config = json.load(f)
            print(f"Config: {args.config}")
        except Exception as e:
            print(f"Warning: Could not load config: {e}")

    # Initialize database
    try:
        db = DatabaseManager()
        conn = db.init(db_path, args.password)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print("Run with --init-db to initialize first.")
        return 1

    # Create analyzer
    analyzer = MFAnalyzer(config=config, conn=conn)

    if args.report_only:
        # Only generate reports
        print("\nGenerating reports (analysis skipped)...")
        try:
            # Get user ID
            cursor = conn.execute("SELECT id FROM users WHERE name = ?", (args.user,))
            row = cursor.fetchone()
            if not row:
                print(f"Error: User '{args.user}' not found")
                return 1
            analyzer.user_id = row[0]
            analyzer.user_name = args.user

            report_path = analyzer.generate_reports(output_dir)
            print(f"\nReport generated: {report_path}")
        except Exception as e:
            print(f"Error generating report: {e}")
            return 1
    else:
        # Run full analysis
        print(f"\nAnalyzing MF statements...")

        try:
            result = analyzer.analyze(
                user_name=args.user,
                mf_folder=mf_folder
            )

            if args.json_output:
                output = {
                    "success": result.success,
                    "files_scanned": result.files_scanned,
                    "holdings_processed": result.holdings_processed,
                    "transactions_processed": result.transactions_processed,
                    "duplicates_skipped": result.duplicates_skipped,
                    "total_current_value": float(result.total_current_value),
                    "total_cost_value": float(result.total_cost_value),
                    "total_appreciation": float(result.total_appreciation),
                    "equity_value": float(result.equity_value),
                    "debt_value": float(result.debt_value),
                    "hybrid_value": float(result.hybrid_value),
                    "weighted_xirr": float(result.weighted_xirr) if result.weighted_xirr else None,
                    "errors": result.errors,
                    "warnings": result.warnings[:10],
                }
                print(json.dumps(output, indent=2))
            else:
                print_result(result, args.user)

            # Generate reports unless --no-report
            if not args.no_report:
                try:
                    report_path = analyzer.generate_reports(output_dir)
                    print(f"\nReport generated: {report_path}")
                except Exception as e:
                    print(f"Warning: Could not generate report: {e}")

            # Run diagnostics if requested
            if args.diagnose:
                run_diagnostics(conn, analyzer.user_id, args.user)

        except Exception as e:
            print(f"Error during analysis: {e}")
            if args.verbose or args.debug:
                import traceback
                traceback.print_exc()
            return 1

    # Run diagnostics only (without analysis)
    if args.diagnose and not args.report_only:
        try:
            cursor = conn.execute("SELECT id FROM users WHERE name = ?", (args.user,))
            row = cursor.fetchone()
            if row:
                user_id = row[0] if isinstance(row, tuple) else row["id"]
                run_diagnostics(conn, user_id, args.user)
            else:
                print(f"User '{args.user}' not found in database")
        except Exception as e:
            print(f"Error running diagnostics: {e}")

    return 0


def run_diagnostics(conn, user_id: int, user_name: str):
    """Run MF diagnostics and print report."""
    try:
        from pfas.analyzers.mf_diagnostics import MFDiagnostics

        print("\n" + "=" * 70)
        print("RUNNING DATABASE DIAGNOSTICS")
        print("=" * 70)

        diagnostics = MFDiagnostics(conn)
        report = diagnostics.run_full_audit(user_id, user_name)
        diagnostics.print_report(report)

        # Also print holdings summary
        summary = diagnostics.get_holdings_summary(user_id)

        print("\n" + "-" * 70)
        print("HOLDINGS BREAKDOWN")
        print("-" * 70)

        print("\nBy RTA:")
        for item in summary.get('by_rta', []):
            print(f"  {item['rta']}: {item['count']} holdings, Rs. {item['value']:,.2f}")

        print("\nBy Scheme Type:")
        for item in summary.get('by_type', []):
            print(f"  {item['type']}: {item['count']} holdings, Rs. {item['value']:,.2f}")

        print("\nBy Source File:")
        for item in summary.get('by_source', []):
            print(f"  {item['file']}: {item['count']} holdings, Rs. {item['value']:,.2f}")

    except ImportError as e:
        print(f"Diagnostics module not available: {e}")
    except Exception as e:
        print(f"Error during diagnostics: {e}")


if __name__ == "__main__":
    sys.exit(main())
