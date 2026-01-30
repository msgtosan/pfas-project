"""
Networth Report CLI - PFAS

Command-line interface for generating networth reports across all asset classes.

Usage:
    python -m pfas.cli.networth_cli --user Sanjay --fy 2024-2026
    python -m pfas.cli.networth_cli --user Sanjay --config custom_config.json
    python -m pfas.cli.networth_cli --user Sanjay --granularity monthly --assets MF,Stocks

Examples:
    # Generate FY-wise networth report
    python -m pfas.cli.networth_cli -u Sanjay -f 2023-2026

    # Monthly breakdown with specific assets
    python -m pfas.cli.networth_cli -u Sanjay -g monthly -a mutual_funds,indian_stocks

    # Use custom config file
    python -m pfas.cli.networth_cli -u Sanjay -c /path/to/config.json

    # Generate with custom output path
    python -m pfas.cli.networth_cli -u Sanjay -o /path/to/output.xlsx
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pfas.core.paths import PathResolver
from pfas.reports.networth_report import NetworthReportGenerator

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

    # Try sqlcipher first, fall back to sqlite3
    try:
        import sqlcipher3
        conn = sqlcipher3.connect(str(db_path))

        if password:
            conn.execute(f"PRAGMA key='{password}'")
            conn.execute("PRAGMA cipher_compatibility=4")
        else:
            # Try to get password from config
            pwd_file = resolver.password_config_file()
            if pwd_file.exists():
                with open(pwd_file) as f:
                    pwd_data = json.load(f)
                    db_password = pwd_data.get("database", {}).get("password")
                    if db_password:
                        conn.execute(f"PRAGMA key='{db_password}'")
                        conn.execute("PRAGMA cipher_compatibility=4")

        # Test connection
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


def load_config(config_path: Optional[Path], resolver: PathResolver) -> Optional[dict]:
    """Load configuration from file or use defaults."""
    if config_path and config_path.exists():
        with open(config_path, encoding="utf-8") as f:
            return json.load(f)

    # Try user config
    user_config = resolver.user_config_file("networth_config.json")
    if user_config and user_config.exists():
        with open(user_config, encoding="utf-8") as f:
            return json.load(f)

    # Use global config (handled by NetworthReportGenerator)
    return None


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="networth_cli",
        description="Generate comprehensive networth reports across all asset classes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -u Sanjay                           # Current FY networth
  %(prog)s -u Sanjay -f 2023-2026              # Multi-year analysis
  %(prog)s -u Sanjay -g monthly                # Monthly granularity
  %(prog)s -u Sanjay -a mutual_funds,stocks    # Specific assets only
  %(prog)s -u Sanjay --detailed                # Include detailed holdings
        """
    )

    # Required arguments
    parser.add_argument(
        "-u", "--user",
        required=True,
        help="User name (as stored in database)"
    )

    # Optional arguments
    parser.add_argument(
        "-f", "--fy-range",
        default="current",
        help="Financial year range: 'current', '2024-25', '2023-2026' (default: current)"
    )

    parser.add_argument(
        "-g", "--granularity",
        choices=["monthly", "quarterly", "fy"],
        default="fy",
        help="Time granularity for snapshots (default: fy)"
    )

    parser.add_argument(
        "-a", "--assets",
        help="Comma-separated list of asset categories to include (default: all enabled)"
    )

    parser.add_argument(
        "-c", "--config",
        type=Path,
        help="Path to custom configuration JSON file"
    )

    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output file path (default: auto-generated in reports directory)"
    )

    parser.add_argument(
        "--detailed/--no-detailed",
        dest="detailed",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Include detailed holdings breakdown (default: yes)"
    )

    # Database options
    parser.add_argument(
        "--data-root",
        type=Path,
        default=PROJECT_ROOT / "Data",
        help="Root data directory (default: Data/)"
    )

    parser.add_argument(
        "--db-password",
        help="Database encryption password (or use passwords.json)"
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
        help="Show what would be generated without writing files"
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

        # Verify user directory exists
        if not resolver.user_dir.exists():
            print(f"Error: User directory not found: {resolver.user_dir}")
            print(f"Run 'pfas init --user {args.user}' to create user structure")
            return 1

        # Load config
        config = load_config(args.config, resolver)

        # Connect to database
        conn = get_db_connection(resolver, args.db_password)
        logger.info("Connected to database")

        # Get user ID
        user_id = get_user_id(conn, args.user)
        logger.info(f"User: {args.user} (ID: {user_id})")

        # Parse assets list
        assets = None
        if args.assets:
            assets = [a.strip() for a in args.assets.split(",")]

        # Dry run - show what would be generated
        if args.dry_run:
            print("\n=== DRY RUN ===")
            print(f"User: {args.user} (ID: {user_id})")
            print(f"FY Range: {args.fy_range}")
            print(f"Granularity: {args.granularity}")
            print(f"Assets: {assets or 'all enabled'}")
            print(f"Detailed: {args.detailed}")

            output_path = args.output or resolver.report_file(
                asset_type="networth",
                report_type="summary",
                extension="xlsx"
            )
            print(f"Output: {output_path}")
            return 0

        # Generate report
        print(f"\nGenerating networth report for {args.user}...")
        print(f"  FY Range: {args.fy_range}")
        print(f"  Granularity: {args.granularity}")

        generator = NetworthReportGenerator(conn, resolver, config)

        report_path = generator.generate(
            user_id=user_id,
            user_name=args.user,
            fy_range=args.fy_range,
            assets=assets,
            granularity=args.granularity,
            detailed=args.detailed,
            output_path=args.output
        )

        print(f"\nReport generated: {report_path}")

        # Show summary
        summary = generator.calculator.calculate(
            user_id=user_id,
            user_name=args.user,
            fy_range=args.fy_range,
            assets=assets,
            granularity=args.granularity,
            detailed=False
        )

        print(f"\n=== Networth Summary ===")
        print(f"Total Networth: ₹{float(summary.total_networth):,.2f}")
        print(f"Total Cost Basis: ₹{float(summary.total_cost_basis):,.2f}")
        print(f"Total Gain/Loss: ₹{float(summary.total_gain):,.2f}")

        if summary.overall_cagr:
            print(f"Overall CAGR: {summary.overall_cagr:.2f}%")

        print(f"\n=== Asset Allocation ===")
        for cat, pct in sorted(summary.allocation_pct.items(), key=lambda x: x[1], reverse=True):
            value = summary.by_category.get(cat, 0)
            print(f"  {cat}: {pct:.1f}% (₹{float(value):,.0f})")

        conn.close()
        return 0

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


if __name__ == "__main__":
    sys.exit(main())
