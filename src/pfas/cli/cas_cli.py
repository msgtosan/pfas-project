#!/usr/bin/env python3
"""
CAS CLI - Command-line interface for CAS PDF processing.

Usage:
    python -m pfas.cli.cas_cli parse <pdf_file> -p <password> [-o <output_dir>]
    python -m pfas.cli.cas_cli --help

Examples:
    # Parse CAS and generate all reports (with user config)
    python -m pfas.cli.cas_cli parse Sanjay_CAS.pdf -p AAPPS0793R -o ./reports --user Sanjay

    # Parse with JSON output only
    python -m pfas.cli.cas_cli parse Sanjay_CAS.pdf -p AAPPS0793R --format json

    # Parse without folio consolidation
    python -m pfas.cli.cas_cli parse Sanjay_CAS.pdf -p AAPPS0793R --no-consolidate

    # Use specific user configuration
    python -m pfas.cli.cas_cli parse Sanjay_CAS.pdf -p AAPPS0793R --user Sanjay
"""

import argparse
import sys
import time
from pathlib import Path
from decimal import Decimal

# Add src to path if needed
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_ROOT = PROJECT_ROOT / "Data"
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pfas.parsers.mf.cas_pdf_parser import CASPDFParser, parse_cas_pdf, ConsolidationResult
from pfas.parsers.mf.cas_report_generator import CASReportGenerator, generate_cas_reports
from pfas.core.preferences import UserPreferences, CASConfig


def load_user_config(user: str) -> CASConfig:
    """Load CAS configuration for a specific user."""
    user_config_dir = DATA_ROOT / "Users" / user / "config"

    if user_config_dir.exists():
        try:
            prefs = UserPreferences.load(user_config_dir)
            print(f"Loaded user preferences from: {user_config_dir}")
            return prefs.cas
        except Exception as e:
            print(f"Warning: Failed to load user preferences: {e}")

    # Return defaults
    return CASConfig()


def parse_cas(args):
    """Parse CAS PDF and generate reports."""
    pdf_path = Path(args.pdf_file)

    if not pdf_path.exists():
        print(f"Error: File not found: {pdf_path}")
        return 1

    print(f"Parsing: {pdf_path}")
    if args.password:
        print(f"Using password: {'*' * len(args.password)}")

    # Load configuration
    if args.user:
        cas_config = load_user_config(args.user)
    else:
        cas_config = CASConfig()

    # Override with command-line arguments if specified
    consolidate = cas_config.consolidate_folios
    if args.consolidate is not None:
        consolidate = args.consolidate

    clean_names = cas_config.clean_scheme_names
    if args.clean_names is not None:
        clean_names = args.clean_names

    print(f"\nConfiguration:")
    print(f"  Folio Consolidation: {'Enabled' if consolidate else 'Disabled'}")
    print(f"  Scheme Name Cleaning: {'Enabled' if clean_names else 'Disabled'}")
    if args.user:
        print(f"  User: {args.user}")

    start_time = time.time()

    try:
        # Parse CAS with options
        cas_data, consolidation_result = parse_cas_pdf(
            pdf_path,
            args.password,
            consolidate_folios=consolidate,
            clean_scheme_names=clean_names,
            parse_stamp_duty=cas_config.parse_stamp_duty,
            parse_valuation=cas_config.parse_valuation
        )
        parse_time = time.time() - start_time

        # Display summary
        print("\n" + "=" * 70)
        print("CAS PARSING RESULTS")
        print("=" * 70)

        print(f"\nStatement Period: {cas_data.statement_period}")
        print(f"CAS Type: {cas_data.cas_type.value}")
        print(f"CAS Source: {cas_data.cas_source.value}")

        print(f"\nInvestor Info:")
        print(f"  Name:   {cas_data.investor_info.name}")
        print(f"  Email:  {cas_data.investor_info.email}")
        print(f"  Mobile: {cas_data.investor_info.mobile}")
        print(f"  PAN:    {cas_data.investor_info.pan}")

        print(f"\nPortfolio Summary:")
        print(f"  Total Folios: {len(cas_data.folios)}")
        print(f"  Total Schemes: {cas_data.total_schemes}")
        print(f"  Total Transactions: {cas_data.total_transactions}")
        print(f"  Total Value: Rs. {cas_data.total_value:,.2f}")

        # Count schemes with transactions
        schemes_with_txn = sum(
            1 for f in cas_data.folios
            for s in f.schemes
            if s.transactions
        )
        print(f"  Schemes with Transactions: {schemes_with_txn}")

        # Balance mismatches
        mismatches = cas_data.get_schemes_with_mismatch()
        if mismatches:
            print(f"  Balance Mismatches: {len(mismatches)}")

        # Show consolidation details
        if consolidation_result and args.show_consolidation:
            print("\n" + "-" * 70)
            print("CONSOLIDATION DETAILS")
            print("-" * 70)
            print(consolidation_result.consolidation_summary)

        print(f"\nParsing Time: {parse_time:.1f}s")

        # Generate reports
        if args.output:
            output_dir = Path(args.output)
            output_dir.mkdir(parents=True, exist_ok=True)

            prefix = pdf_path.stem.lower().replace(" ", "_")

            formats = args.format.split(",") if args.format else ["json", "text", "csv"]

            generator = CASReportGenerator(cas_data)
            reports = {}

            if "json" in formats:
                reports["json"] = generator.export_json(output_dir / f"{prefix}.json")
            if "text" in formats:
                reports["text"] = generator.export_text_summary(output_dir / f"{prefix}_summary.txt")
            if "csv" in formats:
                reports["csv"] = generator.export_capital_gains_csv(output_dir / f"{prefix}_capital_gains.csv")

            # Save consolidation report if enabled
            if consolidation_result and args.show_consolidation:
                consolidation_file = output_dir / f"{prefix}_consolidation.txt"
                with open(consolidation_file, 'w') as f:
                    f.write("CAS FOLIO CONSOLIDATION REPORT\n")
                    f.write("=" * 70 + "\n\n")
                    f.write(f"Source: {pdf_path}\n")
                    f.write(f"User: {args.user or 'default'}\n\n")
                    f.write(consolidation_result.consolidation_summary)
                    f.write("\n\n")

                    # Detailed folio list
                    f.write("CONSOLIDATED FOLIO LIST\n")
                    f.write("-" * 70 + "\n")
                    for folio in cas_data.folios:
                        f.write(f"\nFolio: {folio.folio}\n")
                        f.write(f"  AMC: {folio.amc}\n")
                        f.write(f"  Schemes: {len(folio.schemes)}\n")
                        for scheme in folio.schemes:
                            txn_count = len(scheme.transactions)
                            value = scheme.valuation.value if scheme.valuation else Decimal("0")
                            f.write(f"    - {scheme.scheme[:50]}...\n")
                            f.write(f"      Transactions: {txn_count}, Value: Rs.{value:,.2f}\n")

                reports["consolidation"] = consolidation_file

            print("\nReports Generated:")
            for report_type, path in reports.items():
                print(f"  {report_type.upper()}: {path}")

        print("\n" + "=" * 70)
        print("SUCCESS")
        print("=" * 70)

        return 0

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        if args.verbose:
            traceback.print_exc()
        return 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="PFAS CAS CLI - Parse CAS PDFs and generate reports"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Parse command
    parse_parser = subparsers.add_parser("parse", help="Parse CAS PDF")
    parse_parser.add_argument(
        "pdf_file",
        help="Path to CAS PDF file"
    )
    parse_parser.add_argument(
        "-p", "--password",
        help="PDF password"
    )
    parse_parser.add_argument(
        "-o", "--output",
        help="Output directory for reports"
    )
    parse_parser.add_argument(
        "--format",
        help="Output formats (comma-separated): json,text,csv",
        default="json,text,csv"
    )
    parse_parser.add_argument(
        "--user",
        help="User name to load preferences from (e.g., Sanjay)"
    )

    # Folio consolidation options
    consolidation_group = parse_parser.add_mutually_exclusive_group()
    consolidation_group.add_argument(
        "--consolidate",
        dest="consolidate",
        action="store_true",
        default=None,
        help="Enable folio consolidation (merge schemes under same folio)"
    )
    consolidation_group.add_argument(
        "--no-consolidate",
        dest="consolidate",
        action="store_false",
        help="Disable folio consolidation"
    )

    # Scheme name cleaning options
    clean_group = parse_parser.add_mutually_exclusive_group()
    clean_group.add_argument(
        "--clean-names",
        dest="clean_names",
        action="store_true",
        default=None,
        help="Enable scheme name cleaning (remove prefix codes)"
    )
    clean_group.add_argument(
        "--no-clean-names",
        dest="clean_names",
        action="store_false",
        help="Disable scheme name cleaning"
    )

    parse_parser.add_argument(
        "--show-consolidation",
        action="store_true",
        default=True,
        help="Show consolidation details in output (default: True)"
    )
    parse_parser.add_argument(
        "--no-show-consolidation",
        dest="show_consolidation",
        action="store_false",
        help="Hide consolidation details"
    )

    parse_parser.set_defaults(func=parse_cas)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
