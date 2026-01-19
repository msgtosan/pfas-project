#!/usr/bin/env python3
"""
CAS Capital Gains Comparison Test

Compares PFAS calculated capital gains with casparser results
to validate the FIFO-based capital gains calculation engine.
"""

import sys
from pathlib import Path
from datetime import date
from decimal import Decimal
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple

# Add src to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pfas.parsers.mf.cas_pdf_parser import CASPDFParser
from pfas.parsers.mf.fifo_tracker import PortfolioFIFOTracker, GainResult
from pfas.parsers.mf.models import AssetClass, TransactionType
from pfas.parsers.mf.classifier import classify_scheme


@dataclass
class FYCapitalGains:
    """Capital gains summary for a financial year."""
    fy: str
    ltcg: Decimal = Decimal("0")
    ltcg_taxable: Decimal = Decimal("0")
    stcg: Decimal = Decimal("0")
    schemes: Dict[str, Tuple[Decimal, Decimal]] = None  # scheme -> (ltcg, stcg)

    def __post_init__(self):
        if self.schemes is None:
            self.schemes = {}


def classify_transaction_type(txn_type: str) -> TransactionType:
    """Map CAS transaction type to TransactionType enum."""
    txn_type_upper = txn_type.upper()

    # Purchases
    if "PURCHASE" in txn_type_upper or "NEW PURCHASE" in txn_type_upper:
        if "SIP" in txn_type_upper or "SYSTEMATIC" in txn_type_upper:
            return TransactionType.PURCHASE_SIP
        return TransactionType.PURCHASE

    # Switch In
    if "SWITCH" in txn_type_upper and ("IN" in txn_type_upper or "FROM" in txn_type_upper):
        if "MERGER" in txn_type_upper:
            return TransactionType.SWITCH_IN_MERGER
        return TransactionType.SWITCH_IN

    # Redemptions
    if "REDEMPTION" in txn_type_upper or "REDEEM" in txn_type_upper:
        return TransactionType.REDEMPTION

    # Switch Out
    if "SWITCH" in txn_type_upper and ("OUT" in txn_type_upper or "TO" in txn_type_upper):
        if "MERGER" in txn_type_upper:
            return TransactionType.SWITCH_OUT_MERGER
        return TransactionType.SWITCH_OUT

    # Dividend
    if "DIVIDEND" in txn_type_upper:
        if "REINVEST" in txn_type_upper or "PAYOUT REINVEST" in txn_type_upper:
            return TransactionType.DIVIDEND_REINVEST
        return TransactionType.DIVIDEND_PAYOUT

    # Other
    if "STAMP" in txn_type_upper:
        return TransactionType.STAMP_DUTY_TAX

    return TransactionType.DIVIDEND  # Default to skip


def get_financial_year(txn_date: date) -> str:
    """Get financial year string for a date (e.g., '2024-25')."""
    if txn_date.month >= 4:
        return f"{txn_date.year}-{str(txn_date.year + 1)[-2:]}"
    else:
        return f"{txn_date.year - 1}-{str(txn_date.year)[-2:]}"


def calculate_capital_gains_from_cas(cas_data) -> Dict[str, FYCapitalGains]:
    """
    Calculate capital gains from CAS data using FIFO matching.

    Args:
        cas_data: Parsed CAS data from CASPDFParser

    Returns:
        Dictionary of FY -> FYCapitalGains
    """
    portfolio_tracker = PortfolioFIFOTracker()

    fy_gains: Dict[str, FYCapitalGains] = defaultdict(lambda: FYCapitalGains(fy=""))

    # Process all transactions in chronological order
    all_transactions = []

    for folio in cas_data.folios:
        for scheme in folio.schemes:
            # Classify scheme
            asset_class = classify_scheme(scheme.scheme)
            if asset_class == AssetClass.EQUITY:
                asset_class_enum = AssetClass.EQUITY
            elif asset_class == AssetClass.HYBRID:
                asset_class_enum = AssetClass.HYBRID
            else:
                asset_class_enum = AssetClass.DEBT

            for txn in scheme.transactions:
                all_transactions.append({
                    "folio": folio.folio,
                    "scheme": scheme.scheme,
                    "asset_class": asset_class_enum,
                    "txn": txn
                })

    # Sort by date
    all_transactions.sort(key=lambda x: x["txn"].date)

    # Process transactions
    for item in all_transactions:
        txn = item["txn"]
        txn_type = classify_transaction_type(txn.description)

        # Skip non-buy/sell transactions
        if txn_type in (TransactionType.STAMP_DUTY_TAX, TransactionType.DIVIDEND_PAYOUT, TransactionType.DIVIDEND):
            continue

        # Get NAV (use amount/units if available)
        nav = txn.nav if txn.nav else Decimal("0")
        if nav == Decimal("0") and txn.units and txn.units != Decimal("0"):
            nav = abs(txn.amount / txn.units)

        units = txn.units if txn.units else Decimal("0")
        amount = txn.amount if txn.amount else Decimal("0")

        if units == Decimal("0") or amount == Decimal("0"):
            continue

        try:
            gains = portfolio_tracker.process_transaction(
                folio=item["folio"],
                scheme_name=item["scheme"],
                asset_class=item["asset_class"],
                txn_type=txn_type,
                txn_date=txn.date,
                units=units,
                nav=nav,
                amount=amount,
                stt=Decimal("0"),
                stamp_duty=Decimal("0")
            )

            # Aggregate gains by FY
            if gains:
                for gain in gains:
                    fy = get_financial_year(gain.sale_date)
                    if fy_gains[fy].fy == "":
                        fy_gains[fy].fy = fy

                    if gain.is_long_term:
                        fy_gains[fy].ltcg += gain.taxable_gain
                    else:
                        fy_gains[fy].stcg += gain.taxable_gain

                    # Track by scheme
                    scheme_key = item["scheme"][:50]
                    if scheme_key not in fy_gains[fy].schemes:
                        fy_gains[fy].schemes[scheme_key] = (Decimal("0"), Decimal("0"))

                    curr_ltcg, curr_stcg = fy_gains[fy].schemes[scheme_key]
                    if gain.is_long_term:
                        fy_gains[fy].schemes[scheme_key] = (curr_ltcg + gain.taxable_gain, curr_stcg)
                    else:
                        fy_gains[fy].schemes[scheme_key] = (curr_ltcg, curr_stcg + gain.taxable_gain)

        except Exception as e:
            print(f"Warning: Error processing {item['scheme']}: {e}")
            continue

    return dict(fy_gains)


def parse_indian_number(num_str: str) -> Decimal:
    """Parse Indian formatted number (e.g., 13,37,220.69 or -₹43,065.8)."""
    if not num_str or num_str in ['-', '', '0.0']:
        return Decimal("0")

    # Clean the string
    cleaned = num_str.strip()

    # Handle negative sign at beginning or end, or with ₹ symbol
    is_negative = False
    if cleaned.startswith('-') or cleaned.startswith('-₹'):
        is_negative = True
        cleaned = cleaned.lstrip('-')

    # Remove currency symbol and spaces
    cleaned = cleaned.replace('₹', '').replace(' ', '')

    # Remove all commas (Indian format uses commas differently)
    cleaned = cleaned.replace(',', '')

    try:
        value = Decimal(cleaned)
        return -value if is_negative else value
    except:
        return Decimal("0")


def parse_casparser_results(csv_path: str) -> Dict[str, FYCapitalGains]:
    """Parse casparser capital gains results from the formatted output."""
    fy_gains: Dict[str, FYCapitalGains] = {}

    with open(csv_path, 'r') as f:
        content = f.read()

    # Parse the table format
    current_fy = None
    lines = content.split('\n')

    for line in lines:
        # Look for FY headers
        if line.startswith('│ FY20'):
            parts = line.split('│')
            if len(parts) > 1:
                fy_text = parts[1].strip()
                if fy_text.startswith('FY') and '-' in fy_text:
                    current_fy = fy_text.replace('FY', '')
                    if current_fy not in fy_gains:
                        fy_gains[current_fy] = FYCapitalGains(fy=current_fy)

        # Look for Total Gains lines
        if 'Total Gains' in line and current_fy:
            parts = line.split('│')
            if len(parts) >= 6:
                # Format: │ (empty) │ FYxxxx-xx - Total Gains │ LTCG │ LTCG_Taxable │ STCG │
                # parts[0] = before first │
                # parts[1] = FY column (empty for total rows)
                # parts[2] = Fund/Description column ("FY - Total Gains")
                # parts[3] = LTCG
                # parts[4] = LTCG_Taxable
                # parts[5] = STCG
                try:
                    ltcg = parse_indian_number(parts[3].strip())
                    ltcg_taxable = parse_indian_number(parts[4].strip())
                    stcg = parse_indian_number(parts[5].strip())

                    fy_gains[current_fy].ltcg = ltcg
                    fy_gains[current_fy].ltcg_taxable = ltcg_taxable
                    fy_gains[current_fy].stcg = stcg
                except Exception as e:
                    print(f"Error parsing line for {current_fy}: {line} - {e}")

    return fy_gains


def compare_capital_gains(
    pfas_gains: Dict[str, FYCapitalGains],
    casparser_gains: Dict[str, FYCapitalGains]
) -> None:
    """Compare and print capital gains comparison."""

    print("\n" + "=" * 100)
    print("CAPITAL GAINS COMPARISON: PFAS vs casparser")
    print("=" * 100)

    all_fys = sorted(set(pfas_gains.keys()) | set(casparser_gains.keys()))

    print(f"\n{'FY':<12} {'Source':<12} {'LTCG':>15} {'LTCG Taxable':>15} {'STCG':>15} {'Diff LTCG':>15} {'Diff STCG':>15}")
    print("-" * 100)

    total_diff_ltcg = Decimal("0")
    total_diff_stcg = Decimal("0")

    for fy in all_fys:
        pfas = pfas_gains.get(fy, FYCapitalGains(fy=fy))
        casparser = casparser_gains.get(fy, FYCapitalGains(fy=fy))

        diff_ltcg = pfas.ltcg - casparser.ltcg
        diff_stcg = pfas.stcg - casparser.stcg

        total_diff_ltcg += abs(diff_ltcg)
        total_diff_stcg += abs(diff_stcg)

        # Print PFAS row
        print(f"{fy:<12} {'PFAS':<12} {pfas.ltcg:>15,.2f} {'-':>15} {pfas.stcg:>15,.2f}")

        # Print casparser row
        print(f"{'':<12} {'casparser':<12} {casparser.ltcg:>15,.2f} {casparser.ltcg_taxable:>15,.2f} {casparser.stcg:>15,.2f}")

        # Print difference
        diff_ltcg_pct = (diff_ltcg / casparser.ltcg * 100) if casparser.ltcg else Decimal("0")
        diff_stcg_pct = (diff_stcg / casparser.stcg * 100) if casparser.stcg else Decimal("0")

        print(f"{'':<12} {'DIFF':<12} {'':<15} {'':<15} {'':<15} {diff_ltcg:>15,.2f} {diff_stcg:>15,.2f}")
        print("-" * 100)

    print(f"\nTotal Absolute Differences: LTCG = {total_diff_ltcg:,.2f}, STCG = {total_diff_stcg:,.2f}")


def main():
    """Main comparison test."""
    # File paths
    cas_pdf_path = Path("/home/sshankar/CASTest/usr-inbox/Sanjay_CAS.pdf")
    casparser_csv_path = Path("/home/sshankar/CASTest/sanjay_cas_cg_test.csv")
    password = "AAPPS0793R"

    print("=" * 100)
    print("CAS CAPITAL GAINS CALCULATION TEST")
    print("=" * 100)

    # Step 1: Parse CAS PDF
    print(f"\n1. Parsing CAS PDF: {cas_pdf_path}")
    parser = CASPDFParser()
    cas_data = parser.parse(str(cas_pdf_path), password)

    print(f"   - Investor: {cas_data.investor_info.name}")
    print(f"   - Period: {cas_data.statement_period}")
    print(f"   - Folios: {len(cas_data.folios)}")
    print(f"   - Total Schemes: {cas_data.total_schemes}")
    print(f"   - Total Transactions: {cas_data.total_transactions}")

    # Step 2: Calculate capital gains using PFAS FIFO
    print(f"\n2. Calculating capital gains using PFAS FIFO engine...")
    pfas_gains = calculate_capital_gains_from_cas(cas_data)

    print(f"   - Financial years with gains: {len(pfas_gains)}")
    for fy in sorted(pfas_gains.keys()):
        gains = pfas_gains[fy]
        print(f"     {fy}: LTCG={gains.ltcg:,.2f}, STCG={gains.stcg:,.2f}")

    # Step 3: Parse casparser results
    print(f"\n3. Parsing casparser results: {casparser_csv_path}")
    casparser_gains = parse_casparser_results(str(casparser_csv_path))

    print(f"   - Financial years: {len(casparser_gains)}")

    # Step 4: Compare results
    print(f"\n4. Comparing results...")
    compare_capital_gains(pfas_gains, casparser_gains)

    # Focus on recent FYs
    print("\n" + "=" * 100)
    print("DETAILED COMPARISON FOR FY 2024-25")
    print("=" * 100)

    fy = "2024-25"
    if fy in pfas_gains and fy in casparser_gains:
        pfas = pfas_gains[fy]
        casparser = casparser_gains[fy]

        print(f"\nPFAS Calculated:")
        print(f"  LTCG: Rs. {pfas.ltcg:,.2f}")
        print(f"  STCG: Rs. {pfas.stcg:,.2f}")

        print(f"\ncasparser Reference:")
        print(f"  LTCG: Rs. {casparser.ltcg:,.2f}")
        print(f"  LTCG (Taxable): Rs. {casparser.ltcg_taxable:,.2f}")
        print(f"  STCG: Rs. {casparser.stcg:,.2f}")

        print(f"\nDifferences:")
        print(f"  LTCG: Rs. {pfas.ltcg - casparser.ltcg:,.2f}")
        print(f"  STCG: Rs. {pfas.stcg - casparser.stcg:,.2f}")

        # Show top scheme differences if available
        if pfas.schemes:
            print(f"\nTop 10 Schemes by Gains (PFAS):")
            sorted_schemes = sorted(
                pfas.schemes.items(),
                key=lambda x: abs(x[1][0]) + abs(x[1][1]),
                reverse=True
            )[:10]
            for scheme, (ltcg, stcg) in sorted_schemes:
                print(f"  {scheme[:60]}: LTCG={ltcg:,.2f}, STCG={stcg:,.2f}")


if __name__ == "__main__":
    main()
