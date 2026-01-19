"""
CAS Report Generator - Generate casparser-compatible reports from PFAS CAS data.

Outputs:
1. JSON export - Full structured CAS data
2. Text summary - Portfolio summary table
3. Capital gains CSV - FY-wise LTCG/STCG breakdown
"""

import json
import csv
import logging
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict

from .models import CASData, CASFolio, CASScheme, CASTransaction, TransactionType

logger = logging.getLogger(__name__)


def decimal_serializer(obj):
    """JSON serializer for Decimal and date objects."""
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, (date, datetime)):
        return obj.strftime("%d-%b-%Y")
    raise TypeError(f"Type {type(obj)} not serializable")


class CASReportGenerator:
    """
    Generate casparser-compatible reports from CAS data.

    Usage:
        generator = CASReportGenerator(cas_data)
        generator.export_json("output.json")
        generator.export_text_summary("output.txt")
        generator.export_capital_gains_csv("output.csv")
    """

    def __init__(self, cas_data: CASData):
        """
        Initialize report generator.

        Args:
            cas_data: Parsed CAS data from CASPDFParser
        """
        self.cas_data = cas_data

    def export_json(self, output_path: Path) -> Path:
        """
        Export CAS data to JSON (casparser-compatible format).

        Args:
            output_path: Path for output JSON file

        Returns:
            Path to created file
        """
        output_path = Path(output_path)

        # Build JSON structure similar to casparser
        data = {
            "statement_period": {
                "from": self.cas_data.statement_period.from_date.strftime("%d-%b-%Y"),
                "to": self.cas_data.statement_period.to_date.strftime("%d-%b-%Y"),
            },
            "investor_info": {
                "name": self.cas_data.investor_info.name,
                "email": self.cas_data.investor_info.email,
                "mobile": self.cas_data.investor_info.mobile,
                "pan": self.cas_data.investor_info.pan,
            },
            "cas_type": self.cas_data.cas_type.value,
            "file_type": self.cas_data.cas_source.value,
            "folios": []
        }

        for folio in self.cas_data.folios:
            folio_data = {
                "folio": folio.folio,
                "amc": folio.amc,
                "PAN": folio.pan or "",
                "KYC": folio.kyc or "",
                "PANKYC": folio.pankyc or "",
                "schemes": []
            }

            for scheme in folio.schemes:
                scheme_data = {
                    "scheme": scheme.scheme,
                    "advisor": scheme.advisor,
                    "rta_code": scheme.rta_code,
                    "rta": scheme.rta,
                    "type": scheme.scheme_type or "UNKNOWN",
                    "isin": scheme.isin,
                    "amfi": scheme.amfi,
                    "nominees": scheme.nominees,
                    "open": str(scheme.open),
                    "close": str(scheme.close),
                    "close_calculated": str(scheme.close_calculated),
                    "valuation": None,
                    "transactions": []
                }

                # Add valuation if available
                if scheme.valuation:
                    scheme_data["valuation"] = {
                        "date": scheme.valuation.date.strftime("%Y-%m-%d"),
                        "nav": str(scheme.valuation.nav),
                        "cost": str(scheme.valuation.cost) if scheme.valuation.cost else None,
                        "value": str(scheme.valuation.value),
                    }

                # Add transactions
                for txn in scheme.transactions:
                    txn_data = {
                        "date": txn.date.strftime("%Y-%m-%d"),
                        "description": txn.description,
                        "amount": str(txn.amount) if txn.amount else None,
                        "units": str(txn.units) if txn.units else None,
                        "nav": str(txn.nav) if txn.nav else None,
                        "balance": str(txn.balance) if txn.balance else None,
                        "type": txn.transaction_type.value,
                        "dividend_rate": str(txn.dividend_rate) if txn.dividend_rate else None,
                    }
                    scheme_data["transactions"].append(txn_data)

                folio_data["schemes"].append(scheme_data)

            data["folios"].append(folio_data)

        # Write JSON
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2, default=decimal_serializer)

        logger.info(f"JSON report exported to {output_path}")
        return output_path

    def export_text_summary(self, output_path: Path) -> Path:
        """
        Export portfolio summary as formatted text table.

        Args:
            output_path: Path for output text file

        Returns:
            Path to created file
        """
        output_path = Path(output_path)
        lines = []

        # Header
        lines.append("=" * 100)
        lines.append(f"{'Portfolio Summary':^100}")
        lines.append("=" * 100)
        lines.append(f"Statement Period: {self.cas_data.statement_period}")
        lines.append(f"Investor: {self.cas_data.investor_info.name}")
        lines.append(f"PAN: {self.cas_data.investor_info.pan}")
        lines.append("=" * 100)
        lines.append("")

        # Table header
        header = f"{'Scheme':<45} {'Open':>12} {'Close':>12} {'Calculated':>12} {'Value':>15} {'Txns':>6} {'Status':<6}"
        lines.append(header)
        lines.append("-" * len(header))

        total_value = Decimal("0")
        total_schemes = 0
        total_txns = 0
        mismatches = 0

        current_amc = ""

        for folio in self.cas_data.folios:
            for scheme in folio.schemes:
                # AMC header
                if folio.amc != current_amc:
                    current_amc = folio.amc
                    lines.append("")
                    lines.append(f"--- {current_amc} ---")
                    lines.append("")

                # Scheme row
                scheme_name = scheme.scheme[:44]
                open_bal = f"{scheme.open:,.3f}"
                close_bal = f"{scheme.close:,.3f}"
                calc_bal = f"{scheme.close_calculated:,.3f}"
                txn_count = len(scheme.transactions)

                # Value
                value = Decimal("0")
                if scheme.valuation:
                    value = scheme.valuation.value
                value_str = f"Rs.{value:>11,.2f}"

                # Status (balance check)
                status = "OK" if not scheme.has_mismatch else "MISMATCH"
                if scheme.has_mismatch:
                    mismatches += 1

                lines.append(
                    f"{scheme_name:<45} {open_bal:>12} {close_bal:>12} {calc_bal:>12} {value_str:>15} {txn_count:>6} {status:<6}"
                )
                lines.append(f"  Folio: {folio.folio}")

                total_value += value
                total_schemes += 1
                total_txns += txn_count

        # Summary
        lines.append("")
        lines.append("=" * 100)
        lines.append(f"Total Schemes: {total_schemes}")
        lines.append(f"Total Transactions: {total_txns}")
        lines.append(f"Total Portfolio Value: Rs. {total_value:,.2f}")
        if mismatches:
            lines.append(f"Balance Mismatches: {mismatches}")
        lines.append("=" * 100)

        # Write file
        with open(output_path, 'w') as f:
            f.write('\n'.join(lines))

        logger.info(f"Text summary exported to {output_path}")
        return output_path

    def export_capital_gains_csv(self, output_path: Path, fy: Optional[str] = None) -> Path:
        """
        Export capital gains report as CSV.

        Args:
            output_path: Path for output CSV file
            fy: Optional financial year filter (e.g., "2024-25")

        Returns:
            Path to created file
        """
        output_path = Path(output_path)

        # Collect capital gains from transactions
        gains_by_fy: Dict[str, List[Dict]] = {}

        for folio in self.cas_data.folios:
            for scheme in folio.schemes:
                for txn in scheme.transactions:
                    # Only process redemptions for capital gains
                    if txn.transaction_type not in (
                        TransactionType.REDEMPTION,
                        TransactionType.SWITCH_OUT,
                        TransactionType.SWITCH_OUT_MERGER
                    ):
                        continue

                    # Determine FY
                    txn_fy = self._get_financial_year(txn.date)

                    if fy and txn_fy != fy:
                        continue

                    if txn_fy not in gains_by_fy:
                        gains_by_fy[txn_fy] = []

                    gains_by_fy[txn_fy].append({
                        "folio": folio.folio,
                        "scheme": scheme.scheme,
                        "date": txn.date,
                        "amount": txn.amount or Decimal("0"),
                        "units": txn.units or Decimal("0"),
                    })

        # Write CSV
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                "Financial Year", "Scheme", "Folio", "Redemption Date",
                "Units", "Amount", "LTCG", "LTCG_Taxable", "STCG"
            ])

            for fy_key in sorted(gains_by_fy.keys()):
                for gain in gains_by_fy[fy_key]:
                    writer.writerow([
                        fy_key,
                        gain["scheme"],
                        gain["folio"],
                        gain["date"].strftime("%Y-%m-%d"),
                        str(gain["units"]),
                        str(gain["amount"]),
                        "0",  # LTCG - requires FIFO calculation
                        "0",  # LTCG_Taxable
                        "0",  # STCG
                    ])

        logger.info(f"Capital gains CSV exported to {output_path}")
        return output_path

    def _get_financial_year(self, d: date) -> str:
        """Get financial year string for a date."""
        if d.month >= 4:
            return f"{d.year}-{str(d.year + 1)[2:]}"
        else:
            return f"{d.year - 1}-{str(d.year)[2:]}"

    def generate_all_reports(self, output_dir: Path, prefix: str = "cas") -> Dict[str, Path]:
        """
        Generate all report types.

        Args:
            output_dir: Output directory
            prefix: Filename prefix

        Returns:
            Dict mapping report type to file path
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        return {
            "json": self.export_json(output_dir / f"{prefix}_data.json"),
            "text": self.export_text_summary(output_dir / f"{prefix}_summary.txt"),
            "csv": self.export_capital_gains_csv(output_dir / f"{prefix}_capital_gains.csv"),
        }


def generate_cas_reports(
    cas_data: CASData,
    output_dir: Path,
    prefix: str = "cas"
) -> Dict[str, Path]:
    """
    Convenience function to generate all CAS reports.

    Args:
        cas_data: Parsed CAS data
        output_dir: Output directory
        prefix: Filename prefix

    Returns:
        Dict mapping report type to file path
    """
    generator = CASReportGenerator(cas_data)
    return generator.generate_all_reports(output_dir, prefix)
