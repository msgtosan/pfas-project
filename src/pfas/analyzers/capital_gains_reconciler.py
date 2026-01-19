"""
Capital Gains Reconciliation Module

Calculates capital gains using FIFO from CAS transactions and
provides reconciliation with RTA-provided capital gains.

Features:
- FIFO-based capital gains calculation from CAS transactions
- Support for grandfathering (31-Jan-2018)
- Debt fund tax rule changes (post-April 2023)
- Reconciliation between calculated and RTA values
- Detailed audit trail
"""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from enum import Enum
import logging
import sqlite3

from pfas.parsers.mf.models import AssetClass, TransactionType
from pfas.parsers.mf.classifier import classify_scheme
from pfas.parsers.mf.fifo_tracker import PortfolioFIFOTracker, GainResult

logger = logging.getLogger(__name__)

# Key dates
GRANDFATHERING_DATE = date(2018, 1, 31)
DEBT_TAX_CHANGE_DATE = date(2023, 4, 1)  # Debt funds taxed at slab rate from this date

# Holding periods for LTCG
EQUITY_LTCG_DAYS = 365   # >12 months
DEBT_LTCG_DAYS_OLD = 1095  # >36 months (before April 2023)
DEBT_LTCG_DAYS_NEW = 1095  # Still 36 months but taxed at slab rate


class ReconciliationStatus(Enum):
    """Status of capital gains reconciliation."""
    MATCHED = "MATCHED"           # Difference < 1%
    MINOR_DIFF = "MINOR_DIFF"     # Difference 1-5%
    MAJOR_DIFF = "MAJOR_DIFF"     # Difference > 5%
    RTA_ONLY = "RTA_ONLY"         # Only RTA values available
    FIFO_ONLY = "FIFO_ONLY"       # Only FIFO values available
    NOT_RECONCILED = "NOT_RECONCILED"


@dataclass
class SchemeCapitalGains:
    """Capital gains for a single scheme."""
    scheme_name: str
    folio: str
    isin: Optional[str] = None
    asset_class: AssetClass = AssetClass.OTHER

    # FIFO calculated values
    fifo_ltcg: Decimal = Decimal("0")
    fifo_stcg: Decimal = Decimal("0")
    fifo_ltcg_taxable: Decimal = Decimal("0")  # After exemption

    # RTA provided values
    rta_ltcg: Decimal = Decimal("0")
    rta_stcg: Decimal = Decimal("0")
    rta_ltcg_taxable: Decimal = Decimal("0")

    # Transaction details
    redemption_count: int = 0
    redemption_amount: Decimal = Decimal("0")
    purchase_lots_matched: int = 0

    # Grandfathering
    has_grandfathered_gains: bool = False
    grandfathered_value: Decimal = Decimal("0")

    @property
    def ltcg_difference(self) -> Decimal:
        return self.fifo_ltcg - self.rta_ltcg

    @property
    def stcg_difference(self) -> Decimal:
        return self.fifo_stcg - self.rta_stcg


@dataclass
class FYCapitalGains:
    """Capital gains summary for a financial year."""
    financial_year: str
    user_id: int

    # Aggregated FIFO values
    fifo_equity_ltcg: Decimal = Decimal("0")
    fifo_equity_stcg: Decimal = Decimal("0")
    fifo_debt_ltcg: Decimal = Decimal("0")
    fifo_debt_stcg: Decimal = Decimal("0")
    fifo_hybrid_ltcg: Decimal = Decimal("0")
    fifo_hybrid_stcg: Decimal = Decimal("0")

    # Aggregated RTA values
    rta_equity_ltcg: Decimal = Decimal("0")
    rta_equity_stcg: Decimal = Decimal("0")
    rta_debt_ltcg: Decimal = Decimal("0")
    rta_debt_stcg: Decimal = Decimal("0")
    rta_hybrid_ltcg: Decimal = Decimal("0")
    rta_hybrid_stcg: Decimal = Decimal("0")

    # Exemptions
    equity_ltcg_exemption: Decimal = Decimal("125000")  # FY 2024-25 onwards

    # Per-scheme details
    scheme_gains: List[SchemeCapitalGains] = field(default_factory=list)

    # Reconciliation
    reconciliation_status: ReconciliationStatus = ReconciliationStatus.NOT_RECONCILED
    reconciliation_notes: str = ""

    @property
    def total_fifo_ltcg(self) -> Decimal:
        return self.fifo_equity_ltcg + self.fifo_debt_ltcg + self.fifo_hybrid_ltcg

    @property
    def total_fifo_stcg(self) -> Decimal:
        return self.fifo_equity_stcg + self.fifo_debt_stcg + self.fifo_hybrid_stcg

    @property
    def total_rta_ltcg(self) -> Decimal:
        return self.rta_equity_ltcg + self.rta_debt_ltcg + self.rta_hybrid_ltcg

    @property
    def total_rta_stcg(self) -> Decimal:
        return self.rta_equity_stcg + self.rta_debt_stcg + self.rta_hybrid_stcg

    @property
    def taxable_equity_ltcg(self) -> Decimal:
        """Equity LTCG after exemption."""
        return max(Decimal("0"), self.fifo_equity_ltcg - self.equity_ltcg_exemption)


def get_financial_year(txn_date: date) -> str:
    """Get financial year string for a date (e.g., '2024-25')."""
    if txn_date.month >= 4:
        return f"{txn_date.year}-{str(txn_date.year + 1)[-2:]}"
    else:
        return f"{txn_date.year - 1}-{str(txn_date.year)[-2:]}"


def parse_fy_dates(fy: str) -> Tuple[date, date]:
    """Parse FY string to start and end dates."""
    start_year = int(fy.split('-')[0])
    return date(start_year, 4, 1), date(start_year + 1, 3, 31)


def classify_transaction_type(description: str) -> TransactionType:
    """Classify transaction type from description."""
    desc_upper = description.upper()

    # Purchases
    if "PURCHASE" in desc_upper:
        if "SIP" in desc_upper or "SYSTEMATIC" in desc_upper:
            return TransactionType.PURCHASE_SIP
        return TransactionType.PURCHASE

    # Switch In
    if "SWITCH" in desc_upper and ("IN" in desc_upper or "FROM" in desc_upper):
        if "MERGER" in desc_upper:
            return TransactionType.SWITCH_IN_MERGER
        return TransactionType.SWITCH_IN

    # Redemptions
    if "REDEMPTION" in desc_upper or "REDEEM" in desc_upper:
        return TransactionType.REDEMPTION

    # Switch Out
    if "SWITCH" in desc_upper and ("OUT" in desc_upper or "TO" in desc_upper):
        if "MERGER" in desc_upper:
            return TransactionType.SWITCH_OUT_MERGER
        return TransactionType.SWITCH_OUT

    # Dividend
    if "DIVIDEND" in desc_upper or "IDCW" in desc_upper:
        if "REINVEST" in desc_upper:
            return TransactionType.DIVIDEND_REINVEST
        return TransactionType.DIVIDEND_PAYOUT

    # Stamp duty
    if "STAMP" in desc_upper:
        return TransactionType.STAMP_DUTY_TAX

    return TransactionType.DIVIDEND  # Default to skip


class CapitalGainsReconciler:
    """
    Reconciles capital gains between FIFO calculation and RTA statements.

    Usage:
        reconciler = CapitalGainsReconciler(conn)

        # Calculate from CAS data
        fy_gains = reconciler.calculate_from_cas(cas_data, user_id, "2024-25")

        # Load RTA values (if available)
        reconciler.load_rta_values(user_id, "2024-25", rta_data)

        # Reconcile
        result = reconciler.reconcile(user_id, "2024-25")

        # Generate report
        report = reconciler.generate_report(user_id, "2024-25")
    """

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._portfolio_tracker: Optional[PortfolioFIFOTracker] = None
        self._fy_gains: Dict[str, FYCapitalGains] = {}

    def calculate_from_cas(
        self,
        cas_data,
        user_id: int,
        target_fy: Optional[str] = None
    ) -> Dict[str, FYCapitalGains]:
        """
        Calculate capital gains from CAS data using FIFO.

        Args:
            cas_data: Parsed CAS data from CASPDFParser
            user_id: User ID
            target_fy: Optional specific FY to calculate (e.g., "2024-25")

        Returns:
            Dictionary of FY -> FYCapitalGains
        """
        self._portfolio_tracker = PortfolioFIFOTracker()
        fy_gains: Dict[str, FYCapitalGains] = defaultdict(
            lambda: FYCapitalGains(financial_year="", user_id=user_id)
        )

        # Collect all transactions
        all_transactions = []

        for folio in cas_data.folios:
            for scheme in folio.schemes:
                # Classify scheme
                asset_class = classify_scheme(scheme.scheme)

                for txn in scheme.transactions:
                    all_transactions.append({
                        "folio": folio.folio,
                        "scheme": scheme.scheme,
                        "isin": getattr(scheme, 'isin', None),
                        "asset_class": asset_class,
                        "txn": txn
                    })

        # Sort by date
        all_transactions.sort(key=lambda x: x["txn"].date)

        # Process transactions
        for item in all_transactions:
            txn = item["txn"]
            txn_type = classify_transaction_type(txn.description)

            # Skip non-transactional items
            if txn_type in (
                TransactionType.STAMP_DUTY_TAX,
                TransactionType.DIVIDEND_PAYOUT,
                TransactionType.DIVIDEND
            ):
                continue

            # Get values
            nav = txn.nav if txn.nav else Decimal("0")
            units = txn.units if txn.units else Decimal("0")
            amount = txn.amount if txn.amount else Decimal("0")

            if nav == Decimal("0") and units and units != Decimal("0"):
                nav = abs(amount / units)

            if units == Decimal("0") or amount == Decimal("0"):
                continue

            try:
                gains = self._portfolio_tracker.process_transaction(
                    folio=item["folio"],
                    scheme_name=item["scheme"],
                    asset_class=item["asset_class"],
                    txn_type=txn_type,
                    txn_date=txn.date,
                    units=units,
                    nav=nav,
                    amount=amount
                )

                # Aggregate gains by FY
                if gains:
                    for gain in gains:
                        fy = get_financial_year(gain.sale_date)

                        # Skip if not target FY (when specified)
                        if target_fy and fy != target_fy:
                            continue

                        if fy_gains[fy].financial_year == "":
                            fy_gains[fy].financial_year = fy
                            fy_gains[fy].user_id = user_id

                        # Aggregate by asset class
                        asset_class = item["asset_class"]

                        if asset_class == AssetClass.EQUITY:
                            if gain.is_long_term:
                                fy_gains[fy].fifo_equity_ltcg += gain.taxable_gain
                            else:
                                fy_gains[fy].fifo_equity_stcg += gain.taxable_gain
                        elif asset_class == AssetClass.HYBRID:
                            if gain.is_long_term:
                                fy_gains[fy].fifo_hybrid_ltcg += gain.taxable_gain
                            else:
                                fy_gains[fy].fifo_hybrid_stcg += gain.taxable_gain
                        else:  # DEBT and OTHER
                            # Apply post-April 2023 debt fund rules
                            if gain.sale_date >= DEBT_TAX_CHANGE_DATE:
                                # All debt gains are STCG (taxed at slab)
                                fy_gains[fy].fifo_debt_stcg += gain.taxable_gain
                            else:
                                if gain.is_long_term:
                                    fy_gains[fy].fifo_debt_ltcg += gain.taxable_gain
                                else:
                                    fy_gains[fy].fifo_debt_stcg += gain.taxable_gain

                        # Track scheme-level gains
                        self._add_scheme_gain(
                            fy_gains[fy],
                            item["scheme"],
                            item["folio"],
                            item.get("isin"),
                            asset_class,
                            gain
                        )

            except Exception as e:
                logger.warning(f"Error processing {item['scheme']}: {e}")
                continue

        self._fy_gains = dict(fy_gains)
        return self._fy_gains

    def _add_scheme_gain(
        self,
        fy_gain: FYCapitalGains,
        scheme_name: str,
        folio: str,
        isin: Optional[str],
        asset_class: AssetClass,
        gain: GainResult
    ):
        """Add gain to scheme-level tracking."""
        # Find or create scheme entry
        scheme_gain = None
        for sg in fy_gain.scheme_gains:
            if sg.scheme_name == scheme_name and sg.folio == folio:
                scheme_gain = sg
                break

        if scheme_gain is None:
            scheme_gain = SchemeCapitalGains(
                scheme_name=scheme_name,
                folio=folio,
                isin=isin,
                asset_class=asset_class
            )
            fy_gain.scheme_gains.append(scheme_gain)

        # Update values
        if gain.is_long_term:
            scheme_gain.fifo_ltcg += gain.taxable_gain
        else:
            scheme_gain.fifo_stcg += gain.taxable_gain

        scheme_gain.redemption_count += 1
        scheme_gain.redemption_amount += gain.sale_value
        scheme_gain.purchase_lots_matched += 1

        if gain.is_grandfathered:
            scheme_gain.has_grandfathered_gains = True
            if gain.fmv_31jan2018:
                scheme_gain.grandfathered_value += gain.fmv_31jan2018

    def load_rta_values(
        self,
        user_id: int,
        financial_year: str,
        rta_data: Dict
    ):
        """
        Load RTA-provided capital gains values.

        Args:
            user_id: User ID
            financial_year: FY string (e.g., "2024-25")
            rta_data: Dictionary with RTA capital gains data
        """
        if financial_year not in self._fy_gains:
            self._fy_gains[financial_year] = FYCapitalGains(
                financial_year=financial_year,
                user_id=user_id
            )

        fy_gain = self._fy_gains[financial_year]

        # Update RTA values
        fy_gain.rta_equity_ltcg = Decimal(str(rta_data.get("equity_ltcg", 0)))
        fy_gain.rta_equity_stcg = Decimal(str(rta_data.get("equity_stcg", 0)))
        fy_gain.rta_debt_ltcg = Decimal(str(rta_data.get("debt_ltcg", 0)))
        fy_gain.rta_debt_stcg = Decimal(str(rta_data.get("debt_stcg", 0)))
        fy_gain.rta_hybrid_ltcg = Decimal(str(rta_data.get("hybrid_ltcg", 0)))
        fy_gain.rta_hybrid_stcg = Decimal(str(rta_data.get("hybrid_stcg", 0)))

    def reconcile(self, user_id: int, financial_year: str) -> FYCapitalGains:
        """
        Reconcile FIFO and RTA capital gains.

        Args:
            user_id: User ID
            financial_year: FY string

        Returns:
            FYCapitalGains with reconciliation status
        """
        if financial_year not in self._fy_gains:
            return FYCapitalGains(
                financial_year=financial_year,
                user_id=user_id,
                reconciliation_status=ReconciliationStatus.NOT_RECONCILED
            )

        fy_gain = self._fy_gains[financial_year]

        # Calculate differences
        ltcg_diff = abs(fy_gain.total_fifo_ltcg - fy_gain.total_rta_ltcg)
        stcg_diff = abs(fy_gain.total_fifo_stcg - fy_gain.total_rta_stcg)

        # Determine status based on percentage difference
        total_rta = fy_gain.total_rta_ltcg + fy_gain.total_rta_stcg
        total_fifo = fy_gain.total_fifo_ltcg + fy_gain.total_fifo_stcg

        if total_rta == Decimal("0") and total_fifo == Decimal("0"):
            fy_gain.reconciliation_status = ReconciliationStatus.MATCHED
            fy_gain.reconciliation_notes = "No capital gains in this FY"
        elif total_rta == Decimal("0"):
            fy_gain.reconciliation_status = ReconciliationStatus.FIFO_ONLY
            fy_gain.reconciliation_notes = "RTA values not available"
        elif total_fifo == Decimal("0"):
            fy_gain.reconciliation_status = ReconciliationStatus.RTA_ONLY
            fy_gain.reconciliation_notes = "FIFO calculation returned zero"
        else:
            total_diff = ltcg_diff + stcg_diff
            pct_diff = (total_diff / total_rta * 100) if total_rta else Decimal("0")

            if pct_diff < 1:
                fy_gain.reconciliation_status = ReconciliationStatus.MATCHED
                fy_gain.reconciliation_notes = f"Difference: {pct_diff:.2f}%"
            elif pct_diff < 5:
                fy_gain.reconciliation_status = ReconciliationStatus.MINOR_DIFF
                fy_gain.reconciliation_notes = f"Difference: {pct_diff:.2f}% - Review recommended"
            else:
                fy_gain.reconciliation_status = ReconciliationStatus.MAJOR_DIFF
                fy_gain.reconciliation_notes = f"Difference: {pct_diff:.2f}% - Manual verification required"

        return fy_gain

    def save_to_database(self, user_id: int, financial_year: str):
        """Save capital gains to database."""
        if financial_year not in self._fy_gains:
            return

        fy_gain = self._fy_gains[financial_year]

        # Save FY summary
        self.conn.execute("""
            INSERT OR REPLACE INTO mf_capital_gains_reconciliation (
                user_id, financial_year,
                fifo_equity_ltcg, fifo_equity_stcg,
                fifo_debt_ltcg, fifo_debt_stcg,
                fifo_hybrid_ltcg, fifo_hybrid_stcg,
                rta_equity_ltcg, rta_equity_stcg,
                rta_debt_ltcg, rta_debt_stcg,
                rta_hybrid_ltcg, rta_hybrid_stcg,
                reconciliation_status, reconciliation_notes,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            user_id, financial_year,
            str(fy_gain.fifo_equity_ltcg), str(fy_gain.fifo_equity_stcg),
            str(fy_gain.fifo_debt_ltcg), str(fy_gain.fifo_debt_stcg),
            str(fy_gain.fifo_hybrid_ltcg), str(fy_gain.fifo_hybrid_stcg),
            str(fy_gain.rta_equity_ltcg), str(fy_gain.rta_equity_stcg),
            str(fy_gain.rta_debt_ltcg), str(fy_gain.rta_debt_stcg),
            str(fy_gain.rta_hybrid_ltcg), str(fy_gain.rta_hybrid_stcg),
            fy_gain.reconciliation_status.value, fy_gain.reconciliation_notes
        ))

        self.conn.commit()

    def generate_report(self, user_id: int, financial_year: str) -> str:
        """Generate a text report for capital gains reconciliation."""
        if financial_year not in self._fy_gains:
            return f"No capital gains data for FY {financial_year}"

        fy_gain = self._fy_gains[financial_year]

        lines = [
            "=" * 80,
            f"CAPITAL GAINS RECONCILIATION REPORT - FY {financial_year}",
            "=" * 80,
            "",
            "SUMMARY",
            "-" * 40,
            "",
            f"{'Category':<20} {'FIFO LTCG':>15} {'FIFO STCG':>15} {'RTA LTCG':>15} {'RTA STCG':>15}",
            "-" * 80,
            f"{'Equity':<20} {fy_gain.fifo_equity_ltcg:>15,.2f} {fy_gain.fifo_equity_stcg:>15,.2f} {fy_gain.rta_equity_ltcg:>15,.2f} {fy_gain.rta_equity_stcg:>15,.2f}",
            f"{'Debt':<20} {fy_gain.fifo_debt_ltcg:>15,.2f} {fy_gain.fifo_debt_stcg:>15,.2f} {fy_gain.rta_debt_ltcg:>15,.2f} {fy_gain.rta_debt_stcg:>15,.2f}",
            f"{'Hybrid':<20} {fy_gain.fifo_hybrid_ltcg:>15,.2f} {fy_gain.fifo_hybrid_stcg:>15,.2f} {fy_gain.rta_hybrid_ltcg:>15,.2f} {fy_gain.rta_hybrid_stcg:>15,.2f}",
            "-" * 80,
            f"{'TOTAL':<20} {fy_gain.total_fifo_ltcg:>15,.2f} {fy_gain.total_fifo_stcg:>15,.2f} {fy_gain.total_rta_ltcg:>15,.2f} {fy_gain.total_rta_stcg:>15,.2f}",
            "",
            "DIFFERENCES",
            "-" * 40,
            f"LTCG Difference: Rs. {fy_gain.total_fifo_ltcg - fy_gain.total_rta_ltcg:,.2f}",
            f"STCG Difference: Rs. {fy_gain.total_fifo_stcg - fy_gain.total_rta_stcg:,.2f}",
            "",
            f"Reconciliation Status: {fy_gain.reconciliation_status.value}",
            f"Notes: {fy_gain.reconciliation_notes}",
            "",
            "TAX COMPUTATION (based on FIFO)",
            "-" * 40,
            f"Equity LTCG (Gross): Rs. {fy_gain.fifo_equity_ltcg:,.2f}",
            f"Equity LTCG Exemption: Rs. {fy_gain.equity_ltcg_exemption:,.2f}",
            f"Equity LTCG (Taxable): Rs. {fy_gain.taxable_equity_ltcg:,.2f}",
            f"Equity STCG (Taxable): Rs. {fy_gain.fifo_equity_stcg:,.2f}",
            "",
            f"Debt STCG (at slab rate): Rs. {fy_gain.fifo_debt_stcg:,.2f}",
            "",
        ]

        # Top schemes
        if fy_gain.scheme_gains:
            lines.append("TOP 10 SCHEMES BY GAINS")
            lines.append("-" * 40)

            sorted_schemes = sorted(
                fy_gain.scheme_gains,
                key=lambda x: abs(x.fifo_ltcg) + abs(x.fifo_stcg),
                reverse=True
            )[:10]

            for sg in sorted_schemes:
                lines.append(f"  {sg.scheme_name[:50]}")
                lines.append(f"    LTCG: Rs. {sg.fifo_ltcg:,.2f}, STCG: Rs. {sg.fifo_stcg:,.2f}")

        lines.append("")
        lines.append("=" * 80)

        return "\n".join(lines)
