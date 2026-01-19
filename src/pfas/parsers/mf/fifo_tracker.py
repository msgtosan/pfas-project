"""
FIFO Unit Tracker - First-In-First-Out capital gains calculation.

Implements FIFO-based unit matching for mutual fund redemptions to
compute capital gains independently from RTA statements.

Features:
- FIFO unit matching for redemptions
- Grandfathering support (31-Jan-2018)
- LTCG/STCG classification
- Cost of acquisition calculation with 3 scenarios
- STT and stamp duty allocation
"""

from collections import deque
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Deque, List, Optional, Dict, Tuple
import logging

from .models import AssetClass, TransactionType
from .exceptions import FIFOMismatchError, GrandfatheringError

logger = logging.getLogger(__name__)

# Key dates
GRANDFATHERING_DATE = date(2018, 1, 31)
BUDGET_2018_EFFECTIVE = date(2018, 4, 1)

# LTCG holding periods (in days)
EQUITY_LTCG_DAYS = 365   # >12 months
DEBT_LTCG_DAYS = 730     # >24 months (old rule, now at slab)


@dataclass
class PurchaseLot:
    """
    Represents a purchase lot for FIFO tracking.

    Each purchase creates a new lot that is consumed
    when units are redeemed.
    """
    date: date
    units: Decimal
    nav: Decimal
    amount: Decimal
    remaining_units: Decimal = field(init=False)

    def __post_init__(self):
        self.remaining_units = self.units

    @property
    def unit_cost(self) -> Decimal:
        """Cost per unit."""
        if self.units == Decimal("0"):
            return Decimal("0")
        return (self.amount / self.units).quantize(Decimal("0.0001"), ROUND_HALF_UP)

    @property
    def is_exhausted(self) -> bool:
        """Check if all units have been consumed."""
        return self.remaining_units <= Decimal("0.0001")


@dataclass
class GainResult:
    """
    Result of capital gain calculation for a matched lot.

    Contains all details needed for tax computation and reporting.
    """
    purchase_date: date
    purchase_units: Decimal
    purchase_nav: Decimal
    purchase_value: Decimal

    sale_date: date
    sale_units: Decimal
    sale_nav: Decimal
    sale_value: Decimal

    holding_days: int
    is_long_term: bool
    asset_class: AssetClass

    # Cost of acquisition (may differ from purchase_value due to grandfathering)
    cost_of_acquisition: Decimal
    indexed_cost: Optional[Decimal] = None

    # Capital gains
    realized_gain: Decimal = Decimal("0")  # sale_value - purchase_value
    taxable_gain: Decimal = Decimal("0")   # sale_value - cost_of_acquisition

    # Grandfathering details
    is_grandfathered: bool = False
    fmv_31jan2018: Optional[Decimal] = None

    # Tax deductions
    stt_allocated: Decimal = Decimal("0")
    stamp_duty_allocated: Decimal = Decimal("0")

    @property
    def gain_type(self) -> str:
        """Return 'LTCG' or 'STCG'."""
        return "LTCG" if self.is_long_term else "STCG"


class FIFOUnitTracker:
    """
    FIFO-based unit tracker for capital gains calculation.

    Tracks purchase lots and computes capital gains using FIFO
    (First-In-First-Out) matching when units are redeemed.

    Usage:
        tracker = FIFOUnitTracker(scheme_name, folio, asset_class)

        # Add purchases
        tracker.add_purchase(date(2020, 1, 1), Decimal("100"), Decimal("50"), Decimal("5000"))
        tracker.add_purchase(date(2021, 6, 1), Decimal("50"), Decimal("55"), Decimal("2750"))

        # Process redemption (FIFO matching)
        gains = tracker.process_redemption(
            date(2024, 1, 15),
            Decimal("75"),
            Decimal("70"),
            Decimal("5250")
        )

        for gain in gains:
            print(f"{gain.gain_type}: Rs. {gain.taxable_gain}")
    """

    def __init__(
        self,
        scheme_name: str,
        folio: str,
        asset_class: AssetClass,
        fmv_31jan2018: Optional[Decimal] = None
    ):
        self.scheme_name = scheme_name
        self.folio = folio
        self.asset_class = asset_class
        self.fmv_31jan2018 = fmv_31jan2018

        # FIFO queue of purchase lots
        self._lots: Deque[PurchaseLot] = deque()

        # Track all gains computed
        self._gains: List[GainResult] = []

        # Statistics
        self.total_purchased: Decimal = Decimal("0")
        self.total_redeemed: Decimal = Decimal("0")

    @property
    def available_units(self) -> Decimal:
        """Total units available for redemption."""
        return sum(lot.remaining_units for lot in self._lots)

    @property
    def total_gains(self) -> List[GainResult]:
        """All computed gains."""
        return self._gains.copy()

    @property
    def total_stcg(self) -> Decimal:
        """Total short-term capital gains."""
        return sum(g.taxable_gain for g in self._gains if not g.is_long_term)

    @property
    def total_ltcg(self) -> Decimal:
        """Total long-term capital gains."""
        return sum(g.taxable_gain for g in self._gains if g.is_long_term)

    def add_purchase(
        self,
        purchase_date: date,
        units: Decimal,
        nav: Decimal,
        amount: Decimal
    ):
        """
        Add a purchase lot to the FIFO queue.

        Args:
            purchase_date: Date of purchase
            units: Number of units purchased
            nav: NAV at purchase
            amount: Total purchase amount
        """
        if units <= Decimal("0"):
            logger.warning(f"Skipping non-positive purchase: {units} units on {purchase_date}")
            return

        lot = PurchaseLot(
            date=purchase_date,
            units=units,
            nav=nav,
            amount=amount
        )
        self._lots.append(lot)
        self.total_purchased += units

        logger.debug(
            f"Added purchase lot: {units} units @ {nav} on {purchase_date} "
            f"for {self.scheme_name}"
        )

    def process_redemption(
        self,
        sale_date: date,
        units: Decimal,
        nav: Decimal,
        amount: Decimal,
        stt: Decimal = Decimal("0"),
        stamp_duty: Decimal = Decimal("0")
    ) -> List[GainResult]:
        """
        Process a redemption using FIFO matching.

        Args:
            sale_date: Date of redemption
            units: Number of units redeemed (positive)
            nav: NAV at redemption
            amount: Total redemption amount
            stt: Securities Transaction Tax
            stamp_duty: Stamp duty paid

        Returns:
            List of GainResult for each matched lot

        Raises:
            FIFOMismatchError: If redemption units exceed available units
        """
        units = abs(units)  # Ensure positive
        remaining_units = units
        gains = []

        # Check if we have enough units
        if remaining_units > self.available_units + Decimal("0.01"):
            raise FIFOMismatchError(
                self.scheme_name,
                self.folio,
                str(units),
                str(self.available_units)
            )

        while remaining_units > Decimal("0.0001") and self._lots:
            lot = self._lots[0]

            # Determine units to match from this lot
            matched_units = min(remaining_units, lot.remaining_units)

            # Calculate proportional values
            sale_value = (amount * matched_units / units).quantize(
                Decimal("0.01"), ROUND_HALF_UP
            )
            purchase_value = (lot.amount * matched_units / lot.units).quantize(
                Decimal("0.01"), ROUND_HALF_UP
            )

            # Allocate STT and stamp duty proportionally
            stt_allocated = (stt * matched_units / units).quantize(
                Decimal("0.01"), ROUND_HALF_UP
            )
            stamp_allocated = (stamp_duty * matched_units / units).quantize(
                Decimal("0.01"), ROUND_HALF_UP
            )

            # Calculate capital gain
            gain = self._calculate_gain(
                lot=lot,
                matched_units=matched_units,
                sale_date=sale_date,
                sale_nav=nav,
                sale_value=sale_value,
                purchase_value=purchase_value,
                stt=stt_allocated,
                stamp_duty=stamp_allocated
            )

            gains.append(gain)
            self._gains.append(gain)

            # Update lot
            lot.remaining_units -= matched_units
            remaining_units -= matched_units

            # Remove exhausted lot
            if lot.is_exhausted:
                self._lots.popleft()

        self.total_redeemed += units

        logger.debug(
            f"Processed redemption: {units} units @ {nav} on {sale_date} "
            f"for {self.scheme_name}, {len(gains)} lots matched"
        )

        return gains

    def _calculate_gain(
        self,
        lot: PurchaseLot,
        matched_units: Decimal,
        sale_date: date,
        sale_nav: Decimal,
        sale_value: Decimal,
        purchase_value: Decimal,
        stt: Decimal,
        stamp_duty: Decimal
    ) -> GainResult:
        """Calculate capital gain for a matched lot."""
        holding_days = (sale_date - lot.date).days

        # Determine if long-term
        if self.asset_class == AssetClass.EQUITY:
            is_long_term = holding_days > EQUITY_LTCG_DAYS
        else:
            is_long_term = holding_days > DEBT_LTCG_DAYS

        # Calculate cost of acquisition (with grandfathering if applicable)
        coa, is_grandfathered, fmv_used = self._calculate_coa(
            lot=lot,
            matched_units=matched_units,
            sale_date=sale_date,
            sale_value=sale_value,
            purchase_value=purchase_value
        )

        # Realized gain = sale - purchase (always)
        realized_gain = sale_value - purchase_value

        # Taxable gain = sale - COA (only for LTCG when COA differs)
        taxable_gain = sale_value - coa

        return GainResult(
            purchase_date=lot.date,
            purchase_units=matched_units,
            purchase_nav=lot.nav,
            purchase_value=purchase_value,
            sale_date=sale_date,
            sale_units=matched_units,
            sale_nav=sale_nav,
            sale_value=sale_value,
            holding_days=holding_days,
            is_long_term=is_long_term,
            asset_class=self.asset_class,
            cost_of_acquisition=coa,
            realized_gain=realized_gain,
            taxable_gain=taxable_gain,
            is_grandfathered=is_grandfathered,
            fmv_31jan2018=fmv_used,
            stt_allocated=stt,
            stamp_duty_allocated=stamp_duty
        )

    def _calculate_coa(
        self,
        lot: PurchaseLot,
        matched_units: Decimal,
        sale_date: date,
        sale_value: Decimal,
        purchase_value: Decimal
    ) -> Tuple[Decimal, bool, Optional[Decimal]]:
        """
        Calculate Cost of Acquisition with grandfathering.

        Implements 3 scenarios for equity funds:
        1. Purchased before 31-Jan-2018, sold before 1-Apr-2018:
           COA = sale_value (no tax)
        2. Purchased before 31-Jan-2018, sold on/after 1-Apr-2018:
           COA = max(purchase_value, min(FMV_31jan2018, sale_value))
        3. Purchased on/after 31-Jan-2018:
           COA = purchase_value

        Returns:
            Tuple of (cost_of_acquisition, is_grandfathered, fmv_used)
        """
        # Only equity funds have grandfathering
        if self.asset_class != AssetClass.EQUITY:
            return purchase_value, False, None

        # Scenario 3: No grandfathering for post-31-Jan-2018 purchases
        if lot.date > GRANDFATHERING_DATE:
            return purchase_value, False, None

        # Scenario 1: Sold before Budget 2018 effective date
        if sale_date < BUDGET_2018_EFFECTIVE:
            # No tax on LTCG before April 2018
            return sale_value, True, None

        # Scenario 2: Grandfathering applies
        if self.fmv_31jan2018 is None:
            logger.warning(
                f"FMV for 31-Jan-2018 not available for {self.scheme_name}. "
                f"Using purchase value as COA."
            )
            return purchase_value, False, None

        # Calculate FMV for the matched units
        fmv_value = (self.fmv_31jan2018 * matched_units).quantize(
            Decimal("0.01"), ROUND_HALF_UP
        )

        # COA = max(purchase_value, min(FMV, sale_value))
        coa = max(purchase_value, min(fmv_value, sale_value))

        logger.debug(
            f"Grandfathering applied: purchase={purchase_value}, "
            f"FMV={fmv_value}, sale={sale_value}, COA={coa}"
        )

        return coa, True, fmv_value

    def get_summary(self) -> Dict:
        """Get summary of FIFO tracking."""
        return {
            "scheme": self.scheme_name,
            "folio": self.folio,
            "asset_class": self.asset_class.value,
            "total_purchased": str(self.total_purchased),
            "total_redeemed": str(self.total_redeemed),
            "available_units": str(self.available_units),
            "lots_count": len(self._lots),
            "gains_count": len(self._gains),
            "total_stcg": str(self.total_stcg),
            "total_ltcg": str(self.total_ltcg),
        }


class PortfolioFIFOTracker:
    """
    Manages FIFO trackers for an entire portfolio.

    Creates and maintains separate FIFOUnitTracker for each
    scheme/folio combination.
    """

    def __init__(self):
        # Key: (folio, scheme_name) -> FIFOUnitTracker
        self._trackers: Dict[Tuple[str, str], FIFOUnitTracker] = {}

    def get_tracker(
        self,
        folio: str,
        scheme_name: str,
        asset_class: AssetClass,
        fmv_31jan2018: Optional[Decimal] = None
    ) -> FIFOUnitTracker:
        """
        Get or create a FIFO tracker for a scheme/folio.

        Args:
            folio: Folio number
            scheme_name: Scheme name
            asset_class: Asset class for LTCG rules
            fmv_31jan2018: FMV on 31-Jan-2018 for grandfathering

        Returns:
            FIFOUnitTracker for the scheme/folio
        """
        key = (folio, scheme_name)

        if key not in self._trackers:
            self._trackers[key] = FIFOUnitTracker(
                scheme_name=scheme_name,
                folio=folio,
                asset_class=asset_class,
                fmv_31jan2018=fmv_31jan2018
            )

        return self._trackers[key]

    def process_transaction(
        self,
        folio: str,
        scheme_name: str,
        asset_class: AssetClass,
        txn_type: TransactionType,
        txn_date: date,
        units: Decimal,
        nav: Decimal,
        amount: Decimal,
        fmv_31jan2018: Optional[Decimal] = None,
        stt: Decimal = Decimal("0"),
        stamp_duty: Decimal = Decimal("0")
    ) -> Optional[List[GainResult]]:
        """
        Process a transaction (purchase or redemption).

        Args:
            folio: Folio number
            scheme_name: Scheme name
            asset_class: Asset class
            txn_type: Transaction type
            txn_date: Transaction date
            units: Number of units
            nav: NAV
            amount: Transaction amount
            fmv_31jan2018: FMV on 31-Jan-2018
            stt: Securities Transaction Tax
            stamp_duty: Stamp duty

        Returns:
            List of GainResult for redemptions, None for purchases
        """
        tracker = self.get_tracker(folio, scheme_name, asset_class, fmv_31jan2018)

        # Purchase transactions
        if txn_type in (
            TransactionType.PURCHASE,
            TransactionType.PURCHASE_SIP,
            TransactionType.SWITCH_IN,
            TransactionType.SWITCH_IN_MERGER,
            TransactionType.DIVIDEND_REINVEST
        ):
            tracker.add_purchase(txn_date, abs(units), nav, abs(amount))
            return None

        # Redemption transactions
        if txn_type in (
            TransactionType.REDEMPTION,
            TransactionType.SWITCH_OUT,
            TransactionType.SWITCH_OUT_MERGER
        ):
            return tracker.process_redemption(
                txn_date, abs(units), nav, abs(amount), stt, stamp_duty
            )

        return None

    def get_all_gains(self) -> List[GainResult]:
        """Get all computed gains across all trackers."""
        all_gains = []
        for tracker in self._trackers.values():
            all_gains.extend(tracker.total_gains)
        return all_gains

    def get_summary(self) -> Dict:
        """Get portfolio-level summary."""
        total_stcg = sum(t.total_stcg for t in self._trackers.values())
        total_ltcg = sum(t.total_ltcg for t in self._trackers.values())

        return {
            "schemes_tracked": len(self._trackers),
            "total_stcg": str(total_stcg),
            "total_ltcg": str(total_ltcg),
            "total_gains": str(total_stcg + total_ltcg),
            "trackers": [t.get_summary() for t in self._trackers.values()]
        }
