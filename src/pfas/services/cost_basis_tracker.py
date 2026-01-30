"""
Cost Basis Tracker for Inventory Accounting.

Provides FIFO and Average Cost tracking for:
- Mutual Fund units
- Indian stocks
- Foreign stocks (RSU, ESPP)

Ensures ledger entries stay in sync with holdings tables.
"""

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Optional, List, Dict, Tuple

from pfas.core.exceptions import InsufficientSharesError, AccountingBalanceError

logger = logging.getLogger(__name__)


class CostMethod(Enum):
    """Cost basis calculation method."""
    FIFO = "FIFO"  # First In, First Out
    AVERAGE = "AVERAGE"  # Weighted Average Cost


@dataclass
class Lot:
    """
    Represents a purchase lot for FIFO tracking.

    A lot tracks units acquired at a specific price and date.
    """
    lot_id: int
    acquisition_date: date
    units_acquired: Decimal
    units_remaining: Decimal
    cost_per_unit: Decimal
    total_cost: Decimal
    currency: str = "INR"
    reference: str = ""  # e.g., folio number, trade ID

    @property
    def is_depleted(self) -> bool:
        """Check if lot is fully sold."""
        return self.units_remaining <= Decimal("0.0001")


@dataclass
class CostBasisResult:
    """Result of cost basis calculation for a sale."""
    units_sold: Decimal
    total_cost_basis: Decimal
    cost_per_unit: Decimal
    matched_lots: List[Dict] = field(default_factory=list)
    realized_gain: Optional[Decimal] = None
    is_long_term: bool = False
    holding_period_days: int = 0


@dataclass
class HoldingSummary:
    """Summary of holdings with cost basis."""
    symbol: str
    total_units: Decimal
    total_cost: Decimal
    average_cost_per_unit: Decimal
    lots: List[Lot] = field(default_factory=list)


class CostBasisTracker:
    """
    Tracks cost basis for investment holdings using FIFO or Average Cost.

    Features:
    - FIFO lot tracking with automatic depletion
    - Weighted average cost calculation
    - Holding period tracking for LTCG/STCG determination
    - Sync validation between ledger and holdings tables

    Indian Tax Context:
    - Equity MF/Stocks: LTCG if held > 12 months
    - Debt MF: Taxed at slab rate regardless of holding period
    - Foreign Stocks: LTCG if held > 24 months
    """

    # Holding period thresholds (in days)
    EQUITY_LTCG_DAYS = 365  # 12 months
    DEBT_LTCG_DAYS = 730  # 24 months (historical, now slab rate)
    FOREIGN_LTCG_DAYS = 730  # 24 months

    def __init__(
        self,
        db_connection: sqlite3.Connection,
        cost_method: CostMethod = CostMethod.FIFO
    ):
        """
        Initialize cost basis tracker.

        Args:
            db_connection: Database connection
            cost_method: FIFO or AVERAGE cost method
        """
        self.conn = db_connection
        self.cost_method = cost_method
        self._lots_cache: Dict[str, List[Lot]] = {}

    def record_purchase(
        self,
        user_id: int,
        asset_type: str,
        symbol: str,
        purchase_date: date,
        units: Decimal,
        total_cost: Decimal,
        reference: str = "",
        currency: str = "INR"
    ) -> int:
        """
        Record a purchase and create a new lot.

        Args:
            user_id: User ID
            asset_type: 'MF_EQUITY', 'MF_DEBT', 'STOCK', 'FOREIGN_STOCK'
            symbol: Symbol/ISIN/folio identifier
            purchase_date: Purchase date
            units: Units purchased
            total_cost: Total cost including fees
            reference: Reference ID (folio, trade ID)
            currency: Currency code

        Returns:
            Lot ID
        """
        cost_per_unit = (total_cost / units).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_UP
        )

        cursor = self.conn.execute(
            """INSERT INTO cost_basis_lots
            (user_id, asset_type, symbol, acquisition_date, units_acquired,
             units_remaining, cost_per_unit, total_cost, currency, reference)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                user_id,
                asset_type,
                symbol,
                purchase_date.isoformat(),
                str(units),
                str(units),  # Initially all units remaining
                str(cost_per_unit),
                str(total_cost),
                currency,
                reference,
            )
        )

        lot_id = cursor.lastrowid

        # Invalidate cache for this symbol
        cache_key = f"{user_id}:{asset_type}:{symbol}"
        if cache_key in self._lots_cache:
            del self._lots_cache[cache_key]

        logger.debug(f"Created lot {lot_id}: {units} units of {symbol} @ {cost_per_unit}")
        return lot_id

    def calculate_cost_basis(
        self,
        user_id: int,
        asset_type: str,
        symbol: str,
        units_to_sell: Decimal,
        sell_date: date,
        sale_proceeds: Optional[Decimal] = None
    ) -> CostBasisResult:
        """
        Calculate cost basis for a sale using configured method.

        Args:
            user_id: User ID
            asset_type: Asset type
            symbol: Symbol
            units_to_sell: Units being sold
            sell_date: Sale date
            sale_proceeds: Optional sale proceeds for gain calculation

        Returns:
            CostBasisResult with cost basis and matched lots

        Raises:
            InsufficientSharesError: If not enough units available
        """
        if self.cost_method == CostMethod.FIFO:
            return self._calculate_fifo_cost(
                user_id, asset_type, symbol, units_to_sell, sell_date, sale_proceeds
            )
        else:
            return self._calculate_average_cost(
                user_id, asset_type, symbol, units_to_sell, sell_date, sale_proceeds
            )

    def _calculate_fifo_cost(
        self,
        user_id: int,
        asset_type: str,
        symbol: str,
        units_to_sell: Decimal,
        sell_date: date,
        sale_proceeds: Optional[Decimal]
    ) -> CostBasisResult:
        """Calculate cost basis using FIFO method."""
        lots = self._load_lots(user_id, asset_type, symbol)

        # Check total available
        total_available = sum(lot.units_remaining for lot in lots)
        if total_available < units_to_sell:
            raise InsufficientSharesError(
                symbol=symbol,
                requested=str(units_to_sell),
                available=str(total_available)
            )

        matched_lots = []
        remaining_to_sell = units_to_sell
        total_cost = Decimal("0")
        earliest_date = None

        for lot in lots:
            if remaining_to_sell <= Decimal("0"):
                break

            if lot.units_remaining <= Decimal("0"):
                continue

            units_from_lot = min(remaining_to_sell, lot.units_remaining)
            cost_from_lot = units_from_lot * lot.cost_per_unit

            matched_lots.append({
                'lot_id': lot.lot_id,
                'acquisition_date': lot.acquisition_date,
                'units_used': units_from_lot,
                'cost_per_unit': lot.cost_per_unit,
                'cost_total': cost_from_lot,
                'holding_days': (sell_date - lot.acquisition_date).days,
            })

            total_cost += cost_from_lot
            remaining_to_sell -= units_from_lot

            # Track earliest acquisition date for holding period
            if earliest_date is None or lot.acquisition_date < earliest_date:
                earliest_date = lot.acquisition_date

        # Determine holding period and LTCG status
        holding_days = (sell_date - earliest_date).days if earliest_date else 0
        ltcg_threshold = self._get_ltcg_threshold(asset_type)
        is_long_term = holding_days > ltcg_threshold

        cost_per_unit = (total_cost / units_to_sell).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_UP
        )

        result = CostBasisResult(
            units_sold=units_to_sell,
            total_cost_basis=total_cost,
            cost_per_unit=cost_per_unit,
            matched_lots=matched_lots,
            is_long_term=is_long_term,
            holding_period_days=holding_days,
        )

        if sale_proceeds is not None:
            result.realized_gain = sale_proceeds - total_cost

        return result

    def _calculate_average_cost(
        self,
        user_id: int,
        asset_type: str,
        symbol: str,
        units_to_sell: Decimal,
        sell_date: date,
        sale_proceeds: Optional[Decimal]
    ) -> CostBasisResult:
        """Calculate cost basis using weighted average cost method."""
        lots = self._load_lots(user_id, asset_type, symbol)

        # Calculate weighted average
        total_units = sum(lot.units_remaining for lot in lots)
        total_cost = sum(lot.units_remaining * lot.cost_per_unit for lot in lots)

        if total_units < units_to_sell:
            raise InsufficientSharesError(
                symbol=symbol,
                requested=str(units_to_sell),
                available=str(total_units)
            )

        if total_units > Decimal("0"):
            avg_cost_per_unit = (total_cost / total_units).quantize(
                Decimal("0.0001"), rounding=ROUND_HALF_UP
            )
        else:
            avg_cost_per_unit = Decimal("0")

        cost_basis = units_to_sell * avg_cost_per_unit

        # For average cost, use weighted average holding period
        weighted_days = Decimal("0")
        for lot in lots:
            if lot.units_remaining > Decimal("0"):
                days = (sell_date - lot.acquisition_date).days
                weight = lot.units_remaining / total_units
                weighted_days += Decimal(days) * weight

        holding_days = int(weighted_days)
        ltcg_threshold = self._get_ltcg_threshold(asset_type)
        is_long_term = holding_days > ltcg_threshold

        result = CostBasisResult(
            units_sold=units_to_sell,
            total_cost_basis=cost_basis,
            cost_per_unit=avg_cost_per_unit,
            matched_lots=[{
                'method': 'AVERAGE',
                'total_units': str(total_units),
                'avg_cost_per_unit': str(avg_cost_per_unit),
            }],
            is_long_term=is_long_term,
            holding_period_days=holding_days,
        )

        if sale_proceeds is not None:
            result.realized_gain = sale_proceeds - cost_basis

        return result

    def deplete_lots(
        self,
        user_id: int,
        asset_type: str,
        symbol: str,
        cost_result: CostBasisResult
    ) -> None:
        """
        Deplete lots after a sale is recorded.

        Should be called after successful ledger entry.

        Args:
            user_id: User ID
            asset_type: Asset type
            symbol: Symbol
            cost_result: Result from calculate_cost_basis
        """
        if self.cost_method == CostMethod.FIFO:
            for match in cost_result.matched_lots:
                lot_id = match['lot_id']
                units_used = match['units_used']

                self.conn.execute(
                    """UPDATE cost_basis_lots
                    SET units_remaining = units_remaining - ?
                    WHERE id = ? AND user_id = ?""",
                    (str(units_used), lot_id, user_id)
                )
        else:
            # For average cost, deplete proportionally from all lots
            lots = self._load_lots(user_id, asset_type, symbol)
            total_units = sum(lot.units_remaining for lot in lots)

            for lot in lots:
                if lot.units_remaining > Decimal("0"):
                    proportion = lot.units_remaining / total_units
                    units_to_deplete = cost_result.units_sold * proportion

                    self.conn.execute(
                        """UPDATE cost_basis_lots
                        SET units_remaining = units_remaining - ?
                        WHERE id = ? AND user_id = ?""",
                        (str(units_to_deplete), lot.lot_id, user_id)
                    )

        # Invalidate cache
        cache_key = f"{user_id}:{asset_type}:{symbol}"
        if cache_key in self._lots_cache:
            del self._lots_cache[cache_key]

    def get_holding_summary(
        self,
        user_id: int,
        asset_type: str,
        symbol: str
    ) -> HoldingSummary:
        """
        Get holding summary with cost basis.

        Args:
            user_id: User ID
            asset_type: Asset type
            symbol: Symbol

        Returns:
            HoldingSummary with all lots and totals
        """
        lots = self._load_lots(user_id, asset_type, symbol)

        total_units = sum(lot.units_remaining for lot in lots if lot.units_remaining > Decimal("0"))
        total_cost = sum(
            lot.units_remaining * lot.cost_per_unit
            for lot in lots if lot.units_remaining > Decimal("0")
        )

        avg_cost = Decimal("0")
        if total_units > Decimal("0"):
            avg_cost = (total_cost / total_units).quantize(
                Decimal("0.0001"), rounding=ROUND_HALF_UP
            )

        return HoldingSummary(
            symbol=symbol,
            total_units=total_units,
            total_cost=total_cost,
            average_cost_per_unit=avg_cost,
            lots=[lot for lot in lots if lot.units_remaining > Decimal("0")],
        )

    def validate_ledger_sync(
        self,
        user_id: int,
        asset_type: str,
        symbol: str,
        expected_units: Decimal,
        tolerance: Decimal = Decimal("0.01")
    ) -> bool:
        """
        Validate that lot tracker matches expected holdings.

        Args:
            user_id: User ID
            asset_type: Asset type
            symbol: Symbol
            expected_units: Expected units from holdings table
            tolerance: Acceptable difference

        Returns:
            True if in sync, raises AccountingBalanceError otherwise
        """
        summary = self.get_holding_summary(user_id, asset_type, symbol)
        difference = abs(summary.total_units - expected_units)

        if difference > tolerance:
            raise AccountingBalanceError(
                message=f"Cost basis tracker out of sync for {symbol}",
                expected=str(expected_units),
                actual=str(summary.total_units),
                difference=str(difference),
            )

        return True

    def _load_lots(
        self,
        user_id: int,
        asset_type: str,
        symbol: str
    ) -> List[Lot]:
        """Load lots from database, using cache if available."""
        cache_key = f"{user_id}:{asset_type}:{symbol}"

        if cache_key in self._lots_cache:
            return self._lots_cache[cache_key]

        cursor = self.conn.execute(
            """SELECT id, acquisition_date, units_acquired, units_remaining,
                      cost_per_unit, total_cost, currency, reference
            FROM cost_basis_lots
            WHERE user_id = ?
                AND asset_type = ?
                AND symbol = ?
                AND units_remaining > 0
            ORDER BY acquisition_date ASC""",  # FIFO order
            (user_id, asset_type, symbol)
        )

        lots = []
        for row in cursor.fetchall():
            lots.append(Lot(
                lot_id=row['id'],
                acquisition_date=date.fromisoformat(row['acquisition_date'])
                    if isinstance(row['acquisition_date'], str)
                    else row['acquisition_date'],
                units_acquired=Decimal(str(row['units_acquired'])),
                units_remaining=Decimal(str(row['units_remaining'])),
                cost_per_unit=Decimal(str(row['cost_per_unit'])),
                total_cost=Decimal(str(row['total_cost'])),
                currency=row['currency'],
                reference=row['reference'] or "",
            ))

        self._lots_cache[cache_key] = lots
        return lots

    def _get_ltcg_threshold(self, asset_type: str) -> int:
        """Get LTCG threshold days based on asset type."""
        if asset_type in ('MF_EQUITY', 'STOCK'):
            return self.EQUITY_LTCG_DAYS
        elif asset_type in ('MF_DEBT',):
            return self.DEBT_LTCG_DAYS
        elif asset_type in ('FOREIGN_STOCK', 'RSU', 'ESPP'):
            return self.FOREIGN_LTCG_DAYS
        else:
            return self.EQUITY_LTCG_DAYS  # Default


def setup_cost_basis_table(conn: sqlite3.Connection) -> None:
    """
    Create cost basis lots table if not exists.

    Args:
        conn: Database connection
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cost_basis_lots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            asset_type TEXT NOT NULL,
            symbol TEXT NOT NULL,
            acquisition_date TEXT NOT NULL,
            units_acquired TEXT NOT NULL,
            units_remaining TEXT NOT NULL,
            cost_per_unit TEXT NOT NULL,
            total_cost TEXT NOT NULL,
            currency TEXT DEFAULT 'INR',
            reference TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_cost_lots_user_symbol
        ON cost_basis_lots(user_id, asset_type, symbol)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_cost_lots_date
        ON cost_basis_lots(user_id, acquisition_date)
    """)

    conn.commit()
