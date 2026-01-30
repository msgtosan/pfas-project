"""
Cross Correlator for Golden Reference Reconciliation.

Compares system (book) balances against golden (CAS reported) balances.
Generates reconciliation events and manages suspense items.
"""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional, Dict, Tuple, Any
import sqlite3

from .models import (
    MetricType,
    AssetClass,
    SourceType,
    MatchResult,
    ReconciliationStatus,
    Severity,
    SuspenseStatus,
    GoldenHolding,
    SystemHolding,
    ReconciliationEvent,
    ReconciliationSummary,
    SuspenseItem,
)
from .truth_resolver import TruthResolver

logger = logging.getLogger(__name__)


@dataclass
class ReconciliationConfig:
    """Configuration for reconciliation behavior."""

    # Tolerance settings
    absolute_tolerance: Decimal = Decimal("0.01")  # Absolute difference tolerance
    percentage_tolerance: Decimal = Decimal("0.001")  # 0.1% percentage tolerance

    # Behavior settings
    create_suspense_on_mismatch: bool = True
    auto_resolve_within_tolerance: bool = True
    log_all_comparisons: bool = False

    # Severity thresholds
    warning_threshold: Decimal = Decimal("100")  # INR
    error_threshold: Decimal = Decimal("1000")  # INR
    critical_threshold: Decimal = Decimal("10000")  # INR


class CrossCorrelator:
    """
    Cross-correlates system holdings against golden reference holdings.

    Performs reconciliation by:
    1. Matching holdings by ISIN/FolioNumber/Symbol
    2. Comparing values with configurable tolerance (default 0.01)
    3. Generating reconciliation events for each comparison
    4. Creating suspense items for unresolved mismatches
    5. Logging results to audit trail

    Usage:
        correlator = CrossCorrelator(conn, user_id=1)

        # Reconcile MF holdings
        summary = correlator.reconcile_holdings(
            asset_class=AssetClass.MUTUAL_FUND,
            golden_ref_id=123,
            as_of_date=date.today()
        )

        print(f"Match rate: {summary.match_rate:.1f}%")
        print(f"Mismatches: {summary.mismatches}")

        # Get open suspense items
        suspense = correlator.get_open_suspense(AssetClass.MUTUAL_FUND)
    """

    def __init__(
        self,
        db_connection: sqlite3.Connection,
        user_id: int,
        config: Optional[ReconciliationConfig] = None,
        truth_resolver: Optional[TruthResolver] = None
    ):
        """
        Initialize CrossCorrelator.

        Args:
            db_connection: Database connection
            user_id: User ID
            config: Optional reconciliation configuration
            truth_resolver: Optional TruthResolver instance
        """
        self.conn = db_connection
        self.user_id = user_id
        self.config = config or ReconciliationConfig()
        self.truth_resolver = truth_resolver or TruthResolver(db_connection, user_id)

    def reconcile_holdings(
        self,
        asset_class: AssetClass,
        golden_ref_id: int,
        as_of_date: Optional[date] = None,
        metric_type: MetricType = MetricType.NET_WORTH
    ) -> ReconciliationSummary:
        """
        Reconcile system holdings against golden reference holdings.

        Args:
            asset_class: Asset class to reconcile
            golden_ref_id: ID of golden reference to reconcile against
            as_of_date: Date for comparison (default: today)
            metric_type: Type of metric to reconcile

        Returns:
            ReconciliationSummary with comparison results
        """
        if as_of_date is None:
            as_of_date = date.today()

        # Get golden reference info
        golden_ref = self._get_golden_reference(golden_ref_id)
        if not golden_ref:
            raise ValueError(f"Golden reference {golden_ref_id} not found")

        source_type = SourceType(golden_ref["source_type"])

        # Load holdings
        golden_holdings = self._load_golden_holdings(golden_ref_id, asset_class)
        system_holdings = self._load_system_holdings(asset_class, as_of_date)

        # Create summary
        summary = ReconciliationSummary(
            user_id=self.user_id,
            reconciliation_date=as_of_date,
            asset_class=asset_class,
            source_type=source_type,
            golden_ref_id=golden_ref_id,
        )

        # Build lookup maps
        golden_map = {h.reconciliation_key: h for h in golden_holdings}
        system_map = {h.reconciliation_key: h for h in system_holdings}

        all_keys = set(golden_map.keys()) | set(system_map.keys())
        summary.total_items = len(all_keys)

        # Compare each holding
        for key in all_keys:
            golden = golden_map.get(key)
            system = system_map.get(key)

            event = self._compare_holding(
                golden=golden,
                system=system,
                key=key,
                metric_type=metric_type,
                asset_class=asset_class,
                source_type=source_type,
                golden_ref_id=golden_ref_id,
                as_of_date=as_of_date,
            )

            # Update summary counts
            if event.match_result == MatchResult.EXACT:
                summary.matched_exact += 1
            elif event.match_result == MatchResult.WITHIN_TOLERANCE:
                summary.matched_tolerance += 1
            elif event.match_result == MatchResult.MISMATCH:
                summary.mismatches += 1
            elif event.match_result == MatchResult.MISSING_SYSTEM:
                summary.missing_system += 1
            elif event.match_result == MatchResult.MISSING_GOLDEN:
                summary.missing_golden += 1

            # Update value totals
            if event.system_value:
                summary.total_system_value += event.system_value
            if event.golden_value:
                summary.total_golden_value += event.golden_value
            if event.difference:
                summary.total_difference += abs(event.difference)

            # Save event
            event_id = self._save_reconciliation_event(event)
            event.id = event_id
            summary.events.append(event)

            # Create suspense if needed
            if (event.match_result == MatchResult.MISMATCH and
                self.config.create_suspense_on_mismatch):
                self._create_suspense_item(event, golden, system)

        logger.info(
            f"Reconciliation complete for {asset_class.value}: "
            f"{summary.matched_exact + summary.matched_tolerance}/{summary.total_items} matched "
            f"({summary.match_rate:.1f}%)"
        )

        return summary

    def _compare_holding(
        self,
        golden: Optional[GoldenHolding],
        system: Optional[SystemHolding],
        key: str,
        metric_type: MetricType,
        asset_class: AssetClass,
        source_type: SourceType,
        golden_ref_id: int,
        as_of_date: date
    ) -> ReconciliationEvent:
        """Compare a single holding and generate reconciliation event."""

        event = ReconciliationEvent(
            user_id=self.user_id,
            reconciliation_date=as_of_date,
            metric_type=metric_type,
            asset_class=asset_class,
            source_type=source_type,
            golden_ref_id=golden_ref_id,
            tolerance_used=self.config.absolute_tolerance,
        )

        # Extract identification from key
        if key.startswith("ISIN:"):
            event.isin = key[5:]
        elif key.startswith("FOLIO:"):
            event.folio_number = key[6:]
        elif key.startswith("SYMBOL:"):
            event.symbol = key[7:]

        # Handle missing cases
        if golden is None:
            event.system_value = system.market_value if system else Decimal("0")
            event.golden_value = None
            event.match_result = MatchResult.MISSING_GOLDEN
            event.status = ReconciliationStatus.MISMATCH
            event.severity = self._determine_severity(event.system_value or Decimal("0"))
            return event

        if system is None:
            event.golden_value = golden.market_value
            event.system_value = None
            event.match_result = MatchResult.MISSING_SYSTEM
            event.status = ReconciliationStatus.MISMATCH
            event.severity = self._determine_severity(golden.market_value)
            return event

        # Both exist - compare values
        if metric_type == MetricType.UNITS:
            event.system_value = system.units
            event.golden_value = golden.units
        elif metric_type == MetricType.COST_BASIS:
            event.system_value = system.cost_basis
            event.golden_value = golden.cost_basis or Decimal("0")
        else:  # NET_WORTH
            event.system_value = system.market_value
            event.golden_value = golden.market_value

        event.calculate_difference()

        # Determine match result
        if event.difference is None:
            event.match_result = MatchResult.NOT_APPLICABLE
        elif event.difference == Decimal("0"):
            event.match_result = MatchResult.EXACT
            event.status = ReconciliationStatus.MATCHED
            event.severity = Severity.INFO
        elif abs(event.difference) <= self.config.absolute_tolerance:
            event.match_result = MatchResult.WITHIN_TOLERANCE
            event.status = ReconciliationStatus.MATCHED
            event.severity = Severity.INFO
            if self.config.auto_resolve_within_tolerance:
                event.resolved_at = datetime.now()
                event.resolution_action = "AUTO_RESOLVED"
                event.resolution_notes = f"Within tolerance of {self.config.absolute_tolerance}"
        elif (event.difference_pct is not None and
              abs(event.difference_pct) <= self.config.percentage_tolerance * 100):
            event.match_result = MatchResult.WITHIN_TOLERANCE
            event.status = ReconciliationStatus.MATCHED
            event.severity = Severity.INFO
        else:
            event.match_result = MatchResult.MISMATCH
            event.status = ReconciliationStatus.MISMATCH
            event.severity = self._determine_severity(abs(event.difference))

        return event

    def _determine_severity(self, difference: Decimal) -> Severity:
        """Determine severity level based on difference amount."""
        abs_diff = abs(difference)
        if abs_diff >= self.config.critical_threshold:
            return Severity.CRITICAL
        elif abs_diff >= self.config.error_threshold:
            return Severity.ERROR
        elif abs_diff >= self.config.warning_threshold:
            return Severity.WARNING
        return Severity.INFO

    def _load_golden_holdings(
        self,
        golden_ref_id: int,
        asset_class: AssetClass
    ) -> List[GoldenHolding]:
        """Load golden holdings from database."""
        cursor = self.conn.execute("""
            SELECT * FROM golden_holdings
            WHERE golden_ref_id = ? AND asset_type = ? AND user_id = ?
        """, (golden_ref_id, asset_class.value, self.user_id))

        holdings = []
        for row in cursor.fetchall():
            cols = [desc[0] for desc in cursor.description]
            row_dict = dict(zip(cols, row))

            holding = GoldenHolding(
                id=row_dict.get("id"),
                golden_ref_id=row_dict.get("golden_ref_id"),
                user_id=row_dict.get("user_id"),
                asset_type=AssetClass(row_dict.get("asset_type")),
                isin=row_dict.get("isin"),
                symbol=row_dict.get("symbol"),
                name=row_dict.get("name", ""),
                folio_number=row_dict.get("folio_number"),
                units=Decimal(str(row_dict.get("units", 0))),
                nav=Decimal(str(row_dict.get("nav", 0))) if row_dict.get("nav") else None,
                market_value=Decimal(str(row_dict.get("market_value", 0))),
                cost_basis=Decimal(str(row_dict.get("cost_basis", 0))) if row_dict.get("cost_basis") else None,
                currency=row_dict.get("currency", "INR"),
                exchange_rate=Decimal(str(row_dict.get("exchange_rate", 1))),
                as_of_date=date.fromisoformat(row_dict.get("as_of_date")) if row_dict.get("as_of_date") else date.today(),
            )
            holdings.append(holding)

        return holdings

    def _load_system_holdings(
        self,
        asset_class: AssetClass,
        as_of_date: date
    ) -> List[SystemHolding]:
        """Load system holdings from database based on asset class."""
        holdings = []

        if asset_class == AssetClass.MUTUAL_FUND:
            holdings = self._load_mf_holdings(as_of_date)
        elif asset_class == AssetClass.STOCKS:
            holdings = self._load_stock_holdings(as_of_date)
        elif asset_class == AssetClass.NPS:
            holdings = self._load_nps_holdings(as_of_date)
        # Add more asset classes as needed

        return holdings

    def _load_mf_holdings(self, as_of_date: date) -> List[SystemHolding]:
        """Load mutual fund holdings from system."""
        holdings = []

        # First try mf_holdings table (current snapshot)
        cursor = self.conn.execute("""
            SELECT
                isin, scheme_name, folio_number, units, nav,
                current_value, cost_value, statement_date
            FROM mf_holdings
            WHERE user_id = ? AND units > 0.001
        """, (self.user_id,))

        for row in cursor.fetchall():
            cols = ["isin", "scheme_name", "folio_number", "units", "nav",
                   "current_value", "cost_value", "statement_date"]
            row_dict = dict(zip(cols, row))

            units = Decimal(str(row_dict["units"])) if row_dict["units"] else Decimal("0")
            nav = Decimal(str(row_dict["nav"])) if row_dict["nav"] else None
            market_value = Decimal(str(row_dict["current_value"])) if row_dict["current_value"] else Decimal("0")
            cost = Decimal(str(row_dict["cost_value"])) if row_dict["cost_value"] else Decimal("0")

            holding = SystemHolding(
                asset_type=AssetClass.MUTUAL_FUND,
                isin=row_dict["isin"],
                name=row_dict["scheme_name"] or "",
                folio_number=row_dict["folio_number"],
                units=units,
                nav=nav,
                market_value=market_value,
                cost_basis=cost,
                unrealized_gain=market_value - cost if market_value and cost else None,
                as_of_date=date.fromisoformat(row_dict["statement_date"]) if row_dict["statement_date"] else as_of_date,
            )
            holdings.append(holding)

        # If no data in mf_holdings, fall back to transaction calculation
        if not holdings:
            cursor = self.conn.execute("""
                SELECT
                    ms.isin,
                    ms.name,
                    mf.folio_number,
                    SUM(CASE
                        WHEN mt.transaction_type IN ('PURCHASE', 'PURCHASE_SIP', 'SWITCH_IN', 'DIVIDEND_REINVEST')
                        THEN mt.units
                        ELSE -mt.units
                    END) as total_units,
                    SUM(CASE
                        WHEN mt.transaction_type IN ('PURCHASE', 'PURCHASE_SIP', 'SWITCH_IN', 'DIVIDEND_REINVEST')
                        THEN mt.amount
                        ELSE 0
                    END) as total_cost
                FROM mf_transactions mt
                JOIN mf_folios mf ON mt.folio_id = mf.id
                JOIN mf_schemes ms ON mf.scheme_id = ms.id
                WHERE mf.user_id = ? AND mt.date <= ?
                GROUP BY ms.isin, ms.name, mf.folio_number
                HAVING total_units > 0.001
            """, (self.user_id, as_of_date.isoformat()))

            for row in cursor.fetchall():
                units = Decimal(str(row[3])) if row[3] else Decimal("0")
                cost = Decimal(str(row[4])) if row[4] else Decimal("0")

                # Get latest NAV for valuation
                nav = self._get_latest_nav(row[0], as_of_date)  # isin
                market_value = units * nav if nav else Decimal("0")

                holding = SystemHolding(
                    asset_type=AssetClass.MUTUAL_FUND,
                    isin=row[0],
                    name=row[1] or "",
                    folio_number=row[2],
                    units=units,
                    nav=nav,
                    market_value=market_value,
                    cost_basis=cost,
                    unrealized_gain=market_value - cost if market_value else None,
                    as_of_date=as_of_date,
                )
                holdings.append(holding)

        return holdings

    def _load_stock_holdings(self, as_of_date: date) -> List[SystemHolding]:
        """Load stock holdings from system."""
        holdings = []

        # First try stock_holdings table (current snapshot)
        cursor = self.conn.execute("""
            SELECT
                symbol, isin, company_name, quantity_held,
                average_buy_price, total_cost_basis,
                current_price, market_value, as_of_date
            FROM stock_holdings
            WHERE user_id = ? AND quantity_held > 0
        """, (self.user_id,))

        for row in cursor.fetchall():
            cols = ["symbol", "isin", "company_name", "quantity_held",
                   "average_buy_price", "total_cost_basis",
                   "current_price", "market_value", "as_of_date"]
            row_dict = dict(zip(cols, row))

            holding = SystemHolding(
                asset_type=AssetClass.STOCKS,
                symbol=row_dict["symbol"],
                isin=row_dict["isin"],
                name=row_dict["company_name"] or row_dict["symbol"] or "",
                units=Decimal(str(row_dict["quantity_held"])) if row_dict["quantity_held"] else Decimal("0"),
                nav=Decimal(str(row_dict["current_price"])) if row_dict["current_price"] else None,
                market_value=Decimal(str(row_dict["market_value"])) if row_dict["market_value"] else Decimal("0"),
                cost_basis=Decimal(str(row_dict["total_cost_basis"])) if row_dict["total_cost_basis"] else Decimal("0"),
                as_of_date=date.fromisoformat(row_dict["as_of_date"]) if row_dict["as_of_date"] else as_of_date,
            )
            holdings.append(holding)

        # If no data in stock_holdings, fall back to stock_trades calculation
        if not holdings:
            cursor = self.conn.execute("""
                SELECT
                    symbol,
                    isin,
                    MAX(COALESCE(security_name, symbol)) as security_name,
                    SUM(CASE WHEN trade_type = 'BUY' THEN quantity ELSE -quantity END) as total_qty,
                    SUM(CASE WHEN trade_type = 'BUY' THEN quantity * price ELSE 0 END) as total_cost
                FROM stock_trades
                WHERE user_id = ? AND trade_date <= ?
                GROUP BY symbol, isin
                HAVING total_qty > 0
            """, (self.user_id, as_of_date.isoformat()))

            for row in cursor.fetchall():
                cols = ["symbol", "isin", "security_name", "total_qty", "total_cost"]
                row_dict = dict(zip(cols, row))

                units = Decimal(str(row_dict["total_qty"])) if row_dict["total_qty"] else Decimal("0")
                cost = Decimal(str(row_dict["total_cost"])) if row_dict["total_cost"] else Decimal("0")

                holding = SystemHolding(
                    asset_type=AssetClass.STOCKS,
                    symbol=row_dict["symbol"],
                    isin=row_dict["isin"],
                    name=row_dict["security_name"] or row_dict["symbol"] or "",
                    units=units,
                    market_value=Decimal("0"),  # Would need price feed
                    cost_basis=cost,
                    as_of_date=as_of_date,
                )
                holdings.append(holding)

        return holdings

    def _load_nps_holdings(self, as_of_date: date) -> List[SystemHolding]:
        """Load NPS holdings from system."""
        holdings = []

        # Load NPS holdings by joining accounts and transactions
        cursor = self.conn.execute("""
            SELECT
                na.pran,
                nt.scheme,
                nt.tier,
                SUM(nt.units) as total_units,
                SUM(nt.amount) as total_contribution,
                MAX(nt.nav) as last_nav
            FROM nps_transactions nt
            JOIN nps_accounts na ON nt.nps_account_id = na.id
            WHERE na.user_id = ? AND nt.transaction_date <= ?
            GROUP BY na.pran, nt.scheme, nt.tier
            HAVING total_units > 0
        """, (self.user_id, as_of_date.isoformat()))

        for row in cursor.fetchall():
            pran = row[0]
            scheme = row[1] or "NPS"
            tier = row[2] or "I"
            units = Decimal(str(row[3])) if row[3] else Decimal("0")
            contribution = Decimal(str(row[4])) if row[4] else Decimal("0")
            nav = Decimal(str(row[5])) if row[5] else None

            # Calculate market value from units * NAV
            market_value = units * nav if nav else Decimal("0")

            holding = SystemHolding(
                asset_type=AssetClass.NPS,
                name=f"{scheme} - TIER {tier}",
                account_number=pran,
                units=units,
                nav=nav,
                market_value=market_value,
                cost_basis=contribution,
                unrealized_gain=market_value - contribution if market_value else None,
                as_of_date=as_of_date,
            )
            holdings.append(holding)

        return holdings

    def _get_latest_nav(self, isin: str, as_of_date: date) -> Optional[Decimal]:
        """Get latest NAV for a scheme."""
        cursor = self.conn.execute("""
            SELECT nav FROM mf_nav_history
            WHERE scheme_id = (SELECT id FROM mf_schemes WHERE isin = ?)
            AND nav_date <= ?
            ORDER BY nav_date DESC
            LIMIT 1
        """, (isin, as_of_date.isoformat()))

        row = cursor.fetchone()
        return Decimal(str(row[0])) if row else None

    def _get_golden_reference(self, golden_ref_id: int) -> Optional[Dict[str, Any]]:
        """Get golden reference record."""
        cursor = self.conn.execute(
            "SELECT * FROM golden_reference WHERE id = ? AND user_id = ?",
            (golden_ref_id, self.user_id)
        )
        row = cursor.fetchone()
        if row:
            cols = [desc[0] for desc in cursor.description]
            return dict(zip(cols, row))
        return None

    def _save_reconciliation_event(self, event: ReconciliationEvent) -> int:
        """Save reconciliation event to database."""
        cursor = self.conn.execute("""
            INSERT INTO reconciliation_events (
                user_id, reconciliation_date, metric_type, asset_class,
                source_type, golden_ref_id, isin, folio_number, symbol,
                system_value, golden_value, difference, difference_pct,
                tolerance_used, status, match_result, severity,
                resolved_at, resolved_by, resolution_action, resolution_notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event.user_id,
            event.reconciliation_date.isoformat(),
            event.metric_type.value,
            event.asset_class.value,
            event.source_type.value,
            event.golden_ref_id,
            event.isin,
            event.folio_number,
            event.symbol,
            str(event.system_value) if event.system_value is not None else None,
            str(event.golden_value) if event.golden_value is not None else None,
            str(event.difference) if event.difference is not None else None,
            str(event.difference_pct) if event.difference_pct is not None else None,
            str(event.tolerance_used),
            event.status.value,
            event.match_result.value,
            event.severity.value,
            event.resolved_at.isoformat() if event.resolved_at else None,
            event.resolved_by,
            event.resolution_action,
            event.resolution_notes,
        ))
        self.conn.commit()
        return cursor.lastrowid

    def _create_suspense_item(
        self,
        event: ReconciliationEvent,
        golden: Optional[GoldenHolding],
        system: Optional[SystemHolding]
    ) -> int:
        """Create suspense item for unresolved mismatch."""
        item = SuspenseItem(
            user_id=self.user_id,
            event_id=event.id or 0,
            asset_type=event.asset_class,
            isin=event.isin,
            symbol=event.symbol,
            name=golden.name if golden else (system.name if system else None),
            folio_number=event.folio_number,
            suspense_value=event.difference,
            suspense_reason=f"Mismatch: System={event.system_value}, Golden={event.golden_value}",
            opened_date=date.today(),
            priority="HIGH" if event.severity in [Severity.ERROR, Severity.CRITICAL] else "NORMAL",
        )

        cursor = self.conn.execute("""
            INSERT INTO reconciliation_suspense (
                user_id, event_id, asset_type, isin, symbol, name, folio_number,
                suspense_value, suspense_reason, opened_date, status, priority
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            item.user_id, item.event_id, item.asset_type.value,
            item.isin, item.symbol, item.name, item.folio_number,
            str(item.suspense_value) if item.suspense_value else None,
            item.suspense_reason, item.opened_date.isoformat(),
            item.status.value, item.priority,
        ))
        self.conn.commit()
        return cursor.lastrowid

    def get_open_suspense(
        self,
        asset_class: Optional[AssetClass] = None
    ) -> List[SuspenseItem]:
        """Get open suspense items."""
        query = """
            SELECT * FROM reconciliation_suspense
            WHERE user_id = ? AND status IN ('OPEN', 'IN_PROGRESS')
        """
        params = [self.user_id]

        if asset_class:
            query += " AND asset_type = ?"
            params.append(asset_class.value)

        query += " ORDER BY priority DESC, opened_date ASC"

        cursor = self.conn.execute(query, params)
        items = []

        for row in cursor.fetchall():
            cols = [desc[0] for desc in cursor.description]
            row_dict = dict(zip(cols, row))

            item = SuspenseItem(
                id=row_dict.get("id"),
                user_id=row_dict.get("user_id"),
                event_id=row_dict.get("event_id"),
                asset_type=AssetClass(row_dict.get("asset_type")),
                isin=row_dict.get("isin"),
                symbol=row_dict.get("symbol"),
                name=row_dict.get("name"),
                folio_number=row_dict.get("folio_number"),
                suspense_value=Decimal(str(row_dict.get("suspense_value"))) if row_dict.get("suspense_value") else None,
                suspense_reason=row_dict.get("suspense_reason"),
                opened_date=date.fromisoformat(row_dict.get("opened_date")) if row_dict.get("opened_date") else date.today(),
                status=SuspenseStatus(row_dict.get("status", "OPEN")),
                priority=row_dict.get("priority", "NORMAL"),
            )
            items.append(item)

        return items

    def resolve_suspense(
        self,
        suspense_id: int,
        resolution_action: str,
        notes: str,
        resolved_by: str = "SYSTEM"
    ) -> bool:
        """Resolve a suspense item."""
        cursor = self.conn.execute("""
            UPDATE reconciliation_suspense
            SET status = 'RESOLVED',
                actual_resolution_date = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND user_id = ?
        """, (date.today().isoformat(), suspense_id, self.user_id))

        if cursor.rowcount == 0:
            return False

        # Also update the reconciliation event
        cursor = self.conn.execute("""
            UPDATE reconciliation_events
            SET status = 'RESOLVED',
                resolved_at = CURRENT_TIMESTAMP,
                resolved_by = ?,
                resolution_action = ?,
                resolution_notes = ?
            WHERE id = (
                SELECT event_id FROM reconciliation_suspense WHERE id = ?
            )
        """, (resolved_by, resolution_action, notes, suspense_id))

        self.conn.commit()
        return True

    def get_reconciliation_history(
        self,
        asset_class: Optional[AssetClass] = None,
        limit: int = 100
    ) -> List[ReconciliationSummary]:
        """Get reconciliation history summaries."""
        query = """
            SELECT
                reconciliation_date,
                asset_class,
                source_type,
                golden_ref_id,
                COUNT(*) as total_items,
                SUM(CASE WHEN match_result = 'EXACT' THEN 1 ELSE 0 END) as matched_exact,
                SUM(CASE WHEN match_result = 'WITHIN_TOLERANCE' THEN 1 ELSE 0 END) as matched_tolerance,
                SUM(CASE WHEN match_result = 'MISMATCH' THEN 1 ELSE 0 END) as mismatches,
                SUM(CASE WHEN match_result = 'MISSING_SYSTEM' THEN 1 ELSE 0 END) as missing_system,
                SUM(CASE WHEN match_result = 'MISSING_GOLDEN' THEN 1 ELSE 0 END) as missing_golden,
                SUM(CAST(system_value AS DECIMAL)) as total_system,
                SUM(CAST(golden_value AS DECIMAL)) as total_golden
            FROM reconciliation_events
            WHERE user_id = ?
        """
        params = [self.user_id]

        if asset_class:
            query += " AND asset_class = ?"
            params.append(asset_class.value)

        query += """
            GROUP BY reconciliation_date, asset_class, source_type, golden_ref_id
            ORDER BY reconciliation_date DESC
            LIMIT ?
        """
        params.append(limit)

        cursor = self.conn.execute(query, params)
        summaries = []

        for row in cursor.fetchall():
            summary = ReconciliationSummary(
                user_id=self.user_id,
                reconciliation_date=date.fromisoformat(row[0]) if row[0] else date.today(),
                asset_class=AssetClass(row[1]),
                source_type=SourceType(row[2]),
                golden_ref_id=row[3],
                total_items=row[4] or 0,
                matched_exact=row[5] or 0,
                matched_tolerance=row[6] or 0,
                mismatches=row[7] or 0,
                missing_system=row[8] or 0,
                missing_golden=row[9] or 0,
                total_system_value=Decimal(str(row[10])) if row[10] else Decimal("0"),
                total_golden_value=Decimal(str(row[11])) if row[11] else Decimal("0"),
            )
            summaries.append(summary)

        return summaries
