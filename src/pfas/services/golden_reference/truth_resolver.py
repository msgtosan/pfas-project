"""
Truth Resolver for Golden Reference Reconciliation.

Determines the authoritative source of truth for each metric type and asset class.
Supports per-user configuration overrides.
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Any
import sqlite3

from .models import (
    MetricType,
    AssetClass,
    SourceType,
    TruthSourceConfig,
)

logger = logging.getLogger(__name__)


class TruthResolver:
    """
    Resolves the authoritative source of truth for reconciliation.

    The Truth Resolver determines which data source should be considered
    authoritative for different types of metrics and asset classes:

    - NET_WORTH: NSDL/CDSL CAS is authoritative for holdings valuation
    - CAPITAL_GAINS: Broker/RTA is authoritative for realized gains
    - UNITS: RTA is authoritative for MF units, Depository for stocks
    - COST_BASIS: System tracks this; external sources for validation

    Supports:
    - Default configurations stored in database
    - Per-user overrides via truth_sources table
    - Config file overrides via user's config directory

    Usage:
        resolver = TruthResolver(conn, user_id=1)

        # Get authoritative source for MF net worth
        source = resolver.get_truth_source(MetricType.NET_WORTH, AssetClass.MUTUAL_FUND)
        # Returns: SourceType.NSDL_CAS

        # Get prioritized source list
        sources = resolver.get_source_priority(MetricType.UNITS, AssetClass.STOCKS)
        # Returns: [SourceType.DEPOSITORY, SourceType.BROKER, SourceType.SYSTEM]
    """

    def __init__(
        self,
        db_connection: sqlite3.Connection,
        user_id: int,
        config_path: Optional[Path] = None
    ):
        """
        Initialize TruthResolver.

        Args:
            db_connection: Database connection
            user_id: User ID for personalized configurations
            config_path: Optional path to user config directory
        """
        self.conn = db_connection
        self.user_id = user_id
        self.config_path = config_path
        self._cache: Dict[str, TruthSourceConfig] = {}
        self._load_configurations()

    def _cache_key(self, metric: MetricType, asset: AssetClass) -> str:
        """Generate cache key for metric/asset combination."""
        return f"{metric.value}:{asset.value}"

    def _load_configurations(self) -> None:
        """Load truth source configurations from database and config file."""
        # Load from database
        try:
            cursor = self.conn.execute("""
                SELECT metric_type, asset_class, source_priority, description, user_id, is_default
                FROM truth_sources
                WHERE user_id IS NULL OR user_id = ?
                ORDER BY user_id NULLS FIRST
            """, (self.user_id,))

            rows = cursor.fetchall()

            if not rows:
                # Table exists but is empty - load defaults
                self._load_defaults()
            else:
                for row in rows:
                    row_dict = {
                        "metric_type": row[0],
                        "asset_class": row[1],
                        "source_priority": row[2],
                        "description": row[3],
                        "user_id": row[4],
                        "is_default": row[5]
                    }
                    config = TruthSourceConfig.from_db_row(row_dict)
                    key = self._cache_key(config.metric_type, config.asset_class)
                    # User-specific overrides take precedence
                    if config.user_id == self.user_id or key not in self._cache:
                        self._cache[key] = config

        except sqlite3.OperationalError as e:
            logger.warning(f"Could not load truth sources from database: {e}")
            self._load_defaults()

        # Load config file overrides
        if self.config_path:
            self._load_config_file_overrides()

    def _load_defaults(self) -> None:
        """Load default truth source configurations."""
        defaults = [
            # Net Worth
            (MetricType.NET_WORTH, AssetClass.MUTUAL_FUND,
             [SourceType.NSDL_CAS, SourceType.CDSL_CAS, SourceType.RTA_CAS, SourceType.SYSTEM]),
            (MetricType.NET_WORTH, AssetClass.STOCKS,
             [SourceType.NSDL_CAS, SourceType.CDSL_CAS, SourceType.BROKER, SourceType.SYSTEM]),
            (MetricType.NET_WORTH, AssetClass.NPS,
             [SourceType.NSDL_CAS, SourceType.NPS_STATEMENT, SourceType.SYSTEM]),
            (MetricType.NET_WORTH, AssetClass.US_STOCKS,
             [SourceType.BROKER_STATEMENT, SourceType.SYSTEM]),

            # Capital Gains
            (MetricType.CAPITAL_GAINS, AssetClass.MUTUAL_FUND,
             [SourceType.RTA_CAS, SourceType.NSDL_CAS, SourceType.SYSTEM]),
            (MetricType.CAPITAL_GAINS, AssetClass.STOCKS,
             [SourceType.BROKER, SourceType.NSDL_CAS, SourceType.SYSTEM]),

            # Units
            (MetricType.UNITS, AssetClass.MUTUAL_FUND,
             [SourceType.RTA_CAS, SourceType.NSDL_CAS, SourceType.SYSTEM]),
            (MetricType.UNITS, AssetClass.STOCKS,
             [SourceType.DEPOSITORY, SourceType.BROKER, SourceType.SYSTEM]),

            # Cost Basis - System is primary (we track purchases)
            (MetricType.COST_BASIS, AssetClass.MUTUAL_FUND,
             [SourceType.SYSTEM, SourceType.RTA_CAS]),
            (MetricType.COST_BASIS, AssetClass.STOCKS,
             [SourceType.SYSTEM, SourceType.BROKER]),
        ]

        for metric, asset, sources in defaults:
            config = TruthSourceConfig(
                metric_type=metric,
                asset_class=asset,
                source_priority=sources,
            )
            self._cache[self._cache_key(metric, asset)] = config

    def _load_config_file_overrides(self) -> None:
        """Load user config file overrides."""
        config_file = self.config_path / "truth_sources.json"
        if not config_file.exists():
            return

        try:
            with open(config_file, encoding="utf-8") as f:
                config_data = json.load(f)

            for entry in config_data.get("overrides", []):
                metric = MetricType(entry["metric_type"])
                asset = AssetClass(entry["asset_class"])
                sources = [SourceType(s) for s in entry["source_priority"]]

                config = TruthSourceConfig(
                    metric_type=metric,
                    asset_class=asset,
                    source_priority=sources,
                    description=entry.get("description", "User override"),
                    user_id=self.user_id,
                    is_default=False,
                )
                self._cache[self._cache_key(metric, asset)] = config

            logger.info(f"Loaded {len(config_data.get('overrides', []))} config overrides")

        except Exception as e:
            logger.warning(f"Failed to load config file overrides: {e}")

    def get_truth_source(
        self,
        metric_type: MetricType,
        asset_class: AssetClass
    ) -> SourceType:
        """
        Get the primary authoritative source for a metric/asset combination.

        Args:
            metric_type: Type of metric (NET_WORTH, CAPITAL_GAINS, etc.)
            asset_class: Asset class (MUTUAL_FUND, STOCKS, etc.)

        Returns:
            The highest priority SourceType for this combination
        """
        sources = self.get_source_priority(metric_type, asset_class)
        return sources[0] if sources else SourceType.SYSTEM

    def get_source_priority(
        self,
        metric_type: MetricType,
        asset_class: AssetClass
    ) -> List[SourceType]:
        """
        Get the prioritized list of sources for a metric/asset combination.

        Args:
            metric_type: Type of metric
            asset_class: Asset class

        Returns:
            List of SourceType in priority order (highest first)
        """
        key = self._cache_key(metric_type, asset_class)
        config = self._cache.get(key)

        if config:
            return config.source_priority

        # Fallback to SYSTEM
        logger.warning(f"No truth source config for {metric_type.value}/{asset_class.value}")
        return [SourceType.SYSTEM]

    def get_config(
        self,
        metric_type: MetricType,
        asset_class: AssetClass
    ) -> Optional[TruthSourceConfig]:
        """
        Get the full configuration for a metric/asset combination.

        Args:
            metric_type: Type of metric
            asset_class: Asset class

        Returns:
            TruthSourceConfig or None
        """
        key = self._cache_key(metric_type, asset_class)
        return self._cache.get(key)

    def set_user_override(
        self,
        metric_type: MetricType,
        asset_class: AssetClass,
        source_priority: List[SourceType],
        description: str = ""
    ) -> None:
        """
        Set a user-specific source priority override.

        Args:
            metric_type: Type of metric
            asset_class: Asset class
            source_priority: New priority list
            description: Optional description
        """
        sources_json = json.dumps([s.value for s in source_priority])

        # Upsert into database
        self.conn.execute("""
            INSERT INTO truth_sources (metric_type, asset_class, source_priority, description, user_id, is_default)
            VALUES (?, ?, ?, ?, ?, 0)
            ON CONFLICT(metric_type, asset_class, user_id) DO UPDATE SET
                source_priority = excluded.source_priority,
                description = excluded.description,
                updated_at = CURRENT_TIMESTAMP
        """, (metric_type.value, asset_class.value, sources_json, description, self.user_id))
        self.conn.commit()

        # Update cache
        config = TruthSourceConfig(
            metric_type=metric_type,
            asset_class=asset_class,
            source_priority=source_priority,
            description=description,
            user_id=self.user_id,
            is_default=False,
        )
        self._cache[self._cache_key(metric_type, asset_class)] = config
        logger.info(f"Set user override for {metric_type.value}/{asset_class.value}")

    def clear_user_override(
        self,
        metric_type: MetricType,
        asset_class: AssetClass
    ) -> bool:
        """
        Clear a user-specific override, reverting to default.

        Args:
            metric_type: Type of metric
            asset_class: Asset class

        Returns:
            True if override was cleared
        """
        cursor = self.conn.execute("""
            DELETE FROM truth_sources
            WHERE metric_type = ? AND asset_class = ? AND user_id = ?
        """, (metric_type.value, asset_class.value, self.user_id))
        self.conn.commit()

        if cursor.rowcount > 0:
            # Reload to get default
            key = self._cache_key(metric_type, asset_class)
            if key in self._cache:
                del self._cache[key]
            self._load_configurations()
            return True
        return False

    def is_authoritative(
        self,
        source_type: SourceType,
        metric_type: MetricType,
        asset_class: AssetClass
    ) -> bool:
        """
        Check if a source is the authoritative source for a metric/asset.

        Args:
            source_type: Source to check
            metric_type: Type of metric
            asset_class: Asset class

        Returns:
            True if source_type is the primary authority
        """
        truth_source = self.get_truth_source(metric_type, asset_class)
        return source_type == truth_source

    def should_reconcile(
        self,
        source_type: SourceType,
        metric_type: MetricType,
        asset_class: AssetClass
    ) -> bool:
        """
        Check if a source should be used for reconciliation.

        A source should be used for reconciliation if it's in the priority list.

        Args:
            source_type: Source to check
            metric_type: Type of metric
            asset_class: Asset class

        Returns:
            True if source should be reconciled against
        """
        sources = self.get_source_priority(metric_type, asset_class)
        return source_type in sources

    def get_reconciliation_direction(
        self,
        source_a: SourceType,
        source_b: SourceType,
        metric_type: MetricType,
        asset_class: AssetClass
    ) -> str:
        """
        Determine which source is authoritative when comparing two sources.

        Args:
            source_a: First source
            source_b: Second source
            metric_type: Type of metric
            asset_class: Asset class

        Returns:
            "A_AUTHORITATIVE", "B_AUTHORITATIVE", or "EQUAL"
        """
        sources = self.get_source_priority(metric_type, asset_class)

        try:
            idx_a = sources.index(source_a)
        except ValueError:
            idx_a = len(sources)

        try:
            idx_b = sources.index(source_b)
        except ValueError:
            idx_b = len(sources)

        if idx_a < idx_b:
            return "A_AUTHORITATIVE"
        elif idx_b < idx_a:
            return "B_AUTHORITATIVE"
        return "EQUAL"

    def get_all_configs(self) -> List[TruthSourceConfig]:
        """Get all truth source configurations."""
        return list(self._cache.values())

    def export_config(self) -> Dict[str, Any]:
        """Export current configuration as JSON-serializable dict."""
        return {
            "user_id": self.user_id,
            "configurations": [
                {
                    "metric_type": cfg.metric_type.value,
                    "asset_class": cfg.asset_class.value,
                    "source_priority": [s.value for s in cfg.source_priority],
                    "description": cfg.description,
                    "is_default": cfg.is_default,
                }
                for cfg in self._cache.values()
            ]
        }
