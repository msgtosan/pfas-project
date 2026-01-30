"""
User Configuration for Golden Reference Reconciliation.

Provides user-specific settings for reconciliation behavior:
- Manual vs scheduled reconciliation mode
- Reconciliation frequency
- Tolerance settings
- Notification preferences
"""

import json
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


class ReconciliationMode(Enum):
    """Reconciliation execution mode."""
    MANUAL = "MANUAL"           # User triggers reconciliation manually
    SCHEDULED = "SCHEDULED"     # System runs on schedule
    ON_INGEST = "ON_INGEST"    # Auto-run after ingesting golden reference


class ReconciliationFrequency(Enum):
    """Frequency for scheduled reconciliation."""
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    QUARTERLY = "QUARTERLY"


@dataclass
class UserReconciliationSettings:
    """
    User-configurable reconciliation settings.

    Loaded from: Data/Users/{user}/config/reconciliation.json

    Example config file:
    {
        "mode": "MANUAL",
        "frequency": "MONTHLY",
        "auto_reconcile_on_ingest": false,
        "tolerances": {
            "absolute": "0.01",
            "percentage": "0.1"
        },
        "severity_thresholds": {
            "warning": "100",
            "error": "1000",
            "critical": "10000"
        },
        "notifications": {
            "on_mismatch": true,
            "on_critical": true,
            "email": false
        },
        "asset_classes": ["MUTUAL_FUND", "STOCKS", "NPS"],
        "default_sources": {
            "MUTUAL_FUND": "NSDL_CAS",
            "STOCKS": "NSDL_CAS"
        }
    }
    """
    # Mode settings
    mode: ReconciliationMode = ReconciliationMode.MANUAL
    frequency: ReconciliationFrequency = ReconciliationFrequency.MONTHLY
    auto_reconcile_on_ingest: bool = False

    # Tolerance settings
    absolute_tolerance: Decimal = Decimal("0.01")
    percentage_tolerance: Decimal = Decimal("0.1")  # 0.1%

    # Severity thresholds (in INR)
    warning_threshold: Decimal = Decimal("100")
    error_threshold: Decimal = Decimal("1000")
    critical_threshold: Decimal = Decimal("10000")

    # Notification preferences
    notify_on_mismatch: bool = True
    notify_on_critical: bool = True
    email_notifications: bool = False

    # Asset classes to reconcile
    enabled_asset_classes: List[str] = field(default_factory=lambda: [
        "MUTUAL_FUND", "STOCKS", "NPS"
    ])

    # Default sources per asset class
    default_sources: Dict[str, str] = field(default_factory=dict)

    # Auto-created suspense behavior
    create_suspense_on_mismatch: bool = True
    auto_resolve_within_tolerance: bool = True

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "UserReconciliationSettings":
        """Create settings from JSON data."""
        settings = cls()

        # Mode settings
        if "mode" in data:
            settings.mode = ReconciliationMode(data["mode"])
        if "frequency" in data:
            settings.frequency = ReconciliationFrequency(data["frequency"])
        if "auto_reconcile_on_ingest" in data:
            settings.auto_reconcile_on_ingest = data["auto_reconcile_on_ingest"]

        # Tolerances
        tolerances = data.get("tolerances", {})
        if "absolute" in tolerances:
            settings.absolute_tolerance = Decimal(str(tolerances["absolute"]))
        if "percentage" in tolerances:
            settings.percentage_tolerance = Decimal(str(tolerances["percentage"]))

        # Severity thresholds
        thresholds = data.get("severity_thresholds", {})
        if "warning" in thresholds:
            settings.warning_threshold = Decimal(str(thresholds["warning"]))
        if "error" in thresholds:
            settings.error_threshold = Decimal(str(thresholds["error"]))
        if "critical" in thresholds:
            settings.critical_threshold = Decimal(str(thresholds["critical"]))

        # Notifications
        notifications = data.get("notifications", {})
        if "on_mismatch" in notifications:
            settings.notify_on_mismatch = notifications["on_mismatch"]
        if "on_critical" in notifications:
            settings.notify_on_critical = notifications["on_critical"]
        if "email" in notifications:
            settings.email_notifications = notifications["email"]

        # Asset classes
        if "asset_classes" in data:
            settings.enabled_asset_classes = data["asset_classes"]

        # Default sources
        if "default_sources" in data:
            settings.default_sources = data["default_sources"]

        # Suspense behavior
        if "create_suspense_on_mismatch" in data:
            settings.create_suspense_on_mismatch = data["create_suspense_on_mismatch"]
        if "auto_resolve_within_tolerance" in data:
            settings.auto_resolve_within_tolerance = data["auto_resolve_within_tolerance"]

        return settings

    def to_json(self) -> Dict[str, Any]:
        """Export settings as JSON-serializable dict."""
        return {
            "mode": self.mode.value,
            "frequency": self.frequency.value,
            "auto_reconcile_on_ingest": self.auto_reconcile_on_ingest,
            "tolerances": {
                "absolute": str(self.absolute_tolerance),
                "percentage": str(self.percentage_tolerance),
            },
            "severity_thresholds": {
                "warning": str(self.warning_threshold),
                "error": str(self.error_threshold),
                "critical": str(self.critical_threshold),
            },
            "notifications": {
                "on_mismatch": self.notify_on_mismatch,
                "on_critical": self.notify_on_critical,
                "email": self.email_notifications,
            },
            "asset_classes": self.enabled_asset_classes,
            "default_sources": self.default_sources,
            "create_suspense_on_mismatch": self.create_suspense_on_mismatch,
            "auto_resolve_within_tolerance": self.auto_resolve_within_tolerance,
        }


class UserConfigLoader:
    """
    Loads user-specific configuration for reconciliation.

    Usage:
        loader = UserConfigLoader(Path("Data/Users/Sanjay/config"))
        settings = loader.load_reconciliation_settings()

        if settings.mode == ReconciliationMode.MANUAL:
            print("User prefers manual reconciliation")
    """

    def __init__(self, config_path: Path):
        """
        Initialize config loader.

        Args:
            config_path: Path to user's config directory
        """
        self.config_path = config_path

    def load_reconciliation_settings(self) -> UserReconciliationSettings:
        """
        Load reconciliation settings from user config file.

        Returns:
            UserReconciliationSettings (defaults if file not found)
        """
        config_file = self.config_path / "reconciliation.json"

        if not config_file.exists():
            logger.debug(f"No reconciliation config found at {config_file}, using defaults")
            return UserReconciliationSettings()

        try:
            with open(config_file, encoding="utf-8") as f:
                data = json.load(f)
            settings = UserReconciliationSettings.from_json(data)
            logger.info(f"Loaded reconciliation config: mode={settings.mode.value}")
            return settings
        except Exception as e:
            logger.warning(f"Failed to load reconciliation config: {e}")
            return UserReconciliationSettings()

    def save_reconciliation_settings(self, settings: UserReconciliationSettings) -> bool:
        """
        Save reconciliation settings to user config file.

        Args:
            settings: Settings to save

        Returns:
            True if saved successfully
        """
        config_file = self.config_path / "reconciliation.json"

        try:
            # Ensure directory exists
            self.config_path.mkdir(parents=True, exist_ok=True)

            with open(config_file, "w", encoding="utf-8") as f:
                json.dump(settings.to_json(), f, indent=2)
            logger.info(f"Saved reconciliation config to {config_file}")
            return True
        except Exception as e:
            logger.error(f"Failed to save reconciliation config: {e}")
            return False

    def get_password(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get password from passwords.json.

        Args:
            key: Password key (e.g., "golden.nsdl")
            default: Default value if not found

        Returns:
            Password string or default
        """
        passwords_file = self.config_path / "passwords.json"

        if not passwords_file.exists():
            return default

        try:
            with open(passwords_file, encoding="utf-8") as f:
                data = json.load(f)

            # Support dot-notation keys (e.g., "golden.nsdl")
            parts = key.split(".")
            value = data
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    return default
            return value if isinstance(value, str) else default
        except Exception:
            return default
