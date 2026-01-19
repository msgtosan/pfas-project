"""User Preferences Management for PFAS.

Provides data-driven, per-user configuration with sensible defaults.
"""

import json
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import date

logger = logging.getLogger(__name__)

# Supported output formats
SUPPORTED_FORMATS = {'xlsx', 'pdf', 'json', 'csv', 'html', 'txt'}

# Default preferences (used when user hasn't configured)
DEFAULT_PREFERENCES = {
    "$schema": "user_preferences_v1",
    "version": "1.0",

    "reports": {
        "default_format": "xlsx",
        "formats_by_type": {
            "balance_sheet": ["xlsx"],
            "cash_flow": ["xlsx"],
            "income_statement": ["xlsx"],
            "capital_gains": ["xlsx"],
            "portfolio": ["xlsx"],
            "tax_computation": ["xlsx"]
        },
        "naming": {
            "include_timestamp": True,
            "include_fy": True,
            "pattern": "{report_type}_{fy}_{date}"
        },
        "auto_open": False
    },

    "financial_year": {
        "default": None,  # Will be computed
        "start_month": 4  # April for India
    },

    "display": {
        "currency_symbol": "₹",
        "decimal_places": 2,
        "date_format": "DD-MMM-YYYY",
        "negative_in_brackets": True
    },

    "parsers": {
        "auto_archive": True,
        "duplicate_handling": "skip",
        "default_sources": {}
    },

    "cas": {
        "consolidate_folios": True,
        "show_consolidation_details": True,
        "clean_scheme_names": True,
        "parse_stamp_duty": True,
        "parse_valuation": True,
        "balance_tolerance": 0.01
    },

    "notifications": {
        "on_parse_complete": True,
        "on_error": True,
        "summary_after_ingest": True
    }
}


@dataclass
class ReportConfig:
    """Configuration for report generation."""
    default_format: str = "xlsx"
    formats_by_type: Dict[str, List[str]] = field(default_factory=dict)
    naming_pattern: str = "{report_type}_{fy}_{date}"
    include_timestamp: bool = True
    include_fy: bool = True
    auto_open: bool = False

    def get_formats(self, report_type: str) -> List[str]:
        """Get output formats for a specific report type."""
        return self.formats_by_type.get(report_type, [self.default_format])

    def generate_filename(self, report_type: str, fy: str, extension: str) -> str:
        """Generate filename based on naming pattern."""
        today = date.today().strftime("%Y-%m-%d")
        name = self.naming_pattern.format(
            report_type=report_type,
            fy=fy.replace("-", ""),
            date=today
        )
        return f"{name}.{extension}"


@dataclass
class DisplayConfig:
    """Configuration for display formatting."""
    currency_symbol: str = "₹"
    decimal_places: int = 2
    date_format: str = "DD-MMM-YYYY"
    negative_in_brackets: bool = True

    def format_currency(self, amount: float) -> str:
        """Format amount with currency symbol."""
        if amount < 0 and self.negative_in_brackets:
            return f"({self.currency_symbol}{abs(amount):,.{self.decimal_places}f})"
        return f"{self.currency_symbol}{amount:,.{self.decimal_places}f}"


@dataclass
class ParserConfig:
    """Configuration for parsers."""
    auto_archive: bool = True
    duplicate_handling: str = "skip"  # skip, update, error
    default_sources: Dict[str, str] = field(default_factory=dict)


@dataclass
class CASConfig:
    """Configuration for CAS (Consolidated Account Statement) processing."""
    consolidate_folios: bool = True  # Merge schemes under same folio number
    show_consolidation_details: bool = True  # Show what was consolidated
    clean_scheme_names: bool = True  # Remove prefix codes like "B92Z-"
    parse_stamp_duty: bool = True  # Parse stamp duty as separate transactions
    parse_valuation: bool = True  # Parse NAV, Cost, Value from valuation lines
    balance_tolerance: float = 0.01  # Tolerance for balance mismatch detection


class UserPreferences:
    """
    User-specific preferences for PFAS.

    Loads from user's config/preferences.json with fallback to defaults.

    Usage:
        prefs = UserPreferences.load(path_resolver)
        formats = prefs.reports.get_formats("balance_sheet")
        formatted = prefs.display.format_currency(1234.56)
    """

    def __init__(self, data: Dict[str, Any]):
        """Initialize from preference dictionary."""
        self._raw = data

        # Reports configuration
        reports = data.get("reports", {})
        self.reports = ReportConfig(
            default_format=reports.get("default_format", "xlsx"),
            formats_by_type=reports.get("formats_by_type", {}),
            naming_pattern=reports.get("naming", {}).get("pattern", "{report_type}_{fy}_{date}"),
            include_timestamp=reports.get("naming", {}).get("include_timestamp", True),
            include_fy=reports.get("naming", {}).get("include_fy", True),
            auto_open=reports.get("auto_open", False)
        )

        # Display configuration
        display = data.get("display", {})
        self.display = DisplayConfig(
            currency_symbol=display.get("currency_symbol", "₹"),
            decimal_places=display.get("decimal_places", 2),
            date_format=display.get("date_format", "DD-MMM-YYYY"),
            negative_in_brackets=display.get("negative_in_brackets", True)
        )

        # Parser configuration
        parsers = data.get("parsers", {})
        self.parsers = ParserConfig(
            auto_archive=parsers.get("auto_archive", True),
            duplicate_handling=parsers.get("duplicate_handling", "skip"),
            default_sources=parsers.get("default_sources", {})
        )

        # CAS configuration
        cas = data.get("cas", {})
        self.cas = CASConfig(
            consolidate_folios=cas.get("consolidate_folios", True),
            show_consolidation_details=cas.get("show_consolidation_details", True),
            clean_scheme_names=cas.get("clean_scheme_names", True),
            parse_stamp_duty=cas.get("parse_stamp_duty", True),
            parse_valuation=cas.get("parse_valuation", True),
            balance_tolerance=cas.get("balance_tolerance", 0.01)
        )

        # Financial year settings
        fy = data.get("financial_year", {})
        self.default_fy = fy.get("default") or self._compute_current_fy()
        self.fy_start_month = fy.get("start_month", 4)

    def _compute_current_fy(self) -> str:
        """Compute current financial year (Apr-Mar for India)."""
        today = date.today()
        if today.month >= 4:
            return f"{today.year}-{str(today.year + 1)[-2:]}"
        else:
            return f"{today.year - 1}-{str(today.year)[-2:]}"

    @classmethod
    def load(cls, user_config_dir: Path, global_config_dir: Optional[Path] = None) -> "UserPreferences":
        """
        Load user preferences with fallback to defaults.

        Args:
            user_config_dir: User's config directory (Data/Users/<user>/config)
            global_config_dir: Global config directory (Data/config) - optional

        Returns:
            UserPreferences instance
        """
        # Start with defaults
        data = DEFAULT_PREFERENCES.copy()

        # Load global defaults if available
        if global_config_dir:
            global_defaults = global_config_dir / "defaults.json"
            if global_defaults.exists():
                try:
                    with open(global_defaults, encoding='utf-8') as f:
                        global_data = json.load(f)
                    data = cls._deep_merge(data, global_data)
                    logger.debug(f"Loaded global defaults from {global_defaults}")
                except Exception as e:
                    logger.warning(f"Failed to load global defaults: {e}")

        # Load user preferences (highest priority)
        user_prefs = user_config_dir / "preferences.json"
        if user_prefs.exists():
            try:
                with open(user_prefs, encoding='utf-8') as f:
                    user_data = json.load(f)
                data = cls._deep_merge(data, user_data)
                logger.debug(f"Loaded user preferences from {user_prefs}")
            except Exception as e:
                logger.warning(f"Failed to load user preferences: {e}")

        return cls(data)

    @staticmethod
    def _deep_merge(base: Dict, override: Dict) -> Dict:
        """Deep merge two dictionaries, override takes precedence."""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = UserPreferences._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def save(self, user_config_dir: Path) -> None:
        """Save current preferences to user's config."""
        user_config_dir.mkdir(parents=True, exist_ok=True)
        prefs_file = user_config_dir / "preferences.json"

        with open(prefs_file, 'w', encoding='utf-8') as f:
            json.dump(self._raw, f, indent=2)

        logger.info(f"Saved preferences to {prefs_file}")

    def get_report_output_path(self, reports_dir: Path, report_type: str, fy: str, fmt: str) -> Path:
        """
        Get full output path for a report.

        Args:
            reports_dir: Base reports directory (from PathResolver)
            report_type: Type of report (balance_sheet, cash_flow, etc.)
            fy: Financial year (e.g., "2024-25")
            fmt: Output format (xlsx, pdf, json, etc.)

        Returns:
            Full path for the report file
        """
        filename = self.reports.generate_filename(report_type, fy, fmt)
        return reports_dir / report_type / filename


def create_default_preferences(user_config_dir: Path, user_name: str) -> UserPreferences:
    """
    Create default preferences file for a new user.

    Args:
        user_config_dir: User's config directory
        user_name: User's name (for personalization)

    Returns:
        UserPreferences instance
    """
    data = DEFAULT_PREFERENCES.copy()

    # Personalize naming pattern
    data["reports"]["naming"]["pattern"] = f"{user_name}_{{report_type}}_FY{{fy}}"

    # Save to file
    user_config_dir.mkdir(parents=True, exist_ok=True)
    prefs_file = user_config_dir / "preferences.json"

    with open(prefs_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

    logger.info(f"Created default preferences for {user_name} at {prefs_file}")

    return UserPreferences(data)
