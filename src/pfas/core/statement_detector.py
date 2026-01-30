"""
Statement Type Detector - Hybrid detection for Transactions vs Holdings statements.

Uses a layered detection approach:
1. Folder hints (inbox/transactions/, inbox/holdings/)
2. Filename keywords (configurable)
3. Content analysis (configurable)
4. Default fallback (Transactions)

Scalable to multiple users with per-user config overrides.

Usage:
    from pfas.core.statement_detector import StatementTypeDetector, StatementType
    from pfas.core.paths import PathResolver

    resolver = PathResolver(project_root, "Sanjay")
    detector = StatementTypeDetector(resolver)

    # Detect type for a file
    result = detector.detect(Path("inbox/Mutual-Fund/CAMS/statement.xlsx"))
    print(f"Type: {result.statement_type}, Method: {result.detection_method}")
"""

import json
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Set

logger = logging.getLogger(__name__)


class StatementType(Enum):
    """Type of financial statement."""
    TRANSACTIONS = "transactions"  # Buy/sell/redemption/dividend history
    HOLDINGS = "holdings"          # Current snapshot (value/units/NAV)
    UNKNOWN = "unknown"


class DetectionMethod(Enum):
    """How the statement type was detected."""
    FOLDER = "folder"           # From folder structure (transactions/ or holdings/)
    FILENAME = "filename"       # From filename keywords
    CONTENT = "content"         # From file content analysis
    CONFIG = "config"           # From user config override
    DEFAULT = "default"         # Fallback to default


@dataclass
class DetectionResult:
    """Result of statement type detection."""
    statement_type: StatementType
    detection_method: DetectionMethod
    confidence: float = 1.0  # 0.0 to 1.0
    matched_keywords: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        return f"{self.statement_type.value} (via {self.detection_method.value}, confidence={self.confidence:.2f})"


@dataclass
class StatementRulesConfig:
    """
    Configuration for statement type detection rules.

    Loaded from config/statement_rules.json with user overrides.
    """
    # Keywords indicating transaction statements
    transactions_keywords: List[str] = field(default_factory=lambda: [
        # Filename keywords
        "txn", "transaction", "transactions", "trade", "trades",
        "buy", "sell", "redemption", "switch", "dividend",
        "capital_gain", "capital-gain", "capitalgain", "cg",
        "p&l", "pnl", "profit", "tax_statement", "tax-statement",
        # Content keywords (for content analysis)
        "purchase", "sale", "transfer", "allotment"
    ])

    # Keywords indicating holdings statements
    holdings_keywords: List[str] = field(default_factory=lambda: [
        # Filename keywords
        "holding", "holdings", "portfolio", "consolidated",
        "summary", "valuation", "current_value", "current-value",
        "snapshot", "position", "positions", "balance",
        # Content keywords
        "as on", "as of", "current nav", "market value"
    ])

    # File-specific overrides (filename -> type)
    file_overrides: Dict[str, str] = field(default_factory=dict)

    # Folder names that indicate transactions
    transactions_folders: List[str] = field(default_factory=lambda: [
        "transactions", "txn", "trades", "capital_gains"
    ])

    # Folder names that indicate holdings
    holdings_folders: List[str] = field(default_factory=lambda: [
        "holdings", "portfolio", "valuation", "snapshot"
    ])

    # Default type when detection is ambiguous
    default_type: str = "transactions"

    # Minimum confidence threshold for content-based detection
    min_content_confidence: float = 0.6

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StatementRulesConfig":
        """Create config from dictionary."""
        return cls(
            transactions_keywords=data.get("transactions_keywords", cls().transactions_keywords),
            holdings_keywords=data.get("holdings_keywords", cls().holdings_keywords),
            file_overrides=data.get("file_overrides", {}),
            transactions_folders=data.get("transactions_folders", cls().transactions_folders),
            holdings_folders=data.get("holdings_folders", cls().holdings_folders),
            default_type=data.get("default_type", "transactions"),
            min_content_confidence=data.get("min_content_confidence", 0.6)
        )

    def merge_with(self, override: "StatementRulesConfig") -> "StatementRulesConfig":
        """Merge with another config (override takes precedence for lists, extends keywords)."""
        return StatementRulesConfig(
            transactions_keywords=list(set(self.transactions_keywords + override.transactions_keywords)),
            holdings_keywords=list(set(self.holdings_keywords + override.holdings_keywords)),
            file_overrides={**self.file_overrides, **override.file_overrides},
            transactions_folders=list(set(self.transactions_folders + override.transactions_folders)),
            holdings_folders=list(set(self.holdings_folders + override.holdings_folders)),
            default_type=override.default_type if override.default_type != "transactions" else self.default_type,
            min_content_confidence=override.min_content_confidence
        )


class StatementTypeDetector:
    """
    Hybrid statement type detector.

    Detection order (first match wins):
    1. User config file override (explicit filename -> type mapping)
    2. Folder structure hints (transactions/ or holdings/ subfolder)
    3. Filename keyword matching
    4. Content analysis (for Excel/CSV files)
    5. Default fallback (configurable, defaults to Transactions)
    """

    def __init__(
        self,
        path_resolver=None,
        config: Optional[StatementRulesConfig] = None,
        project_root: Optional[Path] = None
    ):
        """
        Initialize detector.

        Args:
            path_resolver: Optional PathResolver for loading user config
            config: Optional pre-loaded config (overrides file loading)
            project_root: Project root for global config (if no path_resolver)
        """
        self.path_resolver = path_resolver
        self.project_root = project_root or (path_resolver.root if path_resolver else Path.cwd())

        if config:
            self.config = config
        else:
            self.config = self._load_config()

        # Pre-compile keyword patterns for efficiency
        self._txn_patterns = self._compile_patterns(self.config.transactions_keywords)
        self._holdings_patterns = self._compile_patterns(self.config.holdings_keywords)

    def _load_config(self) -> StatementRulesConfig:
        """
        Load config from global and user-specific files.

        Priority:
        1. User config (overrides global)
        2. Global config
        3. Default built-in config
        """
        config = StatementRulesConfig()

        # Load global config
        global_config_path = self.project_root / "config" / "statement_rules.json"
        if global_config_path.exists():
            try:
                with open(global_config_path, encoding='utf-8') as f:
                    global_data = json.load(f)
                config = StatementRulesConfig.from_dict(global_data)
                logger.debug(f"Loaded global statement rules from {global_config_path}")
            except Exception as e:
                logger.warning(f"Failed to load global config: {e}")

        # Load user config (if path_resolver available)
        if self.path_resolver:
            user_config_path = self.path_resolver.user_config_dir() / "statement_rules.json"
            if user_config_path.exists():
                try:
                    with open(user_config_path, encoding='utf-8') as f:
                        user_data = json.load(f)
                    user_config = StatementRulesConfig.from_dict(user_data)
                    config = config.merge_with(user_config)
                    logger.debug(f"Merged user statement rules from {user_config_path}")
                except Exception as e:
                    logger.warning(f"Failed to load user config: {e}")

        return config

    def _compile_patterns(self, keywords: List[str]) -> List[re.Pattern]:
        """Compile keyword patterns for efficient matching."""
        patterns = []
        for keyword in keywords:
            # Simple substring match with word boundary awareness
            escaped = re.escape(keyword)
            # Match keyword as substring (case-insensitive)
            pattern = re.compile(escaped, re.IGNORECASE)
            patterns.append((keyword, pattern))
        return patterns

    def detect(self, file_path: Path) -> DetectionResult:
        """
        Detect statement type for a file.

        Args:
            file_path: Path to the statement file

        Returns:
            DetectionResult with type, method, and confidence
        """
        file_path = Path(file_path)
        filename = file_path.name.lower()
        stem = file_path.stem.lower()

        # 1. Check file-specific overrides in config
        if filename in self.config.file_overrides:
            override_type = self.config.file_overrides[filename]
            return DetectionResult(
                statement_type=StatementType(override_type),
                detection_method=DetectionMethod.CONFIG,
                confidence=1.0,
                matched_keywords=[f"config:{filename}"]
            )

        # 2. Check folder structure hints
        folder_result = self._detect_from_folder(file_path)
        if folder_result:
            return folder_result

        # 3. Check filename keywords
        filename_result = self._detect_from_filename(stem)
        if filename_result:
            return filename_result

        # 4. Try content analysis (for supported file types)
        content_result = self._detect_from_content(file_path)
        if content_result and content_result.confidence >= self.config.min_content_confidence:
            return content_result

        # 5. Return default with warning
        default_type = StatementType(self.config.default_type)
        logger.warning(
            f"Could not determine statement type for {file_path.name}, "
            f"defaulting to {default_type.value}"
        )

        return DetectionResult(
            statement_type=default_type,
            detection_method=DetectionMethod.DEFAULT,
            confidence=0.5,
            warnings=[f"Ambiguous statement type, defaulted to {default_type.value}"]
        )

    def _detect_from_folder(self, file_path: Path) -> Optional[DetectionResult]:
        """
        Detect statement type from folder structure.

        Checks if file is in transactions/ or holdings/ subfolder.
        """
        path_parts = [p.lower() for p in file_path.parts]

        # Check for transactions folders
        for folder in self.config.transactions_folders:
            if folder.lower() in path_parts:
                return DetectionResult(
                    statement_type=StatementType.TRANSACTIONS,
                    detection_method=DetectionMethod.FOLDER,
                    confidence=1.0,
                    matched_keywords=[f"folder:{folder}"]
                )

        # Check for holdings folders
        for folder in self.config.holdings_folders:
            if folder.lower() in path_parts:
                return DetectionResult(
                    statement_type=StatementType.HOLDINGS,
                    detection_method=DetectionMethod.FOLDER,
                    confidence=1.0,
                    matched_keywords=[f"folder:{folder}"]
                )

        return None

    def _detect_from_filename(self, stem: str) -> Optional[DetectionResult]:
        """
        Detect statement type from filename keywords.

        Returns result with higher confidence for more specific matches.
        """
        txn_matches = []
        holdings_matches = []

        # Check transaction keywords
        for keyword, pattern in self._txn_patterns:
            if pattern.search(stem):
                txn_matches.append(keyword)

        # Check holdings keywords
        for keyword, pattern in self._holdings_patterns:
            if pattern.search(stem):
                holdings_matches.append(keyword)

        # Determine winner based on match count
        if txn_matches and not holdings_matches:
            return DetectionResult(
                statement_type=StatementType.TRANSACTIONS,
                detection_method=DetectionMethod.FILENAME,
                confidence=min(0.7 + len(txn_matches) * 0.1, 1.0),
                matched_keywords=txn_matches
            )

        if holdings_matches and not txn_matches:
            return DetectionResult(
                statement_type=StatementType.HOLDINGS,
                detection_method=DetectionMethod.FILENAME,
                confidence=min(0.7 + len(holdings_matches) * 0.1, 1.0),
                matched_keywords=holdings_matches
            )

        # Ambiguous - both have matches
        if txn_matches and holdings_matches:
            if len(txn_matches) > len(holdings_matches):
                return DetectionResult(
                    statement_type=StatementType.TRANSACTIONS,
                    detection_method=DetectionMethod.FILENAME,
                    confidence=0.6,
                    matched_keywords=txn_matches,
                    warnings=["Ambiguous: both transaction and holdings keywords found"]
                )
            elif len(holdings_matches) > len(txn_matches):
                return DetectionResult(
                    statement_type=StatementType.HOLDINGS,
                    detection_method=DetectionMethod.FILENAME,
                    confidence=0.6,
                    matched_keywords=holdings_matches,
                    warnings=["Ambiguous: both transaction and holdings keywords found"]
                )

        return None

    def _detect_from_content(self, file_path: Path) -> Optional[DetectionResult]:
        """
        Detect statement type from file content.

        Analyzes first rows of Excel/CSV files for keywords.
        """
        suffix = file_path.suffix.lower()

        if suffix not in ['.xlsx', '.xls', '.csv']:
            return None

        try:
            import pandas as pd

            # Read first 50 rows for analysis
            if suffix == '.csv':
                df = pd.read_csv(file_path, nrows=50, encoding='utf-8', on_bad_lines='skip')
            else:
                df = pd.read_excel(file_path, nrows=50)

            # Convert to string for keyword matching
            content = df.to_string().lower()

            # Also check column names
            columns = ' '.join(df.columns.astype(str)).lower()
            content = f"{columns} {content}"

            # Count keyword matches
            txn_score = 0
            holdings_score = 0
            txn_matched = []
            holdings_matched = []

            for keyword in self.config.transactions_keywords:
                if keyword.lower() in content:
                    txn_score += 1
                    txn_matched.append(keyword)

            for keyword in self.config.holdings_keywords:
                if keyword.lower() in content:
                    holdings_score += 1
                    holdings_matched.append(keyword)

            # Determine winner
            if txn_score > holdings_score:
                confidence = min(0.5 + txn_score * 0.1, 0.95)
                return DetectionResult(
                    statement_type=StatementType.TRANSACTIONS,
                    detection_method=DetectionMethod.CONTENT,
                    confidence=confidence,
                    matched_keywords=txn_matched[:5]  # Limit to top 5
                )
            elif holdings_score > txn_score:
                confidence = min(0.5 + holdings_score * 0.1, 0.95)
                return DetectionResult(
                    statement_type=StatementType.HOLDINGS,
                    detection_method=DetectionMethod.CONTENT,
                    confidence=confidence,
                    matched_keywords=holdings_matched[:5]
                )

        except Exception as e:
            logger.debug(f"Content analysis failed for {file_path.name}: {e}")

        return None

    def detect_batch(self, files: List[Path]) -> Dict[Path, DetectionResult]:
        """
        Detect statement types for multiple files.

        Args:
            files: List of file paths

        Returns:
            Dict mapping file path to detection result
        """
        return {file_path: self.detect(file_path) for file_path in files}

    def get_transactions_files(self, files: List[Path]) -> List[Path]:
        """Filter files to only transaction statements."""
        return [
            f for f in files
            if self.detect(f).statement_type == StatementType.TRANSACTIONS
        ]

    def get_holdings_files(self, files: List[Path]) -> List[Path]:
        """Filter files to only holdings statements."""
        return [
            f for f in files
            if self.detect(f).statement_type == StatementType.HOLDINGS
        ]


def detect_statement_type(
    file_path: Path,
    path_resolver=None,
    config: Optional[StatementRulesConfig] = None
) -> DetectionResult:
    """
    Convenience function to detect statement type for a single file.

    Args:
        file_path: Path to the statement file
        path_resolver: Optional PathResolver for user config
        config: Optional pre-loaded config

    Returns:
        DetectionResult
    """
    detector = StatementTypeDetector(path_resolver=path_resolver, config=config)
    return detector.detect(file_path)


def create_default_config() -> Dict[str, Any]:
    """Create default statement rules config as dict (for saving to file)."""
    config = StatementRulesConfig()
    return {
        "transactions_keywords": config.transactions_keywords,
        "holdings_keywords": config.holdings_keywords,
        "file_overrides": config.file_overrides,
        "transactions_folders": config.transactions_folders,
        "holdings_folders": config.holdings_folders,
        "default_type": config.default_type,
        "min_content_confidence": config.min_content_confidence
    }
