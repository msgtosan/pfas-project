"""Base parser infrastructure with plugin registration and normalization pipeline.

This module provides:
1. BaseParser - Abstract base class for all parsers
2. ParserRegistry - Plugin registration and discovery
3. StrictOpenXMLConverter - Handle ISO 29500 Strict format Excel files
4. NormalizationPipeline - Staging and normalization flow
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Dict, Any, Optional, Type, Callable
import json
import zipfile
import tempfile
import sqlite3
import logging
import hashlib

logger = logging.getLogger(__name__)


# =============================================================================
# Strict Open XML Converter
# =============================================================================

class StrictOpenXMLConverter:
    """
    Convert ISO 29500 Strict Open XML Excel files to Transitional format.

    Root Cause: Excel files saved in Strict conformance mode use different
    XML namespaces that openpyxl/pandas cannot read, resulting in empty
    sheet lists despite valid data being present.

    Solution: Convert namespace URIs from Strict to Transitional format.

    Example:
        converter = StrictOpenXMLConverter()
        if converter.is_strict_format(file_path):
            converted_path = converter.convert(file_path)
            df = pd.read_excel(converted_path)
    """

    STRICT_NS = "http://purl.oclc.org/ooxml/spreadsheetml/main"
    TRANSITIONAL_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"

    STRICT_REL_NS = "http://purl.oclc.org/ooxml/officeDocument/relationships"
    TRANSITIONAL_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"

    def is_strict_format(self, file_path: Path) -> bool:
        """Check if Excel file uses Strict Open XML format."""
        try:
            with zipfile.ZipFile(file_path, 'r') as z:
                if 'xl/workbook.xml' in z.namelist():
                    content = z.read('xl/workbook.xml').decode('utf-8')
                    return self.STRICT_NS in content
        except Exception as e:
            logger.warning(f"Error checking file format: {e}")
        return False

    def convert(self, file_path: Path) -> Path:
        """
        Convert Strict format to Transitional format.

        Args:
            file_path: Path to Strict format Excel file

        Returns:
            Path to converted file (temp file if conversion needed,
            original path if already Transitional)
        """
        if not self.is_strict_format(file_path):
            return file_path

        logger.info(f"Converting Strict Open XML format: {file_path.name}")

        # Create temp file
        temp_path = Path(tempfile.mktemp(suffix='.xlsx'))

        with zipfile.ZipFile(file_path, 'r') as zin:
            with zipfile.ZipFile(temp_path, 'w', zipfile.ZIP_DEFLATED) as zout:
                for item in zin.namelist():
                    content = zin.read(item)

                    # Convert XML files
                    if item.endswith('.xml') or item.endswith('.rels'):
                        content_str = content.decode('utf-8')
                        content_str = content_str.replace(
                            self.STRICT_NS,
                            self.TRANSITIONAL_NS
                        )
                        content_str = content_str.replace(
                            self.STRICT_REL_NS,
                            self.TRANSITIONAL_REL_NS
                        )
                        # Remove conformance="strict" attribute
                        content_str = content_str.replace(
                            ' conformance="strict"', ''
                        )
                        content = content_str.encode('utf-8')

                    zout.writestr(item, content)

        logger.info(f"Converted to: {temp_path}")
        return temp_path


# =============================================================================
# Base Parser Interface
# =============================================================================

@dataclass
class ParsedRecord:
    """
    Raw parsed record before normalization.

    Attributes:
        source_type: Parser identifier (CAMS, KARVY, ZERODHA, etc.)
        source_file: Original file path
        raw_data: Original data as dictionary
        row_index: Row number in source file
        checksum: Hash for deduplication
    """
    source_type: str
    source_file: str
    raw_data: Dict[str, Any]
    row_index: int = 0
    checksum: str = ""

    def __post_init__(self):
        if not self.checksum:
            # Generate checksum from raw data for deduplication
            data_str = json.dumps(self.raw_data, sort_keys=True, default=str)
            self.checksum = hashlib.md5(data_str.encode()).hexdigest()


@dataclass
class NormalizationResult:
    """Result of normalizing parsed records."""
    success: bool
    normalized_count: int = 0
    error_count: int = 0
    duplicate_count: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class BaseParser(ABC):
    """
    Abstract base class for all parsers.

    Implements the template method pattern for parsing:
    1. validate() - Check file validity
    2. parse_raw() - Extract raw records
    3. normalize() - Convert to NormalizedTransaction

    Subclasses must implement:
    - parse_raw(): Extract source-specific data
    - normalize_record(): Convert single record to normalized format
    - get_source_type(): Return parser identifier
    - get_supported_formats(): Return list of supported extensions
    """

    def __init__(self, db_connection: sqlite3.Connection):
        """Initialize parser with database connection."""
        self.conn = db_connection
        self._converter = StrictOpenXMLConverter()

    @abstractmethod
    def get_source_type(self) -> str:
        """Return unique identifier for this parser (e.g., 'CAMS', 'KARVY')."""
        pass

    @abstractmethod
    def get_supported_formats(self) -> List[str]:
        """Return list of supported file extensions (e.g., ['.xlsx', '.pdf'])."""
        pass

    @abstractmethod
    def parse_raw(self, file_path: Path) -> List[ParsedRecord]:
        """
        Parse file and return raw records.

        Args:
            file_path: Path to source file

        Returns:
            List of ParsedRecord objects with raw data
        """
        pass

    @abstractmethod
    def normalize_record(self, record: ParsedRecord) -> Optional[Dict[str, Any]]:
        """
        Normalize a single raw record.

        Args:
            record: ParsedRecord with raw data

        Returns:
            Dictionary with normalized fields, or None if invalid
        """
        pass

    def validate(self, file_path: Path) -> bool:
        """
        Validate if file can be processed by this parser.

        Args:
            file_path: Path to file

        Returns:
            True if file is valid for this parser
        """
        if not file_path.exists():
            return False
        return file_path.suffix.lower() in self.get_supported_formats()

    def parse(self, file_path: Path) -> NormalizationResult:
        """
        Full parsing pipeline: validate → parse_raw → normalize.

        Args:
            file_path: Path to source file

        Returns:
            NormalizationResult with status and counts
        """
        result = NormalizationResult(success=True)
        file_path = Path(file_path)

        # Validate
        if not self.validate(file_path):
            result.success = False
            result.errors.append(f"Invalid file: {file_path}")
            return result

        # Handle Strict Open XML format
        working_path = self._converter.convert(file_path)

        try:
            # Parse raw records
            raw_records = self.parse_raw(working_path)

            if not raw_records:
                result.warnings.append("No records found in file")
                return result

            # Normalize each record
            for record in raw_records:
                try:
                    normalized = self.normalize_record(record)
                    if normalized:
                        # Store in staging
                        self._store_normalized(record, normalized)
                        result.normalized_count += 1
                    else:
                        result.warnings.append(f"Row {record.row_index}: Could not normalize")
                except Exception as e:
                    result.error_count += 1
                    result.errors.append(f"Row {record.row_index}: {str(e)}")

        except Exception as e:
            result.success = False
            result.errors.append(f"Parse error: {str(e)}")
        finally:
            # Clean up temp file
            if working_path != file_path and working_path.exists():
                working_path.unlink()

        return result

    def _store_normalized(self, raw: ParsedRecord, normalized: Dict[str, Any]):
        """Store normalized record in staging table."""
        # Will be implemented when staging tables are created
        pass


# =============================================================================
# Parser Registry
# =============================================================================

class ParserRegistry:
    """
    Registry for parser plugins.

    Allows dynamic registration and discovery of parsers.

    Example:
        # Register a parser
        ParserRegistry.register('CAMS', CAMSParser)

        # Get parser by name
        parser = ParserRegistry.get_parser('CAMS', db_connection)

        # Auto-detect parser for file
        parser = ParserRegistry.detect_parser(file_path, db_connection)
    """

    _parsers: Dict[str, Type[BaseParser]] = {}
    _file_patterns: Dict[str, str] = {}  # Pattern → parser name

    @classmethod
    def register(cls, name: str, parser_class: Type[BaseParser],
                 file_patterns: List[str] = None):
        """
        Register a parser class.

        Args:
            name: Unique parser identifier
            parser_class: Parser class (must extend BaseParser)
            file_patterns: Optional filename patterns for auto-detection
        """
        cls._parsers[name.upper()] = parser_class

        if file_patterns:
            for pattern in file_patterns:
                cls._file_patterns[pattern.lower()] = name.upper()

        logger.debug(f"Registered parser: {name}")

    @classmethod
    def get_parser(cls, name: str, db_connection: sqlite3.Connection) -> BaseParser:
        """
        Get parser instance by name.

        Args:
            name: Parser identifier
            db_connection: Database connection

        Returns:
            Parser instance

        Raises:
            KeyError if parser not found
        """
        name = name.upper()
        if name not in cls._parsers:
            raise KeyError(f"Unknown parser: {name}. Available: {list(cls._parsers.keys())}")
        return cls._parsers[name](db_connection)

    @classmethod
    def detect_parser(cls, file_path: Path,
                      db_connection: sqlite3.Connection) -> Optional[BaseParser]:
        """
        Auto-detect appropriate parser for a file.

        Args:
            file_path: Path to file
            db_connection: Database connection

        Returns:
            Parser instance or None if no match
        """
        filename = file_path.name.lower()

        # Check filename patterns
        for pattern, parser_name in cls._file_patterns.items():
            if pattern in filename:
                return cls.get_parser(parser_name, db_connection)

        # Try each parser's validate method
        for name, parser_class in cls._parsers.items():
            parser = parser_class(db_connection)
            if parser.validate(file_path):
                return parser

        return None

    @classmethod
    def list_parsers(cls) -> List[str]:
        """Return list of registered parser names."""
        return list(cls._parsers.keys())


# =============================================================================
# Column Mapping Configuration
# =============================================================================

@dataclass
class ColumnMapping:
    """
    Configuration for mapping source columns to normalized fields.

    Attributes:
        source_column: Column name in source file
        target_field: Field name in NormalizedTransaction
        transform: Optional transform function name
        default: Default value if source is empty
        required: Whether field is required
    """
    source_column: str
    target_field: str
    transform: Optional[str] = None
    default: Any = None
    required: bool = False


class ColumnMappingConfig:
    """
    JSON-based column mapping configuration loader.

    Example config file (cams_columns.json):
    {
        "source_type": "CAMS",
        "sheet_name": "TRXN_DETAILS",
        "header_row": 3,
        "mappings": [
            {
                "source_column": "Scheme Name",
                "target_field": "asset_name",
                "required": true
            },
            {
                "source_column": "Date",
                "target_field": "date",
                "transform": "parse_date",
                "required": true
            },
            {
                "source_column": "Amount",
                "target_field": "amount",
                "transform": "parse_decimal",
                "required": true
            }
        ],
        "transforms": {
            "parse_date": "datetime.strptime(value, '%d-%b-%Y').date()",
            "parse_decimal": "Decimal(str(value).replace(',', ''))"
        }
    }
    """

    def __init__(self, config_path: Path = None):
        """Initialize with optional config file path."""
        self.config_path = config_path or Path("config/parser_configs")
        self._configs: Dict[str, Dict] = {}
        self._transforms: Dict[str, Callable] = self._default_transforms()

    def _default_transforms(self) -> Dict[str, Callable]:
        """Default transform functions."""
        return {
            "parse_date_dmy": lambda v: datetime.strptime(str(v), '%d/%m/%Y').date() if v else None,
            "parse_date_mdy": lambda v: datetime.strptime(str(v), '%m/%d/%Y').date() if v else None,
            "parse_date_ymd": lambda v: datetime.strptime(str(v), '%Y-%m-%d').date() if v else None,
            "parse_date_dby": lambda v: datetime.strptime(str(v), '%d-%b-%Y').date() if v else None,
            "parse_decimal": lambda v: Decimal(str(v).replace(',', '').replace(' ', '')) if v else Decimal("0"),
            "parse_int": lambda v: int(float(str(v).replace(',', ''))) if v else 0,
            "strip": lambda v: str(v).strip() if v else "",
            "upper": lambda v: str(v).upper().strip() if v else "",
            "extract_isin": lambda v: self._extract_isin(v),
        }

    def _extract_isin(self, value: str) -> str:
        """Extract ISIN from scheme name or text."""
        import re
        if not value:
            return ""
        match = re.search(r'INF[A-Z0-9]{9}[0-9]', str(value))
        return match.group() if match else ""

    def load_config(self, source_type: str) -> Dict:
        """Load configuration for a source type."""
        if source_type in self._configs:
            return self._configs[source_type]

        config_file = self.config_path / f"{source_type.lower()}_columns.json"

        if config_file.exists():
            with open(config_file) as f:
                config = json.load(f)
                self._configs[source_type] = config
                return config

        return {}

    def apply_mapping(self, row: Dict[str, Any], source_type: str) -> Dict[str, Any]:
        """
        Apply column mapping to a row of data.

        Args:
            row: Source data dictionary
            source_type: Parser source type

        Returns:
            Normalized data dictionary
        """
        config = self.load_config(source_type)
        if not config:
            return row  # Return as-is if no config

        result = {}

        for mapping in config.get('mappings', []):
            source_col = mapping['source_column']
            target_field = mapping['target_field']
            transform = mapping.get('transform')
            default = mapping.get('default')
            required = mapping.get('required', False)

            # Get source value
            value = row.get(source_col)

            # Apply transform
            if transform and value is not None:
                if transform in self._transforms:
                    try:
                        value = self._transforms[transform](value)
                    except Exception as e:
                        logger.warning(f"Transform {transform} failed for {source_col}: {e}")
                        value = default

            # Handle missing required fields
            if value is None:
                if required:
                    raise ValueError(f"Required field missing: {source_col}")
                value = default

            result[target_field] = value

        return result

    def register_transform(self, name: str, func: Callable):
        """Register a custom transform function."""
        self._transforms[name] = func


# =============================================================================
# Staging Pipeline
# =============================================================================

class StagingPipeline:
    """
    Manages the staging → normalization → final tables pipeline.

    Flow:
    1. Raw records stored in staging_raw (preserves original data)
    2. Normalization creates records in staging_normalized
    3. User context service moves to final tables with user_id

    Tables:
    - staging_raw: Original parsed data as JSON
    - staging_normalized: Unified NormalizedTransaction format
    """

    def __init__(self, db_connection: sqlite3.Connection):
        self.conn = db_connection

    def store_raw(self, record: ParsedRecord) -> int:
        """
        Store raw record in staging_raw table.

        Args:
            record: ParsedRecord with source data

        Returns:
            Record ID
        """
        cursor = self.conn.execute("""
            INSERT INTO staging_raw (
                source_type, source_file, raw_data, row_index, checksum, created_at
            ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(checksum) DO NOTHING
        """, (
            record.source_type,
            record.source_file,
            json.dumps(record.raw_data, default=str),
            record.row_index,
            record.checksum,
        ))
        self.conn.commit()
        return cursor.lastrowid or 0

    def store_normalized(self, raw_id: int, normalized: Dict[str, Any]) -> int:
        """
        Store normalized record in staging_normalized table.

        Args:
            raw_id: ID from staging_raw table
            normalized: Normalized data dictionary

        Returns:
            Record ID
        """
        cursor = self.conn.execute("""
            INSERT INTO staging_normalized (
                staging_raw_id, transaction_date, amount, transaction_type,
                asset_category, asset_identifier, asset_name,
                quantity, unit_price, activity_type, flow_direction,
                source_type, extra_data, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            raw_id,
            normalized.get('date'),
            float(normalized.get('amount', 0)),
            normalized.get('transaction_type'),
            normalized.get('asset_category'),
            normalized.get('asset_identifier', ''),
            normalized.get('asset_name', ''),
            float(normalized.get('quantity')) if normalized.get('quantity') else None,
            float(normalized.get('unit_price')) if normalized.get('unit_price') else None,
            normalized.get('activity_type'),
            normalized.get('flow_direction'),
            normalized.get('source_type'),
            json.dumps({k: v for k, v in normalized.items()
                       if k not in ('date', 'amount', 'transaction_type', 'asset_category',
                                   'asset_identifier', 'asset_name', 'quantity', 'unit_price',
                                   'activity_type', 'flow_direction', 'source_type')},
                      default=str),
        ))
        self.conn.commit()
        return cursor.lastrowid

    def get_pending_records(self, source_type: str = None,
                           limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Get normalized records pending user assignment.

        Args:
            source_type: Optional filter by source
            limit: Maximum records to return

        Returns:
            List of normalized records
        """
        query = """
            SELECT sn.*, sr.source_file
            FROM staging_normalized sn
            JOIN staging_raw sr ON sn.staging_raw_id = sr.id
            WHERE sn.user_id IS NULL
        """
        params = []

        if source_type:
            query += " AND sn.source_type = ?"
            params.append(source_type)

        query += f" ORDER BY sn.created_at LIMIT {limit}"

        cursor = self.conn.execute(query, params)
        columns = [desc[0] for desc in cursor.description]

        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def assign_user(self, record_ids: List[int], user_id: int):
        """
        Assign user to normalized records.

        Args:
            record_ids: List of staging_normalized IDs
            user_id: User ID to assign
        """
        placeholders = ','.join(['?'] * len(record_ids))
        self.conn.execute(f"""
            UPDATE staging_normalized
            SET user_id = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id IN ({placeholders})
        """, [user_id] + record_ids)
        self.conn.commit()

    def migrate_to_final(self, user_id: int, target_table: str) -> int:
        """
        Migrate normalized records to final table.

        Args:
            user_id: User ID
            target_table: Target table name (e.g., 'mf_transactions')

        Returns:
            Number of records migrated
        """
        # This would be customized per target table
        # For now, return count of pending records
        cursor = self.conn.execute("""
            SELECT COUNT(*) FROM staging_normalized
            WHERE user_id = ? AND migrated_at IS NULL
        """, (user_id,))
        return cursor.fetchone()[0]
