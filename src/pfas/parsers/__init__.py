"""
PFAS Parsers - Bank and investment statement parsers.

Supports parsing of:
- Bank statements (ICICI, SBI, HDFC)
- Mutual fund statements (CAMS, Karvy)
- Stock broker statements (Zerodha, ICICI Direct)
- Retirement accounts (EPF, NPS, PPF)
- Salary slips and Form 16

Architecture:
- BaseParser: Abstract base class for all parsers
- ParserRegistry: Plugin registration and discovery
- StagingPipeline: Raw → Normalized → Final table flow
- ColumnMappingConfig: JSON-based column mapping
"""

from .base import (
    BaseParser,
    ParsedRecord,
    NormalizationResult,
    ParserRegistry,
    StrictOpenXMLConverter,
    ColumnMappingConfig,
    StagingPipeline,
)

__version__ = "0.2.0"

__all__ = [
    # Base infrastructure
    "BaseParser",
    "ParsedRecord",
    "NormalizationResult",
    "ParserRegistry",
    "StrictOpenXMLConverter",
    "ColumnMappingConfig",
    "StagingPipeline",
]
