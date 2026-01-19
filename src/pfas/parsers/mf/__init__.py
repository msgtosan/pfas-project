"""Mutual Fund parsers for CAMS CAS, Karvy/KFintech, and other formats."""

from .models import (
    MFScheme, MFTransaction, AssetClass, TransactionType, ParseResult,
    # CAS-specific models
    CASFileType, CASSource, InvestorInfo, StatementPeriod, CASTransaction,
    SchemeValuation, CASScheme, CASFolio, CASData
)
from .cams import CAMSParser
from .karvy import KarvyParser
from .classifier import classify_scheme
from .capital_gains import CapitalGainsCalculator, CapitalGainsSummary
from .pdf_extractor import check_pdf_support
from .scanner import MFStatementScanner, ScannedFile, ScanResult, RTA, FileType, scan_mf_inbox
from .ingester import MFIngester, IngestionResult, ingest_mf_statements

# Phase 1 gap fixes
from .exceptions import (
    MFParserError, CASParseError, HeaderParseError, IncorrectPasswordError,
    UnsupportedFormatError, IntegrityError, BalanceMismatchError,
    IncompleteDataError, GainsCalculationError, FIFOMismatchError, GrandfatheringError
)
from .fifo_tracker import (
    PurchaseLot, GainResult, FIFOUnitTracker, PortfolioFIFOTracker
)
from .cas_pdf_parser import (
    CASPDFParser, parse_cas_pdf, check_cas_support,
    ConsolidationResult, FolioConsolidationEntry
)
from .cas_report_generator import CASReportGenerator, generate_cas_reports

__all__ = [
    # Core models
    "MFScheme",
    "MFTransaction",
    "AssetClass",
    "TransactionType",
    "ParseResult",
    # CAS models
    "CASFileType",
    "CASSource",
    "InvestorInfo",
    "StatementPeriod",
    "CASTransaction",
    "SchemeValuation",
    "CASScheme",
    "CASFolio",
    "CASData",
    # Parsers
    "CAMSParser",
    "KarvyParser",
    "CASPDFParser",
    "parse_cas_pdf",
    "classify_scheme",
    "CapitalGainsCalculator",
    "CapitalGainsSummary",
    "check_pdf_support",
    "check_cas_support",
    "ConsolidationResult",
    "FolioConsolidationEntry",
    "CASReportGenerator",
    "generate_cas_reports",
    # Scanner and ingester
    "MFStatementScanner",
    "ScannedFile",
    "ScanResult",
    "RTA",
    "FileType",
    "scan_mf_inbox",
    "MFIngester",
    "IngestionResult",
    "ingest_mf_statements",
    # Exceptions
    "MFParserError",
    "CASParseError",
    "HeaderParseError",
    "IncorrectPasswordError",
    "UnsupportedFormatError",
    "IntegrityError",
    "BalanceMismatchError",
    "IncompleteDataError",
    "GainsCalculationError",
    "FIFOMismatchError",
    "GrandfatheringError",
    # FIFO tracking
    "PurchaseLot",
    "GainResult",
    "FIFOUnitTracker",
    "PortfolioFIFOTracker",
]
