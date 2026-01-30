"""
Golden Reference Reconciliation Engine.

Provides services for reconciling system data against authoritative external sources:
- TruthResolver: Determines source of truth per metric/asset class
- CrossCorrelator: Compares system vs golden holdings with tolerance
- NSDLCASParser: Parses NSDL Consolidated Account Statements
- GoldenReferenceIngester: Ingests parsed data into golden reference tables

Usage:
    from pfas.services.golden_reference import (
        TruthResolver,
        CrossCorrelator,
        NSDLCASParser,
        GoldenReferenceIngester,
        AssetClass,
        MetricType,
        SourceType,
    )

    # Parse NSDL CAS
    parser = NSDLCASParser()
    cas_data = parser.parse("/path/to/cas.pdf", password="secret")

    # Ingest as golden reference
    ingester = GoldenReferenceIngester(conn, user_id=1)
    ref_id = ingester.ingest_nsdl_cas(cas_data, file_path="/path/to/cas.pdf")

    # Reconcile
    correlator = CrossCorrelator(conn, user_id=1)
    summary = correlator.reconcile_holdings(AssetClass.MUTUAL_FUND, ref_id)
    print(f"Match rate: {summary.match_rate:.1f}%")
"""

from .models import (
    MetricType,
    AssetClass,
    SourceType,
    MatchResult,
    ReconciliationStatus,
    Severity,
    SuspenseStatus,
    TruthSourceConfig,
    GoldenReference,
    GoldenHolding,
    SystemHolding,
    ReconciliationEvent,
    ReconciliationSummary,
    SuspenseItem,
    GoldenCapitalGains,
)

from .truth_resolver import TruthResolver

from .cross_correlator import (
    CrossCorrelator,
    ReconciliationConfig,
)

from .nsdl_cas_parser import (
    NSDLCASParser,
    NSDLCASData,
    NSDLHolding,
    NSDLInvestorInfo,
    GoldenReferenceIngester,
    NSDLCASParseError,
    PasswordRequiredError,
    InvalidPasswordError,
    UnsupportedFormatError,
)

from .user_config import (
    ReconciliationMode,
    ReconciliationFrequency,
    UserReconciliationSettings,
    UserConfigLoader,
)

from .reports import ReconciliationReporter

__all__ = [
    # Enums
    "MetricType",
    "AssetClass",
    "SourceType",
    "MatchResult",
    "ReconciliationStatus",
    "Severity",
    "SuspenseStatus",
    # Models
    "TruthSourceConfig",
    "GoldenReference",
    "GoldenHolding",
    "SystemHolding",
    "ReconciliationEvent",
    "ReconciliationSummary",
    "SuspenseItem",
    "GoldenCapitalGains",
    # Parser
    "NSDLCASParser",
    "NSDLCASData",
    "NSDLHolding",
    "NSDLInvestorInfo",
    "GoldenReferenceIngester",
    # Services
    "TruthResolver",
    "CrossCorrelator",
    "ReconciliationConfig",
    # Exceptions
    "NSDLCASParseError",
    "PasswordRequiredError",
    "InvalidPasswordError",
    "UnsupportedFormatError",
    # User Config
    "ReconciliationMode",
    "ReconciliationFrequency",
    "UserReconciliationSettings",
    "UserConfigLoader",
    # Reports
    "ReconciliationReporter",
]
