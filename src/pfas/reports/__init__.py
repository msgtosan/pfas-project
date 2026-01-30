"""Reports module for generating financial reports.

Provides report generators for various asset classes:
- Mutual Fund Capital Gains Statement (Excel/PDF)
- Stock Holdings Report (Excel)
- Advance Tax Report (Excel)
- Networth Report (Excel) - Comprehensive multi-asset networth tracking
"""

from .mf_capital_gains_report import MFCapitalGainsReport, QuarterlySummary
from .stock_holdings_report import StockHoldingsReport, HoldingsReportData
from .advance_tax_report import (
    AdvanceTaxReportGenerator,
    AdvanceTaxCalculator,
    AdvanceTaxSummary,
    FinancialYear,
    TaxSlabs,
    generate_advance_tax_report,
)
from .networth_report import (
    NetworthCalculator,
    NetworthReportGenerator,
    NetworthSummary,
    AssetHolding,
    PeriodSnapshot,
    AssetMetrics,
    XIRRCalculator,
)

__all__ = [
    # MF Reports
    "MFCapitalGainsReport",
    "QuarterlySummary",
    # Stock Reports
    "StockHoldingsReport",
    "HoldingsReportData",
    # Advance Tax Reports
    "AdvanceTaxReportGenerator",
    "AdvanceTaxCalculator",
    "AdvanceTaxSummary",
    "FinancialYear",
    "TaxSlabs",
    "generate_advance_tax_report",
    # Networth Reports
    "NetworthCalculator",
    "NetworthReportGenerator",
    "NetworthSummary",
    "AssetHolding",
    "PeriodSnapshot",
    "AssetMetrics",
    "XIRRCalculator",
]
