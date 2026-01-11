"""Reports module for generating financial reports.

Provides report generators for various asset classes:
- Mutual Fund Capital Gains Statement (Excel/PDF)
- Stock Holdings Report (Excel)
- Advance Tax Report (Excel)
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

__all__ = [
    "MFCapitalGainsReport",
    "QuarterlySummary",
    "StockHoldingsReport",
    "HoldingsReportData",
    "AdvanceTaxReportGenerator",
    "AdvanceTaxCalculator",
    "AdvanceTaxSummary",
    "FinancialYear",
    "TaxSlabs",
    "generate_advance_tax_report",
]
