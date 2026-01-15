"""
Asset Parsers for PFAS - Phase 1 Assets.

Supports:
- Rental Income (from bank statements)
- SGB Holdings and Interest (from Excel + bank statements)
- REIT/InvIT Distributions (from bank statements)
- Dividends (from bank statements)

New Tax Regime only - simplified tax handling.
"""

from .models import (
    Property,
    PropertyType,
    RentalIncome,
    RentalIncomeCalculation,
    SGBHolding,
    SGBInterest,
    SGBSummary,
    REITHolding,
    REITDistribution,
    DistributionType,
    DividendRecord,
    DividendSummary,
    AssetIncomeSummary,
)
from .rental import RentalIncomeCalculator, RentalIncomeManager
from .sgb import SGBParser, SGBTracker
from .reit import REITTracker
from .dividends import DividendTracker
from .bank_integration import BankAssetIntegration, extract_all_from_bank

__all__ = [
    # Models
    "Property",
    "PropertyType",
    "RentalIncome",
    "RentalIncomeCalculation",
    "SGBHolding",
    "SGBInterest",
    "SGBSummary",
    "REITHolding",
    "REITDistribution",
    "DistributionType",
    "DividendRecord",
    "DividendSummary",
    "AssetIncomeSummary",
    # Managers/Trackers
    "RentalIncomeCalculator",
    "RentalIncomeManager",
    "SGBParser",
    "SGBTracker",
    "REITTracker",
    "DividendTracker",
    # Integration
    "BankAssetIntegration",
    "extract_all_from_bank",
]
