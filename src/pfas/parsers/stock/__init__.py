"""Stock parsers module.

Provides parsers for stock broker statements and trade files.
Supports Zerodha Tax P&L and ICICI Direct Capital Gains formats.
"""

from .models import (
    StockTrade,
    StockDividend,
    StockHolding,
    STTEntry,
    TradeType,
    TradeCategory,
    ParseResult,
    CapitalGainsSummary,
    DividendSummary,
    STTSummary,
)
from .zerodha import ZerodhaParser
from .icici import ICICIDirectParser

__all__ = [
    "StockTrade",
    "StockDividend",
    "StockHolding",
    "STTEntry",
    "TradeType",
    "TradeCategory",
    "ParseResult",
    "CapitalGainsSummary",
    "DividendSummary",
    "STTSummary",
    "ZerodhaParser",
    "ICICIDirectParser",
]
