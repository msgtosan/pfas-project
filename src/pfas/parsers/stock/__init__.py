"""Stock parsers module.

Provides parsers for stock broker statements and trade files.
Supports Zerodha Tax P&L and other broker formats.
"""

from .models import (
    StockTrade,
    TradeType,
    TradeCategory,
    ParseResult,
    CapitalGainsSummary,
)
from .zerodha import ZerodhaParser

__all__ = [
    "StockTrade",
    "TradeType",
    "TradeCategory",
    "ParseResult",
    "CapitalGainsSummary",
    "ZerodhaParser",
]
