# S3: ICICI Direct Stock Parser Design

## Overview

Parser for ICICI Direct Capital Gains reports (CSV format) that provides pre-matched buy-sell pairs with capital gains calculations.

## File Format: ICICI Direct Capital Gains CSV

### Structure
```
Row 0: Account | 8500480693
Row 1: Name | SANJAY SHANKAR
Row 2: Capital Gain | 2024-2025
Row 3: Stock Symbol | ISIN | Qty | Sale Date | Sale Rate | Sale Value | Sale Expenses | Purchase Date | Purchase Rate | Price as on 31st Jan 2018 | Purchase Price Considered | Purchase Value | Purchase Expenses | Profit/Loss(-)
Row 4: Short Term Capital Gain (STT paid)
Row 5-N: STCG data rows...
Row N+1: Total | ... | STCG totals
Row N+2: Long Term Capital Gain (STT paid)
Row N+3-M: LTCG data rows...
Row M+1: Total | ... | LTCG totals
Row M+2: Grand Total | ... | totals
```

### Columns (Header at Row 3)
| Column | Description | Type |
|--------|-------------|------|
| Stock Symbol | Trading symbol (e.g., DATPAT) | str |
| ISIN | International Securities ID | str |
| Qty | Quantity sold | int |
| Sale Date | Date of sale (DD-MMM-YY) | date |
| Sale Rate | Price per share at sale | Decimal |
| Sale Value | Total sale value | Decimal |
| Sale Expenses | Brokerage + STT + other | Decimal |
| Purchase Date | Original buy date | date |
| Purchase Rate | Price per share at purchase | Decimal |
| Price as on 31st Jan 2018 | Grandfathering FMV | Decimal |
| Purchase Price Considered | For LTCG grandfathering | Decimal |
| Purchase Value | Total purchase value | Decimal |
| Purchase Expenses | Buy-side charges | Decimal |
| Profit/Loss(-) | Capital gain/loss | Decimal |

### Section Markers
- `Short Term Capital Gain (STT paid)` - STCG section start
- `Long Term Capital Gain (STT paid)` - LTCG section start
- `Total` - Section totals (skip)
- `Grand Total` - File totals (skip)
- `Note:` - Footer notes (skip)

## Tax Treatment (FY 2024-25)

### Short-Term Capital Gains (STCG)
- Holding period < 12 months
- Tax rate: 20% (equity with STT paid)

### Long-Term Capital Gains (LTCG)
- Holding period >= 12 months
- Tax rate: 12.5% (equity with STT paid)
- Exemption: ₹1.25 lakh per financial year
- Grandfathering: For purchases before 31-Jan-2018, higher of purchase price or FMV on 31-Jan-2018 (capped at sale price)

## Implementation

### Class: ICICIDirectParser

```python
class ICICIDirectParser:
    """
    Parser for ICICI Direct Capital Gains CSV reports.

    Handles:
    - Pre-matched buy-sell pairs
    - STCG/LTCG section separation
    - Grandfathering for pre-Jan-2018 purchases
    - Expense tracking (brokerage, STT)
    """

    def parse(self, file_path: Path) -> ParseResult:
        """Parse ICICI Direct Capital Gains CSV."""

    def _parse_csv(self, df: DataFrame, result: ParseResult) -> list[StockTrade]:
        """Parse CSV data into StockTrade objects."""

    def _parse_date(self, value) -> Optional[date]:
        """Parse date from DD-MMM-YY format."""

    def _determine_trade_category(self, is_long_term: bool) -> TradeCategory:
        """Return DELIVERY category (STT paid = listed stocks)."""

    def calculate_capital_gains(
        self,
        trades: list[StockTrade],
        fy: str
    ) -> CapitalGainsSummary:
        """Calculate capital gains summary for financial year."""
```

### Parsing Logic

1. Read CSV with header at row 3
2. Identify section markers:
   - STCG section: `Stock Symbol == "Short Term Capital Gain (STT paid)"`
   - LTCG section: `Stock Symbol == "Long Term Capital Gain (STT paid)"`
3. Skip non-data rows (Total, Grand Total, Note)
4. For each data row:
   - Create SELL trade with pre-matched buy info
   - Set `is_long_term` based on section
   - Calculate holding period from purchase date
   - Include expenses in net amount calculation

### Date Format
- ICICI uses DD-MMM-YY format (e.g., "21-May-24")
- Parser must handle this format: `datetime.strptime(value, "%d-%b-%y")`

## Database Schema

Uses existing `stock_trades` table from database.py:

```sql
CREATE TABLE IF NOT EXISTS stock_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    broker_id INTEGER,
    user_id INTEGER,
    symbol TEXT NOT NULL,
    isin TEXT,
    trade_date DATE NOT NULL,
    trade_type TEXT NOT NULL CHECK(trade_type IN ('BUY', 'SELL')),
    quantity INTEGER NOT NULL,
    price DECIMAL(15,2) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    brokerage DECIMAL(15,2) DEFAULT 0,
    stt DECIMAL(15,2) DEFAULT 0,
    exchange_charges DECIMAL(15,2) DEFAULT 0,
    gst DECIMAL(15,2) DEFAULT 0,
    sebi_charges DECIMAL(15,2) DEFAULT 0,
    stamp_duty DECIMAL(15,2) DEFAULT 0,
    net_amount DECIMAL(15,2) NOT NULL,
    trade_category TEXT CHECK(trade_category IN ('INTRADAY', 'DELIVERY', 'FNO')),
    buy_date DATE,
    buy_price DECIMAL(15,2),
    cost_of_acquisition DECIMAL(15,2),
    holding_period_days INTEGER,
    is_long_term BOOLEAN,
    capital_gain DECIMAL(15,2),
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (broker_id) REFERENCES stock_brokers(id) ON DELETE RESTRICT,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);
```

## File Locations

- Source: `src/pfas/parsers/stock/icici.py`
- Tests: `tests/unit/test_parsers/test_stock/test_icici.py`
- Test Data: `Data/Users/Sanjay/Indian-Stocks/ICICIDirect/`

## ICICI Direct Holdings XLSX

The XLSX file (`ICICID_Stock_holding_Sanjay_FY24-25.xlsx`) contains current holdings, not capital gains:

| Column | Description |
|--------|-------------|
| Stock Name | Full company name |
| Stock | Trading symbol |
| ISIN | Securities ID |
| Allocated Quantity | Current holding |
| Current Market Price | Latest price |

This file can be used for portfolio valuation but is **not** used for capital gains calculation.

## Integration with Zerodha

Both parsers produce the same `StockTrade` and `ParseResult` models, allowing unified:
- Capital gains calculation
- Database storage
- Tax reporting
