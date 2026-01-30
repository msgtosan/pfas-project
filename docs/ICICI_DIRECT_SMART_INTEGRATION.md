# ICICI Direct Smart Integration Strategy

## Overview

ICICI Direct provides multiple statement types, each serving different purposes:

| Statement Type | Format | Contains | Best For |
|----------------|--------|----------|----------|
| **Transaction PDF** (TRX-Equity_*.PDF) | PDF | Individual trades with exact timestamps | Cost basis tracking, audit trail |
| **Capital Gains CSV** (*ICICIDirect_FY*.csv) | CSV | Matched buy-sell pairs with P&L | Tax filing, capital gains calculation |
| **P&L XLS** (*ProfitLossDetails*.xls) | TSV | Simplified P&L summary | Quick overview |
| **Holdings** | XLSX | Current portfolio | Portfolio valuation |

## Smart Processing Strategy

### 1. Transaction PDF → `stock_trades` Table
**Purpose**: Complete transaction ledger with actual cost basis

```
PDF Transaction Data → Normalize → stock_trades
                                   ├── contract_number (unique)
                                   ├── trade_date, trade_time
                                   ├── isin, symbol, security_name
                                   ├── buy_sell, quantity, price
                                   ├── brokerage, gst, stt, stamp_duty
                                   └── net_amount (actual cost/proceeds)
```

**Key Intelligence:**
- Exact cost basis for each purchase (price + all charges)
- Exact proceeds for each sale (price - all charges)
- Contract number for audit trail
- Settlement dates for cash flow tracking

### 2. Capital Gains CSV → `stock_capital_gains_detail` Table
**Purpose**: Tax-ready capital gains with FIFO matching

```
CSV Capital Gains → Normalize → stock_capital_gains_detail
                                ├── buy_date, sell_date
                                ├── buy_price, sell_price (grandfathered if applicable)
                                ├── profit_loss, taxable_profit
                                ├── holding_period_days
                                └── gain_type (STCG/LTCG)
```

**Key Intelligence:**
- FIFO matched buy-sell pairs (done by ICICI)
- Grandfathered cost (Price as on 31st Jan 2018)
- Ready for ITR Schedule CG
- Handles corporate actions implicitly

### 3. Cross-Validation

```
stock_trades (from PDF)     stock_capital_gains_detail (from CSV)
        │                              │
        └──────────┬───────────────────┘
                   │
              ┌────▼────┐
              │ VERIFY  │
              └────┬────┘
                   │
    ┌──────────────┼──────────────┐
    │              │              │
Total Sells   Total Buys    Cost Basis
 Match?        Match?       Consistent?
```

## Data Flow

```
inbox/Indian-Stocks/ICICIDirect/
├── transactions/
│   ├── TRX-Equity_06-01-2026_*.PDF  →  stock_trades (individual trades)
│   ├── 8500480693_EQProfitLossDetails_FY24_25.xls  →  stock_capital_gains_detail
│   └── 8500480693_EQProfitLossDetails_FY25_26.xls  →  stock_capital_gains_detail
├── holdings/
│   └── *holding*.xlsx  →  stock_holdings (current positions)
└── *ICICIDirect_FY*.csv  →  stock_capital_gains_detail (alternate format)
```

## Implementation

### ICICIDirectProcessor Class

```python
class ICICIDirectProcessor:
    """Smart processor for all ICICI Direct statement types."""

    def __init__(self, conn, user_id: int, path_resolver):
        self.conn = conn
        self.user_id = user_id
        self.resolver = path_resolver
        self.broker_id = self._get_broker_id("ICICI")

    def process_all(self, base_path: Path) -> ProcessResult:
        """Process all ICICI Direct statements intelligently."""
        result = ProcessResult()

        # 1. Process Transaction PDFs first (establishes cost basis)
        pdf_files = list(base_path.glob("**/TRX-Equity*.PDF"))
        for pdf in sorted(pdf_files):
            self._process_transaction_pdf(pdf, result)

        # 2. Process Capital Gains CSV/XLS (tax calculations)
        cg_files = list(base_path.glob("**/*ProfitLoss*.xls")) + \
                   list(base_path.glob("**/*ICICIDirect_FY*.csv"))
        for cg_file in cg_files:
            self._process_capital_gains(cg_file, result)

        # 3. Process Holdings
        holding_files = list(base_path.glob("**/*holding*.xlsx"))
        for hf in holding_files:
            self._process_holdings(hf, result)

        # 4. Cross-validate
        self._cross_validate(result)

        return result
```

### Database Schema Enhancements

Add to `stock_trades` table:
```sql
ALTER TABLE stock_trades ADD COLUMN contract_number TEXT;
ALTER TABLE stock_trades ADD COLUMN settlement_no TEXT;
ALTER TABLE stock_trades ADD COLUMN trade_no TEXT;
ALTER TABLE stock_trades ADD COLUMN trade_time TEXT;
ALTER TABLE stock_trades ADD COLUMN settlement_date DATE;
ALTER TABLE stock_trades ADD COLUMN transaction_charges DECIMAL(15,2) DEFAULT 0;

-- Unique constraint for deduplication
CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_trades_contract
ON stock_trades(user_id, contract_number, trade_no);
```

### Processing Rules

1. **Deduplication**: Use contract_number + trade_no as unique key
2. **Cost Basis**: PDF net_amount includes all charges
3. **FIFO Matching**: Trust ICICI's CSV for tax filing
4. **Grandfathering**: Use CSV's "Purchase Price Considered" field
5. **Corporate Actions**: CSV handles splits/bonuses implicitly

### Example Usage

```python
from pfas.parsers.stock.icici_processor import ICICIDirectProcessor

processor = ICICIDirectProcessor(conn, user_id, resolver)
result = processor.process_all(
    Path("inbox/Indian-Stocks/ICICIDirect")
)

print(f"Trades ingested: {result.trades_count}")
print(f"Capital gains records: {result.cg_count}")
print(f"Validation status: {result.validation_status}")
```

## Intelligence Derived

### 1. Actual Cost Basis
```
From PDF:
  Buy 100 RELIANCE @ ₹2,500 = ₹250,000
  + Brokerage: ₹250
  + GST: ₹45
  + STT: ₹25
  + Stamp Duty: ₹37.50
  ─────────────────
  Actual Cost: ₹250,357.50
  Cost per share: ₹2,503.58
```

### 2. Holding Period Tracking
```
From PDF contract dates:
  Buy: 15-Jan-2024 (contract ISEC/2024015/xxxxx)
  Sell: 20-Jul-2024 (contract ISEC/2024202/xxxxx)

  Holding period: 186 days → STCG
```

### 3. Tax-Ready Capital Gains
```
From CSV (with grandfathering):
  Buy Date: 15-Mar-2017
  Buy Price: ₹500
  FMV 31-Jan-2018: ₹800
  Sell Date: 20-Dec-2024
  Sell Price: ₹1,200

  Grandfathered Cost: ₹800 (higher of buy price and FMV)
  Capital Gain: ₹1,200 - ₹800 = ₹400 (LTCG)
```

### 4. STT Deduction Tracking
```
From Summary section:
  Total STT Paid: ₹5,334

  Deductible under "Income from Business" if trading frequently
```

## Report Enhancements

### Cost Basis Report
| Date | Symbol | Qty | Buy Price | Charges | Total Cost | Per Share |
|------|--------|-----|-----------|---------|------------|-----------|
| 01-Oct-25 | GOLDBEES | 1000 | 96,650 | 114.60 | 96,764.60 | 96.76 |

### Settlement Report
| Settlement | Date | Trades | STT | Charges | Stamp | Net |
|------------|------|--------|-----|---------|-------|-----|
| 2025188 | 03-Oct | 1 | 0 | 2.97 | 15.00 | 96,782.66 |

### Tax Summary
| FY | STCG Trades | STCG Amount | LTCG Trades | LTCG Amount | Exemption |
|----|-------------|-------------|-------------|-------------|-----------|
| 2024-25 | 228 | ₹9,930 | 337 | ₹20,95,256 | ₹1,25,000 |

## Future Enhancements

1. **Auto-download**: Integration with ICICI Direct API (if available)
2. **Dividend Tracking**: Parse dividend statements
3. **Corporate Action Alerts**: Detect splits, bonuses from price anomalies
4. **Portfolio Analytics**: Compare cost basis vs market value
5. **Tax Loss Harvesting**: Identify STCG loss candidates
