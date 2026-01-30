# Stock Statement Analyzer - Design Document

## Overview

The Stock Analyzer is a multi-broker statement scanning, normalization, ingestion, and reporting system for Indian stock market investments. It follows the same architectural patterns as the MF Analyzer for consistency.

## Supported Brokers

| Broker | Code | Detection Methods | Statement Types |
|--------|------|-------------------|-----------------|
| ICICI Direct | ICICI | Folder, filename, columns | Holdings, Capital Gains |
| Zerodha | ZERODHA | Folder, filename, columns | Holdings, Tax P&L |
| Groww | GROWW | Folder, filename | Holdings, Transactions |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         StockAnalyzer (Orchestrator)                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐       │
│  │ StockStatement   │  │ StockField       │  │ StockDB          │       │
│  │ Scanner          │  │ Normalizer       │  │ Ingester         │       │
│  │                  │  │                  │  │                  │       │
│  │ • Recursive scan │  │ • ICICI mapping  │  │ • Idempotent     │       │
│  │ • Broker detect  │  │ • Zerodha mapping│  │ • Deduplication  │       │
│  │ • Type detect    │  │ • Groww mapping  │  │ • Upsert logic   │       │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘       │
│           │                     │                     │                  │
│           └─────────────────────┼─────────────────────┘                  │
│                                 ▼                                        │
│                    ┌──────────────────────┐                              │
│                    │ StockReportGenerator │                              │
│                    │                      │                              │
│                    │ • Portfolio Summary  │                              │
│                    │ • Holdings Detail    │                              │
│                    │ • Capital Gains      │                              │
│                    │ • Quarterly CG       │                              │
│                    │ • XIRR Performance   │                              │
│                    └──────────────────────┘                              │
└─────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. BrokerDetector

Detects broker from:
1. **Folder structure** - `inbox/Indian-Stocks/ICICIDirect/`
2. **Filename keywords** - `ICICI_Direct_holdings.xlsx`
3. **Content analysis** - Column signatures unique to each broker

```python
detector = BrokerDetector(config)
broker, method = detector.detect(file_path)  # (BrokerType.ICICIDIRECT, "folder:ICICIDirect")
```

### 2. StockStatementScanner

Recursively scans folders for statement files:

```python
scanner = StockStatementScanner(config, path_resolver)
files = scanner.scan(base_path, recursive=True, include_archive=False)
# Returns: List[ScannedFile] with broker, statement_type, file_hash
```

### 3. StockFieldNormalizer

Normalizes broker-specific fields to canonical schema:

**Holdings Schema:**
- symbol, isin, company_name, sector
- quantity_held, quantity_lt, quantity_pledged
- average_buy_price, current_price, market_value
- unrealized_pnl, unrealized_pnl_pct

**Transactions Schema:**
- symbol, isin, quantity
- buy_date, sell_date, holding_period_days
- buy_value, sell_value, profit_loss
- gain_type (STCG/LTCG), quarter, financial_year

### 4. StockDBIngester

Idempotent database operations:

```python
ingester = StockDBIngester(conn, config)
inserted, skipped = ingester.ingest_holdings(holdings, user_id, as_of_date)
inserted, skipped = ingester.ingest_transactions(transactions, user_id)
```

### 5. XIRRCalculator

XIRR calculation using Newton-Raphson method:

```python
cashflows = [
    (date(2023, 1, 1), Decimal("-10000")),  # Investment (outflow)
    (date(2024, 1, 1), Decimal("11000"))     # Return (inflow)
]
xirr = XIRRCalculator.calculate(cashflows)  # 0.10 (10% annual return)
```

### 6. StockReportGenerator

Multi-sheet Excel reports:
- Portfolio Summary
- Holdings Detail
- Sector Allocation
- Transactions
- Capital Gains - FY
- Capital Gains - Quarterly
- XIRR Performance
- Dividend Summary

## Database Schema

### New Tables

```sql
-- Current holdings snapshot
CREATE TABLE stock_holdings (
    id, user_id, broker_id,
    symbol, isin, company_name, sector,
    quantity_held, quantity_lt, quantity_pledged,
    average_buy_price, total_cost_basis,
    current_price, market_value,
    unrealized_pnl, unrealized_pnl_pct,
    as_of_date, source_file
);

-- Per-transaction capital gains detail
CREATE TABLE stock_capital_gains_detail (
    id, user_id, broker_id, financial_year, quarter,
    symbol, isin, quantity,
    buy_date, sell_date, holding_period_days,
    buy_price, sell_price, buy_value, sell_value,
    fmv_31jan2018, grandfathered_price, is_grandfathered,
    gross_profit_loss, taxable_profit,
    is_long_term, gain_type, stt_paid, turnover
);

-- XIRR calculations
CREATE TABLE stock_xirr_performance (
    id, user_id, calculation_date,
    scope_type, scope_value,
    xirr_annual, xirr_status,
    total_invested, total_current_value
);
```

### Existing Tables Used

- `stock_trades` - Individual buy/sell trades
- `stock_capital_gains` - FY summary
- `stock_dividends` - Dividend income
- `stock_stt_ledger` - STT tracking
- `stock_brokers` - Broker registry

## Configuration

### Global Config: `config/stock_analyzer_config.json`

```json
{
  "brokers": {
    "icicidirect": {
      "name": "ICICI Direct",
      "folder_patterns": ["ICICIDirect"],
      "file_patterns": {
        "holdings": ["*holding*.xlsx"],
        "transactions": ["*capital_gain*.xlsx"]
      }
    }
  },
  "field_mappings": {
    "holdings": {
      "icicidirect": {
        "Stock Name": "company_name",
        "Stock ISIN": "isin"
      }
    }
  },
  "capital_gains_rules": {
    "ltcg_threshold_days": 365,
    "ltcg_exemption_limit": 125000,
    "ltcg_tax_rate": 0.125,
    "stcg_tax_rate": 0.20
  }
}
```

## CLI Usage

```bash
# Full analysis
python stock_analyzer_cli.py --user Sanjay --fy 2025-26

# Scan only (no ingestion)
python stock_analyzer_cli.py --user Sanjay --fy 2025-26 --scan-only

# Specific broker
python stock_analyzer_cli.py --user Sanjay --fy 2025-26 --broker zerodha

# Custom output path
python stock_analyzer_cli.py --user Sanjay --fy 2025-26 --output my_report.xlsx

# Verbose logging
python stock_analyzer_cli.py --user Sanjay --fy 2025-26 -v
```

## Indian Capital Gains Rules

### STCG (Short-Term Capital Gains)
- Holding period < 365 days for listed equity
- Tax rate: 20% (Section 111A)

### LTCG (Long-Term Capital Gains)
- Holding period ≥ 365 days for listed equity
- Tax rate: 12.5% (Section 112A)
- Exemption: ₹1.25 lakh per FY

### Grandfathering (Post-2018)
- For shares held before Feb 1, 2018
- Cost of acquisition = Higher of:
  - Actual purchase price
  - FMV as on Jan 31, 2018 (capped at sale price)

### Indian FY Quarters (Advance Tax)
| Quarter | Period | Due Date |
|---------|--------|----------|
| Q1 | Apr 1 - Jun 15 | Jun 15 |
| Q2 | Jun 16 - Sep 15 | Sep 15 |
| Q3 | Sep 16 - Dec 15 | Dec 15 |
| Q4 | Dec 16 - Mar 15 | Mar 15 |
| Q5 | Mar 16 - Mar 31 | - |

## Testing

### Unit Tests (15 tests)

| Test Category | Count | Coverage |
|---------------|-------|----------|
| Broker Detection | 7 | Folder, filename, content detection |
| Field Normalization | 6 | ICICI, Zerodha mappings, date parsing |
| XIRR Calculation | 6 | Profit, loss, zero return, edge cases |
| Capital Gains | 4 | STCG/LTCG classification |
| Edge Cases | 5 | Empty data, NaN handling |

### Integration Tests (6 tests)

1. Full analysis pipeline with mock files
2. Database consistency check
3. Report generation validation
4. Duplicate handling
5. Multi-broker scan
6. Quarterly breakdown accuracy

## File Structure

```
src/pfas/
├── analyzers/
│   ├── __init__.py           # Exports StockAnalyzer
│   ├── mf_analyzer.py        # Existing MF analyzer
│   └── stock_analyzer.py     # NEW: Stock analyzer
├── cli/
│   ├── mf_analyzer_cli.py    # Existing MF CLI
│   └── stock_analyzer_cli.py # NEW: Stock CLI
└── core/
    └── database.py           # Extended with stock tables

config/
└── stock_analyzer_config.json  # NEW: Stock config

docs/
├── stock_analyzer_schema_updates.sql  # NEW: DB schema
└── STOCK_ANALYZER_DESIGN.md           # NEW: This document

tests/
└── unit/
    └── test_analyzers/
        └── test_stock_analyzer.py  # NEW: Unit tests
```

## Integration Roadmap

### Phase 1: Current (Complete)
- ✅ Stock Analyzer module
- ✅ Multi-broker detection (ICICI, Zerodha, Groww)
- ✅ Holdings & transactions normalization
- ✅ XIRR calculation
- ✅ Multi-sheet Excel reports
- ✅ CLI with scan-only mode
- ✅ Unit tests

### Phase 2: Database Integration
- [ ] Run schema migration
- [ ] Integration tests with actual DB
- [ ] Performance optimization for large portfolios

### Phase 3: Advanced Features
- [ ] Dividend tracking integration
- [ ] STT reconciliation
- [ ] Corporate actions (splits, bonuses)
- [ ] Multi-year trend analysis

### Phase 4: ITR Integration
- [ ] Schedule CG export
- [ ] Schedule FA (foreign assets) for US stocks
- [ ] DTAA calculations
- [ ] Form 67 integration

### Phase 5: Advance Tax
- [ ] Quarterly capital gains tracking
- [ ] Advance tax liability calculation
- [ ] Tax optimization suggestions

## Usage Examples

### Python API

```python
from pfas.analyzers import StockAnalyzer
from pfas.core.paths import PathResolver
from pfas.core.database import DatabaseManager

# Initialize
resolver = PathResolver(project_root, "Sanjay")
db = DatabaseManager(resolver.db_path())

# Analyze
analyzer = StockAnalyzer(conn=db.conn, path_resolver=resolver)
result = analyzer.analyze(user_name="Sanjay", financial_year="2025-26")

# Check results
print(f"Holdings: {result.holdings_processed}")
print(f"Transactions: {result.transactions_processed}")
print(f"Total STCG: ₹{result.total_stcg:,.2f}")
print(f"Total LTCG: ₹{result.total_ltcg:,.2f}")
print(f"XIRR: {result.xirr_overall * 100:.2f}%")

# Generate report
report_path = analyzer.generate_reports()
```

### Extending for New Broker

1. Add broker config in `stock_analyzer_config.json`:
```json
{
  "brokers": {
    "newbroker": {
      "name": "New Broker",
      "code": "NEWBRKR",
      "folder_patterns": ["NewBroker"],
      "file_patterns": {
        "holdings": ["*holding*.xlsx"],
        "transactions": ["*trade*.xlsx"]
      }
    }
  }
}
```

2. Add field mappings:
```json
{
  "field_mappings": {
    "holdings": {
      "newbroker": {
        "Stock Symbol": "symbol",
        "ISIN Code": "isin"
      }
    }
  }
}
```

3. Add detection keywords:
```json
{
  "broker_detection": {
    "keywords": {
      "newbroker": ["New Broker", "NBK"]
    },
    "column_signatures": {
      "newbroker": ["Unique Column Name"]
    }
  }
}
```

No code changes required for basic support.

## Dependencies

- Python 3.10+
- pandas
- openpyxl
- numpy (for XIRR edge cases)
- SQLite/SQLCipher

## Author

PFAS Project - Personal Financial Accounting System
