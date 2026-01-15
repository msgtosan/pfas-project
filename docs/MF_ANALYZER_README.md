# MF Analyzer - Mutual Fund Statement Analysis

Comprehensive analyzer for CAMS and Karvy/KFintech mutual fund statements.

## Features

- **Multi-RTA Support**: Parses both CAMS and Karvy/KFintech statements
- **Statement Types**: Holdings statements and Capital Gains (CG) statements
- **Format Support**: Excel (.xlsx, .xls) and PDF
- **Idempotent Ingestion**: Safe re-runs without duplicate data
- **Automatic Classification**: Equity/Debt/Hybrid based on scheme name
- **Excel Reports**: Professional multi-sheet reports

## Quick Start

```bash
# Navigate to project directory
cd /path/to/pfas-project

# Run analyzer for user Sanjay
./mf-analyzer --user Sanjay
```

## Data Structure

```
Data/Users/Sanjay/
  ├── Mutual-Fund/           # Input: MF statements
  │   ├── CAMS/
  │   │   ├── Sanjay_CAMS_CG_FY2024-25.xlsx
  │   │   └── Sanjay_CAMS_Consolidated.xlsx
  │   └── Karvy/
  │       └── MF_Karvy_CG_FY2024-25.xlsx
  ├── Reports/Mutual-Fund/   # Output: Generated reports
  │   └── MF_Holdings_Report_Sanjay_2024-03-31.xlsx
  └── pfas.db                # Database
```

## CLI Usage

### Basic Analysis

```bash
# Analyze and generate reports
./mf-analyzer --user Sanjay

# With custom config
./mf-analyzer --user Sanjay --config config/mf_analyzer_config.json

# Verbose output
./mf-analyzer --user Sanjay --verbose
```

### Report Options

```bash
# Only generate reports (skip analysis)
./mf-analyzer --user Sanjay --report-only

# Skip report generation
./mf-analyzer --user Sanjay --no-report

# JSON output
./mf-analyzer --user Sanjay --json-output
```

### Custom Paths

```bash
# Custom MF folder
./mf-analyzer --user Sanjay --mf-folder /path/to/mf/files

# Custom output directory
./mf-analyzer --user Sanjay --output-dir /path/to/reports

# Custom database
./mf-analyzer --user Sanjay --db /path/to/database.db
```

## Python API

```python
from pfas.analyzers import MFAnalyzer
from pfas.core.database import DatabaseManager
from pathlib import Path

# Initialize database
db = DatabaseManager()
conn = db.init("Data/Users/Sanjay/pfas.db", "password")

# Create analyzer
analyzer = MFAnalyzer(
    config_path="config/mf_analyzer_config.json",
    conn=conn
)

# Run analysis
result = analyzer.analyze(user_name="Sanjay")

# Print summary
print(f"Total Value: Rs. {result.total_current_value:,.2f}")
print(f"Appreciation: Rs. {result.total_appreciation:,.2f}")
print(f"Equity: Rs. {result.equity_value:,.2f}")
print(f"Debt: Rs. {result.debt_value:,.2f}")

# Generate reports
report_path = analyzer.generate_reports()
print(f"Report: {report_path}")
```

## Field Normalization

### CAMS to Common Schema

| CAMS Column | Common Field |
|------------|--------------|
| AMCName | amc_name |
| Scheme | scheme_name |
| Type | scheme_type |
| Folio | folio_number |
| UnitBal | units |
| NAVDate | nav_date |
| CurrentValue | current_value |
| CostValue | cost_value |
| Appreciation | appreciation |
| WtgAvg | average_holding_days |
| Annualised XIRR | annualized_return |

### Karvy to Common Schema

| Karvy Column | Common Field |
|-------------|--------------|
| Fund Name | amc_name |
| Scheme Name | scheme_name |
| Folio Number | folio_number |
| Unit Balance | units |
| Nav Date | nav_date |
| Current Value (Rs.) | current_value |
| Cost Value (Rs.) | cost_value |
| Appreciation (Rs.) | appreciation |
| AvgAgeDays | average_holding_days |
| Annualized Yield (%) | annualized_return |
| Dividend Payout | dividend_payout |
| Dividend Re-Invest | dividend_reinvest |

## Report Output

The generated Excel report contains:

| Sheet | Description |
|-------|-------------|
| Summary | Total value, appreciation, allocation |
| By Category | Equity/Debt/Hybrid breakdown |
| By AMC | Holdings grouped by AMC |
| By Folio | Holdings grouped by folio |
| Holdings | Detailed holdings list |
| History | Historical snapshots (if available) |

## Configuration

Create a JSON config file for custom settings:

```json
{
  "user": {
    "name": "Sanjay"
  },
  "paths": {
    "data_root": "Data/Users/Sanjay",
    "mf_folder": "Mutual-Fund",
    "reports_output": "Reports/Mutual-Fund"
  },
  "file_patterns": {
    "cams": {
      "patterns": ["*CAMS*", "*cams*"]
    },
    "karvy": {
      "patterns": ["*Karvy*", "*KFintech*"]
    }
  },
  "processing": {
    "skip_zero_holdings": true,
    "use_xirr_over_yield": true,
    "classify_unknown_schemes": true
  }
}
```

## Database Tables

### mf_holdings

Point-in-time holdings from statements:

```sql
CREATE TABLE mf_holdings (
    user_id INTEGER NOT NULL,
    amc_name TEXT NOT NULL,
    scheme_name TEXT NOT NULL,
    scheme_type TEXT,
    folio_number TEXT NOT NULL,
    units DECIMAL(15,4) NOT NULL,
    nav_date DATE NOT NULL,
    current_value DECIMAL(15,2) NOT NULL,
    cost_value DECIMAL(15,2),
    appreciation DECIMAL(15,2),
    annualized_return DECIMAL(8,4),
    rta TEXT NOT NULL,
    UNIQUE(user_id, folio_number, scheme_name, nav_date)
);
```

### mf_holdings_history

Historical snapshots for tracking over time:

```sql
CREATE TABLE mf_holdings_history (
    user_id INTEGER NOT NULL,
    snapshot_date DATE NOT NULL,
    total_value DECIMAL(15,2) NOT NULL,
    equity_value DECIMAL(15,2),
    debt_value DECIMAL(15,2),
    hybrid_value DECIMAL(15,2),
    weighted_xirr DECIMAL(8,4),
    UNIQUE(user_id, snapshot_date)
);
```

## Integration Roadmap

### Balance Sheet Integration

Holdings feed into Balance Sheet (asset side):

```python
from pfas.services import BalanceSheetService

# MF holdings contribute to:
# - mutual_funds_equity: Sum of EQUITY scheme holdings
# - mutual_funds_debt: Sum of DEBT scheme holdings
# - mutual_funds_hybrid: Sum of HYBRID scheme holdings
```

### Cash Flow Integration

Transactions feed into Cash Flow (investing activities):

```python
from pfas.services import CashFlowStatementService

# MF transactions contribute to:
# - mf_purchases: Sum of BUY transactions
# - mf_redemptions: Sum of SELL transactions
# - dividends_received: Sum of DIVIDEND payouts
```

### Tax Integration

Capital gains feed into tax calculations:

```python
from pfas.parsers.mf import CapitalGainsCalculator

# CG data contributes to:
# - STCG (Short Term Capital Gains)
# - LTCG (Long Term Capital Gains)
# - Advance Tax liability
```

## Running Tests

```bash
# Run MF analyzer tests
pytest tests/unit/test_analyzers/test_mf_analyzer.py -v

# Run with coverage
pytest tests/unit/test_analyzers/ --cov=src/pfas/analyzers
```

## Sample Output

```
============================================================
MF ANALYSIS RESULT - Sanjay
============================================================

Files Scanned:          5
Holdings Processed:     45
Transactions Processed: 200
Duplicates Skipped:     10

----------------------------------------
PORTFOLIO SUMMARY
----------------------------------------
Total Current Value:    Rs. 25,00,000.00
Total Cost Value:       Rs. 20,00,000.00
Total Appreciation:     Rs. 5,00,000.00
Appreciation %:         25.00%
Weighted XIRR:          15.50%

----------------------------------------
ALLOCATION
----------------------------------------
Equity:   Rs. 18,00,000.00 (72.0%)
Debt:     Rs. 5,00,000.00 (20.0%)
Hybrid:   Rs. 2,00,000.00 (8.0%)

Unique Schemes: 12
Unique Folios:  8

Report generated: Data/Users/Sanjay/Reports/Mutual-Fund/MF_Holdings_Report_Sanjay_2024-03-31.xlsx
============================================================
```
