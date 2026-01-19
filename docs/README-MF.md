# PFAS Mutual Fund Analyzer

Comprehensive Mutual Fund Statement Analyzer and Reporting Module for the Personal Financial Accounting System (PFAS).

## Features

- **Multi-RTA Support**: Parse statements from CAMS and KFintech (Karvy)
- **Multiple Formats**: Excel (.xlsx, .xls) and PDF support
- **Idempotent Ingestion**: Safe re-runs with duplicate detection
- **Automatic Classification**: Scheme type classification (Equity/Debt/Hybrid)
- **Capital Gains Reconciliation**: Compare calculated vs reported gains
- **FY-Specific Reports**: Transaction summaries by financial year
- **YoY Growth Tracking**: Year-over-year portfolio growth analysis
- **Configurable Reports**: Data-driven Excel reports via JSON config
- **20+ Year Support**: Designed for long-term data retention

## Installation

```bash
# From project root
pip install -e .

# Or install dependencies
pip install pandas openpyxl pdfplumber sqlcipher3
```

## Quick Start

### 1. Prepare Folder Structure

```
Data/Users/Sanjay/
├── inbox/
│   └── Mutual-Fund/
│       ├── CAMS/
│       │   ├── CAMS_CG_FY2024-25.xlsx
│       │   └── CAMS_Holdings_Mar2024.xlsx
│       └── KARVY/
│           ├── KFintech_CG_FY2024-25.xlsx
│           └── KFintech_Holdings.xlsx
├── archive/
│   └── Mutual-Fund/      # Processed files moved here
├── reports/
│   └── Mutual-Fund/      # Generated reports
└── db/
    └── finance.db        # SQLCipher encrypted database
```

### 2. Run MF Analyzer

```bash
# Full pipeline: ingest + analyze + report
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay

# With specific financial year
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --fy 2024-25

# With reconciliation
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --fy 2024-25 --reconcile

# Report only (no ingestion)
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --report-only

# Custom config
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --config config/mf_analyzer_config_v2.json
```

### 3. CLI Options

```
Options:
  --user, -u USER       User name (required)
  --config, -c FILE     JSON config file (default: config/mf_analyzer_config_v2.json)
  --fy YEAR             Financial year (e.g., 2024-25)
  --ingest-only         Only run ingestion
  --analyze-only        Only run analysis
  --report-only         Only generate reports
  --reconcile           Run capital gains reconciliation
  --reconcile-file FILE Path to RTA CG statement for reconciliation
  --snapshot TYPE       Take holdings snapshot (FY_START, FY_END, QUARTERLY, MONTHLY, ADHOC)
  --output-dir, -o DIR  Output directory for reports
  --force, -f           Force reprocess already ingested files
  --verbose, -v         Enable verbose logging
  --db FILE             Database path
  --root PATH           Root path for data
```

## Configuration

### JSON Config Structure

```json
{
  "version": "2.0.0",
  "user": {
    "name": "Sanjay",
    "pan_prefix": "AAPPS"
  },
  "paths": {
    "data_root": "Data/Users/{user}",
    "inbox": "inbox/Mutual-Fund",
    "archive": "archive/Mutual-Fund",
    "reports_output": "reports/Mutual-Fund"
  },
  "file_patterns": {
    "cams": {
      "transaction_patterns": ["*CG_FY*", "*CAMS*CG*"],
      "holding_patterns": ["*CAMS*holding*", "*CAMS*consolidated*"],
      "extensions": [".xlsx", ".xls", ".pdf"]
    },
    "karvy": {
      "transaction_patterns": ["*Karvy*CG*", "*KFintech*CG*"],
      "holding_patterns": ["*Karvy*holding*"],
      "extensions": [".xlsx", ".xls", ".pdf"]
    }
  },
  "report_settings": {
    "excel": {
      "sheets": ["Summary", "By Category", "By AMC", "Scheme Details", "Capital Gains"]
    }
  },
  "reconciliation": {
    "enabled": true,
    "tolerance_amount": 1.00
  },
  "processing": {
    "skip_zero_holdings": true,
    "use_xirr_over_yield": true,
    "archive_processed_files": true
  }
}
```

## Report Output

### Generated Excel Sheets

| Sheet | Description |
|-------|-------------|
| Summary | Portfolio overview with total value, cost, appreciation |
| By Category | Breakdown by Equity/Debt/Hybrid |
| By AMC | Holdings grouped by Asset Management Company |
| By Folio | Holdings grouped by folio number |
| Scheme Details | Individual scheme analysis with % distribution |
| FY Transactions | Financial year transaction summary |
| Capital Gains | STCG/LTCG summary by FY and asset class |
| Holdings History | Historical snapshots for trend analysis |
| YoY Growth | Year-over-year comparison |
| Reconciliation | Capital gains reconciliation status |

### Sample Report Location

```
Data/Users/Sanjay/reports/Mutual-Fund/
└── MF_Report_Sanjay_2024-03-31_2024-25.xlsx
```

## Database Schema

### Core MF Tables

```sql
-- Holdings snapshot from statements
mf_holdings (
    user_id, amc_name, scheme_name, scheme_type, folio_number,
    units, nav, nav_date, current_value, cost_value, appreciation,
    annualized_return, isin, rta
    UNIQUE(user_id, folio_number, scheme_name, nav_date)
)

-- Transactions with capital gains
mf_transactions (
    folio_id, transaction_type, date, units, nav, amount,
    short_term_gain, long_term_gain, purchase_date
    UNIQUE(folio_id, date, transaction_type, amount, purchase_date)
)

-- Capital gains summary
mf_capital_gains (
    user_id, financial_year, asset_class,
    stcg_amount, ltcg_amount, taxable_stcg, taxable_ltcg
    UNIQUE(user_id, financial_year, asset_class)
)
```

### Enhanced Tables (v2.0)

```sql
-- Capital gains reconciliation
mf_cg_reconciliation (
    user_id, financial_year, rta, asset_class,
    calc_stcg, calc_ltcg, reported_stcg, reported_ltcg,
    total_difference, is_reconciled
)

-- FY transaction summary
mf_fy_summary (
    user_id, financial_year, scheme_type,
    purchase_amount, redemption_amount,
    stcg_realized, ltcg_realized
)

-- Holdings snapshots for YoY
mf_holdings_snapshot (
    user_id, snapshot_date, snapshot_type,
    total_value, equity_value, debt_value
)
```

## Field Normalization

### CAMS to Common Schema

| CAMS Field | Common Field |
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

### KFintech to Common Schema

| KFintech Field | Common Field |
|----------------|--------------|
| Fund Name | amc_name |
| Scheme Name | scheme_name |
| Folio | folio_number |
| Unit Balance | units |
| Nav Date | nav_date |
| Current Value (Rs.) | current_value |
| Cost Value (Rs.) | cost_value |
| Appreciation (Rs.) | appreciation |
| AvgAgeDays | average_holding_days |
| Annualized Yield (%) | annualized_return |

## Asset Class Classification

Automatic classification based on scheme name keywords:

| Asset Class | Keywords |
|-------------|----------|
| EQUITY | Equity, Bluechip, Large Cap, Midcap, Smallcap, ELSS, Index, Nifty |
| DEBT | Liquid, Bond, Gilt, Money Market, Ultra Short, Corporate Bond |
| HYBRID | Balanced, Aggressive Hybrid, Arbitrage, Multi Asset |
| OTHER | Default for unclassified schemes |

## Testing

```bash
# Run all MF tests
pytest tests/unit/test_analyzers/test_mf_enhanced.py -v

# Run with coverage
pytest tests/unit/test_analyzers/test_mf_enhanced.py --cov=pfas.analyzers

# Run specific test
pytest tests/unit/test_analyzers/test_mf_enhanced.py::TestMFFieldNormalizer -v
```

## Usage Examples

### Programmatic Usage

```python
from pfas.analyzers.mf_analyzer import MFAnalyzer
from pfas.core.database import DatabaseManager

# Initialize
db = DatabaseManager("Data/Users/Sanjay/db/finance.db")
conn = db.get_connection()

# Analyze
analyzer = MFAnalyzer(config_path="config/mf_analyzer_config_v2.json", conn=conn)
result = analyzer.analyze(user_name="Sanjay")

print(f"Total Value: Rs. {result.total_current_value:,.2f}")
print(f"Appreciation: Rs. {result.total_appreciation:,.2f}")

# Generate report
report_path = analyzer.generate_reports()
print(f"Report: {report_path}")
```

### FY Analysis

```python
from pfas.analyzers.mf_fy_analyzer import MFFYAnalyzer

analyzer = MFFYAnalyzer(conn)

# Generate FY summary
summaries = analyzer.generate_fy_summary(user_id=1, financial_year="2024-25")

for s in summaries:
    print(f"{s.scheme_type}: Purchases={s.purchase_amount}, Redemptions={s.redemption_amount}")

# Take snapshot
snapshot = analyzer.take_holdings_snapshot(
    user_id=1,
    snapshot_date=date(2025, 3, 31),
    snapshot_type="FY_END",
    financial_year="2024-25"
)
analyzer.save_holdings_snapshot(snapshot)
```

### Reconciliation

```python
from pfas.analyzers.mf_reconciler import MFReconciler
from pathlib import Path

reconciler = MFReconciler(conn)

result = reconciler.reconcile(
    user_id=1,
    financial_year="2024-25",
    rta="CAMS",
    reported_cg_file=Path("Data/Users/Sanjay/inbox/Mutual-Fund/CAMS/CAMS_CG_FY2024-25.xlsx")
)

if result.is_reconciled:
    print("Capital gains reconciled successfully!")
else:
    print(f"Mismatch: {result.total_difference}")

reconciler.save_result(result)
```

## Troubleshooting

### Common Issues

**1. "No files found in inbox"**
- Check folder structure matches config
- Verify file patterns in config match your files

**2. "Could not detect RTA"**
- Place files in CAMS/ or KARVY/ subfolders
- Or ensure filename contains "CAMS" or "Karvy"

**3. "Duplicate entry skipped"**
- This is expected for re-runs (idempotency)
- Use `--force` to reprocess

**4. "PDF password required"**
- Create `CAS-Passwd.txt` in MF folder with password
- Format: plain password or `filename : password`

### Logging

```bash
# Enable debug logging
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay -v

# Log to file
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --log-file mf_analyzer.log
```

## Integration Points

### Balance Sheet (Asset Side)
- MF holdings feed into `Investments - Mutual Funds` account
- Current value used for asset valuation

### Cash Flow (Investing Activities)
- Redemptions → Cash inflow from investments
- Purchases → Cash outflow for investments
- Dividends → Income from investments

### Advance Tax
- STCG/LTCG used for quarterly advance tax calculation
- Dividend income included in taxable income

### Net Worth
- MF holdings contribute to total assets
- YoY growth feeds into wealth tracking

## Roadmap

- [ ] PDF parsing improvements for CAS statements
- [ ] XIRR calculation from transactions
- [ ] Automatic NAV fetching for latest valuations
- [ ] Integration with ITR-2 Schedule 112A export
- [ ] Mobile-friendly report generation
