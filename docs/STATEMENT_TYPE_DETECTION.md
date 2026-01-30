# Transaction vs Holdings Statement Detection

## Overview

PFAS uses a hybrid detection system to automatically distinguish between **transaction statements** (buy/sell/redemption history) and **holdings statements** (current portfolio snapshot).

## Detection Priority (First Match Wins)

1. **Config Override** - Explicit filename mapping in `statement_rules.json`
2. **Folder Structure** - Files in `transactions/` or `holdings/` subfolders
3. **Filename Keywords** - Keywords like "txn", "trade", "holding", "portfolio"
4. **Content Analysis** - Scanning file content for keywords
5. **Default Fallback** - Defaults to "transactions" with warning

## Recommended Folder Structure

```
Data/Users/{username}/inbox/
├── Mutual-Fund/
│   ├── transactions/          # Explicitly transaction files
│   │   ├── CAMS/
│   │   │   └── CAMS_CG_FY2024-25.xlsx
│   │   └── KARVY/
│   │       └── Karvy_txn_2024.pdf
│   ├── holdings/              # Explicitly holdings files
│   │   ├── CAMS/
│   │   │   └── CAMS_consolidated_Mar2024.xlsx
│   │   └── KARVY/
│   │       └── Karvy_portfolio_snapshot.pdf
│   └── CAMS/                  # Flat structure (auto-detect)
│       └── statement.xlsx     # Will be auto-detected
├── Indian-Stocks/
│   ├── transactions/
│   │   └── Zerodha_tax_pnl_FY2024.xlsx
│   └── holdings/
│       └── Zerodha_holdings_Mar2024.xlsx
├── Bank/
│   └── ICICI/
│       └── statement_2024.xlsx  # Banks are transaction-only
└── EPF/
    └── passbook_2024.pdf        # EPF is transaction-only
```

## Configuration

### Global Config: `config/statement_rules.json`

```json
{
  "transactions_keywords": [
    "txn", "transaction", "trade", "buy", "sell",
    "redemption", "switch", "dividend", "capital_gain",
    "p&l", "pnl", "profit", "tax_statement"
  ],
  "holdings_keywords": [
    "holding", "holdings", "portfolio", "consolidated",
    "summary", "valuation", "current_value", "snapshot",
    "position", "balance", "as on", "as of"
  ],
  "file_overrides": {
    "special_report.xlsx": "holdings"
  },
  "transactions_folders": ["transactions", "txn", "trades"],
  "holdings_folders": ["holdings", "portfolio", "valuation"],
  "default_type": "transactions",
  "min_content_confidence": 0.6
}
```

### User Override: `Users/{user}/config/statement_rules.json`

Users can add custom keywords or file overrides:

```json
{
  "transactions_keywords": ["my_custom_txn_keyword"],
  "file_overrides": {
    "my_special_file.xlsx": "holdings"
  }
}
```

User config is **merged** with global config (keywords combined, overrides take precedence).

## Usage Examples

### Basic Detection

```python
from pfas.core.statement_detector import StatementTypeDetector, detect_statement_type
from pfas.core.paths import PathResolver

# With PathResolver (recommended)
resolver = PathResolver(project_root, "Sanjay")
detector = StatementTypeDetector(resolver)

result = detector.detect(Path("inbox/Mutual-Fund/CAMS/statement.xlsx"))
print(f"Type: {result.statement_type.value}")  # "transactions" or "holdings"
print(f"Method: {result.detection_method.value}")  # "folder", "filename", "content", etc.
print(f"Confidence: {result.confidence}")  # 0.0 to 1.0

# Convenience function
result = detect_statement_type(Path("statement.xlsx"))
```

### Batch Detection

```python
files = list(Path("inbox/Mutual-Fund").glob("**/*.xlsx"))
results = detector.detect_batch(files)

for file_path, result in results.items():
    print(f"{file_path.name}: {result.statement_type.value}")
```

### Filter by Type

```python
# Get only transaction files
txn_files = detector.get_transactions_files(files)

# Get only holdings files
holdings_files = detector.get_holdings_files(files)
```

### Asset Scanner with Detection

```python
from pfas.core.asset_scanner import AssetScanner

scanner = AssetScanner(resolver)
result = scanner.scan_asset("Mutual-Fund")

# Access by type
print(f"Transaction files: {len(result.transaction_files)}")
print(f"Holdings files: {len(result.holdings_files)}")

# Group by FY
for fy, files in result.by_financial_year.items():
    print(f"FY {fy}: {len(files)} files")
```

### Enhanced Ingestion

```python
from pfas.core.enhanced_ingester import EnhancedIngester

ingester = EnhancedIngester(conn, resolver)
result = ingester.ingest_asset("Mutual-Fund")

print(f"Transactions processed: {result.transactions_processed}")
print(f"Holdings processed: {result.holdings_processed}")

# FY breakdown
for fy, stats in result.by_fy.items():
    print(f"FY {fy}: {stats['transactions']} txns, {stats['holdings']} holdings")
```

## Statement Type Definitions

### Transactions (History)
- Buy/purchase orders
- Sell/redemption orders
- Dividend receipts/reinvestments
- Switch in/out
- Capital gains reports
- Tax P&L statements

**Database tables**: `mf_transactions`, `stock_trades`, `bank_transactions`

### Holdings (Snapshot)
- Current portfolio valuation
- Units/shares held as of date
- Current NAV/price
- Market value
- Consolidated statements

**Database tables**: `mf_holdings`, `stock_holdings`

## Multi-File Handling

When multiple files exist for the same asset:

1. **Chronological Processing**: Files sorted by FY in filename
2. **Deduplication**: Duplicate files (same hash) skipped
3. **FY Grouping**: Files grouped by detected financial year

```python
from pfas.core.file_processor import MultiFileProcessor

# Sort files by date
sorted_files = MultiFileProcessor.sort_by_date(files)

# Detect FY from filename
fy = MultiFileProcessor.detect_financial_year(Path("statement_FY2024-25.xlsx"))
# Returns: "2024-25"

# Group by FY
grouped = MultiFileProcessor.group_by_fy(files)
# Returns: {"2024-25": [file1, file2], "2023-24": [file3]}
```

## Failed File Handling

Files that fail parsing are moved to `failed/` subfolder:

```
inbox/Mutual-Fund/
├── transactions/
│   └── good_file.xlsx
└── failed/                    # Auto-created
    └── bad_file.xlsx          # Moved here on parse failure
```

The `failed/` folder is excluded from future scans.

## CLI Usage

```bash
# Scan and show detected types
./mf-analyzer --user Sanjay --scan-only

# Process only transactions
./mf-analyzer --user Sanjay --transactions-only

# Process only holdings
./mf-analyzer --user Sanjay --holdings-only
```

## Troubleshooting

### File detected as wrong type

1. **Use explicit folder**: Move file to `transactions/` or `holdings/` subfolder
2. **Add file override**: Add to `config/statement_rules.json`:
   ```json
   {"file_overrides": {"myfile.xlsx": "holdings"}}
   ```
3. **Add custom keyword**: If filename has unique pattern, add to keywords

### Detection ambiguous (low confidence)

Check the detection result:
```python
result = detector.detect(file_path)
if result.confidence < 0.7:
    print(f"Warning: {result.warnings}")
    print(f"Matched: {result.matched_keywords}")
```

### Content detection not working

- Ensure file is Excel/CSV (PDFs use filename only)
- Check `min_content_confidence` in config
- Verify file has relevant column names/data

## API Reference

### StatementType Enum
- `TRANSACTIONS` - Transaction/trade history
- `HOLDINGS` - Portfolio snapshot
- `UNKNOWN` - Could not determine

### DetectionMethod Enum
- `FOLDER` - From folder structure
- `FILENAME` - From filename keywords
- `CONTENT` - From file content analysis
- `CONFIG` - From config file override
- `DEFAULT` - Fallback default

### DetectionResult
```python
@dataclass
class DetectionResult:
    statement_type: StatementType
    detection_method: DetectionMethod
    confidence: float  # 0.0 to 1.0
    matched_keywords: List[str]
    warnings: List[str]
```
