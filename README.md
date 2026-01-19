# PFAS - Personal Financial Accounting System

A comprehensive personal finance management system for Indian tax residents, supporting 18 asset classes with tax computation capabilities.

## Quick Start

```bash
# 1. Clone and setup
cd /path/to/pfas-project
pip install -e .

# 2. Set your data root (optional - defaults to ./Data)
export PFAS_DATA_ROOT=/path/to/your/data

# 3. Run the CLI
pfas ingest --user YourName --asset Mutual-Fund
pfas status --user YourName
```

## Supported Asset Classes

| Asset Type | Parser Status | Tax Computation |
|-----------|---------------|-----------------|
| Mutual-Fund | CAMS, KARVY/KFintech | STCG/LTCG with grandfathering |
| Indian-Stocks | Zerodha, ICICI Direct | STCG/LTCG with 112A |
| Bank | ICICI, HDFC, SBI | Interest income |
| Salary | Form 16, Payslips | HRA, 80C, RSU tax credit |
| EPF | EPFO Passbook | 80C, taxable interest |
| NPS | NPS Statement | 80CCD(1), 80CCD(1B), 80CCD(2) |
| PPF | PPF Statement | 80C (EEE status) |
| SGB | RBI Bonds | Interest, LTCG exempt |
| USA-Stocks | Morgan Stanley, E-Trade | DTAA, Schedule FA |
| FD-Bonds | Bank FDs, Bonds | Interest income |

## Data Directory Structure

```
Data/
├── config/
│   └── paths.json          # Path configuration (auto-created)
└── Users/
    └── YourName/
        ├── db/
        │   └── finance.db  # Encrypted SQLite database
        ├── inbox/          # Drop files here for ingestion
        │   ├── Mutual-Fund/
        │   ├── Bank/
        │   ├── Indian-Stocks/
        │   └── ...
        ├── archive/        # Processed files moved here
        ├── reports/        # Generated reports
        └── config/
            └── passwords.json  # Optional: file passwords
```

## CLI Commands

### Scan inbox for files
```bash
pfas scan --user Sanjay --asset Mutual-Fund
```

### Ingest statements
```bash
pfas ingest --user Sanjay --asset Mutual-Fund --archive --report
pfas ingest --user Sanjay --asset Bank
pfas ingest --user Sanjay --asset Indian-Stocks
```

### Generate reports
```bash
pfas report --user Sanjay --asset Mutual-Fund --type transactions
```

### Check status
```bash
pfas status --user Sanjay
```

### Reconcile with audit file
```bash
pfas audit --user Sanjay --asset Mutual-Fund --file holdings.xlsx
```

## Password Management

For encrypted PDFs (like CAMS statements), create `config/passwords.json`:

```json
{
  "files": {
    "specific_file.pdf": "exact_password"
  },
  "patterns": {
    "CAMS*": "your_pan_number",
    "*.pdf": "default_pdf_password"
  }
}
```

## Testing

```bash
# Quick smoke tests
make test-quick

# Full regression suite
make test-regression

# Run with specific user data
PFAS_TEST_USER=YourName make test-regression
```

## Key Technical Details

- **Database**: SQLCipher (AES-256 encrypted SQLite)
- **Currency Conversion**: SBI TT Buying Rate for USD→INR
- **LTCG Holding Period**: 12 months (Indian equity), 24 months (foreign/unlisted)
- **RSU Tax Credit**: Negative deduction in payslip = credit when shares vest

## Documentation

| Document | Description |
|----------|-------------|
| [docs/TESTING.md](docs/TESTING.md) | Testing guide and regression suite |
| [README_CLI.md](README_CLI.md) | Reports CLI documentation |
| [CLAUDE.md](CLAUDE.md) | AI assistant instructions |
| [docs/requirements/](docs/requirements/) | Detailed requirements |
| [docs/design/](docs/design/) | Design documents |

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run unit tests
pytest tests/unit/ -v

# Run integration tests
pytest tests/integration/ -v

# Install pre-commit hooks
make install-hooks
```

## License

Private - All rights reserved.
