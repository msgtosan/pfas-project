# PFAS Financial Reports CLI

Generate financial statements from your PFAS database.

## Data Structure

Each user has their own isolated data directory:

```
Data/Users/{username}/
  ├── pfas.db                    # User's encrypted database
  ├── Reports/                   # Generated reports
  │   ├── balance_sheet_FY202425.txt
  │   ├── cash_flow_FY202425.txt
  │   └── financial_reports_FY202425.xlsx
  ├── Mutual-Fund/               # Source data files
  ├── Indian-Stocks/
  ├── Bank/
  └── ...
```

## Quick Start

```bash
# Navigate to project directory
cd /path/to/pfas-project

# Initialize user's database (first time only)
./pfas-report --user Sanjay --init-db

# Generate all reports
./pfas-report --user Sanjay --fy 2024-25
```

## Installation Options

### Option 1: Direct Execution (Recommended)

No installation needed. Run from the project root:

```bash
cd /path/to/pfas-project
./pfas-report --user Sanjay --fy 2024-25
```

### Option 2: Install as Package

```bash
cd /path/to/pfas-project
pip install -e .

# Now use from anywhere
pfas-report --user Sanjay --fy 2024-25
```

## Available Reports

| Report | Flag | Description |
|--------|------|-------------|
| **All Reports** | `--report all` | Generate all reports (default) |
| **Balance Sheet** | `--report balance-sheet` | Assets, Liabilities, Net Worth |
| **Cash Flow** | `--report cash-flow` | Operating, Investing, Financing activities |
| **Income Statement** | `--report income` | Capital Gains (STCG/LTCG) by asset class |
| **Portfolio** | `--report portfolio` | Holdings valuation with XIRR |

## Usage Examples

### Initialize User Database
```bash
# First time setup for a new user
./pfas-report --user Sanjay --init-db

# For another user
./pfas-report --user Priya --init-db
```

### Generate Reports
```bash
# All reports for user Sanjay
./pfas-report --user Sanjay --fy 2024-25

# Balance Sheet only
./pfas-report --user Sanjay --fy 2024-25 --report balance-sheet

# Cash Flow Statement only
./pfas-report --user Sanjay --fy 2024-25 --report cash-flow

# Income Statement (Capital Gains) only
./pfas-report --user Sanjay --fy 2024-25 --report income

# Portfolio Valuation
./pfas-report --user Sanjay --fy 2024-25 --report portfolio
```

### Multiple Financial Years
```bash
# Specific years (comma-separated)
./pfas-report --user Sanjay --fy 2023-24,2024-25

# All available years
./pfas-report --user Sanjay --fy all
```

### Export Formats
```bash
# Text format (default) - saves to Data/Users/Sanjay/Reports/
./pfas-report --user Sanjay --fy 2024-25

# Excel format
./pfas-report --user Sanjay --fy 2024-25 --format xlsx

# JSON format
./pfas-report --user Sanjay --fy 2024-25 --format json

# Custom output path
./pfas-report --user Sanjay --fy 2024-25 --format xlsx --output /path/to/report.xlsx
```

### Console Only (No File Save)
```bash
./pfas-report --user Sanjay --fy 2024-25 --no-save
```

## Command Reference

```
usage: pfas-report --user USER [options]

Required:
  --user, -u      User name - determines database and report location

Options:
  --fy, -y        Financial year (e.g., 2024-25) or 'all'
  --report, -r    Report type: all, balance-sheet, cash-flow, income, portfolio
  --format, -f    Output format: text (default), json, or xlsx
  --output, -o    Custom output path (optional)
  --db            Custom database path (default: Data/Users/{user}/pfas.db)
  --password      Database password
  --init-db       Initialize user's database
  --no-save       Print to console only, don't save files
  -h, --help      Show help message
```

## Sample Output

### Balance Sheet
```
============================================================
BALANCE SHEET as of 2025-03-31
============================================================

ASSETS
----------------------------------------
  Bank Savings:          Rs.1,50,000.00
  Mutual Funds (Equity): Rs.25,00,000.00
  Mutual Funds (Debt):   Rs.5,00,000.00
  Stocks (Indian):       Rs.15,00,000.00
  EPF Balance:           Rs.12,00,000.00
  PPF Balance:           Rs.8,00,000.00
----------------------------------------
  TOTAL ASSETS:          Rs.66,50,000.00

LIABILITIES
----------------------------------------
  Home Loans:            Rs.35,00,000.00
----------------------------------------
  TOTAL LIABILITIES:     Rs.35,00,000.00

============================================================
  NET WORTH:             Rs.31,50,000.00
============================================================
```

### Cash Flow Statement
```
============================================================
CASH FLOW STATEMENT - FY 2024-25
Period: 2024-04-01 to 2025-03-31
============================================================

OPERATING ACTIVITIES
----------------------------------------
  Salary Received:       Rs.24,00,000.00
  Dividends Received:    Rs.50,000.00
  Interest Received:     Rs.1,20,000.00
  Taxes Paid:           (Rs.3,50,000.00)
----------------------------------------
  NET OPERATING:         Rs.22,20,000.00

INVESTING ACTIVITIES
----------------------------------------
  MF Purchases:         (Rs.6,00,000.00)
  MF Redemptions:        Rs.2,00,000.00
  Stock Buys:           (Rs.3,00,000.00)
  Stock Sells:           Rs.1,50,000.00
----------------------------------------
  NET INVESTING:        (Rs.5,50,000.00)

FINANCING ACTIVITIES
----------------------------------------
  Loan Repayments:      (Rs.4,80,000.00)
----------------------------------------
  NET FINANCING:        (Rs.4,80,000.00)

============================================================
  NET CHANGE IN CASH:    Rs.11,90,000.00
============================================================
```

## Troubleshooting

### "No module named 'pfas'" Error

Use the wrapper script instead of `python -m`:

```bash
# Use this:
./pfas-report --user Sanjay --fy 2024-25

# Or install the package:
pip install -e .
pfas-report --user Sanjay --fy 2024-25
```

### Database Not Found

Initialize the user's database first:

```bash
./pfas-report --user Sanjay --init-db
```

### No Data for User

Ensure you have parsed financial data using the parsers. Example:

```python
import sys
sys.path.insert(0, 'src')

from pfas.parsers.mf import CAMSParser
from pfas.core.database import DatabaseManager
from pathlib import Path

# Use user-specific database
db = DatabaseManager()
conn = db.init("Data/Users/Sanjay/pfas.db", "your_password")

parser = CAMSParser(conn)
result = parser.parse(Path("Data/Users/Sanjay/Mutual-Fund/CAMS/cams_file.xlsx"))
```

### Permission Denied

```bash
chmod +x pfas-report
```

### Multiple Users

Each user has completely isolated data:

```bash
# Sanjay's reports
./pfas-report --user Sanjay --fy 2024-25

# Priya's reports (separate database)
./pfas-report --user Priya --fy 2024-25
```
