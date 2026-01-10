# Integration Test Guide - All Parsers

**Status:** ✅ Ready for Testing
**Data Available:** Yes (Complete FY2024-25)
**User:** Sanjay Shankar

---

## Test Data Files

### 1. Stock Parser (Zerodha)
```
File: ~/Data/Users/Sanjay/Indian-Stocks/Zerodha/taxpnl-QY6347-2024_2025-Q1-Q4.xlsx
Size: Test file available
Format: Excel (.xlsx)
Sheets: TRADEWISE, SPECULATIVE, SCRIPWISE, SUMMARY
Expected: ~50-100 trades
```

### 2. EPF Parser
```
Files:
  - ~/Data/Users/Sanjay/EPF/EPF_Interest_APHYD00476720000003193_2024.pdf
  - ~/Data/Users/Sanjay/EPF/APHYD00476720000003193_2025.pdf
UAN: 100379251525
Format: PDF (bilingual)
Expected: 12 monthly entries + interest
```

### 3. PPF Parser
```
File: ~/Data/Users/Sanjay/PPF/FY24-25-PPF-Sanjay.xlsx
Format: Excel (.xlsx)
Expected: Multiple deposits + interest
80C Eligible: Up to ₹1.5L
```

### 4. NPS Parser
```
File: ~/Data/Users/Sanjay/NPS/110091211424_NPS.csv
PRAN: 110091211424
Format: CSV
Expected: Multiple contributions (Tier I and II)
Deductions: 80CCD(1), 80CCD(1B), 80CCD(2)
```

---

## Test Execution Steps

### Prerequisites

```bash
# Install required dependencies
pip install pandas openpyxl pdfplumber sqlcipher3

# Verify installation
python3 -c "import pandas; import openpyxl; import pdfplumber; print('✅ All dependencies installed')"
```

### Run Each Test

Follow the steps below for each parser...
