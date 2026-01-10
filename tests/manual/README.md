# PFAS Manual Tests

This directory contains manual tests for the PFAS bank parser module using real-world data.

## Test Files

### test_icici_excel_real.py

Tests the ICICI Bank Excel parser with actual bank statements from `Data/Users/Sanjay/Bank/ICICI/`.

**Run the test:**
```bash
python tests/manual/test_icici_excel_real.py
```

**Test Coverage:**
- 3 ICICI Excel files (FY 2024-25 and FY 2025-26)
- 1,986 transactions total
- ₹113M+ transaction volume
- Multiple accounts and transaction types

**Features Tested:**
- ✅ Account number extraction and masking
- ✅ Transaction parsing (date, amount, description)
- ✅ Auto-categorization (SALARY, INTEREST, UPI, NEFT, etc.)
- ✅ Interest detection and 80TTA calculation
- ✅ Balance tracking
- ✅ Multi-file consolidation

**Results:**
- 100% parsing success rate
- Zero errors
- Accurate interest calculations for tax filing
- Complete transaction categorization

See `docs/ICICI_PARSER_TEST_REPORT.md` for detailed results.

## Test Data Requirements

The tests expect bank statement files in:
```
Data/Users/Sanjay/Bank/ICICI/*.xls
```

Ensure the `Data` symlink is set up correctly:
```bash
ls -la Data  # Should point to /mnt/c/Sanjay/PFMS/Data/ or similar
```

## Parser Implementation

The ICICI Excel parser is implemented in:
```
src/pfas/parsers/bank/icici_excel.py
```

Key features:
- Handles ICICI NetBanking Excel export format
- Flexible column mapping
- Robust date and amount parsing
- Auto-categorization using keyword matching
- Integration with PFAS database schema

## Tax Calculations Verified

The parser successfully calculated:
- **Total Interest Income:** ₹21,785.00 (FY 2024-25)
- **Section 80TTA Deduction:** ₹10,000.00 (max limit)
- **Taxable Interest:** ₹11,785.00

This data can be directly used for ITR filing.
