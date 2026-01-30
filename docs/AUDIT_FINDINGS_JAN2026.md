# PFAS Project Audit Findings - January 2026

## Context Summary for Claude CLI

**Project**: PFAS (Personal Financial Accounting System) for Indian Tax Residents
**Audit Date**: January 27, 2026
**Last Updated**: January 27, 2026
**Auditor**: Claude Opus 4.5
**Project Path**: `/home/sshankar/projects/pfas-project`

### Quick Resume Instructions
If continuing after rate limit, provide this context:
```
Resume PFAS audit from /home/sshankar/projects/pfas-project/docs/AUDIT_FINDINGS_JAN2026.md
NPS and PPF parsers need separate review - see section on deferred tasks.
```

---

## Executive Summary

| Metric | Value |
|--------|-------|
| Total Tests | 968 (was 932) |
| Tests Passing | ~940 |
| Tests Skipped | 14 (NPS, PPF, full_fy) |
| Hardcoded Paths Found | 15+ |
| Critical Fixes Completed | 4 of 4 ✅ |
| High Priority Fixes Completed | 3 of 4 ✅ |
| New Utilities Created | 2 (MultiFileProcessor, Validators) |
| New Tests Added | 36 |

---

## 1. Audit Summary Table

| Area | Issues Found | Fragility Level | Priority |
|------|--------------|-----------------|----------|
| **NPS Parser** | Missing files skip tests; header detection fragile | HIGH | P1 |
| **PPF Parser** | Empty worksheet handling present but test skips | MEDIUM | P2 |
| **EPF Parser** | Works well; no empty file handling | MEDIUM | P2 |
| **Hardcoded Paths** | 15+ instances in CLI/parser files | HIGH | P1 |
| **Transaction vs Holdings** | Manual folder separation; no auto-detect | MEDIUM | P3 |
| **Multi-File/FY Handling** | No chronological sorting or FY detection | MEDIUM | P3 |
| **Integration Test Skips** | 14 tests skipped due to missing data/config | MEDIUM | P2 |
| **Password Handling** | Fallback works; no graceful archive-on-failure | LOW | P4 |

---

## 2. Hardcoded Paths - CRITICAL FIXES

### 2.1 Files with Hardcoded Paths

| File | Line | Hardcoded Path | Status |
|------|------|----------------|--------|
| `src/pfas/cli/capital_gains_cli.py` | 266 | `/home/sshankar/CASTest/venv/lib/python3.12/site-packages` | **REMOVE** |
| `src/pfas/cli/mf_analyzer_cli.py` | 198-202 | `PROJECT_ROOT / "Data" / "Users"` | Use PathResolver |
| `src/pfas/cli/mf_analyzer_v2_cli.py` | 34 | `"Data/Users/{user}/config"` | Use PathResolver |
| `src/pfas/cli/mf_analyzer_v2_cli.py` | 92-93 | `Data/Users/{user}/config/...` | Use PathResolver |
| `src/pfas/cli/mf_analyzer_v2_cli.py` | 107 | `"data_root": "Data/Users/{user}"` | Use PathResolver |
| `src/pfas/analyzers/mf_analyzer.py` | 1217 | `f"Data/Users/{user_name}/inbox"` | Use PathResolver |
| `src/pfas/parsers/mf/normalized_mf_parser.py` | 441, 448 | `"Data/Users/Sanjay/..."` | Remove (test code in docstring) |
| `src/pfas/parsers/mf/scanner.py` | 419 | `"Data/Users/Sanjay/inbox/..."` | Remove (docstring example) |
| `src/pfas/services/bank_intelligence/intelligent_analyzer.py` | 119, 772 | `"Data/Users"` | Use PathResolver |
| `src/pfas/services/bank_intelligence/run.py` | 96 | `Data/Users` | Use PathResolver |

### 2.2 Fix Template for mf_analyzer_cli.py (lines 198-202)

**BEFORE:**
```python
user_data_dir = PROJECT_ROOT / "Data" / "Users" / args.user
db_path = args.db or str(user_data_dir / "pfas.db")
mf_folder = Path(args.mf_folder) if args.mf_folder else user_data_dir / "Mutual-Fund"
output_dir = Path(args.output_dir) if args.output_dir else PROJECT_ROOT / "Data" / "Reports" / args.user / "Mutual-Funds"
```

**AFTER:**
```python
from pfas.core.paths import PathResolver

resolver = PathResolver(PROJECT_ROOT, args.user)
db_path = args.db or str(resolver.db_path())
mf_folder = Path(args.mf_folder) if args.mf_folder else resolver.inbox() / "Mutual-Fund"
output_dir = Path(args.output_dir) if args.output_dir else resolver.reports() / "Mutual-Funds"
```

### 2.3 Fix for capital_gains_cli.py (line 266)

**REMOVE THIS LINE ENTIRELY:**
```python
sys.path.insert(0, str(Path("/home/sshankar/CASTest/venv/lib/python3.12/site-packages")))
```

---

## 3. Parser Fragility Issues

### 3.1 NPS Parser (`src/pfas/parsers/nps/nps.py`)

**Problem**: All 5 NPS tests SKIPPED - no files found + fragile header detection

**Affected Tests**:
- `test_nps_parse_basic[Sanjay]` - SKIPPED
- `test_nps_account_details[Sanjay]` - SKIPPED
- `test_nps_tiers[Sanjay]` - SKIPPED
- `test_nps_save_to_db[Sanjay]` - SKIPPED
- `test_nps_deductions[Sanjay]` - SKIPPED

**Fix Location**: Lines 289-310 (`_find_header_row` method)

**Recommended Fix**:
```python
def _find_header_row(self, df: pd.DataFrame) -> Optional[int]:
    """Find the row index containing column headers."""
    header_keywords = ['PRAN', 'TRANSACTION', 'DATE', 'TIER', 'AMOUNT', 'NAV', 'UNITS']

    for idx in range(min(20, len(df))):
        row_values = df.iloc[idx].astype(str).str.upper()
        matches = sum(1 for kw in header_keywords if any(kw in val for val in row_values))
        if matches >= 3:
            return idx

    # FALLBACK: Try first row if it has any relevant keywords
    if len(df) > 0:
        first_row = df.iloc[0].astype(str).str.upper()
        if any(kw in str(first_row.values) for kw in ['PRAN', 'AMOUNT', 'DATE']):
            logger.warning("Using first row as header (fallback)")
            return 0

    return None
```

### 3.2 PPF Parser (`src/pfas/parsers/ppf/ppf.py`)

**Good**: Already has empty worksheet handling (lines 120-126)
**Issue**: Tests skip when no PPF data files exist

**Affected Tests**:
- `test_ppf_parse_basic[Sanjay]` - SKIPPED
- `test_ppf_save_to_db[Sanjay]` - SKIPPED

**Solution**: Create mock test fixture in `tests/fixtures/ppf/sample_ppf.xlsx`

### 3.3 EPF Parser (`src/pfas/parsers/epf/epf.py`)

**Status**: Working well (all 7 tests PASSED)
**Missing**: Multi-file handling for yearly passbooks

**Recommended Addition** - New method `parse_multiple()`:
```python
def parse_multiple(self, file_paths: List[Path]) -> ParseResult:
    """Parse multiple EPF passbook PDFs and merge chronologically."""
    combined_result = ParseResult(success=True, source_file="multiple")
    all_transactions = []

    sorted_files = sorted(file_paths, key=self._get_file_date)

    for file_path in sorted_files:
        result = self.parse(file_path)
        if result.success:
            all_transactions.extend(result.transactions)
            if not combined_result.account and result.account:
                combined_result.account = result.account

    # Deduplicate by (wage_month, transaction_date, transaction_type)
    seen = set()
    unique_txns = []
    for txn in all_transactions:
        key = (txn.wage_month, txn.transaction_date, txn.transaction_type)
        if key not in seen:
            seen.add(key)
            unique_txns.append(txn)

    combined_result.transactions = sorted(unique_txns, key=lambda t: t.transaction_date)
    return combined_result
```

---

## 4. Test Status Summary

### 4.1 Integration Tests - Skipped

| Test File | Tests Skipped | Reason |
|-----------|---------------|--------|
| `test_nps_integration.py` | 5 | No NPS files in inbox/archive |
| `test_ppf_integration.py` | 2 | No data to save |
| `test_full_fy.py` | 7 | Missing config (not user-specific) |
| `test_stock_integration.py` | 1 | No data to save |
| `test_sanjay_full_fy.py` | 1 | No NPS files |

### 4.2 Tests Passing

- All EPF tests (7/7)
- All Bank Intelligence tests (12/12)
- All CAMS MF tests (3/3)
- All thread safety tests (8/8)
- All regression tests (15/15)
- Most unit tests (~800+)

---

## 5. Prioritized Task List

### ✅ COMPLETED FIXES (January 27, 2026)

| Task ID | Description | Status | Details |
|---------|-------------|--------|---------|
| P1-001 | Remove hardcoded venv path | ✅ DONE | Removed `/home/sshankar/CASTest/...` from capital_gains_cli.py:266 |
| P1-002 | Use PathResolver in mf_analyzer_cli | ✅ DONE | Refactored lines 198-202 to use PathResolver |
| P1-004 | Fix mf_analyzer_v2_cli hardcoded paths | ✅ DONE | Updated to use PathResolver throughout |
| P2-003 | Fix bank_intelligence hardcoded paths | ✅ DONE | Updated intelligent_analyzer.py to use config |
| P2-004 | Create MultiFileProcessor utility | ✅ DONE | New file: `src/pfas/core/file_processor.py` |
| P3-002 | Add ParserValidator base class | ✅ DONE | New file: `src/pfas/parsers/validators.py` |
| - | Fix mf_analyzer.py hardcoded path | ✅ DONE | Line 1217 now uses PathResolver |
| - | Add unit tests for new utilities | ✅ DONE | 36 new tests in test_file_processor.py and test_validators.py |

### 🔄 DEFERRED - Separate Review Required

| Task ID | Description | Reason |
|---------|-------------|--------|
| P1-003 | Add mock NPS test data | NPS parser needs full review of statement format |
| P2-001 | Add fallback header detection to NPS | Deferred pending NPS statement review |
| P2-002 | Add mock PPF test data | PPF parser needs full review |

### 📋 REMAINING TASKS

#### P3 - MEDIUM (Next Sprint)

| Task ID | Description | File | Notes |
|---------|-------------|------|-------|
| P3-001 | Implement statement type auto-detection | `src/pfas/parsers/mf/scanner.py` | Holdings vs Transactions |
| P3-003 | Create subfolder structure for MF | `docs/` | transactions/ vs holdings/ |
| P3-004 | Add FY detection to all parsers | Multiple | Use MultiFileProcessor |

#### P4 - LOW (Backlog)

| Task ID | Description | Notes |
|---------|-------------|-------|
| P4-001 | Archive-on-failure for password files | Error handling improvement |
| P4-002 | Comprehensive fragility test suite | Edge case coverage |
| P4-003 | Golden master tests for all parsers | Regression prevention |

---

## 6. Key File Locations

### Source Code
- **Parsers**: `src/pfas/parsers/` (epf/, nps/, ppf/, mf/, bank/, stock/, salary/)
- **Analyzers**: `src/pfas/analyzers/` (mf_analyzer.py, mf_diagnostics.py)
- **CLI**: `src/pfas/cli/` (mf_analyzer_cli.py, reports_cli.py, capital_gains_cli.py)
- **Core**: `src/pfas/core/` (paths.py, database.py, preferences.py)
- **Services**: `src/pfas/services/` (bank_intelligence/, foreign/, itr/)

### Test Files
- **Integration**: `tests/integration/` (test_epf_integration.py, test_nps_integration.py, etc.)
- **Unit**: `tests/unit/` (test_parsers/, test_analyzers/, test_core/)
- **Fixtures**: `tests/fixtures/` (bank/, sample files)
- **Golden Masters**: `tests/integration/golden_masters/`

### Configuration
- **Path Config**: `config/paths.json`
- **MF Analyzer Config**: `config/mf_analyzer_config.json`
- **Test Config**: `config/test_config.json`

---

## 7. PathResolver Reference

The PathResolver class (`src/pfas/core/paths.py`) is the canonical way to resolve paths:

```python
from pfas.core.paths import PathResolver

resolver = PathResolver(root_path="/path/to/pfas", user_name="Sanjay")

# Key methods:
resolver.db_path()           # Data/Users/Sanjay/db/finance.db
resolver.inbox()             # Data/Users/Sanjay/inbox
resolver.archive()           # Data/Users/Sanjay/archive
resolver.reports()           # Data/Users/Sanjay/reports
resolver.user_config_dir()   # Data/Users/Sanjay/config
resolver.get_file_password() # Password from config or prompt
resolver.ensure_user_structure()  # Create all directories
```

---

## 8. Quick Commands

### Run All Tests
```bash
cd /home/sshankar/projects/pfas-project
python -m pytest tests/ -v --tb=short
```

### Run Specific Parser Tests
```bash
# EPF (working)
python -m pytest tests/integration/test_epf_integration.py -v

# NPS (skipped - needs mock data)
python -m pytest tests/integration/test_nps_integration.py -v

# PPF (partially working)
python -m pytest tests/integration/test_ppf_integration.py -v
```

### Search for Hardcoded Paths
```bash
grep -rn "Data/Users" src/pfas/ --include="*.py"
grep -rn "/home/" src/pfas/ --include="*.py"
```

---

## 9. Next Steps for Implementation

1. **Start with P1-001**: Remove the hardcoded venv path in capital_gains_cli.py (1 line delete)
2. **Then P1-002**: Refactor mf_analyzer_cli.py to use PathResolver (4 lines change)
3. **Create mock test data**: NPS and PPF fixtures for CI
4. **Add NPS header fallback**: Improve robustness of header detection

---

---

## 10. New Files Created

### Core Utilities

**`src/pfas/core/file_processor.py`** - Multi-file processing utility
- `MultiFileProcessor` class with methods:
  - `sort_by_date()` - Chronological file sorting
  - `detect_financial_year()` - FY detection from filename
  - `detect_fy_from_date()` - FY from transaction date
  - `group_by_fy()` - Group files by financial year
  - `deduplicate_records()` - Generic record deduplication
  - `get_latest_file()` - Find most recent file

**`src/pfas/parsers/validators.py`** - Input file validators
- `ParserValidator` - Abstract base class
- `ExcelValidator` - Excel file validation (worksheets, columns, rows)
- `PDFValidator` - PDF validation (pages, text, password)
- `CSVValidator` - CSV validation (encoding, columns)
- `CompositeValidator` - Multi-format validator
- Pre-configured: `NPSValidator`, `PPFValidator`, `EPFValidator`, `MFValidator`

### New Tests

**`tests/unit/test_core/test_file_processor.py`** - 20 tests
- FY detection from filenames
- FY calculation from dates
- File sorting (chronological)
- File grouping by FY
- Record deduplication
- Latest file retrieval

**`tests/unit/test_parsers/test_validators.py`** - 16 tests
- Excel validation (valid, empty, missing columns)
- CSV validation (valid, empty, missing columns)
- PDF validation (via composite)
- Composite validator routing
- NPS/PPF specific validators

---

## 11. Files Modified

| File | Changes |
|------|---------|
| `src/pfas/cli/capital_gains_cli.py` | Removed hardcoded venv path (line 266) |
| `src/pfas/cli/mf_analyzer_cli.py` | Added PathResolver import; refactored path logic |
| `src/pfas/cli/mf_analyzer_v2_cli.py` | Removed USER_CONFIG_DIR; use PathResolver in load_config |
| `src/pfas/analyzers/mf_analyzer.py` | Added PathResolver import; fixed data_root path |
| `src/pfas/services/bank_intelligence/intelligent_analyzer.py` | Config-driven data_root; PathResolver fallback |

---

## 12. NPS/PPF Separate Review Notes

These parsers are deferred for separate review because:

1. **NPS Parser Issues**:
   - All 5 integration tests SKIPPED (no test files)
   - Header detection may need adjustment for actual NPS portal exports
   - Need sample NPS statement files to validate parser logic

2. **PPF Parser Issues**:
   - 2 tests SKIPPED (no data to save)
   - Need to verify column mappings with actual bank PPF statements
   - Different banks may have different formats

**Recommendation**: Obtain actual NPS/PPF statement samples from user's inbox and:
1. Analyze actual file format and column headers
2. Adjust parser header detection accordingly
3. Create mock test fixtures based on real format
4. Add integration tests with real data patterns

---

## Document Version
- Created: 2026-01-27
- Last Updated: 2026-01-27
- Author: Claude Opus 4.5
- Status: ACTIVE - Most P1-P2 fixes complete; NPS/PPF deferred
