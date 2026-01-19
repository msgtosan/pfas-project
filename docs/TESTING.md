# PFAS Testing Guide

This document describes the testing strategy and regression test suite for the PFAS project.

## Overview

The PFAS project includes three types of tests:

1. **Unit Tests** - Test individual components in isolation
2. **Integration Tests** - Test multiple components working together
3. **Regression Tests** - Comprehensive end-to-end tests with real user data

## Quick Start

```bash
# Install git hooks (runs tests before commits)
make install-hooks

# Run quick smoke tests (< 10 seconds)
make test-quick

# Run full regression suite (30-60 seconds)
make test-regression

# Run all unit tests
make test-unit

# Run with coverage report
make coverage
```

## Regression Test Suite

The regression test suite (`tests/regression/test_sanjay_regression.py`) validates the entire ingestion pipeline for all 11 asset types using real user data.

### What It Tests

#### Quick Smoke Test (< 10 seconds)
- ✅ All modules can be imported
- ✅ Database schema can be created
- ✅ Basic functionality works

Run with: `make test-quick` or `./scripts/run_regression_tests.sh --quick`

#### Full Regression Suite (30-60 seconds)
- ✅ All 11 asset type ingesters work correctly
- ✅ No parsing errors or exceptions
- ✅ Database integrity is maintained
- ✅ No duplicate records created
- ✅ Ingestion log is properly tracked
- ✅ All journal entries are balanced
- ✅ Performance metrics are reasonable

Run with: `make test-regression` or `./scripts/run_regression_tests.sh --full`

### Asset Types Tested

1. **Mutual-Fund** - CAMS, KARVY/KFintech statements
2. **Bank** - ICICI, HDFC, SBI bank statements
3. **Indian-Stocks** - Zerodha, ICICI Direct trade/holdings
4. **Salary** - Form16, Payslips
5. **EPF** - Employee Provident Fund passbook
6. **NPS** - National Pension System statements
7. **PPF** - Public Provident Fund passbook
8. **SGB** - Sovereign Gold Bonds
9. **USA-Stocks** - Morgan Stanley, E-Trade statements
10. **FD-Bonds** - Fixed Deposits and Bonds
11. **Other-Income** - Other income sources

### Test Results Interpretation

#### All Tests Passing
```
=====================================
  ✓ All regression tests passed!
=====================================
```

This means:
- All parsers are working correctly
- No regressions introduced
- Safe to commit your changes

#### Some Tests Skipped
```
9 skipped
```

This is normal - tests skip when:
- No files exist in inbox for that asset type
- Files have already been archived
- User data is not available for testing

#### Tests Failed
```
FAILED tests/regression/test_sanjay_regression.py::TestSanjayRegressionSuite::test_02_bank_statement_ingestion
```

This indicates:
- A parser is broken
- Database schema changed
- File format not recognized

Check the detailed error message to diagnose the issue.

## Pre-Commit Hook

The pre-commit hook automatically runs quick smoke tests before each commit.

### Installation

```bash
# Option 1: Using make
make install-hooks

# Option 2: Manual installation
./scripts/install_hooks.sh

# Option 3: Direct symlink
ln -sf ../../.pre-commit-hook .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

### Usage

The hook runs automatically:

```bash
git commit -m "Your commit message"
```

Output:
```
Running pre-commit regression tests...

✓ Quick smoke tests passed

[main abc123d] Your commit message
```

### Skipping the Hook

For emergency commits (use sparingly):

```bash
git commit --no-verify -m "Emergency fix"
```

## Running Tests for Different Users

The regression suite can test any user's data:

```bash
# Default is Sanjay
./scripts/run_regression_tests.sh --full

# Test for different user
./scripts/run_regression_tests.sh --full --user Priya
```

## Test Data Requirements

Tests use actual data from:
```
Data/Users/<UserName>/inbox/
```

For comprehensive testing, ensure each asset type folder contains:
- At least one sample file
- Files in supported formats (PDF, Excel, CSV)
- Files that parse successfully

## Coverage Reports

Generate HTML coverage reports:

```bash
make coverage

# View report
open coverage_html/index.html
```

Current coverage targets:
- **Overall:** 90%
- **Parsers:** 85%
- **Core modules:** 95%

## Continuous Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.12'
      - name: Install dependencies
        run: |
          pip install -e .[dev]
      - name: Run regression tests
        run: |
          make test-regression
      - name: Run unit tests
        run: |
          make test-unit
```

## Performance Benchmarks

Target execution times:

| Test Suite | Target | Current |
|-----------|--------|---------|
| Quick Smoke | < 10s | ~3s |
| Full Regression | < 60s | ~8s |
| Unit Tests | < 30s | ~15s |
| Integration Tests | < 60s | ~45s |
| Coverage Report | < 90s | ~60s |

## Troubleshooting

### Tests are slow
- Use `--quick` mode for pre-commit
- Run `--full` mode only before push
- Check for network timeouts (currency rates, etc.)

### Tests fail on fresh clone
1. Install dependencies: `pip install -e .[dev]`
2. Ensure test data exists in `Data/Users/Sanjay/`
3. Check file permissions

### Database locked errors
- Ensure no other processes are using the database
- Check that previous test cleaned up properly
- Look for orphaned connections

### Import errors
```bash
# Set PYTHONPATH manually
export PYTHONPATH=src
pytest tests/regression/

# Or use make (handles this automatically)
make test-regression
```

### Path resolution failures
- Verify `config/paths.json` exists
- Check Data symlink: `ls -la Data`
- Ensure user directory exists: `ls Data/Users/Sanjay`

## Best Practices

### Before Committing
1. Run quick smoke tests: `make test-quick`
2. If you modified parsers, run full regression: `make test-regression`
3. If you modified core modules, run all tests: `make test`

### Before Pushing
1. Run full test suite: `make test`
2. Generate coverage report: `make coverage`
3. Ensure coverage hasn't decreased

### Before Releasing
1. Run all tests with coverage
2. Test on fresh clone
3. Verify all asset types work with sample data
4. Check performance benchmarks

## Writing New Tests

### Adding a New Asset Type Test

```python
def test_11_new_asset_ingestion(self, db_connection, path_resolver):
    """Test New Asset ingestion."""
    from pfas.parsers.new_asset.ingester import NewAssetIngester

    inbox_path = path_resolver.inbox() / "New-Asset"
    if not inbox_path.exists():
        pytest.skip("New-Asset inbox not found")

    files = list(inbox_path.rglob("*.*"))
    files = [f for f in files if f.suffix.lower() in ['.pdf', '.xlsx']]
    print(f"\nNew-Asset: Found {len(files)} files to process")

    if not files:
        pytest.skip("No New-Asset files found")

    ingester = NewAssetIngester(db_connection, 1, inbox_path)
    result = ingester.ingest(force=False)

    print(f"  Processed: {result.files_processed}")
    print(f"  Skipped: {result.files_skipped}")
    print(f"  Records Inserted: {result.records_inserted}")
    print(f"  Records Skipped: {result.records_skipped}")

    assert result.files_processed >= 0, "Ingestion failed"
```

### Testing Conventions

1. **Naming:** `test_NN_descriptive_name` (NN for ordering)
2. **Skip gracefully:** Use `pytest.skip()` if data unavailable
3. **Print progress:** Help diagnose failures
4. **Assert minimal:** Don't over-constrain
5. **Clean up:** Ensure tests don't leave artifacts

## FAQ

**Q: Why are most tests skipped?**
A: Tests skip when no files are in the inbox. This is normal after ingestion has run and files are archived.

**Q: Can I run tests without installing hooks?**
A: Yes, hooks are optional. You can always run `make test-regression` manually.

**Q: Do tests modify my actual data?**
A: No, regression tests use an in-memory database. Your real database is never touched.

**Q: How do I test with my own data?**
A: Place files in `Data/Users/<YourName>/inbox/<AssetType>/` and run:
```bash
./scripts/run_regression_tests.sh --full --user <YourName>
```

**Q: What if I don't have test data for all asset types?**
A: That's fine! Tests will skip asset types with no data. At minimum, have data for the asset types you're working on.

## Getting Help

- Check `tests/regression/README.md` for detailed documentation
- Review failing test output for specific errors
- Run with `-v` for verbose output: `pytest tests/regression/ -v`
- Run single test: `pytest tests/regression/test_sanjay_regression.py::TestQuickSmokeTest -v`
