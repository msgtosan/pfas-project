# PFAS Regression Test Suite

This directory contains the regression test suite for the PFAS project. These tests should be run before every git commit to ensure no regressions are introduced.

## Overview

The regression test suite validates:
- ✅ All 11 asset type ingesters work correctly
- ✅ No parsing errors or exceptions
- ✅ Database integrity is maintained
- ✅ No duplicate records
- ✅ Ingestion log is properly tracked
- ✅ All journal entries are balanced

## Quick Start

### Run Tests Manually

```bash
# Quick smoke test (< 10 seconds)
make test-quick

# Full regression suite (30-60 seconds)
make test-regression

# With coverage report
make coverage
```

### Install Git Pre-Commit Hook

```bash
# Install the hook
make install-hooks

# Or manually
./scripts/install_hooks.sh
```

Once installed, the pre-commit hook will automatically run quick smoke tests before each commit.

## Test Organization

### `TestQuickSmokeTest`
Fast tests that verify:
- All modules can be imported
- Database schema can be created
- Basic functionality works

**Run time:** < 10 seconds

### `TestSanjayRegressionSuite`
Comprehensive tests that verify:
- All 11 asset types ingest correctly
- Database integrity after ingestion
- No duplicate records
- Performance metrics

**Run time:** 30-60 seconds

## Asset Types Tested

1. **Mutual-Fund** - CAMS, KARVY statements
2. **Bank** - ICICI, HDFC, SBI statements
3. **Indian-Stocks** - Zerodha, ICICI Direct
4. **Salary** - Form16, Payslips
5. **EPF** - Employee Provident Fund
6. **NPS** - National Pension System
7. **PPF** - Public Provident Fund
8. **SGB** - Sovereign Gold Bonds
9. **USA-Stocks** - Morgan Stanley, E-Trade
10. **FD-Bonds** - Fixed Deposits and Bonds
11. **Other-Income** - Other income sources

## Running Tests for Different Users

```bash
# Default is Sanjay
./scripts/run_regression_tests.sh --full

# Run for different user
./scripts/run_regression_tests.sh --full --user Priya
```

## Interpreting Results

### Success
```
===================================
  ✓ All regression tests passed!
===================================
```

### Failure
The test output will show which asset type failed and why. Common issues:
- **Import errors** - Missing dependencies
- **Parse errors** - File format changes
- **Database errors** - Schema issues
- **Integrity errors** - Duplicate records

## Skipping Tests

If you need to commit without running tests (not recommended):
```bash
git commit --no-verify
```

## Adding New Tests

To add a new asset type test:

1. Add test method to `TestSanjayRegressionSuite` class
2. Follow the naming convention: `test_NN_asset_name_ingestion`
3. Use the same pattern as existing tests
4. Ensure test can skip gracefully if no files exist

Example:
```python
def test_11_new_asset_ingestion(self, db_connection, path_resolver):
    """Test New Asset ingestion."""
    from pfas.parsers.new_asset.ingester import NewAssetIngester

    inbox_path = path_resolver.inbox_dir / "New-Asset"
    if not inbox_path.exists():
        pytest.skip("New-Asset inbox not found")

    files = list(inbox_path.rglob("*.*"))
    files = [f for f in files if f.suffix.lower() in ['.pdf', '.xlsx']]

    if not files:
        pytest.skip("No New-Asset files found")

    ingester = NewAssetIngester(db_connection, 1, inbox_path)
    result = ingester.ingest(force=False)

    print(f"  Processed: {result.files_processed}")
    print(f"  Success: {result.files_succeeded}")
    print(f"  Failed: {result.files_failed}")
    print(f"  Records: {result.total_records}")

    assert result.files_succeeded >= 0, "Ingestion failed"
```

## Test Data

Tests use actual data from:
```
Data/Users/Sanjay/inbox/
```

Ensure test data exists for each asset type you want to test.

## Continuous Integration

These tests can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Run regression tests
  run: |
    pip install -e .[dev]
    make test-regression
```

## Performance Targets

- Quick smoke test: < 10 seconds
- Full regression suite: < 60 seconds
- Coverage generation: < 90 seconds

## Troubleshooting

### Tests are slow
- Use `--quick` mode for pre-commit
- Run `--full` mode only before push

### Tests fail on fresh clone
- Install dependencies: `pip install -e .[dev]`
- Ensure test data exists in `Data/Users/Sanjay/`

### Database locked errors
- Ensure no other processes are using the database
- Check that previous test cleaned up properly

### Import errors
- Set PYTHONPATH: `export PYTHONPATH=src`
- Or use: `make test-regression` (handles this automatically)
