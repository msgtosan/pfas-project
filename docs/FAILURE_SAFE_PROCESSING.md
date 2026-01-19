# Failure-Safe File Processing

## Overview

PFAS implements failure-safe file processing across all asset classes. Files are **only archived on complete success**. On any failure, files are preserved for re-processing.

## Key Principle

**FILES ONLY MOVE TO ARCHIVE ON FULL SUCCESS**

- ✅ **Success:** Parse ✓ + Validate ✓ + Ingest ✓ → File moves to `archive/<asset>/`
- ✗ **Failure:** Any error → File moves to `inbox/<asset>/failed/` with timestamp
- ⊘ **Skipped:** Already processed → File stays in `inbox/<asset>/`

## Behavior

### Before (Old - Problematic)
```
inbox/Mutual-Fund/file.pdf
  ↓ Process (even if error)
archive/Mutual-Fund/2026-01-17_file.pdf
  ↓ Result: Inbox empty, can't re-test failures
```

### After (New - Failure-Safe)
```
inbox/Mutual-Fund/file1.pdf  → Success  → archive/Mutual-Fund/2026-01-17_file1.pdf
inbox/Mutual-Fund/file2.pdf  → Failure  → inbox/Mutual-Fund/failed/2026-01-17_file2.pdf
inbox/Mutual-Fund/file3.pdf  → Skipped → stays in inbox/Mutual-Fund/file3.pdf
```

##Failure Handling

### Failure Types

1. **PARSE** - Failed to parse file
   - Example: Corrupted PDF, unsupported format, encrypted without password
   - Action: Move to `failed/`, log error details

2. **VALIDATION** - Parsed but data validation failed
   - Example: Missing required fields, invalid dates, negative amounts
   - Action: Move to `failed/`, log validation errors

3. **INGESTION** - Parsed but database insertion failed
   - Example: Database constraint violation, connection error
   - Action: Move to `failed/`, log database error

4. **EXCEPTION** - Unexpected error
   - Example: Out of memory, permission denied, timeout
   - Action: Move to `failed/`, log exception with traceback

### Failed File Location

Failed files are moved to:
```
inbox/<asset>/failed/YYYY-MM-DD_<original_filename>
```

Examples:
```
inbox/Mutual-Fund/failed/2026-01-17_CAMS_statement.pdf
inbox/Bank/failed/2026-01-17_ICICI_Jan2024.xlsx
inbox/Indian-Stocks/failed/2026-01-17_Zerodha_PnL.csv
```

If the same file fails multiple times on the same day:
```
inbox/Mutual-Fund/failed/2026-01-17_statement_1.pdf
inbox/Mutual-Fund/failed/2026-01-17_statement_2.pdf
```

## Summary Output

### Success Case
```
======================================================================
  Mutual-Fund Ingestion Summary
======================================================================
  ✓ Success: 8 files archived
  ⊘ Skipped: 2 files (already processed)

  Records: 1,234 inserted, 56 duplicates
======================================================================
```

### Mixed Success/Failure
```
======================================================================
  Bank Ingestion Summary
======================================================================
  ✓ Success: 3 files archived
  ✗ Failed:  2 files (kept in inbox/failed/)
     - ICICI_Jan2024.xlsx: PARSE - File is password-protected
     - HDFC_statement.pdf: INGESTION - Database constraint violation
  ⊘ Skipped: 1 files (already processed)

  Records: 245 inserted, 12 duplicates

  💡 Re-run after fixing issues:
     pfas --user Sanjay ingest --asset Bank --force
======================================================================
```

### All Failed
```
======================================================================
  Salary Ingestion Summary
======================================================================
  ✓ Success: 0 files archived
  ✗ Failed:  3 files (kept in inbox/failed/)
     - Form16_2024.pdf: PARSE - PDF is encrypted, password required
     - Payslip_Jan.xlsx: VALIDATION - Missing required field 'basic_salary'
     - Payslip_Feb.xlsx: EXCEPTION - PermissionError: Access denied

  Records: 0 inserted, 0 duplicates

  💡 Re-run after fixing issues:
     pfas --user Sanjay ingest --asset Salary --force
======================================================================
```

## CLI Usage

### Basic Ingestion
```bash
# Ingest with failure-safe handling (default)
pfas --user Sanjay ingest --asset Mutual-Fund

# Files will:
# - Succeed → Stay in inbox (for manual archiving)
# - Fail → Move to inbox/Mutual-Fund/failed/
```

### Ingestion with Archiving
```bash
# Ingest and archive successful files
pfas --user Sanjay ingest --asset Bank --archive

# Files will:
# - Succeed → Move to archive/Bank/
# - Fail → Move to inbox/Bank/failed/
```

### Force Re-processing
```bash
# Re-process files (including previously successful)
pfas --user Sanjay ingest --asset Indian-Stocks --force

# Useful after:
# - Fixing parser bugs
# - Adding new validation rules
# - Database schema changes
```

### Disable Failed File Movement
```bash
# Keep failed files in inbox (don't move to failed/)
pfas --user Sanjay ingest --asset EPF --no-move-failed

# Files will:
# - Succeed → Processed
# - Fail → Stay in inbox/EPF/ (not moved to failed/)
```

## Python API

### Basic Usage
```python
from pathlib import Path
from pfas.parsers.bank.ingester import BankIngester

# Create ingester
ingester = BankIngester(conn, user_id, inbox_path)

# Run with failure-safe handling
result = ingester.ingest(force=False, move_failed=True)

# Check results
print(f"Succeeded: {result.files_succeeded}")
print(f"Failed: {result.files_failed}")

# Archive only successful files
if result.succeeded_files:
    from pfas.services.archiver import archive_processed_files
    archive_result = archive_processed_files(
        processed_files=result.succeeded_files,  # NOT all processed!
        inbox_base=inbox_path.parent,
        archive_base=archive_path,
        user_name="Sanjay"
    )
```

### Detailed Error Handling
```python
# Process and handle errors
result = ingester.ingest(force=False, move_failed=True)

# Print summary
result.print_summary()

# Process failed files
for failed in result.failed_files:
    print(f"File: {failed.file_name}")
    print(f"Error: {failed.error_message}")
    print(f"Type: {failed.error_type}")
    print(f"Time: {failed.timestamp}")

    # Take action based on error type
    if failed.error_type == 'PARSE':
        print("  → Check file format and encoding")
    elif failed.error_type == 'VALIDATION':
        print("  → Review data quality")
    elif failed.error_type == 'INGESTION':
        print("  → Check database constraints")
    elif failed.error_type == 'EXCEPTION':
        print("  → Review logs for exception details")
```

### Custom Failure Handling
```python
# Don't move failed files (handle manually)
result = ingester.ingest(force=False, move_failed=False)

# Manual handling
for failed in result.failed_files:
    file_path = failed.file_path

    if failed.error_type == 'PARSE':
        # Move to special folder
        special_folder = inbox_path / "needs_password"
        special_folder.mkdir(exist_ok=True)
        shutil.move(file_path, special_folder / file_path.name)

    elif failed.error_type == 'VALIDATION':
        # Keep for manual review
        review_folder = inbox_path / "review"
        review_folder.mkdir(exist_ok=True)
        shutil.move(file_path, review_folder / file_path.name)
```

## Integration with Archiver

### Correct Usage (Failure-Safe)
```python
# CORRECT: Only archive succeeded files
result = ingester.ingest()

if result.succeeded_files:
    archive_result = archive_processed_files(
        processed_files=result.succeeded_files,  # ✓ Only succeeded
        inbox_base=inbox_path.parent,
        archive_base=archive_path,
        user_name="Sanjay"
    )
```

### Incorrect Usage (Not Failure-Safe)
```python
# WRONG: Archiving all processed files (including failed)
result = ingester.ingest()

if result.processed_files:  # ✗ Includes failed files!
    archive_result = archive_processed_files(
        processed_files=result.processed_files,  # ✗ WRONG!
        inbox_base=inbox_path.parent,
        archive_base=archive_path,
        user_name="Sanjay"
    )
```

## Workflow Examples

### Example 1: First-Time Ingestion
```bash
# Step 1: Place files in inbox
ls Data/Users/Sanjay/inbox/Mutual-Fund/
# CAMS_statement.pdf
# KARVY_statement.xlsx
# corrupt_file.pdf

# Step 2: Run ingestion
pfas --user Sanjay ingest --asset Mutual-Fund --archive

# Output:
# ======================================================================
#   Mutual-Fund Ingestion Summary
# ======================================================================
#   ✓ Success: 2 files archived
#   ✗ Failed:  1 files (kept in inbox/failed/)
#      - corrupt_file.pdf: PARSE - Cannot read PDF file
#
#   Records: 487 inserted, 0 duplicates
# ======================================================================

# Step 3: Check results
ls Data/Users/Sanjay/archive/Mutual-Fund/
# 2026-01-17_Sanjay_CAMS_statement.pdf
# 2026-01-17_Sanjay_KARVY_statement.xlsx

ls Data/Users/Sanjay/inbox/Mutual-Fund/failed/
# 2026-01-17_corrupt_file.pdf
```

### Example 2: Fix and Re-process Failed Files
```bash
# Step 1: Check failed files
ls Data/Users/Sanjay/inbox/Bank/failed/
# 2026-01-17_ICICI_statement.pdf

# Step 2: Diagnose issue
# Error was: "PDF is password-protected"

# Step 3: Add password to passwords.json
cat Data/Users/Sanjay/config/passwords.json
# {
#   "files": {
#     "ICICI_statement.pdf": "MyPassword123"
#   }
# }

# Step 4: Move file back to inbox
mv Data/Users/Sanjay/inbox/Bank/failed/2026-01-17_ICICI_statement.pdf \
   Data/Users/Sanjay/inbox/Bank/ICICI_statement.pdf

# Step 5: Re-process
pfas --user Sanjay ingest --asset Bank --archive

# Output:
#   ✓ Success: 1 files archived
#   Records: 234 inserted, 0 duplicates
```

### Example 3: Force Re-processing After Parser Fix
```bash
# Scenario: Fixed a bug in MF parser, need to re-process all files

# Step 1: Move archived files back to inbox (if needed)
# Or process from archive directly

# Step 2: Force re-processing
pfas --user Sanjay ingest --asset Mutual-Fund --force --archive

# This will:
# - Reprocess ALL files (even previously successful)
# - Update records in database
# - Archive successful ones again
```

## Asset-Specific Considerations

### Mutual Fund (MF)
- Common failures: Encrypted PDFs (CAMS/KARVY), unsupported Excel format
- Solution: Add passwords to `passwords.json`, use latest Excel files

### Bank Statements
- Common failures: PDF password, unrecognized bank format
- Solution: Add passwords, check bank is supported (ICICI/HDFC/SBI)

### Indian Stocks
- Common failures: Broker format changed, missing columns in CSV
- Solution: Update parser for new format, check CSV headers

### Salary (Form16/Payslips)
- Common failures: Encrypted PDF, missing required fields
- Solution: Add password, ensure all required fields present

### EPF/NPS/PPF
- Common failures: PDF parsing issues, date format changes
- Solution: Check PDF is readable, verify date formats

## Monitoring and Debugging

### Check Failed Files
```bash
# List all failed files across all assets
find Data/Users/Sanjay/inbox/*/failed/ -type f

# Count failed files per asset
for asset in Mutual-Fund Bank Indian-Stocks Salary EPF NPS PPF SGB; do
    count=$(find Data/Users/Sanjay/inbox/$asset/failed/ -type f 2>/dev/null | wc -l)
    [ $count -gt 0 ] && echo "$asset: $count failed files"
done
```

### Check Ingestion Log
```sql
-- Recent failures
SELECT source_file, error_message, created_at
FROM ingestion_log
WHERE user_id = 1 AND status = 'FAILED'
ORDER BY created_at DESC
LIMIT 10;

-- Failure summary by asset
SELECT asset_type, COUNT(*) as failures
FROM ingestion_log
WHERE user_id = 1 AND status = 'FAILED'
GROUP BY asset_type;

-- Failures by error type
SELECT
    CASE
        WHEN error_message LIKE '%password%' THEN 'PASSWORD'
        WHEN error_message LIKE '%parse%' THEN 'PARSE'
        WHEN error_message LIKE '%validation%' THEN 'VALIDATION'
        ELSE 'OTHER'
    END as error_category,
    COUNT(*) as count
FROM ingestion_log
WHERE user_id = 1 AND status = 'FAILED'
GROUP BY error_category;
```

### Debug Mode
```bash
# Run with debug output
pfas --user Sanjay ingest --asset Bank --debug

# This will:
# - Show full stack traces
# - Print detailed parsing steps
# - Display SQL queries
```

## Best Practices

### 1. Always Use Archive Flag
```bash
# Good: Archive successful files
pfas --user Sanjay ingest --asset Mutual-Fund --archive

# Avoid: Manual archiving later (error-prone)
pfas --user Sanjay ingest --asset Mutual-Fund
# ... manually move files ...
```

### 2. Review Failed Files Regularly
```bash
# Weekly: Check for failed files
find Data/Users/Sanjay/inbox/*/failed/ -type f

# Fix issues and re-process
```

### 3. Monitor Ingestion Log
```bash
# Check recent failures
pfas --user Sanjay status

# Or query database directly
```

### 4. Use Force Sparingly
```bash
# Force only when necessary (after parser fixes, schema changes)
pfas --user Sanjay ingest --asset Bank --force

# Not for regular ingestion (wastes time on duplicates)
```

### 5. Keep Failed Files Clean
```bash
# After fixing and re-processing, clean up old failed files
find Data/Users/Sanjay/inbox/*/failed/ -mtime +30 -delete

# Or move to archive/failed/ for record keeping
```

## Troubleshooting

### Files Keep Failing
1. Check error message in summary
2. Look at detailed logs: `tail -f pfas.log`
3. Verify file format is supported
4. Check passwords.json for encrypted files
5. Try opening file manually to verify it's not corrupted

### Files Not Moving to Failed
1. Check permissions on inbox/failed/ directory
2. Verify move_failed=True (default)
3. Check disk space

### All Files Skipped
1. Files were already processed (check ingestion_log)
2. Use `--force` to reprocess
3. Or delete from ingestion_log to mark as unprocessed

### Archive Not Working
1. Check archive path exists and is writable
2. Verify you're using `succeeded_files` not `processed_files`
3. Check file archiver logs for errors

## Summary

The failure-safe file processing system ensures:

- ✅ **Data Integrity**: No data loss from failed processing
- ✅ **Debuggability**: Failed files preserved for analysis
- ✅ **Re-processability**: Easy to fix and retry failed files
- ✅ **Visibility**: Clear summary of what succeeded/failed
- ✅ **Maintainability**: Consistent behavior across all asset types

**Remember:** Files only move to archive on COMPLETE SUCCESS!
