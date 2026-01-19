# CASParser vs PFAS MF Module - Gap Analysis

## Executive Summary

After analyzing the [casparser](https://github.com/codereverser/casparser) library and comparing it with PFAS MF module, I've identified **15 significant gaps** that should be addressed to bring PFAS up to production quality for CAS statement parsing.

**Priority Levels:**
- 🔴 **Critical** - Core functionality gaps
- 🟠 **High** - Important for accuracy and usability
- 🟡 **Medium** - Nice to have improvements
- 🟢 **Low** - Future enhancements

---

## 1. Data Model Gaps

### 1.1 🔴 Missing Transaction Types

**CASParser supports 16 transaction types:**
```python
PURCHASE, PURCHASE_SIP, REDEMPTION
DIVIDEND_PAYOUT, DIVIDEND_REINVEST
SWITCH_IN, SWITCH_IN_MERGER, SWITCH_OUT, SWITCH_OUT_MERGER
STT_TAX, STAMP_DUTY_TAX, TDS_TAX
SEGREGATION, MISC, UNKNOWN, REVERSAL
```

**PFAS supports only 6:**
```python
PURCHASE, REDEMPTION
SWITCH_IN, SWITCH_OUT
DIVIDEND, DIVIDEND_REINVEST
```

**Missing in PFAS:**
| Transaction Type | Impact |
|-----------------|--------|
| `PURCHASE_SIP` | Cannot distinguish SIP from lumpsum purchases |
| `SWITCH_IN_MERGER` / `SWITCH_OUT_MERGER` | Scheme merger transactions misclassified |
| `STT_TAX`, `STAMP_DUTY_TAX`, `TDS_TAX` | Tax deductions not tracked separately |
| `SEGREGATION` | Side-pocketed units not handled |
| `REVERSAL` | Transaction reversals not identified |
| `MISC` / `UNKNOWN` | No fallback for unclassified transactions |

**Fix Required:**
```python
# pfas/parsers/mf/models.py
class TransactionType(Enum):
    PURCHASE = "PURCHASE"
    PURCHASE_SIP = "PURCHASE_SIP"       # NEW
    REDEMPTION = "REDEMPTION"
    SWITCH_IN = "SWITCH_IN"
    SWITCH_IN_MERGER = "SWITCH_IN_MERGER"   # NEW
    SWITCH_OUT = "SWITCH_OUT"
    SWITCH_OUT_MERGER = "SWITCH_OUT_MERGER" # NEW
    DIVIDEND = "DIVIDEND"
    DIVIDEND_REINVEST = "DIVIDEND_REINVEST"
    STT_TAX = "STT_TAX"                 # NEW
    STAMP_DUTY_TAX = "STAMP_DUTY_TAX"   # NEW
    TDS_TAX = "TDS_TAX"                 # NEW
    SEGREGATION = "SEGREGATION"         # NEW
    REVERSAL = "REVERSAL"               # NEW
    MISC = "MISC"                       # NEW
    UNKNOWN = "UNKNOWN"                 # NEW
```

---

### 1.2 🔴 Missing Investor Information Model

**CASParser has:**
```python
@dataclass
class InvestorInfo:
    name: str
    email: str
    address: str
    mobile: str
```

**PFAS lacks:** A dedicated investor info structure per CAS statement. This data is extracted during parsing but not systematically stored.

**Fix Required:** Add `InvestorInfo` dataclass and link to CAS parsing results.

---

### 1.3 🟠 Missing Statement Period Tracking

**CASParser has:**
```python
@dataclass
class StatementPeriod:
    from_: str  # aliased as "from"
    to: str
```

**PFAS lacks:** Explicit tracking of CAS statement period (from/to dates). This is important for:
- Ensuring complete transaction coverage
- Detecting gaps between statements
- Validating data continuity

---

### 1.4 🟠 Missing Scheme Metadata Fields

**CASParser tracks:**
```python
@dataclass
class Scheme:
    scheme: str
    advisor: Optional[str]     # MISSING in PFAS
    rta_code: str              # MISSING in PFAS
    rta: str
    type: Optional[str]
    isin: Optional[str]
    amfi: Optional[str]        # MISSING in PFAS
    nominees: List[str]        # MISSING in PFAS
    open: Decimal              # Opening balance - MISSING
    close: Decimal             # Closing balance
    close_calculated: Decimal  # Calculated vs stated mismatch
```

**Missing fields in PFAS:**
- `advisor` - Distributor/advisor name
- `rta_code` - RTA-specific scheme code
- `amfi` - AMFI code for scheme identification
- `nominees` - Nominee information
- `open` / `close_calculated` - Balance reconciliation fields

---

## 2. PDF Parsing Gaps

### 2.1 🔴 Weak PDF Text Extraction

**CASParser uses:**
- Primary: PyMuPDF (fast, accurate) via `casparser[fast]`
- Fallback: pdfminer.six (pure Python)
- Handles password-protected PDFs with proper error messages

**PFAS uses:**
- pdfplumber only (no PyMuPDF option)
- Limited regex patterns for text extraction
- PDF parsing is partial/incomplete for CAS statements

**Fix Required:**
```python
# Add PyMuPDF support with fallback
try:
    import fitz  # PyMuPDF
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False

def extract_pdf_text(pdf_path: Path, password: str) -> str:
    if HAS_PYMUPDF:
        return extract_with_pymupdf(pdf_path, password)
    return extract_with_pdfplumber(pdf_path, password)
```

---

### 2.2 🔴 No CAS-Specific Parsing Logic

**CASParser has:**
- `process_detailed_text()` - Full transaction history parsing
- `process_summary_text()` - Holdings-only parsing
- Multiple regex patterns for transaction extraction with fallback
- Header parsing for statement period
- Folio → Scheme → Transaction hierarchy construction

**PFAS lacks:**
- Dedicated CAS PDF parser (currently relies on Excel exports)
- Transaction extraction from PDF text
- Handling of both SUMMARY and DETAILED CAS formats

**Fix Required:** Implement `CASPDFParser` class with:
```python
class CASPDFParser:
    def parse(self, pdf_path: Path, password: str) -> CASData:
        text = self._extract_text(pdf_path, password)
        cas_type = self._detect_cas_type(text)

        if cas_type == CASFileType.DETAILED:
            return self._parse_detailed(text)
        else:
            return self._parse_summary(text)

    def _parse_detailed(self, text: str) -> CASData:
        # Parse header, folios, schemes, transactions
        pass

    def _parse_summary(self, text: str) -> CASData:
        # Parse holdings only
        pass
```

---

### 2.3 🟠 Missing NSDL CAS Support

**CASParser supports:**
- CAMS CAS
- KFintech CAS
- **NSDL CAS** (Demat + MF consolidated)
- **CDSL CAS**

**PFAS supports:**
- CAMS CAS (partial)
- KFintech CAS (partial)
- ❌ No NSDL/CDSL support

**Impact:** Cannot parse consolidated statements from depositories that include both stocks and mutual funds.

---

## 3. Capital Gains Calculation Gaps

### 3.1 🔴 No FIFO Unit Matching

**CASParser implements:**
```python
class FIFOUnits:
    """First-In-First-Out unit matching for capital gains."""

    def add_units(self, date, units, nav, value):
        self._queue.append(Transaction(date, units, nav, value))

    def remove_units(self, units, sale_date, sale_nav):
        """Match units FIFO style, compute gains."""
        gains = []
        while units > 0 and self._queue:
            purchase = self._queue[0]
            matched_units = min(units, purchase.units)
            gain = self._compute_gain(purchase, matched_units, sale_date, sale_nav)
            gains.append(gain)
            # ... handle partial matches
        return gains
```

**PFAS lacks:**
- No FIFO tracking for redemptions
- Relies on pre-computed gains from CG statements
- Cannot independently verify capital gains

**Fix Required:** Implement `FIFOTracker` class for each folio/scheme.

---

### 3.2 🔴 Incomplete Grandfathering Implementation

**CASParser implements (3 scenarios):**
```python
def cost_of_acquisition(purchase_date, purchase_value, sale_date, sale_value, fmv_31jan2018):
    if purchase_date < date(2018, 1, 31):
        if sale_date < date(2018, 4, 1):
            # Scenario 1: Sold before Budget 2018 effective
            return sale_value  # No tax
        else:
            # Scenario 2: Grandfathering applies
            return max(purchase_value, min(fmv_31jan2018, sale_value))
    else:
        # Scenario 3: No grandfathering
        return purchase_value
```

**PFAS implements:**
- Basic grandfathering check
- ❌ Missing pre-April-2018 sale handling
- ❌ No FMV lookup mechanism

**Fix Required:** Add complete grandfathering logic with FMV data.

---

### 3.3 🟠 Missing Cost Inflation Index for Debt Gains

**CASParser implements:**
```python
def indexed_cost(purchase_value, purchase_fy, sale_fy, cii_data):
    """Apply CII indexation for debt fund LTCG (pre-April 2023)."""
    index_ratio = cii_data[sale_fy] / cii_data[purchase_fy]
    return purchase_value * index_ratio
```

**PFAS lacks:** CII indexation calculation for historical debt fund gains.

---

## 4. Error Handling Gaps

### 4.1 🔴 No Hierarchical Exception System

**CASParser has:**
```python
class ParserException(Exception): pass
class HeaderParseError(ParserException): pass
class CASParseError(ParserException): pass
class IncorrectPasswordError(CASParseError): pass
class CASIntegrityError(ParserException): pass
class IncompleteCASError(ParserException): pass
class GainsError(ParserException): pass
```

**PFAS has:**
- Generic `Exception` usage
- No MF-specific exception hierarchy
- Error messages stored in `ParseResult.errors` list

**Fix Required:**
```python
# pfas/parsers/mf/exceptions.py
class MFParserError(Exception):
    """Base exception for MF parsing."""
    pass

class CASParseError(MFParserError):
    """Error parsing CAS file."""
    pass

class IncorrectPasswordError(CASParseError):
    """Wrong password for PDF."""
    pass

class IntegrityError(MFParserError):
    """Data integrity check failed."""
    pass

class IncompleteDataError(MFParserError):
    """CAS data incomplete for analysis."""
    pass

class GainsCalculationError(MFParserError):
    """Error computing capital gains."""
    pass
```

---

### 4.2 🟠 Missing Data Integrity Validation

**CASParser validates:**
- Opening balance + transactions = Closing balance
- Unit balance reconciliation per scheme
- `close_calculated` vs `close` mismatch detection

**PFAS lacks:**
- Balance reconciliation checks
- Transaction completeness validation
- Mismatch alerts

**Fix Required:**
```python
def validate_scheme_integrity(scheme: Scheme) -> List[str]:
    errors = []
    calculated_close = scheme.open
    for txn in scheme.transactions:
        calculated_close += txn.units

    if abs(calculated_close - scheme.close) > Decimal("0.001"):
        errors.append(
            f"Balance mismatch for {scheme.name}: "
            f"calculated={calculated_close}, stated={scheme.close}"
        )
    return errors
```

---

## 5. CLI & Output Gaps

### 5.1 🟠 Missing Schedule 112A Export

**CASParser provides:**
```bash
casparser file.pdf -p password --gains-112a 2024-25
# Generates: file-2024-25-gains-112a.csv
```

Output format matches ITR Schedule 112A requirements:
- ISIN
- Name of Share/Unit
- No. of shares/units sold
- Sale consideration
- Cost of acquisition
- LTCG

**PFAS lacks:** Direct ITR-compatible export format.

**Fix Required:** Add `--export-112a` option to CLI.

---

### 5.2 🟠 Missing JSON Output

**CASParser provides:**
```bash
casparser file.pdf -o output.json
# Full structured JSON with all parsed data
```

**PFAS provides:**
- Excel reports only
- No JSON export option

**Fix Required:** Add JSON export capability for API integration.

---

### 5.3 🟡 Missing Summary Table Output

**CASParser provides:**
```bash
casparser file.pdf -s
# Prints formatted portfolio summary table to console
```

**PFAS provides:**
- Reports generated to files only
- No console summary display

---

## 6. Feature Comparison Matrix

| Feature | CASParser | PFAS | Gap |
|---------|-----------|------|-----|
| **Input Formats** |
| PDF (CAMS) | ✅ Full | ⚠️ Partial | 🔴 |
| PDF (KFintech) | ✅ Full | ⚠️ Partial | 🔴 |
| PDF (NSDL) | ✅ Yes | ❌ No | 🟠 |
| Excel (CAMS) | ❌ No | ✅ Yes | - |
| Excel (KFintech) | ❌ No | ✅ Yes | - |
| **Parsing** |
| Transaction types | 16 | 6 | 🔴 |
| Statement period tracking | ✅ Yes | ❌ No | 🟠 |
| Balance reconciliation | ✅ Yes | ❌ No | 🟠 |
| ISIN/AMFI lookup | ✅ Yes | ⚠️ Basic | 🟡 |
| **Capital Gains** |
| FIFO matching | ✅ Yes | ❌ No | 🔴 |
| Grandfathering | ✅ Full | ⚠️ Partial | 🔴 |
| CII indexation | ✅ Yes | ❌ No | 🟠 |
| **Output** |
| JSON export | ✅ Yes | ❌ No | 🟠 |
| CSV export | ✅ Yes | ❌ No | 🟡 |
| Excel report | ❌ No | ✅ Yes | - |
| Schedule 112A | ✅ Yes | ❌ No | 🟠 |
| **Error Handling** |
| Exception hierarchy | ✅ Yes | ❌ No | 🔴 |
| Integrity validation | ✅ Yes | ❌ No | 🟠 |
| **Database** |
| SQLite storage | ❌ No | ✅ Yes | - |
| Idempotent ingestion | ❌ No | ✅ Yes | - |
| Historical tracking | ❌ No | ✅ Yes | - |

---

## 7. Recommended Fix Priority

### Phase 1: Critical (Week 1-2)
1. **Add missing transaction types** - Expand `TransactionType` enum
2. **Implement exception hierarchy** - Create `pfas/parsers/mf/exceptions.py`
3. **Improve PDF parsing** - Add PyMuPDF support, implement CAS-specific parser
4. **Add FIFO tracking** - Implement `FIFOUnitTracker` class

### Phase 2: High Priority (Week 3-4)
5. **Complete grandfathering** - Add all 3 scenarios with FMV lookup
6. **Add integrity validation** - Balance reconciliation checks
7. **Add Schedule 112A export** - ITR-compatible format
8. **Track statement periods** - From/to date validation

### Phase 3: Medium Priority (Week 5-6)
9. **Add JSON output** - Full structured export
10. **Add NSDL CAS support** - Demat + MF parsing
11. **Add CII indexation** - Historical debt fund gains
12. **Enhance ISIN/AMFI lookup** - Use casparser-isin database

### Phase 4: Low Priority (Future)
13. **Add CSV export** - Alternative to Excel
14. **Console summary display** - CLI enhancement
15. **CDSL CAS support** - Additional depository

---

## 8. Code Integration Opportunity

Consider using casparser as a dependency for PDF parsing:

```python
# Option 1: Use casparser for PDF, PFAS for Excel + DB
import casparser

def parse_cas_pdf(pdf_path: Path, password: str) -> CASData:
    """Use casparser for PDF extraction, then convert to PFAS models."""
    data = casparser.read_cas_pdf(str(pdf_path), password)
    return convert_to_pfas_format(data)

# Option 2: Port casparser regex patterns to PFAS
# Copy the transaction regex patterns and classification logic
```

**Recommendation:** Option 1 is faster to implement and leverages tested code.

---

## 9. Testing Gaps

**CASParser has:**
- Extensive pytest test suite
- Sample CAS files for testing
- Transaction classification tests
- Capital gains calculation tests

**PFAS should add:**
- CAS PDF parsing tests with sample files
- FIFO unit matching tests
- Grandfathering scenario tests
- Balance reconciliation tests
- Schedule 112A output validation

---

## Summary

| Priority | Count | Key Items |
|----------|-------|-----------|
| 🔴 Critical | 5 | Transaction types, FIFO, PDF parsing, exceptions, grandfathering |
| 🟠 High | 6 | Statement period, integrity validation, 112A export, CII |
| 🟡 Medium | 3 | JSON export, ISIN lookup, CSV export |
| 🟢 Low | 2 | Console summary, CDSL support |

**Total gaps identified: 16**

The most impactful fixes are improving PDF parsing and implementing FIFO-based capital gains calculation, as these enable independent verification of gains rather than relying solely on pre-computed values from RTA statements.
