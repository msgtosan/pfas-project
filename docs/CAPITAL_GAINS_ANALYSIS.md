# Mutual Fund Capital Gains Analysis: PFAS vs casparser

## Executive Summary

This document compares capital gains calculations between:
1. **PFAS FIFO Engine**: First-In-First-Out based calculation from CAS transactions
2. **casparser**: Python library that extracts capital gains from CAS PDFs

### Key Findings for FY 2024-25

| Metric | PFAS | casparser | Difference |
|--------|------|-----------|------------|
| LTCG | Rs. 13,66,830 | Rs. 13,37,221 | +Rs. 29,610 (+2.2%) |
| LTCG (Taxable) | - | Rs. 12,45,839 | - |
| STCG | Rs. 6,07,868 | Rs. 9,38,682 | -Rs. 3,30,814 (-35%) |

## Analysis Methodology

### PFAS Approach (FIFO-based)
- Extracts all transactions from CAS PDF
- Builds purchase lots for each scheme/folio
- Matches redemptions using First-In-First-Out
- Calculates holding period for LTCG/STCG classification
- Applies grandfathering for pre-31-Jan-2018 purchases

### casparser Approach
- Extracts capital gains data directly from CAS statements
- Uses RTA-provided capital gains calculations
- Relies on RTA's transaction matching logic

## Detailed Comparison Results

### Historical Capital Gains Comparison

| FY | PFAS LTCG | casparser LTCG | Diff | PFAS STCG | casparser STCG | Diff |
|----|-----------|----------------|------|-----------|----------------|------|
| 2018-19 | 14,61,381 | 15,51,181 | -89,800 | 2,23,630 | 6,71,074 | -4,47,443 |
| 2019-20 | 8,05,191 | 8,72,815 | -67,623 | 2,70,190 | 2,88,170 | -17,980 |
| 2020-21 | 20,48,465 | 26,71,578 | -6,23,113 | 4,67,051 | 4,98,425 | -31,374 |
| 2021-22 | 9,70,989 | 7,97,692 | +1,73,297 | 1,42,470 | 1,51,412 | -8,942 |
| 2022-23 | 0 | 4,572 | -4,572 | 27,491 | 53,311 | -25,820 |
| 2023-24 | 27,50,700 | 76,83,747 | -49,33,047 | 67,192 | 4,54,163 | -3,86,971 |
| 2024-25 | 13,66,830 | 13,37,221 | +29,610 | 6,07,868 | 9,38,682 | -3,30,814 |
| 2025-26 | 5,56,820 | 26,76,384 | -21,19,565 | 6,62,975 | 2,86,198 | +3,76,777 |

### Root Cause Analysis

#### 1. Transaction Type Classification
**Issue**: PFAS may misclassify transaction types from CAS descriptions.

**Examples**:
- "Switch In - Merger" vs regular "Switch In"
- "Systematic Investment" vs "New Purchase"
- "Dividend Reinvestment" handling

**Impact**: Incorrect purchase/redemption matching

#### 2. Switch Transaction Handling
**Issue**: Switch-out followed by switch-in creates two transactions that should be treated as one taxable event.

**PFAS behavior**: Treats switch-out as redemption (triggers CG)
**RTA behavior**: May calculate CG differently, especially for scheme mergers

#### 3. Holding Period Calculation
**Issue**: PFAS uses transaction date for holding period, while RTA may use settlement date.

**For LTCG classification**:
- Equity funds: >12 months
- Debt funds: Previously >36 months (with indexation), now at slab rate

#### 4. Grandfathering Implementation
**Issue**: FMV on 31-Jan-2018 may not be available for all schemes.

**PFAS behavior**: Falls back to purchase price if FMV unavailable
**casparser**: Uses RTA-provided grandfathered values

#### 5. Debt Fund Tax Changes (April 2023)
**Issue**: Post-April 2023, debt fund gains are taxed at slab rate regardless of holding period.

**Impact**: LTCG/STCG classification for debt funds changed

## Top Schemes Contributing to FY 2024-25 Gains (PFAS)

| Scheme | LTCG | STCG |
|--------|------|------|
| HDFC Short Term Debt Fund - Direct | 7,48,384 | 0 |
| Kotak Corporate Bond Fund Direct | 4,06,059 | 23,322 |
| SBI Constant Maturity 10-Year Gilt Fund | 1,82,781 | 50,779 |
| Mirae Asset Liquid Fund | 0 | 1,60,917 |
| Income Distribution and Capital Withdrawal | 0 | 1,37,661 |
| ICICI Prudential Liquid Fund - Direct | 0 | 82,822 |
| Parag Parikh Liquid Fund Direct | 0 | 64,699 |

## Recommendations

### 1. Use RTA Capital Gains Statements for Tax Filing
**Rationale**: RTAs (CAMS, Karvy/KFintech) provide official capital gains statements that are accepted by Income Tax department.

**Action**:
- Download capital gains statement from CAMS/KFintech portal
- Use the "Capital Gains" section in CAS PDFs
- Cross-verify with Form 26AS

### 2. Implement Dual Calculation Approach
**Rationale**: FIFO calculation is useful for planning and verification, but RTA statements are authoritative.

**Architecture**:
```
CAS PDF
   ├── Transaction Extraction → FIFO Engine → Estimated CG (for planning)
   └── Capital Gains Section → RTA CG → Authoritative CG (for filing)
```

### 3. Enhance PFAS FIFO Engine

#### Short-term Improvements:
1. **Parse Capital Gains section** from CAS PDF directly
2. **Add reconciliation report** comparing FIFO vs RTA values
3. **Handle scheme mergers** properly in switch transactions

#### Long-term Improvements:
1. **NAV history integration** for grandfathering calculations
2. **Debt fund tax rule** implementation (pre/post April 2023)
3. **Exit load and STT** consideration in net gains

### 4. User Workflow for Capital Gains

```
Step 1: Import CAS PDF
   ↓
Step 2: PFAS extracts transactions and calculates FIFO-based CG
   ↓
Step 3: User downloads RTA Capital Gains Statement
   ↓
Step 4: PFAS reconciles FIFO vs RTA and highlights differences
   ↓
Step 5: User uses RTA values for ITR filing
   ↓
Step 6: PFAS stores both for audit trail
```

### 5. Database Schema Updates

Add tables for:
```sql
-- Store RTA-provided capital gains
CREATE TABLE mf_rta_capital_gains (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    financial_year TEXT NOT NULL,
    rta TEXT NOT NULL,  -- 'CAMS' or 'KARVY'
    scheme_name TEXT NOT NULL,
    folio TEXT,
    ltcg DECIMAL(15,2),
    ltcg_taxable DECIMAL(15,2),
    stcg DECIMAL(15,2),
    grandfathered_value DECIMAL(15,2),
    source_file TEXT,
    imported_at TIMESTAMP
);

-- Reconciliation status
CREATE TABLE mf_cg_reconciliation (
    user_id INTEGER NOT NULL,
    financial_year TEXT NOT NULL,
    fifo_ltcg DECIMAL(15,2),
    fifo_stcg DECIMAL(15,2),
    rta_ltcg DECIMAL(15,2),
    rta_stcg DECIMAL(15,2),
    ltcg_difference DECIMAL(15,2),
    stcg_difference DECIMAL(15,2),
    reconciliation_status TEXT,  -- 'MATCHED', 'MINOR_DIFF', 'MAJOR_DIFF'
    notes TEXT
);
```

## Conclusion

1. **For tax filing**: Use RTA-provided capital gains statements
2. **For planning**: PFAS FIFO engine provides useful estimates
3. **For verification**: Compare FIFO calculations with RTA statements
4. **Key enhancement**: Parse capital gains section from CAS PDFs directly

The ~35% difference in STCG for FY 2024-25 suggests that PFAS needs to:
- Better handle liquid/money market fund transactions
- Properly classify short-term vs long-term for debt funds
- Consider switch transactions as single taxable events where applicable

## Test Files Used

- **CAS PDF**: `/home/sshankar/CASTest/usr-inbox/Sanjay_CAS.pdf`
- **casparser output**: `/home/sshankar/CASTest/sanjay_cas_cg_test.csv`
- **Comparison script**: `tests/integration/test_cas_capital_gains_comparison.py`
