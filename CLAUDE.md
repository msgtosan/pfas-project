# PFAS Project - Claude Code Instructions

## Project Overview
Personal Financial Accounting System for Indian Tax Residents
- 18 Asset Classes
- Phase 1: Indian Assets + Salary/Form16
- Phase 2: Foreign Assets + DTAA + ITR-2 Export

## Key Technical Decisions
1. RSU Tax Credit: NEGATIVE deduction in payslip = credit when shares vest
2. Currency: SBI TT Buying Rate for USD→INR
3. LTCG: 12 months (Indian equity), 24 months (foreign/unlisted)
4. Database: SQLCipher (AES-256 encrypted SQLite)

## Testing Requirements
- Every module must have unit tests
- Test coverage target: 90%
- Integration tests after each module
- Use pytest with fixtures

## Code Standards
- Type hints required
- Docstrings for all public functions
- Use dataclasses for models
- Follow PEP 8

## File Locations
- Source: src/pfas/
- Tests: tests/
- Test Data: tests/fixtures/
- Documentation: docs/
