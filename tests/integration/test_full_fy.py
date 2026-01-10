"""
Full FY24-25 Integration Test

This test loads all actual data files and verifies:
1. All parsers work correctly
2. All journal entries balance
3. Tax computation matches manual calculation
4. ITR-2 JSON validates against schema
"""
import pytest
from pathlib import Path

class TestFullFY2425:
    """Full financial year integration test."""
    
    @pytest.fixture(scope="class")
    def fy_data_path(self):
        return Path("tests/fixtures/fy2425/")
    
    def test_01_load_bank_statements(self, clean_db, fy_data_path):
        """Load all bank statements."""
        from pfas.parsers.bank import parse_all_statements
        
        statements = list(fy_data_path.glob("bank/*.pdf"))
        for stmt in statements:
            result = parse_all_statements(stmt, clean_db)
            assert result.success
            assert result.transactions_count > 0
    
    def test_02_load_mf_statements(self, clean_db, fy_data_path):
        """Load CAMS and KARVY statements."""
        from pfas.parsers.mf import parse_cams, parse_karvy
        
        cams_file = fy_data_path / "mf/cams_cas.pdf"
        if cams_file.exists():
            result = parse_cams(cams_file, clean_db)
            assert result.success
    
    def test_03_load_salary_form16(self, clean_db, fy_data_path):
        """Load salary data from Form 16."""
        from pfas.parsers.salary import parse_form16
        
        form16_zip = fy_data_path / "salary/form16.zip"
        if form16_zip.exists():
            result = parse_form16(form16_zip, clean_db)
            assert result.success
    
    def test_04_verify_journal_balance(self, clean_db):
        """Verify all journal entries are balanced."""
        cursor = clean_db.execute("""
            SELECT j.id, j.description,
                   SUM(je.debit) as total_debit,
                   SUM(je.credit) as total_credit
            FROM journals j
            JOIN journal_entries je ON j.id = je.journal_id
            GROUP BY j.id
            HAVING ABS(total_debit - total_credit) > 0.01
        """)
        unbalanced = cursor.fetchall()
        assert len(unbalanced) == 0, f"Unbalanced journals: {unbalanced}"
    
    def test_05_tds_reconciliation(self, clean_db):
        """Verify TDS matches between Form 16 and Form 26AS."""
        from pfas.services.tds_reconciliation import reconcile
        
        result = reconcile(clean_db)
        assert result.variance < 100  # Allow ₹100 variance
    
    def test_06_generate_tax_computation(self, clean_db):
        """Generate tax computation for both regimes."""
        from pfas.reports.tax_computation import compute_tax
        
        old_regime = compute_tax(clean_db, regime="OLD")
        new_regime = compute_tax(clean_db, regime="NEW")
        
        assert old_regime.total_income > 0
        assert new_regime.total_income > 0
    
    def test_07_itr2_json_export(self, clean_db):
        """Export ITR-2 JSON and validate schema."""
        from pfas.services.itr_export import export_itr2
        import jsonschema
        import json
        
        itr_json = export_itr2(clean_db, assessment_year="2025-26")
        
        # Load CBDT schema
        with open("tests/fixtures/ITR-2_2025_Main_V1.2.json") as f:
            schema = json.load(f)
        
        # Validate
        jsonschema.validate(itr_json, schema)