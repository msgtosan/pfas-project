"""Tests for ITR-2 JSON Exporter."""

import pytest
import json
import tempfile
from datetime import date
from decimal import Decimal

from pfas.services.itr.itr2_exporter import (
    ITR2Exporter,
    ITR2Data,
    PersonalInfo,
    SalaryIncome,
    CapitalGains,
    OtherIncome,
    Chapter6ADeductions,
    TaxRelief,
    TDSDetails
)


class TestITR2Exporter:
    """Tests for ITR2Exporter class."""

    def test_exporter_initialization(self, db_connection):
        """Test exporter can be initialized."""
        exporter = ITR2Exporter(db_connection)
        assert exporter.conn is not None
        assert exporter.rate_provider is not None
        assert exporter.schedule_fa_gen is not None

    def test_old_regime_slabs(self, db_connection):
        """Test old regime tax slabs."""
        exporter = ITR2Exporter(db_connection)

        assert len(exporter.OLD_REGIME_SLABS) == 4
        # 0-2.5L: 0%
        assert exporter.OLD_REGIME_SLABS[0] == (Decimal("250000"), Decimal("0"))
        # 2.5-5L: 5%
        assert exporter.OLD_REGIME_SLABS[1] == (Decimal("500000"), Decimal("0.05"))

    def test_new_regime_slabs(self, db_connection):
        """Test new regime tax slabs."""
        exporter = ITR2Exporter(db_connection)

        assert len(exporter.NEW_REGIME_SLABS) == 6
        # 0-3L: 0%
        assert exporter.NEW_REGIME_SLABS[0] == (Decimal("300000"), Decimal("0"))

    def test_cess_rate(self, db_connection):
        """Test cess rate."""
        exporter = ITR2Exporter(db_connection)
        assert exporter.CESS_RATE == Decimal("0.04")


class TestPersonalInfo:
    """Tests for PersonalInfo dataclass."""

    def test_personal_info_creation(self):
        """Test creating personal info."""
        info = PersonalInfo(
            pan="ABCDE1234F",
            name="Test User",
            father_name="Test Father",
            dob=date(1990, 1, 1),
            email="test@example.com",
            mobile="9876543210"
        )

        assert info.pan == "ABCDE1234F"
        assert info.name == "Test User"
        assert info.residential_status == "RES"
        assert info.employer_category == "OTH"

    def test_personal_info_defaults(self):
        """Test personal info defaults."""
        info = PersonalInfo(pan="ABCDE1234F", name="Test")

        assert info.father_name == ""
        assert info.address == ""
        assert info.residential_status == "RES"


class TestSalaryIncome:
    """Tests for SalaryIncome dataclass."""

    def test_salary_income_creation(self):
        """Test creating salary income."""
        salary = SalaryIncome(
            employer_name="Test Corp",
            employer_tan="DELC12345E",
            gross_salary=Decimal("2000000"),
            perquisites=Decimal("500000")
        )

        assert salary.gross_salary == Decimal("2000000")
        assert salary.perquisites == Decimal("500000")

    def test_salary_calculate(self):
        """Test salary calculation."""
        salary = SalaryIncome(
            gross_salary=Decimal("2000000"),
            perquisites=Decimal("500000"),
            profits_in_lieu=Decimal("0"),
            professional_tax=Decimal("2500")
        )

        salary.calculate()

        assert salary.total_salary == Decimal("2500000")
        # Net = 2500000 - 50000 (std ded) - 2500 (PT) = 2447500
        assert salary.net_salary == Decimal("2447500")

    def test_salary_standard_deduction(self):
        """Test standard deduction default."""
        salary = SalaryIncome()
        assert salary.standard_deduction == Decimal("50000")


class TestCapitalGains:
    """Tests for CapitalGains dataclass."""

    def test_capital_gains_creation(self):
        """Test creating capital gains."""
        cg = CapitalGains(
            stcg_111a=Decimal("100000"),
            ltcg_112a_full=Decimal("200000")
        )

        assert cg.stcg_111a == Decimal("100000")
        assert cg.ltcg_112a_full == Decimal("200000")

    def test_capital_gains_calculate(self):
        """Test capital gains calculation."""
        cg = CapitalGains(
            stcg_111a=Decimal("100000"),
            ltcg_112a_full=Decimal("200000"),
            foreign_stcg=Decimal("50000"),
            foreign_ltcg=Decimal("75000")
        )

        cg.calculate()

        # LTCG taxable = 200000 - 125000 (exemption) = 75000
        assert cg.ltcg_112a_taxable == Decimal("75000")
        # Total STCG = 100000 + 50000 = 150000
        assert cg.total_stcg == Decimal("150000")
        # Total LTCG = 75000 + 75000 = 150000
        assert cg.total_ltcg == Decimal("150000")

    def test_ltcg_exemption_default(self):
        """Test LTCG exemption default."""
        cg = CapitalGains()
        assert cg.ltcg_112a_exempt == Decimal("125000")


class TestOtherIncome:
    """Tests for OtherIncome dataclass."""

    def test_other_income_creation(self):
        """Test creating other income."""
        income = OtherIncome(
            dividend_income=Decimal("50000"),
            foreign_dividend=Decimal("25000"),
            interest_savings=Decimal("10000")
        )

        assert income.dividend_income == Decimal("50000")
        assert income.foreign_dividend == Decimal("25000")

    def test_other_income_calculate(self):
        """Test other income calculation."""
        income = OtherIncome(
            dividend_income=Decimal("50000"),
            foreign_dividend=Decimal("25000"),
            interest_savings=Decimal("10000")
        )

        income.calculate()

        assert income.total_other_income == Decimal("85000")


class TestChapter6ADeductions:
    """Tests for Chapter6ADeductions dataclass."""

    def test_deductions_creation(self):
        """Test creating deductions."""
        deductions = Chapter6ADeductions(
            section_80c=Decimal("150000"),
            section_80d=Decimal("25000"),
            section_80ccd_1b=Decimal("50000")
        )

        assert deductions.section_80c == Decimal("150000")
        assert deductions.section_80d == Decimal("25000")

    def test_deductions_calculate(self):
        """Test deductions calculation."""
        deductions = Chapter6ADeductions(
            section_80c=Decimal("150000"),
            section_80d=Decimal("25000"),
            section_80ccd_1b=Decimal("50000"),
            section_80e=Decimal("30000")
        )

        deductions.calculate()

        # 80C capped at 1.5L + 80D 25K + 80CCD(1B) 50K + 80E 30K = 255K
        assert deductions.total_deductions == Decimal("255000")

    def test_deductions_80c_limit(self):
        """Test 80C limit enforcement."""
        deductions = Chapter6ADeductions(
            section_80c=Decimal("200000"),  # Exceeds 1.5L limit
            section_80ccc=Decimal("50000"),
            section_80ccd_1=Decimal("50000")
        )

        deductions.calculate()

        # 80C total should be capped at 1.5L
        # Total = 150000 (capped)
        assert deductions.total_deductions == Decimal("150000")


class TestTaxRelief:
    """Tests for TaxRelief dataclass."""

    def test_relief_creation(self):
        """Test creating tax relief."""
        relief = TaxRelief(
            section_89=Decimal("10000"),
            section_90=Decimal("25000")
        )

        assert relief.section_89 == Decimal("10000")
        assert relief.section_90 == Decimal("25000")

    def test_relief_calculate(self):
        """Test relief calculation."""
        relief = TaxRelief(
            section_89=Decimal("10000"),
            section_90=Decimal("25000"),
            section_91=Decimal("5000")
        )

        relief.calculate()

        assert relief.total_relief == Decimal("40000")


class TestTDSDetails:
    """Tests for TDSDetails dataclass."""

    def test_tds_creation(self):
        """Test creating TDS details."""
        tds = TDSDetails(
            tan="DELC12345E",
            deductor_name="Test Corp",
            income_type="Salary",
            income_amount=Decimal("2500000"),
            tds_deducted=Decimal("400000")
        )

        assert tds.tan == "DELC12345E"
        assert tds.tds_deducted == Decimal("400000")


class TestITR2Data:
    """Tests for ITR2Data dataclass."""

    def test_itr2_data_creation(self):
        """Test creating ITR2 data."""
        itr_data = ITR2Data(
            financial_year="2024-25",
            assessment_year="2025-26",
            personal_info=PersonalInfo(pan="TEST12345F", name="Test User")
        )

        assert itr_data.financial_year == "2024-25"
        assert itr_data.form_name == "ITR2"
        assert itr_data.gross_total_income == Decimal("0")
        assert itr_data.total_income == Decimal("0")

    def test_itr2_data_defaults(self):
        """Test ITR2 data defaults."""
        itr_data = ITR2Data(
            financial_year="2024-25",
            assessment_year="2025-26",
            personal_info=PersonalInfo(pan="TEST12345F", name="Test User")
        )

        assert isinstance(itr_data.personal_info, PersonalInfo)
        assert isinstance(itr_data.salary_income, SalaryIncome)
        assert isinstance(itr_data.capital_gains, CapitalGains)
        assert isinstance(itr_data.other_income, OtherIncome)
        assert isinstance(itr_data.deductions, Chapter6ADeductions)
        assert isinstance(itr_data.tax_relief, TaxRelief)


class TestSlabTaxCalculation:
    """Tests for slab-based tax calculation."""

    def test_calculate_slab_tax_below_exemption(self, db_connection):
        """Test tax calculation below exemption limit."""
        exporter = ITR2Exporter(db_connection)

        tax = exporter._calculate_slab_tax(
            income=Decimal("200000"),
            slabs=exporter.NEW_REGIME_SLABS
        )

        assert tax == Decimal("0")

    def test_calculate_slab_tax_one_slab(self, db_connection):
        """Test tax in single slab."""
        exporter = ITR2Exporter(db_connection)

        # 4L income in new regime
        # 0-3L: 0 tax
        # 3-4L: 1L × 5% = 5000
        tax = exporter._calculate_slab_tax(
            income=Decimal("400000"),
            slabs=exporter.NEW_REGIME_SLABS
        )

        assert tax == Decimal("5000")

    def test_calculate_slab_tax_multiple_slabs(self, db_connection):
        """Test tax across multiple slabs."""
        exporter = ITR2Exporter(db_connection)

        # 10L income in new regime
        # 0-3L: 0
        # 3-6L: 3L × 5% = 15000
        # 6-9L: 3L × 10% = 30000
        # 9-10L: 1L × 15% = 15000
        # Total = 60000
        tax = exporter._calculate_slab_tax(
            income=Decimal("1000000"),
            slabs=exporter.NEW_REGIME_SLABS
        )

        assert tax == Decimal("60000")


class TestSurchargeCalculation:
    """Tests for surcharge calculation."""

    def test_no_surcharge_below_50l(self, db_connection):
        """Test no surcharge below 50L."""
        exporter = ITR2Exporter(db_connection)

        surcharge = exporter._calculate_surcharge(
            tax=Decimal("100000"),
            total_income=Decimal("4000000")  # 40L
        )

        assert surcharge == Decimal("0")

    def test_surcharge_50l_to_1cr(self, db_connection):
        """Test 10% surcharge for 50L-1Cr."""
        exporter = ITR2Exporter(db_connection)

        surcharge = exporter._calculate_surcharge(
            tax=Decimal("200000"),
            total_income=Decimal("7000000")  # 70L
        )

        assert surcharge == Decimal("20000")  # 10% of 2L


class TestExportJSON:
    """Tests for JSON export."""

    def test_export_json_creates_file(self, db_connection, sample_user):
        """Test JSON export creates file."""
        from pfas.services.currency import SBITTRateProvider

        # Add required exchange rate for FY end
        rate_provider = SBITTRateProvider(db_connection)
        rate_provider.add_rate(date(2025, 3, 31), "USD", "INR", Decimal("84.00"), "SBI")

        exporter = ITR2Exporter(db_connection)

        personal_info = PersonalInfo(
            pan="ABCDE1234F",
            name="Test User"
        )

        itr_data = exporter.generate(
            user_id=sample_user["id"],
            financial_year="2024-25",
            personal_info=personal_info
        )

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            output_path = exporter.export_json(itr_data, f.name)

        assert output_path.endswith('.json')

        # Verify file content
        with open(output_path, 'r') as f:
            data = json.load(f)
            assert data['financial_year'] == "2024-25"
            assert data['form_name'] == "ITR2"

    def test_to_json_dict(self, db_connection):
        """Test JSON dict conversion."""
        exporter = ITR2Exporter(db_connection)

        itr_data = ITR2Data(
            financial_year="2024-25",
            assessment_year="2025-26",
            personal_info=PersonalInfo(pan="TEST12345F", name="Test User")
        )
        itr_data.gross_total_income = Decimal("1000000")

        json_dict = exporter._to_json_dict(itr_data)

        assert json_dict['financial_year'] == "2024-25"
        assert json_dict['gross_total_income'] == 1000000.0


class TestGenerateITR2:
    """Tests for ITR2 generation."""

    def test_generate_empty(self, db_connection, sample_user):
        """Test generating ITR2 with no income."""
        from pfas.services.currency import SBITTRateProvider

        # Add required exchange rate for FY end
        rate_provider = SBITTRateProvider(db_connection)
        rate_provider.add_rate(date(2025, 3, 31), "USD", "INR", Decimal("84.00"), "SBI")

        exporter = ITR2Exporter(db_connection)

        personal_info = PersonalInfo(
            pan="ABCDE1234F",
            name="Test User"
        )

        itr_data = exporter.generate(
            user_id=sample_user["id"],
            financial_year="2024-25",
            personal_info=personal_info
        )

        assert itr_data.financial_year == "2024-25"
        assert itr_data.assessment_year == "2025-26"
        assert itr_data.personal_info.pan == "ABCDE1234F"

    def test_assessment_year_calculation(self, db_connection, sample_user):
        """Test assessment year is calculated correctly."""
        from pfas.services.currency import SBITTRateProvider

        # Add required exchange rate for FY end
        rate_provider = SBITTRateProvider(db_connection)
        rate_provider.add_rate(date(2024, 3, 31), "USD", "INR", Decimal("83.50"), "SBI")

        exporter = ITR2Exporter(db_connection)

        personal_info = PersonalInfo(pan="TEST", name="Test")

        itr_data = exporter.generate(
            user_id=sample_user["id"],
            financial_year="2023-24",
            personal_info=personal_info
        )

        assert itr_data.assessment_year == "2024-25"
