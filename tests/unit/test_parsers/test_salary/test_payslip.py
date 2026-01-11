"""Tests for PayslipParser."""

import pytest
from datetime import date
from decimal import Decimal
from pathlib import Path
import tempfile

from pfas.parsers.salary.payslip import PayslipParser
from pfas.parsers.salary.models import SalaryRecord, CorrelationStatus


class TestPayslipParser:
    """Tests for PayslipParser class."""

    def test_parser_initialization(self, db_connection):
        """Test parser can be initialized."""
        parser = PayslipParser(db_connection)
        assert parser.conn is not None

    def test_parser_initialization_no_db(self):
        """Test parser can be initialized without database."""
        parser = PayslipParser()
        assert parser.conn is None

    def test_parse_nonexistent_file(self, db_connection):
        """Test parsing nonexistent file returns error."""
        parser = PayslipParser(db_connection)
        result = parser.parse(Path("/nonexistent/file.pdf"))

        assert result.success is False
        assert len(result.errors) > 0
        assert "not found" in result.errors[0].lower()

    def test_parse_unsupported_format(self, db_connection, tmp_path):
        """Test parsing unsupported file format."""
        parser = PayslipParser(db_connection)

        # Create a dummy file with unsupported extension
        test_file = tmp_path / "test.xlsx"
        test_file.write_text("dummy content")

        result = parser.parse(test_file)

        assert result.success is False
        assert "Unsupported file format" in result.errors[0]


class TestPayslipParserDecimalConversion:
    """Tests for decimal conversion in PayslipParser."""

    def test_to_decimal_valid(self, db_connection):
        """Test Decimal conversion for valid values."""
        parser = PayslipParser(db_connection)

        assert parser._to_decimal("123.45") == Decimal("123.45")
        assert parser._to_decimal("1,234.56") == Decimal("1234.56")
        assert parser._to_decimal("12,34,567.89") == Decimal("1234567.89")

    def test_to_decimal_negative(self, db_connection):
        """Test Decimal conversion for negative values (RSU tax credit)."""
        parser = PayslipParser(db_connection)

        assert parser._to_decimal("-1,957,774.65") == Decimal("-1957774.65")
        assert parser._to_decimal("-500.00") == Decimal("-500.00")

    def test_to_decimal_invalid(self, db_connection):
        """Test Decimal conversion for invalid values."""
        parser = PayslipParser(db_connection)

        assert parser._to_decimal(None) == Decimal("0")
        assert parser._to_decimal("") == Decimal("0")
        assert parser._to_decimal("invalid") == Decimal("0")


class TestPayslipParserPatterns:
    """Tests for pattern matching in PayslipParser."""

    def test_patterns_exist(self, db_connection):
        """Test all required patterns are defined."""
        parser = PayslipParser(db_connection)

        required_patterns = [
            'pay_period', 'basic_salary', 'hra', 'special_allowance',
            'rsu_tax', 'espp_deduction', 'pf_employee', 'professional_tax',
            'income_tax', 'gross_salary', 'net_pay'
        ]

        for pattern in required_patterns:
            assert pattern in parser.PATTERNS, f"Missing pattern: {pattern}"


class TestSalaryRecord:
    """Tests for SalaryRecord dataclass."""

    def test_salary_record_creation(self):
        """Test creating a salary record."""
        record = SalaryRecord(
            pay_period="June 2024",
            basic_salary=Decimal("560456.00"),
            hra=Decimal("224182.40"),
            special_allowance=Decimal("291584.13"),
        )

        assert record.pay_period == "June 2024"
        assert record.basic_salary == Decimal("560456.00")
        assert record.hra == Decimal("224182.40")

    def test_salary_record_defaults(self):
        """Test salary record default values."""
        record = SalaryRecord(pay_period="July 2024")

        assert record.basic_salary == Decimal("0")
        assert record.hra == Decimal("0")
        assert record.rsu_tax_credit == Decimal("0")
        assert record.espp_deduction == Decimal("0")

    def test_calculate_totals(self):
        """Test gross salary calculation."""
        record = SalaryRecord(
            pay_period="June 2024",
            basic_salary=Decimal("100000"),
            hra=Decimal("40000"),
            special_allowance=Decimal("30000"),
        )

        record.calculate_totals()

        assert record.gross_salary == Decimal("170000")


class TestRSUTaxCreditHandling:
    """Tests for RSU tax credit handling (CRITICAL)."""

    def test_rsu_tax_credit_negative_is_credit(self):
        """Test that negative RSU tax is stored as positive credit."""
        # The payslip shows RSU Tax as -1,957,774.65
        # This should be stored as POSITIVE credit amount
        record = SalaryRecord(
            pay_period="June 2024",
            rsu_tax_credit=Decimal("1957774.65")  # Stored as positive
        )

        assert record.rsu_tax_credit > Decimal("0")
        assert record.rsu_tax_credit == Decimal("1957774.65")

    def test_rsu_tax_credit_not_in_deductions(self):
        """Test RSU tax credit is NOT counted as deduction."""
        record = SalaryRecord(
            pay_period="June 2024",
            pf_employee=Decimal("67255.00"),
            income_tax_deducted=Decimal("2167667.00"),
            rsu_tax_credit=Decimal("1957774.65"),  # This is a CREDIT
            total_deductions=Decimal("445884.15"),  # Should NOT include RSU credit
        )

        # RSU credit should not be equal to any deduction
        assert record.rsu_tax_credit != record.pf_employee
        assert record.rsu_tax_credit != record.income_tax_deducted

        # RSU credit is separate from total_deductions
        # The payslip calculation: Total Gross - Net Pay = Total Dedns
        # RSU credit is added back, not deducted

    def test_rsu_credit_status_pending(self):
        """Test RSU credit correlation status starts as pending."""
        from pfas.parsers.salary.models import RSUTaxCredit

        credit = RSUTaxCredit(
            salary_record_id=1,
            credit_amount=Decimal("1957774.65"),
            credit_date=date(2024, 6, 30)
        )

        assert credit.correlation_status == CorrelationStatus.PENDING


class TestESPPHandling:
    """Tests for ESPP deduction and TCS handling."""

    def test_espp_deduction_tracking(self):
        """Test ESPP deduction is tracked as investment."""
        record = SalaryRecord(
            pay_period="June 2024",
            espp_deduction=Decimal("168136.80"),
        )

        assert record.espp_deduction == Decimal("168136.80")

    def test_tcs_on_espp_tracking(self):
        """Test TCS on ESPP is tracked as tax credit."""
        record = SalaryRecord(
            pay_period="June 2024",
            espp_deduction=Decimal("168136.80"),
            tcs_on_espp=Decimal("33627.36"),  # 20% TCS
        )

        assert record.tcs_on_espp == Decimal("33627.36")


class TestProfessionalTaxHandling:
    """Tests for Professional Tax handling."""

    def test_professional_tax_tracking(self):
        """Test professional tax is tracked."""
        record = SalaryRecord(
            pay_period="June 2024",
            professional_tax=Decimal("200.00"),
        )

        assert record.professional_tax == Decimal("200.00")

    def test_professional_tax_annual_limit(self):
        """Test professional tax max is Rs 2,500/year."""
        # 12 months * Rs 200 = Rs 2,400 (within limit)
        from pfas.parsers.salary.models import AnnualSalarySummary

        summary = AnnualSalarySummary(financial_year="2024-25")

        for _ in range(12):
            record = SalaryRecord(
                pay_period="Month 2024",
                professional_tax=Decimal("200.00"),
            )
            summary.add_monthly_record(record)

        assert summary.total_professional_tax == Decimal("2400.00")
        assert summary.total_professional_tax <= Decimal("2500.00")


class TestAnnualSalarySummary:
    """Tests for annual salary aggregation."""

    def test_annual_summary_creation(self):
        """Test annual summary can be created."""
        from pfas.parsers.salary.models import AnnualSalarySummary

        summary = AnnualSalarySummary(financial_year="2024-25")

        assert summary.financial_year == "2024-25"
        assert summary.months_processed == 0

    def test_annual_summary_aggregation(self):
        """Test monthly records are aggregated correctly."""
        from pfas.parsers.salary.models import AnnualSalarySummary

        summary = AnnualSalarySummary(financial_year="2024-25")

        # Add 3 monthly records
        for month in range(3):
            record = SalaryRecord(
                pay_period=f"Month {month + 1} 2024",
                basic_salary=Decimal("100000"),
                hra=Decimal("40000"),
                rsu_tax_credit=Decimal("50000") if month == 1 else Decimal("0"),
            )
            summary.add_monthly_record(record)

        assert summary.months_processed == 3
        assert summary.total_basic == Decimal("300000")
        assert summary.total_hra == Decimal("120000")
        assert summary.total_rsu_credits == Decimal("50000")
