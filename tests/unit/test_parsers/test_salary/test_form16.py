"""Tests for Form 16 Parser."""

import pytest
from datetime import date
from decimal import Decimal
from pathlib import Path

from pfas.parsers.salary.form16 import Form16Parser
from pfas.parsers.salary.models import Form16Record


class TestForm16Parser:
    """Tests for Form16Parser class."""

    def test_parser_initialization(self, db_connection):
        """Test parser can be initialized."""
        parser = Form16Parser(db_connection)
        assert parser.conn is not None

    def test_parser_initialization_no_db(self):
        """Test parser can be initialized without database."""
        parser = Form16Parser()
        assert parser.conn is None

    def test_parse_nonexistent_file(self, db_connection):
        """Test parsing nonexistent file returns error."""
        parser = Form16Parser(db_connection)
        result = parser.parse(Path("/nonexistent/file.zip"))

        assert result.success is False
        assert len(result.errors) > 0
        assert "not found" in result.errors[0].lower()


class TestForm16ParserDecimalConversion:
    """Tests for decimal conversion in Form16Parser."""

    def test_to_decimal_valid(self, db_connection):
        """Test Decimal conversion for valid values."""
        parser = Form16Parser(db_connection)

        assert parser._to_decimal("18,807,413.00") == Decimal("18807413.00")
        assert parser._to_decimal("75,000") == Decimal("75000")
        assert parser._to_decimal("292,277.80") == Decimal("292277.80")

    def test_to_decimal_invalid(self, db_connection):
        """Test Decimal conversion for invalid values."""
        parser = Form16Parser(db_connection)

        assert parser._to_decimal(None) == Decimal("0")
        assert parser._to_decimal("") == Decimal("0")


class TestForm16Record:
    """Tests for Form16Record dataclass."""

    def test_form16_record_creation(self):
        """Test creating a Form 16 record."""
        record = Form16Record(
            assessment_year="2025-26",
            employer_tan="BLRQ12345A",
            employee_pan="AAPPS0793R",
        )

        assert record.assessment_year == "2025-26"
        assert record.employer_tan == "BLRQ12345A"
        assert record.employee_pan == "AAPPS0793R"

    def test_form16_record_defaults(self):
        """Test Form 16 record default values."""
        record = Form16Record(assessment_year="2025-26")

        assert record.q1_tds == Decimal("0")
        assert record.q2_tds == Decimal("0")
        assert record.q3_tds == Decimal("0")
        assert record.q4_tds == Decimal("0")
        assert record.total_tds == Decimal("0")
        assert record.salary_17_1 == Decimal("0")
        assert record.perquisites_17_2 == Decimal("0")

    def test_calculate_total_tds(self):
        """Test total TDS calculation from quarterly values."""
        record = Form16Record(
            assessment_year="2025-26",
            q1_tds=Decimal("3500000"),
            q2_tds=Decimal("3200000"),
            q3_tds=Decimal("3100000"),
            q4_tds=Decimal("3415375"),
        )

        record.calculate_total_tds()

        assert record.total_tds == Decimal("13215375")

    def test_calculate_gross_salary(self):
        """Test gross salary calculation from components."""
        record = Form16Record(
            assessment_year="2025-26",
            salary_17_1=Decimal("18807413"),
            perquisites_17_2=Decimal("16403773"),
            profits_17_3=Decimal("0"),
        )

        record.calculate_gross_salary()

        assert record.gross_salary == Decimal("35211186")


class TestForm16PartA:
    """Tests for Form 16 Part A (TDS Certificate) parsing."""

    def test_quarterly_tds_structure(self):
        """Test quarterly TDS structure is correct."""
        record = Form16Record(
            assessment_year="2025-26",
            q1_tds=Decimal("3500000"),
            q2_tds=Decimal("3200000"),
            q3_tds=Decimal("3100000"),
            q4_tds=Decimal("3415375"),
        )

        # Verify quarters are stored correctly
        assert record.q1_tds == Decimal("3500000")
        assert record.q2_tds == Decimal("3200000")
        assert record.q3_tds == Decimal("3100000")
        assert record.q4_tds == Decimal("3415375")


class TestForm16PartB:
    """Tests for Form 16 Part B (Salary Details) parsing."""

    def test_section_17_components(self):
        """Test Section 17 salary components are captured."""
        record = Form16Record(
            assessment_year="2025-26",
            salary_17_1=Decimal("18807413"),  # Section 17(1)
            perquisites_17_2=Decimal("16403773"),  # Section 17(2)
            profits_17_3=Decimal("0"),  # Section 17(3)
        )

        # 17(1) = Regular salary
        assert record.salary_17_1 == Decimal("18807413")

        # 17(2) = Perquisites (RSU, etc.)
        assert record.perquisites_17_2 == Decimal("16403773")

        # 17(3) = Profits in lieu of salary
        assert record.profits_17_3 == Decimal("0")

    def test_standard_deduction(self):
        """Test standard deduction is captured."""
        record = Form16Record(
            assessment_year="2025-26",
            standard_deduction=Decimal("75000"),  # FY 2024-25 limit
        )

        assert record.standard_deduction == Decimal("75000")

    def test_chapter_via_deductions(self):
        """Test Chapter VI-A deductions are captured."""
        record = Form16Record(
            assessment_year="2025-26",
            section_80c=Decimal("150000"),
            section_80ccd_1b=Decimal("50000"),
            section_80ccd_2=Decimal("292277.80"),  # Employer NPS
            section_80d=Decimal("25000"),
        )

        assert record.section_80c == Decimal("150000")
        assert record.section_80ccd_1b == Decimal("50000")
        assert record.section_80ccd_2 == Decimal("292277.80")
        assert record.section_80d == Decimal("25000")

    def test_employer_nps_80ccd2(self):
        """Test employer NPS contribution under 80CCD(2)."""
        # This is important for new regime (only employer NPS is allowed)
        record = Form16Record(
            assessment_year="2025-26",
            section_80ccd_2=Decimal("292277.80"),
        )

        # 80CCD(2) is employer NPS contribution, allowed even in new regime
        assert record.section_80ccd_2 == Decimal("292277.80")
