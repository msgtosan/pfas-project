"""
Shared pytest fixtures for PFAS tests.

Provides database connections, test data, and common utilities.
"""

import pytest
import sys
from pathlib import Path
from datetime import date

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pfas.core.database import DatabaseManager
from pfas.core.accounts import setup_chart_of_accounts


# Test password for encrypted database
TEST_DB_PASSWORD = "test_password_123"

# Test master key for field encryption
TEST_MASTER_KEY = b"test_master_key_32_bytes_long!!"


@pytest.fixture
def db_manager():
    """Provide a fresh DatabaseManager instance for each test."""
    # Reset singleton to ensure clean state
    DatabaseManager.reset_instance()
    manager = DatabaseManager()
    yield manager
    # Cleanup
    manager.close()
    DatabaseManager.reset_instance()


@pytest.fixture
def db_connection(db_manager):
    """Provide an initialized in-memory database connection."""
    conn = db_manager.init(":memory:", TEST_DB_PASSWORD)
    yield conn
    # Connection is closed by db_manager fixture


@pytest.fixture
def db_with_accounts(db_connection):
    """Provide a database with chart of accounts populated."""
    setup_chart_of_accounts(db_connection)
    yield db_connection


@pytest.fixture
def master_key():
    """Provide a test master key for encryption."""
    return TEST_MASTER_KEY


@pytest.fixture
def sample_user(db_connection, master_key):
    """Create a sample user in the database."""
    from pfas.core.encryption import encrypt_field

    pan = "AAPPS0793R"
    pan_encrypted, pan_salt = encrypt_field(pan, master_key)

    cursor = db_connection.cursor()
    cursor.execute(
        """
        INSERT INTO users (pan_encrypted, pan_salt, name, email, phone)
        VALUES (?, ?, ?, ?, ?)
        """,
        (pan_encrypted, pan_salt, "Test User", "test@example.com", "9876543210"),
    )
    db_connection.commit()

    return {
        "id": cursor.lastrowid,
        "pan": pan,
        "name": "Test User",
        "email": "test@example.com",
        "phone": "9876543210",
    }


@pytest.fixture
def sample_exchange_rates(db_connection):
    """Add sample exchange rates to the database."""
    from pfas.core.currency import CurrencyConverter
    from decimal import Decimal

    converter = CurrencyConverter(db_connection)

    rates = [
        (date(2024, 6, 10), "USD", Decimal("83.00")),
        (date(2024, 6, 11), "USD", Decimal("83.25")),
        (date(2024, 6, 12), "USD", Decimal("83.50")),
        (date(2024, 6, 15), "USD", Decimal("83.75")),
        (date(2024, 6, 10), "EUR", Decimal("90.00")),
        (date(2024, 6, 15), "EUR", Decimal("91.00")),
    ]

    for rate_date, currency, rate in rates:
        converter.add_rate(rate_date, currency, rate)

    return rates


@pytest.fixture
def fixtures_path():
    """Get path to test fixtures directory."""
    return Path(__file__).parent / "fixtures"
