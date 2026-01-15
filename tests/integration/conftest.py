import pytest
from pfas.core.database import DatabaseManager
from pfas.core.accounts import setup_chart_of_accounts

@pytest.fixture(scope="session")
def test_db():
    """Create a test database for integration tests."""
    DatabaseManager.reset_instance()
    db = DatabaseManager()
    conn = db.init(":memory:", "test_password")
    setup_chart_of_accounts(conn)
    yield conn
    db.close()
    DatabaseManager.reset_instance()

@pytest.fixture
def clean_db(test_db):
    """Reset database state between tests."""
    # Clear all transaction tables, keep accounts
    test_db.execute("DELETE FROM journal_entries")
    test_db.execute("DELETE FROM journals")
    test_db.execute("DELETE FROM bank_transactions")
    test_db.execute("DELETE FROM mf_transactions")
    # ... other tables
    test_db.commit()
    yield test_db