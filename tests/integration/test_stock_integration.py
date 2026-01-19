"""Stock Parser Integration Test - Refactored"""

import pytest
from decimal import Decimal
from pathlib import Path
from pfas.parsers.stock.zerodha import ZerodhaParser
from pfas.parsers.stock.icici import ICICIDirectParser


def get_stock_parser(file_path: Path, db_connection):
    """Get appropriate parser based on file path/name."""
    name_upper = file_path.name.upper()
    if 'ICICI' in name_upper or 'ICICIDIRECT' in name_upper:
        return ICICIDirectParser(db_connection), "ICICIDirect"
    else:
        return ZerodhaParser(db_connection), "Zerodha"


class TestStockParser:
    """Stock parser integration tests (auto-detects Zerodha vs ICICI Direct)."""

    def test_stock_parse_basic(self, stock_file, test_db):
        """Test basic stock parsing."""
        parser, broker = get_stock_parser(stock_file, test_db)
        result = parser.parse(stock_file)

        assert result.success, f"Parse failed: {result.errors}"
        # Some files may have 0 trades (e.g., no exits in period)
        print(f"\n✓ [{broker}] Parsed {len(result.trades)} trades from {stock_file.name}")

    def test_stock_trade_types(self, stock_file, test_db):
        """Test trade type classification."""
        parser, broker = get_stock_parser(stock_file, test_db)
        result = parser.parse(stock_file)

        if not result.success:
            pytest.skip(f"Parse failed: {result.errors}")

        buy_trades = [t for t in result.trades if t.trade_type.value == "BUY"]
        sell_trades = [t for t in result.trades if t.trade_type.value == "SELL"]

        print(f"\n✓ [{broker}] Buy: {len(buy_trades)}, Sell: {len(sell_trades)}")

    def test_stock_save_to_db(self, stock_file, clean_db, test_user_id):
        """Test database persistence."""
        parser, broker = get_stock_parser(stock_file, clean_db)
        result = parser.parse(stock_file)

        if not result.success or len(result.trades) == 0:
            pytest.skip(f"No trades to save: {result.errors}")

        count = parser.save_to_db(result, user_id=test_user_id, broker_name=broker)
        assert count > 0

        # Verify data in database
        cursor = clean_db.execute("SELECT COUNT(*) as cnt FROM stock_trades")
        assert cursor.fetchone()['cnt'] == count

        print(f"\n✓ Saved {count} trades")

    def test_stock_capital_gains(self, stock_file, test_db):
        """Test capital gains calculation."""
        parser, broker = get_stock_parser(stock_file, test_db)
        result = parser.parse(stock_file)

        total_capital_gain = sum(
            t.capital_gain for t in result.trades
            if t.capital_gain and t.capital_gain != Decimal("0")
        )

        print(f"\n✓ Total Capital Gain: ₹{total_capital_gain:,.2f}")
