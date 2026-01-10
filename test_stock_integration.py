#!/usr/bin/env python3
"""Integration test for Stock (Zerodha) parser with real data."""

import sys
from pathlib import Path
from datetime import date
from decimal import Decimal

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_stock_parser():
    """Run integration test with real Zerodha Tax P&L data."""

    print("="*70)
    print("STOCK (ZERODHA) INTEGRATION TEST")
    print("="*70 + "\n")

    # File path
    stock_file = Path.home() / "projects/pfas-project/Data/Users/Sanjay/Indian-Stocks/Zerodha/taxpnl-QY6347-2024_2025-Q1-Q4.xlsx"

    print(f"📁 Test File: {stock_file.name}")
    print(f"   Full Path: {stock_file}")
    print(f"   File exists: {stock_file.exists()}")

    if stock_file.exists():
        print(f"   Size: {stock_file.stat().st_size / 1024:.1f} KB")
    print()

    if not stock_file.exists():
        print(f"❌ File not found: {stock_file}")
        return False

    # Import parsers
    print("📦 Importing modules...")
    try:
        import sqlite3
        from pfas.parsers.stock.zerodha import ZerodhaParser
        from pfas.core.database import DatabaseManager
        print("✅ Imports successful\n")
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("   Install dependencies: pip install pandas openpyxl sqlcipher3")
        return False

    # Create in-memory database
    print("📊 Initializing database...")
    try:
        db_manager = DatabaseManager()
        conn = db_manager.init(":memory:", "test_password")
        print("✅ Database initialized\n")
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        return False

    # Parse Zerodha file
    print("📖 Parsing Zerodha Tax P&L...")
    try:
        parser = ZerodhaParser(conn)
        result = parser.parse(stock_file)

        print(f"   Success: {result.success}")
        print(f"   Trades parsed: {len(result.trades)}")
        print(f"   Errors: {len(result.errors)}")
        print(f"   Warnings: {len(result.warnings)}\n")

        if result.errors:
            print("   Errors:")
            for err in result.errors[:3]:
                print(f"      - {err}")
            if len(result.errors) > 3:
                print(f"      ... and {len(result.errors) - 3} more")
            print()

    except Exception as e:
        print(f"❌ Parsing failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Display parsed trades
    if result.trades:
        print("📋 Sample Trades (first 5):")

        # Group by type
        buy_trades = [t for t in result.trades if t.trade_type.value == "BUY"]
        sell_trades = [t for t in result.trades if t.trade_type.value == "SELL"]

        print(f"\n   Buy Trades: {len(buy_trades)}")
        print(f"   Sell Trades: {len(sell_trades)}")
        print(f"   Total Trades: {len(result.trades)}\n")

        # Show sample trades
        for i, trade in enumerate(result.trades[:5], 1):
            print(f"\n   Trade {i}:")
            print(f"      Symbol: {trade.symbol}")
            print(f"      Type: {trade.trade_type.value}")
            print(f"      Date: {trade.trade_date}")
            print(f"      Quantity: {trade.quantity}")
            print(f"      Price: ₹{trade.price}")
            print(f"      Amount: ₹{trade.amount:,.2f}")
            print(f"      Category: {trade.trade_category.value if trade.trade_category else 'N/A'}")

            if trade.is_long_term is not None:
                print(f"      Long Term: {trade.is_long_term}")
            if trade.capital_gain:
                print(f"      Capital Gain: ₹{trade.capital_gain:,.2f}")

        if len(result.trades) > 5:
            print(f"\n   ... and {len(result.trades) - 5} more trades")

        # Calculate summary
        print("\n\n💰 Trade Summary:")

        # By category
        delivery_trades = [t for t in result.trades if t.trade_category and t.trade_category.value == "DELIVERY"]
        intraday_trades = [t for t in result.trades if t.trade_category and t.trade_category.value == "INTRADAY"]
        fno_trades = [t for t in result.trades if t.trade_category and t.trade_category.value == "FNO"]

        print(f"\n   By Category:")
        print(f"      Delivery: {len(delivery_trades)} trades")
        print(f"      Intraday: {len(intraday_trades)} trades")
        print(f"      F&O: {len(fno_trades)} trades")

        # By symbol
        symbols = {}
        for trade in result.trades:
            if trade.symbol not in symbols:
                symbols[trade.symbol] = 0
            symbols[trade.symbol] += 1

        print(f"\n   Unique Symbols: {len(symbols)}")
        if symbols:
            print(f"   Top 5 symbols:")
            for symbol, count in sorted(symbols.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"      {symbol}: {count} trades")

        # Capital gains
        total_capital_gain = sum(
            t.capital_gain for t in result.trades
            if t.capital_gain and t.capital_gain != Decimal("0")
        )

        print(f"\n   Total Capital Gain: ₹{total_capital_gain:,.2f}")

    # Database save test
    print("\n\n💾 Testing database persistence...")
    try:
        count = parser.save_to_db(result, user_id=1, broker_name="Zerodha")
        print(f"✅ Saved {count} trades to database")

        # Verify data in database
        cursor = conn.execute("SELECT COUNT(*) as cnt FROM stock_trades")
        row = cursor.fetchone()
        print(f"✅ Verified: {row['cnt']} trades in database")

        cursor = conn.execute("SELECT COUNT(*) as cnt FROM stock_brokers")
        row = cursor.fetchone()
        print(f"✅ Verified: {row['cnt']} brokers in database")

    except Exception as e:
        print(f"❌ Database save failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n" + "="*70)
    print("✅ STOCK PARSER INTEGRATION TEST PASSED")
    print("="*70)

    return True

if __name__ == "__main__":
    success = test_stock_parser()
    sys.exit(0 if success else 1)
