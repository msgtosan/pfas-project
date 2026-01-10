# Sprint 3: Indian Stock ICICI Direct Parser

## Module Overview
**Sprint:** S3 (Week 5-6)
**Phase:** 1
**Requirements:** REQ-STK-001, REQ-STK-003, REQ-STK-004, REQ-STK-005, REQ-STK-006, REQ-STK-007
**Dependencies:** Core module complete

---

## Requirements to Implement

### REQ-STK-001: ICICI Direct Parser
- **Input:** ICICI Direct trade file (CSV/Excel)
- **Processing:** Extract trades with buy/sell details, quantity, price
- **Output:** Parsed stock transactions in database

### REQ-STK-003: Dividend Tracking
- **Input:** Dividend payments from broker/depository
- **Processing:** Track dividend income with TDS deducted
- **Output:** Dividend income with TDS credit

### REQ-STK-004: Stock STCG (<12 months)
- **Input:** Stock sale within 12 months of purchase
- **Processing:** Calculate gain at 20% tax rate
- **Output:** STCG amount

### REQ-STK-005: Stock LTCG (>12 months)
- **Input:** Stock sale after 12 months
- **Processing:** Calculate gain at 12.5%, apply ₹1.25L exemption
- **Output:** LTCG amount with exemption applied

### REQ-STK-006: STT Tracking
- **Input:** STT paid on transactions
- **Processing:** Track STT for each transaction
- **Output:** STT ledger for record-keeping

### REQ-STK-007: Holdings Report
- **Input:** All buy/sell transactions
- **Processing:** Calculate current holdings using FIFO
- **Output:** Current stock holdings with cost basis

---

## ICICI Direct File Formats

### Trade File (CSV)
```csv
Trade Date,Settlement Date,Exchange,Segment,Symbol,ISIN,Buy/Sell,Quantity,Price,Amount,Brokerage,STT,Other Charges,Net Amount
15-Apr-2024,17-Apr-2024,NSE,EQ,RELIANCE,INE002A01018,BUY,10,2500.50,25005.00,17.50,12.50,2.30,25037.30
20-Jun-2024,22-Jun-2024,NSE,EQ,RELIANCE,INE002A01018,SELL,5,2650.00,13250.00,9.28,6.63,1.20,13232.89
```

### Dividend File
```csv
Record Date,Ex-Dividend Date,Symbol,ISIN,Dividend Type,Rate Per Share,Quantity,Gross Amount,TDS,Net Amount
15-Aug-2024,12-Aug-2024,INFY,INE009A01021,INTERIM,18.50,100,1850.00,185.00,1665.00
```

---

## Database Schema

```sql
CREATE TABLE IF NOT EXISTS stock_holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    symbol TEXT NOT NULL,
    isin TEXT NOT NULL,
    exchange TEXT DEFAULT 'NSE',
    total_quantity INTEGER DEFAULT 0,
    average_cost DECIMAL(15,2) DEFAULT 0,
    account_id INTEGER REFERENCES accounts(id),  -- Link to COA
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, symbol, exchange)
);

CREATE TABLE IF NOT EXISTS stock_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    holding_id INTEGER REFERENCES stock_holdings(id),
    trade_date DATE NOT NULL,
    settlement_date DATE,
    trade_type TEXT NOT NULL CHECK(trade_type IN ('BUY', 'SELL')),
    quantity INTEGER NOT NULL,
    price DECIMAL(15,2) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    brokerage DECIMAL(15,2) DEFAULT 0,
    stt DECIMAL(15,2) DEFAULT 0,
    other_charges DECIMAL(15,2) DEFAULT 0,
    net_amount DECIMAL(15,2) NOT NULL,
    -- For SELL trades - link to original BUY
    buy_trade_id INTEGER REFERENCES stock_trades(id),
    buy_date DATE,
    buy_price DECIMAL(15,2),
    -- Capital gains (for SELL)
    holding_period_days INTEGER,
    is_long_term BOOLEAN,
    cost_of_acquisition DECIMAL(15,2),
    short_term_gain DECIMAL(15,2),
    long_term_gain DECIMAL(15,2),
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stock_dividends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    holding_id INTEGER REFERENCES stock_holdings(id),
    record_date DATE NOT NULL,
    ex_dividend_date DATE,
    dividend_type TEXT,  -- INTERIM, FINAL
    rate_per_share DECIMAL(15,4),
    quantity INTEGER NOT NULL,
    gross_amount DECIMAL(15,2) NOT NULL,
    tds_deducted DECIMAL(15,2) DEFAULT 0,
    net_amount DECIMAL(15,2) NOT NULL,
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stock_capital_gains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    financial_year TEXT NOT NULL,
    stcg_amount DECIMAL(15,2) DEFAULT 0,
    ltcg_amount DECIMAL(15,2) DEFAULT 0,
    ltcg_exemption DECIMAL(15,2) DEFAULT 0,
    stt_total DECIMAL(15,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, financial_year)
);

CREATE INDEX IF NOT EXISTS idx_stock_trades_date ON stock_trades(trade_date);
CREATE INDEX IF NOT EXISTS idx_stock_trades_holding ON stock_trades(holding_id);
CREATE INDEX IF NOT EXISTS idx_stock_dividends_holding ON stock_dividends(holding_id);
```

---

## Files to Create

```
src/pfas/parsers/stock/
├── __init__.py
├── base.py              # Base stock parser class
├── icici.py             # ICICI Direct parser
├── models.py            # StockTrade, StockHolding dataclasses
├── capital_gains.py     # Stock CG calculation
└── fifo.py              # FIFO cost basis calculation

tests/unit/test_parsers/test_stock/
├── __init__.py
├── test_icici.py
├── test_capital_gains.py
└── test_fifo.py

tests/fixtures/stock/
├── icici_trades.csv
├── icici_dividends.csv
```

---

## Implementation

### models.py
```python
"""Stock trading data models."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional
from enum import Enum

class TradeType(Enum):
    BUY = "BUY"
    SELL = "SELL"

@dataclass
class StockTrade:
    symbol: str
    isin: str
    trade_date: date
    trade_type: TradeType
    quantity: int
    price: Decimal
    amount: Decimal
    brokerage: Decimal = Decimal("0")
    stt: Decimal = Decimal("0")
    other_charges: Decimal = Decimal("0")
    net_amount: Decimal = Decimal("0")
    settlement_date: Optional[date] = None
    exchange: str = "NSE"
    
    # For SELL - matched BUY info
    buy_date: Optional[date] = None
    buy_price: Optional[Decimal] = None
    cost_of_acquisition: Optional[Decimal] = None
    
    @property
    def holding_period_days(self) -> Optional[int]:
        if self.buy_date and self.trade_type == TradeType.SELL:
            return (self.trade_date - self.buy_date).days
        return None
    
    @property
    def is_long_term(self) -> bool:
        if self.holding_period_days is None:
            return False
        return self.holding_period_days > 365

@dataclass
class StockDividend:
    symbol: str
    isin: str
    record_date: date
    quantity: int
    gross_amount: Decimal
    tds_deducted: Decimal
    net_amount: Decimal
    dividend_type: str = "INTERIM"
    ex_dividend_date: Optional[date] = None
    rate_per_share: Optional[Decimal] = None

@dataclass
class StockHolding:
    symbol: str
    isin: str
    quantity: int
    average_cost: Decimal
    exchange: str = "NSE"
    
    @property
    def total_cost(self) -> Decimal:
        return self.average_cost * self.quantity
```

### icici.py
```python
"""ICICI Direct trade and dividend parser."""

import pandas as pd
from pathlib import Path
from datetime import datetime
from decimal import Decimal
from typing import List, Tuple

from .models import StockTrade, StockDividend, TradeType

class ICICIDirectParser:
    """Parser for ICICI Direct trade files."""
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def parse_trades(self, file_path: Path) -> List[StockTrade]:
        """Parse trade file (CSV/Excel)."""
        if file_path.suffix.lower() == '.csv':
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
        
        trades = []
        for _, row in df.iterrows():
            trade = StockTrade(
                symbol=str(row['Symbol']).strip(),
                isin=str(row['ISIN']).strip(),
                trade_date=self._parse_date(row['Trade Date']),
                settlement_date=self._parse_date(row.get('Settlement Date')),
                trade_type=TradeType.BUY if str(row['Buy/Sell']).upper() == 'BUY' else TradeType.SELL,
                quantity=int(row['Quantity']),
                price=self._to_decimal(row['Price']),
                amount=self._to_decimal(row['Amount']),
                brokerage=self._to_decimal(row.get('Brokerage', 0)),
                stt=self._to_decimal(row.get('STT', 0)),
                other_charges=self._to_decimal(row.get('Other Charges', 0)),
                net_amount=self._to_decimal(row['Net Amount']),
                exchange=str(row.get('Exchange', 'NSE')).strip()
            )
            trades.append(trade)
        
        return trades
    
    def parse_dividends(self, file_path: Path) -> List[StockDividend]:
        """Parse dividend file."""
        if file_path.suffix.lower() == '.csv':
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
        
        dividends = []
        for _, row in df.iterrows():
            div = StockDividend(
                symbol=str(row['Symbol']).strip(),
                isin=str(row['ISIN']).strip(),
                record_date=self._parse_date(row['Record Date']),
                ex_dividend_date=self._parse_date(row.get('Ex-Dividend Date')),
                dividend_type=str(row.get('Dividend Type', 'INTERIM')),
                rate_per_share=self._to_decimal(row.get('Rate Per Share')),
                quantity=int(row['Quantity']),
                gross_amount=self._to_decimal(row['Gross Amount']),
                tds_deducted=self._to_decimal(row.get('TDS', 0)),
                net_amount=self._to_decimal(row['Net Amount'])
            )
            dividends.append(div)
        
        return dividends
    
    def _parse_date(self, date_val) -> Optional[date]:
        if pd.isna(date_val) or date_val is None:
            return None
        if isinstance(date_val, datetime):
            return date_val.date()
        # Handle DD-MMM-YYYY format
        return pd.to_datetime(date_val, dayfirst=True).date()
    
    def _to_decimal(self, value) -> Decimal:
        if pd.isna(value) or value == '' or value is None:
            return Decimal("0")
        return Decimal(str(value).replace(',', ''))
```

### fifo.py
```python
"""FIFO (First-In-First-Out) cost basis calculation."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import List, Tuple
from collections import deque

@dataclass
class FIFOLot:
    """A single purchase lot."""
    date: date
    quantity: int
    price: Decimal
    remaining_quantity: int = 0
    
    def __post_init__(self):
        self.remaining_quantity = self.quantity

class FIFOCalculator:
    """Calculate cost basis using FIFO method."""
    
    def __init__(self):
        self.lots: deque[FIFOLot] = deque()
    
    def add_purchase(self, date: date, quantity: int, price: Decimal):
        """Add a purchase lot."""
        self.lots.append(FIFOLot(date=date, quantity=quantity, price=price))
    
    def process_sale(self, sale_date: date, quantity: int, sale_price: Decimal) -> List[Tuple[FIFOLot, int, Decimal]]:
        """
        Process a sale using FIFO matching.
        
        Returns:
            List of (matched_lot, quantity_sold, gain/loss) tuples
        """
        matches = []
        remaining_to_sell = quantity
        
        while remaining_to_sell > 0 and self.lots:
            lot = self.lots[0]
            
            if lot.remaining_quantity <= remaining_to_sell:
                # Use entire lot
                qty_sold = lot.remaining_quantity
                remaining_to_sell -= qty_sold
                self.lots.popleft()
            else:
                # Partial lot
                qty_sold = remaining_to_sell
                lot.remaining_quantity -= qty_sold
                remaining_to_sell = 0
            
            # Calculate gain for this match
            cost = lot.price * qty_sold
            proceeds = sale_price * qty_sold
            gain = proceeds - cost
            
            matches.append((lot, qty_sold, gain))
        
        if remaining_to_sell > 0:
            raise ValueError(f"Insufficient holdings: {remaining_to_sell} shares unmatched")
        
        return matches
    
    def get_current_holdings(self) -> Tuple[int, Decimal]:
        """Get total holdings and average cost."""
        total_qty = sum(lot.remaining_quantity for lot in self.lots)
        total_cost = sum(lot.remaining_quantity * lot.price for lot in self.lots)
        avg_cost = total_cost / total_qty if total_qty > 0 else Decimal("0")
        return total_qty, avg_cost
```

### capital_gains.py
```python
"""Stock capital gains calculation."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import List

from .models import StockTrade, TradeType
from .fifo import FIFOCalculator

@dataclass
class StockCGSummary:
    financial_year: str
    stcg_amount: Decimal = Decimal("0")
    ltcg_amount: Decimal = Decimal("0")
    ltcg_exemption: Decimal = Decimal("0")
    taxable_stcg: Decimal = Decimal("0")
    taxable_ltcg: Decimal = Decimal("0")
    stt_total: Decimal = Decimal("0")

class StockCapitalGainsCalculator:
    """Calculate capital gains for stock trades."""
    
    LTCG_EXEMPTION = Decimal("125000")  # ₹1.25 lakh
    STCG_RATE = Decimal("20")  # 20%
    LTCG_RATE = Decimal("12.5")  # 12.5%
    
    def __init__(self, db_connection):
        self.conn = db_connection
    
    def calculate_for_trades(self, trades: List[StockTrade]) -> StockCGSummary:
        """
        Calculate CG for a list of trades using FIFO.
        
        Trades should be sorted by date.
        """
        # Group trades by symbol
        by_symbol = {}
        for trade in sorted(trades, key=lambda t: t.trade_date):
            if trade.symbol not in by_symbol:
                by_symbol[trade.symbol] = []
            by_symbol[trade.symbol].append(trade)
        
        stcg_total = Decimal("0")
        ltcg_total = Decimal("0")
        stt_total = Decimal("0")
        
        for symbol, symbol_trades in by_symbol.items():
            fifo = FIFOCalculator()
            
            for trade in symbol_trades:
                if trade.trade_type == TradeType.BUY:
                    fifo.add_purchase(trade.trade_date, trade.quantity, trade.price)
                else:
                    # SELL
                    matches = fifo.process_sale(trade.trade_date, trade.quantity, trade.price)
                    
                    for lot, qty, gain in matches:
                        holding_days = (trade.trade_date - lot.date).days
                        
                        if holding_days > 365:
                            ltcg_total += gain
                        else:
                            stcg_total += gain
                    
                    stt_total += trade.stt
        
        # Apply exemption
        ltcg_exemption = min(ltcg_total, self.LTCG_EXEMPTION) if ltcg_total > 0 else Decimal("0")
        taxable_ltcg = max(Decimal("0"), ltcg_total - ltcg_exemption)
        
        return StockCGSummary(
            financial_year="",
            stcg_amount=stcg_total,
            ltcg_amount=ltcg_total,
            ltcg_exemption=ltcg_exemption,
            taxable_stcg=stcg_total,
            taxable_ltcg=taxable_ltcg,
            stt_total=stt_total
        )
```

---

## Test Cases

### TC-STK-001: ICICI Direct Parse
```python
def test_icici_direct_parse(test_db, fixtures_path):
    """Test ICICI Direct trade file parsing."""
    parser = ICICIDirectParser(test_db)
    trades = parser.parse_trades(fixtures_path / "stock/icici_trades.csv")
    
    assert len(trades) > 0
    trade = trades[0]
    assert trade.symbol != ""
    assert trade.quantity > 0
    assert trade.price > 0
```

### TC-STK-003: Dividend Tracking
```python
def test_dividend_tracking(test_db, fixtures_path):
    """Test dividend file parsing with TDS."""
    parser = ICICIDirectParser(test_db)
    dividends = parser.parse_dividends(fixtures_path / "stock/icici_dividends.csv")
    
    assert len(dividends) > 0
    div = dividends[0]
    assert div.gross_amount > 0
    assert div.net_amount == div.gross_amount - div.tds_deducted
```

### TC-STK-004: Stock STCG
```python
def test_stock_stcg(test_db):
    """Test STCG calculation for <12 month holding."""
    calc = StockCapitalGainsCalculator(test_db)
    
    trades = [
        StockTrade(symbol="RELIANCE", isin="INE002A01018",
                   trade_date=date(2024, 4, 15), trade_type=TradeType.BUY,
                   quantity=10, price=Decimal("2500"), amount=Decimal("25000"),
                   net_amount=Decimal("25050")),
        StockTrade(symbol="RELIANCE", isin="INE002A01018",
                   trade_date=date(2024, 8, 15), trade_type=TradeType.SELL,
                   quantity=10, price=Decimal("2700"), amount=Decimal("27000"),
                   stt=Decimal("27"), net_amount=Decimal("26960"))
    ]
    
    summary = calc.calculate_for_trades(trades)
    
    assert summary.stcg_amount == Decimal("2000")  # 27000 - 25000
    assert summary.ltcg_amount == Decimal("0")
```

### TC-STK-005: Stock LTCG
```python
def test_stock_ltcg(test_db):
    """Test LTCG calculation for >12 month holding."""
    calc = StockCapitalGainsCalculator(test_db)
    
    trades = [
        StockTrade(symbol="RELIANCE", isin="INE002A01018",
                   trade_date=date(2023, 1, 15), trade_type=TradeType.BUY,
                   quantity=10, price=Decimal("2500"), amount=Decimal("25000"),
                   net_amount=Decimal("25050")),
        StockTrade(symbol="RELIANCE", isin="INE002A01018",
                   trade_date=date(2024, 8, 15), trade_type=TradeType.SELL,
                   quantity=10, price=Decimal("2700"), amount=Decimal("27000"),
                   stt=Decimal("27"), net_amount=Decimal("26960"))
    ]
    
    summary = calc.calculate_for_trades(trades)
    
    assert summary.stcg_amount == Decimal("0")
    assert summary.ltcg_amount == Decimal("2000")  # 27000 - 25000
```

---

## Verification Commands

```bash
pytest tests/unit/test_parsers/test_stock/ -v --cov=src/pfas/parsers/stock
```

---

## Success Criteria

- [ ] ICICI Direct trades parsed correctly
- [ ] Dividends tracked with TDS
- [ ] STCG calculated for <12 month holdings at 20%
- [ ] LTCG calculated for >12 month holdings at 12.5%
- [ ] FIFO cost basis calculation working
- [ ] STT tracked per transaction
- [ ] Holdings report generated
- [ ] All tests passing, coverage > 80%
