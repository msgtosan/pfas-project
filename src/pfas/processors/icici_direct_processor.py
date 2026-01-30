"""
ICICI Direct Unified Processor

Processes all ICICI Direct statement types with deduplication:
- PDF: Individual transactions with exact cost basis → stock_trades
- CSV/XLS: Capital gains with FIFO matching → stock_capital_gains_detail
- Holdings: Current portfolio snapshot → stock_holdings

Features:
- File-level deduplication (MD5 hash)
- Record-level deduplication (unique keys per table)
- Cross-validation between PDF and CSV data
- Automatic broker detection from folder/filename

Usage:
    from pfas.processors.icici_direct_processor import ICICIDirectProcessor

    processor = ICICIDirectProcessor(conn, user_id=1)
    result = processor.process_all(Path("inbox/Indian-Stocks/ICICIDirect"))

    print(f"Trades: {result.trades_count}")
    print(f"Capital Gains: {result.cg_count}")
    print(f"Validation: {result.validation_status}")
"""

import hashlib
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ProcessingResult:
    """Result of ICICI Direct processing."""
    success: bool = True

    # File counts
    files_found: int = 0
    files_processed: int = 0
    files_skipped_duplicate: int = 0
    files_failed: int = 0

    # Record counts
    trades_inserted: int = 0
    trades_skipped: int = 0
    cg_inserted: int = 0
    cg_skipped: int = 0
    holdings_inserted: int = 0
    holdings_skipped: int = 0

    # Aggregates
    total_buy_value: Decimal = Decimal("0")
    total_sell_value: Decimal = Decimal("0")
    total_brokerage: Decimal = Decimal("0")
    total_stt: Decimal = Decimal("0")
    total_stcg: Decimal = Decimal("0")
    total_ltcg: Decimal = Decimal("0")

    # Validation
    validation_status: str = "PENDING"
    validation_messages: List[str] = field(default_factory=list)

    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def trades_count(self) -> int:
        return self.trades_inserted

    @property
    def cg_count(self) -> int:
        return self.cg_inserted


class ICICIDirectProcessor:
    """
    Unified processor for all ICICI Direct statement types.

    Handles PDF transactions, capital gains CSV/XLS, and holdings with
    proper deduplication at both file and record levels.
    """

    # File patterns
    PDF_PATTERN = "TRX-Equity*.PDF"
    CG_PATTERNS = ["*ProfitLoss*.xls", "*ProfitLoss*.xlsx", "*ICICIDirect_FY*.csv"]
    HOLDINGS_PATTERNS = ["*holding*.xlsx", "*holding*.xls"]

    # Capital gains field mapping
    CG_FIELD_MAP = {
        "Stock Symbol": "symbol",
        "ISIN": "isin",
        "Qty": "quantity",
        "Sale Date": "sell_date",
        "Sale Rate": "sell_price",
        "Sale Value": "sell_value",
        "Sale Expenses": "sell_expenses",
        "Purchase Date": "buy_date",
        "Purchase Rate": "buy_price",
        "Price as on 31st Jan 2018": "fmv_31jan2018",
        "Purchase Price Considered": "grandfathered_price",
        "Purchase Value": "buy_value",
        "Purchase Expenses": "buy_expenses",
        "Profit/Loss(-)": "profit_loss",
    }

    # Holdings field mapping
    HOLDINGS_FIELD_MAP = {
        "Stock Name": "company_name",
        "Stock ISIN": "isin",
        "Allocated Quantity": "quantity_held",
        "Blocked for Trade": "quantity_blocked",
        "Block For Margin": "quantity_pledged",
        "Current Market Price": "current_price",
        "% Change": "price_change_pct",
        "Market Value": "market_value",
    }

    def __init__(
        self,
        conn,
        user_id: int,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize ICICI Direct processor.

        Args:
            conn: Database connection
            user_id: User ID for data association
            config: Optional configuration override
        """
        self.conn = conn
        self.user_id = user_id
        self.config = config or {}

        # Ensure tables exist
        self._ensure_schema()

        # Get or create broker
        self.broker_id = self._get_or_create_broker()

        # Deduplication tracking
        self._seen_file_hashes: Set[str] = set()
        self._seen_trades: Set[str] = set()  # contract_number:trade_no
        self._seen_cg: Set[str] = set()  # symbol:buy_date:sell_date:qty

    def _ensure_schema(self):
        """Ensure required tables exist."""
        # Create stock_trades table if not exists
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                broker_id INTEGER,

                -- Unique identifiers
                contract_number TEXT,
                settlement_no TEXT,
                trade_no TEXT,
                order_no TEXT,

                -- Dates
                trade_date DATE NOT NULL,
                trade_time TEXT,
                settlement_date DATE,

                -- Security info
                exchange TEXT DEFAULT 'NSE',
                isin TEXT NOT NULL,
                symbol TEXT,
                security_name TEXT,

                -- Transaction
                buy_sell TEXT NOT NULL CHECK(buy_sell IN ('B', 'S')),
                quantity INTEGER NOT NULL,
                gross_rate DECIMAL(15,4) NOT NULL,
                gross_value DECIMAL(15,2) NOT NULL,

                -- Charges
                brokerage DECIMAL(15,2) DEFAULT 0,
                gst DECIMAL(15,2) DEFAULT 0,
                stt DECIMAL(15,2) DEFAULT 0,
                transaction_charges DECIMAL(15,2) DEFAULT 0,
                stamp_duty DECIMAL(15,2) DEFAULT 0,

                -- Net
                net_amount DECIMAL(15,2) NOT NULL,

                -- Metadata
                source_file TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                UNIQUE(user_id, contract_number, trade_no),
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (broker_id) REFERENCES stock_brokers(id)
            )
        """)

        # Create indexes
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_trades_user
            ON stock_trades(user_id)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_trades_isin
            ON stock_trades(isin)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_trades_date
            ON stock_trades(trade_date)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_trades_contract
            ON stock_trades(contract_number, trade_no)
        """)

        self.conn.commit()

    def _get_or_create_broker(self) -> int:
        """Get or create ICICI Direct broker ID."""
        cursor = self.conn.execute(
            "SELECT id FROM stock_brokers WHERE broker_code = 'ICICI'"
        )
        row = cursor.fetchone()
        if row:
            return row[0]

        cursor = self.conn.execute(
            "INSERT INTO stock_brokers (name, broker_code) VALUES (?, ?)",
            ("ICICI Direct", "ICICI")
        )
        self.conn.commit()
        return cursor.lastrowid

    def process_all(
        self,
        base_path: Path,
        financial_year: Optional[str] = None
    ) -> ProcessingResult:
        """
        Process all ICICI Direct statements in the directory.

        Args:
            base_path: Root directory to scan
            financial_year: Target FY for capital gains (e.g., "2024-25")

        Returns:
            ProcessingResult with counts and validation status
        """
        result = ProcessingResult()
        self._seen_file_hashes.clear()
        self._seen_trades.clear()
        self._seen_cg.clear()

        # Load existing records to avoid re-processing
        self._load_existing_records()

        # Collect all files
        all_files = self._collect_files(base_path)
        result.files_found = len(all_files)

        if not all_files:
            result.warnings.append(f"No ICICI Direct files found in {base_path}")
            return result

        logger.info(f"Found {len(all_files)} files to process")

        # Process in order: Holdings, PDF transactions, Capital Gains
        pdf_files = []
        cg_files = []
        holdings_files = []

        for file_path in all_files:
            file_hash = self._compute_file_hash(file_path)

            # Skip duplicate files
            if file_hash in self._seen_file_hashes:
                logger.debug(f"Skipping duplicate file: {file_path.name}")
                result.files_skipped_duplicate += 1
                continue

            self._seen_file_hashes.add(file_hash)

            # Categorize
            name_lower = file_path.name.lower()
            if name_lower.startswith("trx-equity") and name_lower.endswith(".pdf"):
                pdf_files.append(file_path)
            elif "holding" in name_lower:
                holdings_files.append(file_path)
            elif "profitloss" in name_lower or "icicidirect_fy" in name_lower:
                cg_files.append(file_path)

        # 1. Process Holdings first
        for hf in holdings_files:
            try:
                ins, skip = self._process_holdings_file(hf)
                result.holdings_inserted += ins
                result.holdings_skipped += skip
                result.files_processed += 1
            except Exception as e:
                result.errors.append(f"Holdings error ({hf.name}): {e}")
                result.files_failed += 1
                logger.exception(f"Failed to process holdings: {hf}")

        # 2. Process PDF transactions
        for pdf in sorted(pdf_files):
            try:
                trades_ins, trades_skip, buy_val, sell_val, brok, stt = \
                    self._process_pdf_file(pdf)
                result.trades_inserted += trades_ins
                result.trades_skipped += trades_skip
                result.total_buy_value += buy_val
                result.total_sell_value += sell_val
                result.total_brokerage += brok
                result.total_stt += stt
                result.files_processed += 1
            except Exception as e:
                result.errors.append(f"PDF error ({pdf.name}): {e}")
                result.files_failed += 1
                logger.exception(f"Failed to process PDF: {pdf}")

        # 3. Process Capital Gains CSV/XLS
        for cg in sorted(cg_files):
            try:
                cg_ins, cg_skip, stcg, ltcg = self._process_cg_file(cg, financial_year)
                result.cg_inserted += cg_ins
                result.cg_skipped += cg_skip
                result.total_stcg += stcg
                result.total_ltcg += ltcg
                result.files_processed += 1
            except Exception as e:
                result.errors.append(f"CG error ({cg.name}): {e}")
                result.files_failed += 1
                logger.exception(f"Failed to process capital gains: {cg}")

        # 4. Cross-validate
        self._cross_validate(result)

        result.success = len(result.errors) == 0

        logger.info(
            f"Processing complete: "
            f"Trades={result.trades_inserted}, CG={result.cg_inserted}, "
            f"Holdings={result.holdings_inserted}, "
            f"Skipped={result.trades_skipped + result.cg_skipped}"
        )

        return result

    def _collect_files(self, base_path: Path) -> List[Path]:
        """Collect all ICICI Direct statement files."""
        files = []
        extensions = {".pdf", ".csv", ".xls", ".xlsx"}

        for file_path in base_path.rglob("*"):
            if not file_path.is_file():
                continue

            suffix = file_path.suffix.lower()
            if suffix not in extensions:
                continue

            # Skip temp files
            if file_path.name.startswith("~$") or ".tmp" in file_path.name:
                continue

            files.append(file_path)

        return files

    def _compute_file_hash(self, file_path: Path) -> str:
        """Compute MD5 hash of file for deduplication."""
        md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()

    def _load_existing_records(self):
        """Load existing record keys to avoid re-processing."""
        # Load existing trades
        cursor = self.conn.execute(
            "SELECT contract_number, trade_no FROM stock_trades WHERE user_id = ?",
            (self.user_id,)
        )
        for row in cursor.fetchall():
            if row[0] and row[1]:
                self._seen_trades.add(f"{row[0]}:{row[1]}")

        # Load existing capital gains
        cursor = self.conn.execute(
            "SELECT symbol, buy_date, sell_date, quantity FROM stock_capital_gains_detail WHERE user_id = ?",
            (self.user_id,)
        )
        for row in cursor.fetchall():
            key = f"{row[0]}:{row[1]}:{row[2]}:{row[3]}"
            self._seen_cg.add(key)

        logger.debug(
            f"Loaded {len(self._seen_trades)} existing trades, "
            f"{len(self._seen_cg)} existing CG records"
        )

    def _process_pdf_file(
        self, file_path: Path
    ) -> Tuple[int, int, Decimal, Decimal, Decimal, Decimal]:
        """
        Process a single PDF transaction file.

        Returns:
            (inserted, skipped, buy_value, sell_value, brokerage, stt)
        """
        from pfas.parsers.stock.icici_pdf_parser import ICICIDirectPDFParser

        parser = ICICIDirectPDFParser()
        parse_result = parser.parse(file_path)

        if not parse_result.success:
            raise ValueError(f"PDF parse failed: {parse_result.errors}")

        inserted = 0
        skipped = 0
        total_buy = Decimal("0")
        total_sell = Decimal("0")
        total_brok = Decimal("0")
        total_stt = Decimal("0")

        for txn in parse_result.transactions:
            # Check for duplicate
            key = f"{txn.contract_number}:{txn.trade_no}"
            if key in self._seen_trades:
                skipped += 1
                continue

            self._seen_trades.add(key)

            # Insert into stock_trades
            self.conn.execute("""
                INSERT INTO stock_trades (
                    user_id, broker_id,
                    contract_number, settlement_no, trade_no, order_no,
                    trade_date, trade_time, settlement_date,
                    exchange, isin, symbol, security_name,
                    buy_sell, quantity, gross_rate, gross_value,
                    brokerage, gst, stt, transaction_charges, stamp_duty,
                    net_amount, source_file
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.user_id, self.broker_id,
                txn.contract_number, txn.settlement_no, txn.trade_no, txn.order_no,
                txn.trade_date.isoformat(), txn.trade_time,
                txn.settlement_date.isoformat() if txn.settlement_date else None,
                txn.exchange, txn.isin, txn.symbol, txn.security_name,
                txn.buy_sell, txn.quantity, float(txn.gross_rate), float(txn.total_value),
                float(txn.brokerage), float(txn.gst), float(txn.stt),
                float(txn.transaction_charges), float(txn.stamp_duty),
                float(txn.net_amount), str(file_path)
            ))

            inserted += 1

            if txn.is_buy:
                total_buy += txn.net_amount
            else:
                total_sell += txn.net_amount

            total_brok += txn.brokerage
            total_stt += txn.stt

        self.conn.commit()
        logger.info(f"PDF {file_path.name}: {inserted} inserted, {skipped} skipped")

        return inserted, skipped, total_buy, total_sell, total_brok, total_stt

    def _process_cg_file(
        self,
        file_path: Path,
        financial_year: Optional[str]
    ) -> Tuple[int, int, Decimal, Decimal]:
        """
        Process a capital gains CSV/XLS file.

        Returns:
            (inserted, skipped, stcg_total, ltcg_total)
        """
        # Read file
        df = self._read_tabular_file(file_path, header_row=3)

        if df is None or df.empty:
            logger.warning(f"Empty or unreadable file: {file_path.name}")
            return 0, 0, Decimal("0"), Decimal("0")

        # Normalize column names
        df.columns = [str(c).strip() for c in df.columns]

        inserted = 0
        skipped = 0
        total_stcg = Decimal("0")
        total_ltcg = Decimal("0")

        for _, row in df.iterrows():
            try:
                record = self._normalize_cg_row(row, file_path, financial_year)
                if not record:
                    continue

                # Check for duplicate
                key = f"{record['symbol']}:{record['buy_date']}:{record['sell_date']}:{record['quantity']}"
                if key in self._seen_cg:
                    skipped += 1
                    continue

                self._seen_cg.add(key)

                # Insert
                self._insert_cg_record(record)
                inserted += 1

                # Track totals
                if record["is_long_term"]:
                    total_ltcg += Decimal(str(record["taxable_profit"]))
                else:
                    total_stcg += Decimal(str(record["taxable_profit"]))

            except Exception as e:
                logger.debug(f"Skipping row: {e}")

        self.conn.commit()
        logger.info(f"CG {file_path.name}: {inserted} inserted, {skipped} skipped")

        return inserted, skipped, total_stcg, total_ltcg

    def _process_holdings_file(
        self, file_path: Path
    ) -> Tuple[int, int]:
        """
        Process a holdings file.

        Returns:
            (inserted, skipped)
        """
        df = self._read_tabular_file(file_path, header_row=0)

        if df is None or df.empty:
            logger.warning(f"Empty holdings file: {file_path.name}")
            return 0, 0

        df.columns = [str(c).strip() for c in df.columns]

        inserted = 0
        skipped = 0
        as_of_date = date.today()

        for _, row in df.iterrows():
            try:
                record = self._normalize_holdings_row(row, file_path)
                if not record:
                    continue

                # Upsert holding
                cursor = self.conn.execute("""
                    SELECT id FROM stock_holdings
                    WHERE user_id = ? AND broker_id = ? AND isin = ? AND as_of_date = ?
                """, (self.user_id, self.broker_id, record["isin"], as_of_date.isoformat()))

                if cursor.fetchone():
                    skipped += 1
                    continue

                self.conn.execute("""
                    INSERT INTO stock_holdings (
                        user_id, broker_id, symbol, isin, company_name,
                        quantity_held, quantity_pledged, quantity_blocked,
                        current_price, market_value, price_change_pct,
                        as_of_date, source_file, demat_account
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    self.user_id, self.broker_id,
                    record.get("symbol", ""),
                    record["isin"],
                    record.get("company_name", ""),
                    record["quantity_held"],
                    record.get("quantity_pledged", 0),
                    record.get("quantity_blocked", 0),
                    record.get("current_price", 0),
                    record.get("market_value", 0),
                    record.get("price_change_pct", 0),
                    as_of_date.isoformat(),
                    str(file_path),
                    "ICICI"
                ))
                inserted += 1

            except Exception as e:
                logger.debug(f"Skipping holdings row: {e}")

        self.conn.commit()
        logger.info(f"Holdings {file_path.name}: {inserted} inserted, {skipped} skipped")

        return inserted, skipped

    def _read_tabular_file(
        self, file_path: Path, header_row: int = 0
    ) -> Optional[pd.DataFrame]:
        """Read CSV/XLS/XLSX file."""
        suffix = file_path.suffix.lower()

        try:
            if suffix == ".csv":
                return pd.read_csv(file_path, header=header_row, encoding='utf-8', on_bad_lines='skip')

            elif suffix in [".xls", ".xlsx"]:
                # Check if it's actually TSV
                with open(file_path, 'rb') as f:
                    magic = f.read(8)

                if magic.startswith(b'PK') or magic[:4] == b'\xd0\xcf\x11\xe0':
                    # Real Excel
                    return pd.read_excel(file_path, header=header_row)
                else:
                    # TSV masquerading as Excel
                    return pd.read_csv(file_path, sep='\t', header=header_row, encoding='utf-8', on_bad_lines='skip')

        except Exception as e:
            logger.warning(f"Failed to read {file_path.name}: {e}")
            return None

        return None

    def _normalize_cg_row(
        self,
        row: pd.Series,
        source_file: Path,
        financial_year: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Normalize a capital gains row."""
        # Map fields
        norm = {}
        for src_col, tgt_field in self.CG_FIELD_MAP.items():
            if src_col in row.index:
                norm[tgt_field] = row[src_col]

        # Required fields
        symbol = self._get_str(norm.get("symbol", ""))
        isin = self._get_str(norm.get("isin", ""))

        if not symbol and not isin:
            return None

        # Dates
        buy_date = self._parse_date(norm.get("buy_date"))
        sell_date = self._parse_date(norm.get("sell_date"))

        if not sell_date:
            return None

        # Calculate holding period
        holding_days = (sell_date - buy_date).days if buy_date else 0

        # Quantity and values
        quantity = self._get_int(norm.get("quantity", 0))
        if quantity <= 0:
            return None

        buy_price = self._get_decimal(norm.get("buy_price", 0))
        sell_price = self._get_decimal(norm.get("sell_price", 0))
        buy_value = self._get_decimal(norm.get("buy_value", 0))
        sell_value = self._get_decimal(norm.get("sell_value", 0))
        buy_expenses = self._get_decimal(norm.get("buy_expenses", 0))
        sell_expenses = self._get_decimal(norm.get("sell_expenses", 0))

        # Calculate missing values
        if buy_value == 0 and quantity > 0 and buy_price > 0:
            buy_value = buy_price * quantity
        if sell_value == 0 and quantity > 0 and sell_price > 0:
            sell_value = sell_price * quantity

        # Grandfathering
        fmv = self._get_decimal(norm.get("fmv_31jan2018"))
        grandfathered = self._get_decimal(norm.get("grandfathered_price"))
        is_grandfathered = grandfathered > 0 and grandfathered != buy_price

        # Profit/Loss
        profit_loss = self._get_decimal(norm.get("profit_loss", 0))
        if profit_loss == 0:
            profit_loss = sell_value - buy_value - buy_expenses - sell_expenses

        # Classification
        is_long_term = holding_days >= 365
        gain_type = "LTCG" if is_long_term else "STCG"

        # Financial year
        if not financial_year and sell_date:
            if sell_date.month >= 4:
                financial_year = f"{sell_date.year}-{str(sell_date.year + 1)[-2:]}"
            else:
                financial_year = f"{sell_date.year - 1}-{str(sell_date.year)[-2:]}"

        # Quarter
        quarter = self._determine_quarter(sell_date)

        return {
            "symbol": symbol,
            "isin": isin,
            "quantity": quantity,
            "buy_date": buy_date.isoformat() if buy_date else None,
            "sell_date": sell_date.isoformat(),
            "holding_period_days": holding_days,
            "buy_price": float(buy_price),
            "sell_price": float(sell_price),
            "buy_value": float(buy_value),
            "sell_value": float(sell_value),
            "buy_expenses": float(buy_expenses),
            "sell_expenses": float(sell_expenses),
            "fmv_31jan2018": float(fmv) if fmv else None,
            "grandfathered_price": float(grandfathered) if grandfathered else None,
            "is_grandfathered": is_grandfathered,
            "profit_loss": float(profit_loss),
            "taxable_profit": float(profit_loss),
            "is_long_term": is_long_term,
            "gain_type": gain_type,
            "financial_year": financial_year,
            "quarter": quarter,
            "source_file": str(source_file),
        }

    def _normalize_holdings_row(
        self, row: pd.Series, source_file: Path
    ) -> Optional[Dict[str, Any]]:
        """Normalize a holdings row."""
        norm = {}
        for src_col, tgt_field in self.HOLDINGS_FIELD_MAP.items():
            if src_col in row.index:
                norm[tgt_field] = row[src_col]

        isin = self._get_str(norm.get("isin", ""))
        company_name = self._get_str(norm.get("company_name", ""))

        if not isin:
            return None

        quantity = self._get_int(norm.get("quantity_held", 0))
        if quantity <= 0:
            return None

        return {
            "isin": isin,
            "company_name": company_name,
            "symbol": self._derive_symbol(company_name),
            "quantity_held": quantity,
            "quantity_pledged": self._get_int(norm.get("quantity_pledged", 0)),
            "quantity_blocked": self._get_int(norm.get("quantity_blocked", 0)),
            "current_price": float(self._get_decimal(norm.get("current_price", 0))),
            "market_value": float(self._get_decimal(norm.get("market_value", 0))),
            "price_change_pct": float(self._get_decimal(norm.get("price_change_pct", 0))),
        }

    def _insert_cg_record(self, record: Dict[str, Any]):
        """Insert a capital gains record."""
        self.conn.execute("""
            INSERT INTO stock_capital_gains_detail (
                user_id, broker_id, financial_year, quarter,
                symbol, isin, quantity,
                buy_date, sell_date, holding_period_days,
                buy_price, sell_price, buy_value, sell_value,
                buy_expenses, sell_expenses,
                fmv_31jan2018, grandfathered_price, is_grandfathered,
                gross_profit_loss, cost_of_acquisition, taxable_profit,
                is_long_term, gain_type, source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            self.user_id, self.broker_id,
            record["financial_year"], record["quarter"],
            record["symbol"], record["isin"], record["quantity"],
            record["buy_date"], record["sell_date"], record["holding_period_days"],
            record["buy_price"], record["sell_price"],
            record["buy_value"], record["sell_value"],
            record["buy_expenses"], record["sell_expenses"],
            record["fmv_31jan2018"], record["grandfathered_price"],
            record["is_grandfathered"],
            record["profit_loss"], record["buy_value"] + record["buy_expenses"],
            record["taxable_profit"],
            record["is_long_term"], record["gain_type"],
            record["source_file"]
        ))

    def _cross_validate(self, result: ProcessingResult):
        """Cross-validate PDF trades against capital gains."""
        # Get total sells from PDF
        cursor = self.conn.execute("""
            SELECT SUM(quantity), SUM(gross_value)
            FROM stock_trades
            WHERE user_id = ? AND buy_sell = 'S'
        """, (self.user_id,))
        pdf_row = cursor.fetchone()
        pdf_sell_qty = pdf_row[0] or 0
        pdf_sell_value = Decimal(str(pdf_row[1] or 0))

        # Get total sells from CG
        cursor = self.conn.execute("""
            SELECT SUM(quantity), SUM(sell_value)
            FROM stock_capital_gains_detail
            WHERE user_id = ?
        """, (self.user_id,))
        cg_row = cursor.fetchone()
        cg_sell_qty = cg_row[0] or 0
        cg_sell_value = Decimal(str(cg_row[1] or 0))

        # Compare
        if pdf_sell_qty == 0 and cg_sell_qty == 0:
            result.validation_status = "NO_SELLS"
            result.validation_messages.append("No sell transactions to validate")
        elif pdf_sell_qty == 0:
            result.validation_status = "PDF_ONLY_MISSING"
            result.validation_messages.append(
                f"No PDF sells found, but CG has {cg_sell_qty} units"
            )
        elif cg_sell_qty == 0:
            result.validation_status = "CG_ONLY_MISSING"
            result.validation_messages.append(
                f"No CG records found, but PDF has {pdf_sell_qty} sells"
            )
        else:
            qty_match = abs(pdf_sell_qty - cg_sell_qty) <= 10  # Allow small variance
            value_pct_diff = abs(pdf_sell_value - cg_sell_value) / max(pdf_sell_value, cg_sell_value) * 100
            value_match = value_pct_diff < 5  # Within 5%

            if qty_match and value_match:
                result.validation_status = "VALIDATED"
                result.validation_messages.append(
                    f"PDF sells ({pdf_sell_qty} units, ₹{pdf_sell_value:,.2f}) "
                    f"match CG ({cg_sell_qty} units, ₹{cg_sell_value:,.2f})"
                )
            else:
                result.validation_status = "MISMATCH"
                result.validation_messages.append(
                    f"Mismatch: PDF sells ({pdf_sell_qty} units, ₹{pdf_sell_value:,.2f}) "
                    f"vs CG ({cg_sell_qty} units, ₹{cg_sell_value:,.2f})"
                )

    def _determine_quarter(self, sell_date: date) -> Optional[str]:
        """Determine Indian FY quarter from date."""
        if not sell_date:
            return None

        m, d = sell_date.month, sell_date.day

        # Q1: Apr 1 - Jun 15
        if (m == 4) or (m == 5) or (m == 6 and d <= 15):
            return "Q1"
        # Q2: Jun 16 - Sep 15
        elif (m == 6 and d >= 16) or (m in [7, 8]) or (m == 9 and d <= 15):
            return "Q2"
        # Q3: Sep 16 - Dec 15
        elif (m == 9 and d >= 16) or (m in [10, 11]) or (m == 12 and d <= 15):
            return "Q3"
        # Q4: Dec 16 - Mar 15
        elif (m == 12 and d >= 16) or (m in [1, 2]) or (m == 3 and d <= 15):
            return "Q4"
        # Q5: Mar 16 - Mar 31
        elif m == 3 and d >= 16:
            return "Q5"

        return None

    def _derive_symbol(self, company_name: str) -> str:
        """Derive symbol from company name."""
        if not company_name:
            return ""
        name = company_name.upper()
        for suffix in [" LIMITED", " LTD", " LTD.", " CORPORATION", " CORP", " INDIA"]:
            name = name.replace(suffix, "")
        words = name.split()
        return words[0][:10] if words else name[:10]

    # Value conversion helpers
    def _get_str(self, val: Any) -> str:
        if pd.isna(val) or val is None:
            return ""
        return str(val).strip()

    def _get_int(self, val: Any) -> int:
        if pd.isna(val) or val is None:
            return 0
        try:
            return int(float(str(val).replace(",", "").strip()))
        except (ValueError, TypeError):
            return 0

    def _get_decimal(self, val: Any) -> Decimal:
        if pd.isna(val) or val is None:
            return Decimal("0")
        try:
            cleaned = str(val)
            for char in ["₹", "Rs.", "Rs", "INR", ",", " "]:
                cleaned = cleaned.replace(char, "")
            cleaned = cleaned.strip()
            if cleaned == "" or cleaned == "-":
                return Decimal("0")
            return Decimal(cleaned)
        except InvalidOperation:
            return Decimal("0")

    def _parse_date(self, val: Any) -> Optional[date]:
        if pd.isna(val) or val is None:
            return None
        if isinstance(val, date):
            return val
        if isinstance(val, datetime):
            return val.date()
        if isinstance(val, pd.Timestamp):
            return val.date()

        val_str = str(val).strip()
        formats = ["%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"]

        for fmt in formats:
            try:
                return datetime.strptime(val_str, fmt).date()
            except ValueError:
                continue

        return None


# Convenience function
def process_icici_direct(
    conn,
    user_id: int,
    base_path: Path,
    financial_year: Optional[str] = None
) -> ProcessingResult:
    """
    Process all ICICI Direct files in a directory.

    Args:
        conn: Database connection
        user_id: User ID
        base_path: Directory containing ICICI files
        financial_year: Target FY (optional)

    Returns:
        ProcessingResult
    """
    processor = ICICIDirectProcessor(conn, user_id)
    return processor.process_all(base_path, financial_year)


__all__ = [
    "ICICIDirectProcessor",
    "ProcessingResult",
    "process_icici_direct",
]
