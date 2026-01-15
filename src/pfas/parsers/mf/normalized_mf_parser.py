"""
Normalized Mutual Fund Parser - Example Implementation

This module demonstrates the complete normalization pipeline for MF data:
1. Parse raw data from CAMS/Karvy files
2. Normalize to unified NormalizedTransaction format
3. Store in staging tables
4. Migrate to final mf_transactions table

Flow:
    Raw Excel File
        │
        ▼
    ┌─────────────────┐
    │ parse_raw()     │  Extract source-specific data
    │                 │  Handle Strict Open XML format
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │ staging_raw     │  Store original JSON blob
    │                 │  Preserve source schema
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │ normalize()     │  Apply column mapping
    │                 │  Convert to unified format
    └────────┬────────┘
             │
             ▼
    ┌─────────────────────┐
    │ staging_normalized  │  Unified transaction format
    │                     │  Pending user assignment
    └────────┬────────────┘
             │
             ▼
    ┌─────────────────────┐
    │ assign_user()       │  Link to user_id
    │ migrate_to_final()  │  Move to mf_transactions
    └─────────────────────┘
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal
from typing import List, Dict, Any, Optional
import sqlite3
import json
import re
import logging

from ..base import (
    BaseParser, ParsedRecord, NormalizationResult,
    ParserRegistry, StrictOpenXMLConverter, ColumnMappingConfig,
    StagingPipeline
)
from pfas.core.models import (
    NormalizedTransaction, AssetCategory, ActivityType, FlowDirection
)

logger = logging.getLogger(__name__)


class NormalizedCAMSParser(BaseParser):
    """
    Normalized CAMS parser implementing the full staging pipeline.

    Example usage:
        parser = NormalizedCAMSParser(db_connection)
        result = parser.parse(Path("cams_cg_fy2425.xlsx"))

        print(f"Normalized: {result.normalized_count}")
        print(f"Errors: {result.error_count}")

        # Assign to user and migrate
        pipeline = StagingPipeline(db_connection)
        pending = pipeline.get_pending_records("CAMS")
        pipeline.assign_user([r['id'] for r in pending], user_id=1)
    """

    # Asset class mapping
    ASSET_CLASS_MAP = {
        'EQUITY': AssetCategory.MUTUAL_FUND_EQUITY,
        'DEBT': AssetCategory.MUTUAL_FUND_DEBT,
        'HYBRID': AssetCategory.MUTUAL_FUND_EQUITY,  # Treat hybrid as equity for tax
        'CASH': AssetCategory.MUTUAL_FUND_DEBT,
    }

    # Transaction type mapping
    TXN_TYPE_MAP = {
        'PURCHASE': 'BUY',
        'REDEMPTION': 'SELL',
        'SYSTEMATIC INVESTMENT': 'BUY',
        'SIP': 'BUY',
        'SWITCH IN': 'BUY',
        'SWITCH OUT': 'SELL',
        'DIVIDEND PAYOUT': 'DIVIDEND',
        'DIVIDEND REINVEST': 'DIVIDEND_REINVEST',
    }

    def __init__(self, db_connection: sqlite3.Connection):
        super().__init__(db_connection)
        self._config = ColumnMappingConfig(Path("config/parser_configs"))
        self._pipeline = StagingPipeline(db_connection)

    def get_source_type(self) -> str:
        return "CAMS"

    def get_supported_formats(self) -> List[str]:
        return ['.xlsx', '.xls']

    def parse_raw(self, file_path: Path) -> List[ParsedRecord]:
        """
        Parse CAMS Excel file and return raw records.

        Steps:
        1. Handle Strict Open XML format conversion
        2. Find the correct sheet (TRXN_DETAILS)
        3. Extract each row as ParsedRecord
        """
        records = []

        # Try different sheet names and header rows
        sheet_names = ['TRXN_DETAILS', 'Transaction_Details', 1]
        header_rows = [3, 4, 2]

        df = None
        for sheet in sheet_names:
            for header in header_rows:
                try:
                    df = pd.read_excel(
                        file_path,
                        sheet_name=sheet,
                        header=header,
                        engine='calamine'
                    )
                    if not df.empty and len(df.columns) > 5:
                        logger.debug(f"Successfully read sheet={sheet}, header={header}")
                        break
                except Exception as e:
                    continue
            if df is not None and not df.empty:
                break

        if df is None or df.empty:
            logger.warning(f"No data found in {file_path}")
            return records

        # Convert each row to ParsedRecord
        for idx, row in df.iterrows():
            # Skip empty rows
            if pd.isna(row.iloc[0]) or str(row.iloc[0]).strip() == '':
                continue

            raw_data = row.to_dict()
            # Clean up NaN values
            raw_data = {k: (None if pd.isna(v) else v) for k, v in raw_data.items()}

            record = ParsedRecord(
                source_type=self.get_source_type(),
                source_file=str(file_path),
                raw_data=raw_data,
                row_index=idx
            )
            records.append(record)

        logger.info(f"Parsed {len(records)} raw records from {file_path.name}")
        return records

    def normalize_record(self, record: ParsedRecord) -> Optional[Dict[str, Any]]:
        """
        Normalize a single CAMS record to unified format.

        Mapping:
            CAMS Column          →  Normalized Field
            ─────────────────────────────────────────
            Scheme Name          →  asset_name
            Scheme Name (ISIN)   →  asset_identifier
            ASSET CLASS          →  asset_category
            Desc                 →  transaction_type
            Date                 →  date
            Units                →  quantity
            Amount               →  amount
            Price                →  unit_price
            Short Term           →  stcg
            Long Term*           →  ltcg
        """
        raw = record.raw_data

        try:
            # Extract required fields
            scheme_name = self._get_value(raw, ['Scheme Name', 'SCHEME NAME', 'scheme_name'])
            if not scheme_name:
                return None

            # Extract ISIN from scheme name
            isin = self._extract_isin(scheme_name)

            # Parse date
            date_val = self._get_value(raw, ['Date', 'DATE', 'date', 'Date_1'])
            if date_val:
                parsed_date = self._parse_date(date_val)
            else:
                return None

            # Parse amount
            amount = self._parse_decimal(
                self._get_value(raw, ['Amount', 'AMOUNT', 'amount', 'Original Cost Amount'])
            )

            # Parse quantity
            quantity = self._parse_decimal(
                self._get_value(raw, ['Units', 'UNITS', 'units', 'Current Units'])
            )

            # Parse unit price
            unit_price = self._parse_decimal(
                self._get_value(raw, ['Price', 'PRICE', 'price', 'NAV', 'Original Purchase Cost'])
            )

            # Determine transaction type
            txn_desc = self._get_value(raw, ['Desc', 'DESC', 'desc', 'Trxn.Type', 'Transaction Type'])
            txn_type = self._map_transaction_type(txn_desc)

            # Determine asset class
            asset_class_str = self._get_value(raw, ['ASSET CLASS', 'Asset Class', 'asset_class'])
            asset_category = self._map_asset_class(asset_class_str)

            # Determine flow direction
            flow_direction = FlowDirection.OUTFLOW if txn_type in ['BUY', 'DIVIDEND_REINVEST'] else FlowDirection.INFLOW

            # Extract capital gains data
            stcg = self._parse_decimal(self._get_value(raw, ['Short Term', 'STCG', 'stcg']))
            ltcg = self._parse_decimal(
                self._get_value(raw, ['Long Term With Index', 'Long Term Without Index', 'LTCG', 'ltcg'])
            )

            # Grandfathering data
            gf_nav = self._parse_decimal(
                self._get_value(raw, ['NAV As On 31/01/2018 (Grandfathered NAV)',
                                      'Grandfathered NAV', 'gf_nav'])
            )
            gf_value = self._parse_decimal(
                self._get_value(raw, ['Market Value As On 31/01/2018 (Grandfathered Value)',
                                      'GrandFathered Cost Value', 'gf_value'])
            )

            normalized = {
                'date': parsed_date,
                'amount': amount,
                'transaction_type': txn_type,
                'asset_category': asset_category.value if asset_category else 'MF_EQUITY',
                'asset_identifier': isin,
                'asset_name': scheme_name.strip(),
                'quantity': quantity,
                'unit_price': unit_price,
                'activity_type': ActivityType.INVESTING.value,
                'flow_direction': flow_direction.value,
                'source_type': self.get_source_type(),

                # MF-specific fields (stored in extra_data)
                'folio_number': self._get_value(raw, ['Folio No', 'FOLIO NO', 'folio_number', 'Folio Number']),
                'amc_name': self._get_value(raw, ['AMC Name', 'AMC NAME', 'amc_name', ' Fund Name']),
                'stcg': stcg,
                'ltcg': ltcg,
                'grandfathered_nav': gf_nav,
                'grandfathered_value': gf_value,
                'stt': self._parse_decimal(self._get_value(raw, ['STT', 'stt'])),
            }

            return normalized

        except Exception as e:
            logger.warning(f"Normalization error at row {record.row_index}: {e}")
            return None

    def _get_value(self, data: Dict, keys: List[str]) -> Any:
        """Get value from dict trying multiple possible keys."""
        for key in keys:
            if key in data and data[key] is not None:
                return data[key]
        return None

    def _extract_isin(self, text: str) -> str:
        """Extract ISIN code from text."""
        if not text:
            return ""
        match = re.search(r'INF[A-Z0-9]{9}[0-9]', str(text))
        return match.group() if match else ""

    def _parse_date(self, value) -> Optional[date]:
        """Parse date from various formats."""
        if value is None:
            return None
        if isinstance(value, date):
            return value
        if isinstance(value, datetime):
            return value.date()

        date_formats = ['%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d', '%d-%b-%Y', '%m/%d/%Y']
        for fmt in date_formats:
            try:
                return datetime.strptime(str(value).strip(), fmt).date()
            except:
                continue
        return None

    def _parse_decimal(self, value) -> Decimal:
        """Parse decimal from various formats."""
        if value is None:
            return Decimal("0")
        try:
            # Handle string with commas
            str_val = str(value).replace(',', '').replace(' ', '').strip()
            if str_val == '' or str_val.lower() == 'nan':
                return Decimal("0")
            return Decimal(str_val)
        except:
            return Decimal("0")

    def _map_transaction_type(self, desc: str) -> str:
        """Map source transaction type to normalized type."""
        if not desc:
            return 'UNKNOWN'
        desc_upper = str(desc).upper().strip()
        for pattern, mapped in self.TXN_TYPE_MAP.items():
            if pattern in desc_upper:
                return mapped
        return 'UNKNOWN'

    def _map_asset_class(self, asset_class: str) -> AssetCategory:
        """Map source asset class to AssetCategory enum."""
        if not asset_class:
            return AssetCategory.MUTUAL_FUND_EQUITY
        asset_upper = str(asset_class).upper().strip()
        return self.ASSET_CLASS_MAP.get(asset_upper, AssetCategory.MUTUAL_FUND_EQUITY)


class NormalizedKarvyParser(NormalizedCAMSParser):
    """
    Normalized Karvy parser - extends CAMS parser with Karvy-specific handling.

    Karvy files have similar structure but different column names and
    a multi-section layout (Section A/B/C for subscriptions/outflows/gains).
    """

    def get_source_type(self) -> str:
        return "KARVY"

    def parse_raw(self, file_path: Path) -> List[ParsedRecord]:
        """
        Parse Karvy Excel file.

        Karvy-specific handling:
        - Sheet name often has typo: 'Trasaction_Details'
        - Header row is typically row 4 (0-indexed)
        - Multi-section layout with Section A/B/C headers
        """
        records = []

        # Karvy-specific sheet names
        sheet_names = ['Trasaction_Details', 'Transaction_Details', 2]
        header_rows = [4, 3, 5]

        df = None
        for sheet in sheet_names:
            for header in header_rows:
                try:
                    df = pd.read_excel(
                        file_path,
                        sheet_name=sheet,
                        header=header,
                        engine='calamine'
                    )
                    if not df.empty and len(df.columns) > 5:
                        # Check if it looks like transaction data
                        cols = [str(c).lower() for c in df.columns]
                        if any('fund' in c or 'scheme' in c or 'folio' in c for c in cols):
                            logger.debug(f"Karvy: sheet={sheet}, header={header}")
                            break
                except Exception as e:
                    continue
            if df is not None and not df.empty:
                break

        if df is None or df.empty:
            logger.warning(f"No Karvy data found in {file_path}")
            return records

        # Convert each row to ParsedRecord
        for idx, row in df.iterrows():
            # Skip section header rows and empty rows
            first_val = str(row.iloc[0]).strip() if not pd.isna(row.iloc[0]) else ''
            if not first_val or first_val.lower() in ['fund ', 'fund', 'section', '']:
                continue

            raw_data = row.to_dict()
            raw_data = {k: (None if pd.isna(v) else v) for k, v in raw_data.items()}

            record = ParsedRecord(
                source_type=self.get_source_type(),
                source_file=str(file_path),
                raw_data=raw_data,
                row_index=idx
            )
            records.append(record)

        logger.info(f"Parsed {len(records)} raw Karvy records from {file_path.name}")
        return records


# Register parsers
ParserRegistry.register('CAMS', NormalizedCAMSParser, ['cams', 'CAMS', '_CG_'])
ParserRegistry.register('KARVY', NormalizedKarvyParser, ['karvy', 'KARVY', 'KArvy', 'kfintech'])


def demonstrate_normalization_flow():
    """
    Demonstration of the complete normalization flow.

    This example shows how to:
    1. Initialize the parser
    2. Parse a file and store in staging
    3. Review normalized records
    4. Assign user and migrate to final table
    """
    example_code = '''
    from pfas.core.database import DatabaseManager
    from pfas.parsers.mf.normalized_mf_parser import NormalizedCAMSParser
    from pfas.parsers.base import StagingPipeline, ParserRegistry
    from pathlib import Path

    # Initialize database
    db = DatabaseManager()
    conn = db.init(":memory:", "test_password")

    # Method 1: Direct parser instantiation
    parser = NormalizedCAMSParser(conn)
    result = parser.parse(Path("Data/Users/Sanjay/Mutual-Fund/CAMS/Sanjay_CAMS_CG_FY2024-25_v2.xlsx"))

    print(f"Normalized: {result.normalized_count}")
    print(f"Errors: {result.error_count}")

    # Method 2: Auto-detect parser from registry
    parser = ParserRegistry.detect_parser(
        Path("Data/Users/Sanjay/Mutual-Fund/KARVY/MF_KArvy_CG_FY24-25.xlsx"),
        conn
    )
    if parser:
        result = parser.parse(file_path)

    # Review pending records in staging
    pipeline = StagingPipeline(conn)
    pending = pipeline.get_pending_records()

    print(f"\\nPending records: {len(pending)}")
    for record in pending[:5]:
        print(f"  {record['transaction_date']} | {record['asset_name'][:40]} | {record['amount']}")

    # Assign to user
    user_id = 1  # Sanjay
    record_ids = [r['id'] for r in pending]
    pipeline.assign_user(record_ids, user_id)

    # Migrate to final table (mf_transactions)
    migrated = pipeline.migrate_to_final(user_id, 'mf_transactions')
    print(f"\\nMigrated {migrated} records to mf_transactions")
    '''
    return example_code
