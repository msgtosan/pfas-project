"""
Rental Income Module for PFAS.

Handles:
- Property registration
- Monthly/annual rental income tracking
- 30% standard deduction calculation
- Section 24 home loan interest (max 2L for self-occupied)
- Loss from house property calculation

New Tax Regime: Standard deduction still applies.
"""

import sqlite3
import hashlib
from datetime import date
from decimal import Decimal
from typing import Optional, List, Dict, Any

from pfas.core.transaction_service import (
    TransactionService,
    TransactionSource,
    AssetRecord,
)
from pfas.core.journal import JournalEntry
from pfas.core.accounts import get_account_by_code

from .models import (
    Property,
    PropertyType,
    RentalIncome,
    RentalIncomeCalculation,
)


# Database schema for rental income tables
RENTAL_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS properties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    property_type TEXT NOT NULL DEFAULT 'LET_OUT',
    address TEXT NOT NULL,
    city TEXT,
    pin_code TEXT,
    tenant_name TEXT,
    acquisition_date DATE,
    acquisition_cost DECIMAL(15,2) DEFAULT 0,
    account_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS rental_income (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER NOT NULL,
    financial_year TEXT NOT NULL,
    month TEXT,
    gross_rent DECIMAL(15,2) DEFAULT 0,
    municipal_tax_paid DECIMAL(15,2) DEFAULT 0,
    source TEXT DEFAULT 'BANK_STATEMENT',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(property_id, financial_year, month),
    FOREIGN KEY (property_id) REFERENCES properties(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS home_loan_interest (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER NOT NULL,
    financial_year TEXT NOT NULL,
    lender_name TEXT,
    loan_account_number TEXT,
    total_interest_paid DECIMAL(15,2) DEFAULT 0,
    principal_repaid DECIMAL(15,2) DEFAULT 0,
    pre_construction_interest DECIMAL(15,2) DEFAULT 0,
    section_24_eligible DECIMAL(15,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(property_id, financial_year),
    FOREIGN KEY (property_id) REFERENCES properties(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_properties_user ON properties(user_id);
CREATE INDEX IF NOT EXISTS idx_rental_income_property ON rental_income(property_id);
CREATE INDEX IF NOT EXISTS idx_rental_income_fy ON rental_income(financial_year);
CREATE INDEX IF NOT EXISTS idx_home_loan_property ON home_loan_interest(property_id);
"""


class RentalIncomeCalculator:
    """Calculate income from house property."""

    STANDARD_DEDUCTION_RATE = Decimal("0.30")  # 30%
    MAX_INTEREST_SELF_OCCUPIED = Decimal("200000")  # Rs 2L
    MAX_LOSS_SETOFF = Decimal("200000")  # Rs 2L loss can be set off

    def calculate(
        self,
        gross_rent: Decimal,
        municipal_tax: Decimal,
        home_loan_interest: Decimal = Decimal("0"),
        property_type: PropertyType = PropertyType.LET_OUT
    ) -> RentalIncomeCalculation:
        """
        Calculate income from house property.

        For let-out property:
        1. Gross Annual Value = Actual rent received
        2. Less: Municipal taxes paid (actual)
        3. Net Annual Value (NAV)
        4. Less: Standard deduction (30% of NAV)
        5. Less: Interest on home loan
        6. Income from House Property (can be negative)

        For self-occupied:
        - GAV = 0, only home loan interest deduction allowed (max 2L)
        """
        # Ensure Decimal types
        gross_rent = Decimal(str(gross_rent))
        municipal_tax = Decimal(str(municipal_tax))
        home_loan_interest = Decimal(str(home_loan_interest))

        # For self-occupied: GAV = 0
        if property_type == PropertyType.SELF_OCCUPIED:
            gross_rent = Decimal("0")
            municipal_tax = Decimal("0")

        # Net Annual Value
        nav = gross_rent - municipal_tax

        # Standard deduction (30% of NAV)
        std_deduction = nav * self.STANDARD_DEDUCTION_RATE

        # Cap interest for self-occupied
        if property_type == PropertyType.SELF_OCCUPIED:
            eligible_interest = min(home_loan_interest, self.MAX_INTEREST_SELF_OCCUPIED)
        else:
            eligible_interest = home_loan_interest  # No cap for let-out

        # Income from HP
        income_hp = nav - std_deduction - eligible_interest

        return RentalIncomeCalculation(
            gross_rent=gross_rent,
            municipal_tax=municipal_tax,
            net_annual_value=nav,
            standard_deduction=std_deduction,
            home_loan_interest=eligible_interest,
            income_from_hp=income_hp
        )

    def calculate_loss_setoff(self, hp_loss: Decimal) -> Decimal:
        """
        Calculate how much HP loss can be set off against other income.

        Max Rs 2L can be set off in current year.
        Remaining carried forward for 8 years.
        """
        hp_loss = Decimal(str(hp_loss))

        if hp_loss >= 0:
            return Decimal("0")

        return min(abs(hp_loss), self.MAX_LOSS_SETOFF)


class RentalIncomeManager:
    """Manage rental income records with database operations."""

    def __init__(self, db_connection: sqlite3.Connection):
        """Initialize with database connection."""
        self.conn = db_connection
        self.calculator = RentalIncomeCalculator()
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize database schema."""
        self.conn.executescript(RENTAL_SCHEMA_SQL)
        self.conn.commit()

    def add_property(self, property: Property) -> int:
        """Add a new property via TransactionService."""
        txn_service = TransactionService(self.conn)

        # Generate idempotency key from address
        address_hash = hashlib.sha256(property.address.encode()).hexdigest()[:12]
        idempotency_key = f"property:{property.user_id}:{address_hash}"

        asset_record = AssetRecord(
            table_name="properties",
            data={
                "user_id": property.user_id,
                "property_type": property.property_type.value,
                "address": property.address,
                "city": property.city,
                "pin_code": property.pin_code,
                "tenant_name": property.tenant_name,
                "acquisition_date": property.acquisition_date.isoformat() if property.acquisition_date else None,
                "acquisition_cost": str(property.acquisition_cost),
                "account_id": property.account_id,
            },
            on_conflict="IGNORE"
        )

        result = txn_service.record_asset_only(
            user_id=property.user_id or 1,
            asset_records=[asset_record],
            idempotency_key=idempotency_key,
            source=TransactionSource.MANUAL,
            description=f"Property: {property.address[:50]}",
        )

        if result.asset_record_ids.get("properties"):
            return result.asset_record_ids["properties"]

        # If insert was ignored, fetch existing
        cursor = self.conn.execute(
            "SELECT id FROM properties WHERE user_id = ? AND address = ?",
            (property.user_id, property.address)
        )
        row = cursor.fetchone()
        return row['id'] if isinstance(row, dict) else row[0] if row else 0

    def get_property(self, property_id: int) -> Optional[Property]:
        """Get property by ID."""
        cursor = self.conn.execute(
            "SELECT * FROM properties WHERE id = ?", (property_id,)
        )
        row = cursor.fetchone()
        if row:
            return self._row_to_property(row)
        return None

    def get_properties_by_user(self, user_id: int) -> List[Property]:
        """Get all properties for a user."""
        cursor = self.conn.execute(
            "SELECT * FROM properties WHERE user_id = ?", (user_id,)
        )
        return [self._row_to_property(row) for row in cursor.fetchall()]

    def add_rental_income(self, income: RentalIncome, user_id: int = 1) -> int:
        """
        Add rental income record via TransactionService.

        Creates journal entry:
            Dr Bank Account (1101)     | Gross Rent
            Cr Rental Income (4401)    | Gross Rent
        """
        txn_service = TransactionService(self.conn)

        # Generate idempotency key
        idempotency_key = f"rental:{income.property_id}:{income.financial_year}:{income.month}"

        # Create journal entries
        entries = self._create_rental_journal_entries(income, user_id)

        asset_record = AssetRecord(
            table_name="rental_income",
            data={
                "property_id": income.property_id,
                "financial_year": income.financial_year,
                "month": income.month,
                "gross_rent": str(income.gross_rent),
                "municipal_tax_paid": str(income.municipal_tax_paid),
                "source": income.source,
            },
            on_conflict="REPLACE"  # Allow updates for same property/FY/month
        )

        # Try to parse month to get transaction date
        try:
            from datetime import datetime
            txn_date = datetime.strptime(income.month, "%b-%Y").date()
        except:
            txn_date = date.today()

        result = txn_service.record(
            user_id=user_id,
            entries=entries,
            description=f"Rental income: {income.month}",
            source=TransactionSource.MANUAL,
            idempotency_key=idempotency_key,
            txn_date=txn_date,
            reference_type="RENTAL_INCOME",
            asset_records=[asset_record],
        )

        if result.asset_record_ids.get("rental_income"):
            return result.asset_record_ids["rental_income"]

        # If insert/replace happened, return existing/new
        cursor = self.conn.execute(
            "SELECT id FROM rental_income WHERE property_id = ? AND financial_year = ? AND month = ?",
            (income.property_id, income.financial_year, income.month)
        )
        row = cursor.fetchone()
        return row['id'] if isinstance(row, dict) else row[0] if row else 0

    def _create_rental_journal_entries(self, income: RentalIncome, user_id: int) -> List[JournalEntry]:
        """Create journal entries for rental income."""
        entries = []

        try:
            bank_account = get_account_by_code(self.conn, "1101")  # Bank - Savings
            rental_income_account = get_account_by_code(self.conn, "4401")  # Gross Rental Income

            if not bank_account or not rental_income_account:
                return entries

            if income.gross_rent > Decimal("0"):
                # Dr Bank | Cr Rental Income
                entries.append(JournalEntry(
                    account_id=bank_account.id,
                    debit=income.gross_rent,
                    narration=f"Rental income: {income.month}"
                ))
                entries.append(JournalEntry(
                    account_id=rental_income_account.id,
                    credit=income.gross_rent,
                    narration=f"Rental income: {income.month}"
                ))
        except Exception:
            # If accounts table doesn't exist, skip journal entries
            pass

        return entries

    def add_rental_income_from_bank(
        self,
        property_id: int,
        financial_year: str,
        amount: Decimal,
        payment_date: date,
        source_description: str = ""
    ) -> int:
        """
        Add rental income from bank statement.

        Extracts month from payment date.
        """
        month = payment_date.strftime("%b-%Y")  # e.g., "Apr-2024"

        income = RentalIncome(
            property_id=property_id,
            financial_year=financial_year,
            month=month,
            gross_rent=amount,
            municipal_tax_paid=Decimal("0"),
            source="BANK_STATEMENT"
        )

        return self.add_rental_income(income)

    def get_annual_rental_income(
        self,
        property_id: int,
        financial_year: str
    ) -> Decimal:
        """Get total rental income for a property in a financial year."""
        cursor = self.conn.execute(
            """
            SELECT COALESCE(SUM(CAST(gross_rent AS REAL)), 0) as total
            FROM rental_income
            WHERE property_id = ? AND financial_year = ?
            """,
            (property_id, financial_year)
        )
        row = cursor.fetchone()
        return Decimal(str(row[0])) if row else Decimal("0")

    def get_annual_municipal_tax(
        self,
        property_id: int,
        financial_year: str
    ) -> Decimal:
        """Get total municipal tax paid for a property in a financial year."""
        cursor = self.conn.execute(
            """
            SELECT COALESCE(SUM(CAST(municipal_tax_paid AS REAL)), 0) as total
            FROM rental_income
            WHERE property_id = ? AND financial_year = ?
            """,
            (property_id, financial_year)
        )
        row = cursor.fetchone()
        return Decimal(str(row[0])) if row else Decimal("0")

    def add_home_loan_interest(
        self,
        property_id: int,
        financial_year: str,
        interest_paid: Decimal,
        principal_repaid: Decimal = Decimal("0"),
        lender_name: str = "",
        loan_account: str = ""
    ) -> int:
        """Add home loan interest for Section 24 deduction."""
        # Get property type to determine cap
        prop = self.get_property(property_id)
        if prop and prop.property_type == PropertyType.SELF_OCCUPIED:
            section_24_eligible = min(interest_paid, Decimal("200000"))
        else:
            section_24_eligible = interest_paid

        cursor = self.conn.execute(
            """
            INSERT OR REPLACE INTO home_loan_interest
            (property_id, financial_year, lender_name, loan_account_number,
             total_interest_paid, principal_repaid, section_24_eligible)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                property_id,
                financial_year,
                lender_name,
                loan_account,
                str(interest_paid),
                str(principal_repaid),
                str(section_24_eligible),
            )
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_home_loan_interest(
        self,
        property_id: int,
        financial_year: str
    ) -> Decimal:
        """Get Section 24 eligible home loan interest."""
        cursor = self.conn.execute(
            """
            SELECT section_24_eligible FROM home_loan_interest
            WHERE property_id = ? AND financial_year = ?
            """,
            (property_id, financial_year)
        )
        row = cursor.fetchone()
        return Decimal(str(row[0])) if row else Decimal("0")

    def calculate_hp_income(
        self,
        property_id: int,
        financial_year: str
    ) -> RentalIncomeCalculation:
        """
        Calculate income from house property for a financial year.

        Aggregates all rental income and applies deductions.
        """
        prop = self.get_property(property_id)
        if not prop:
            raise ValueError(f"Property {property_id} not found")

        gross_rent = self.get_annual_rental_income(property_id, financial_year)
        municipal_tax = self.get_annual_municipal_tax(property_id, financial_year)
        home_loan_interest = self.get_home_loan_interest(property_id, financial_year)

        return self.calculator.calculate(
            gross_rent=gross_rent,
            municipal_tax=municipal_tax,
            home_loan_interest=home_loan_interest,
            property_type=prop.property_type
        )

    def get_all_hp_income(
        self,
        user_id: int,
        financial_year: str
    ) -> Dict[str, Any]:
        """
        Get aggregate HP income across all properties for a user.

        Returns dict with total income and property-wise breakdown.
        """
        properties = self.get_properties_by_user(user_id)

        total_hp_income = Decimal("0")
        property_breakdown = []

        for prop in properties:
            calc = self.calculate_hp_income(prop.id, financial_year)
            total_hp_income += calc.income_from_hp

            property_breakdown.append({
                "property_id": prop.id,
                "address": prop.address,
                "type": prop.property_type.value,
                "gross_rent": calc.gross_rent,
                "municipal_tax": calc.municipal_tax,
                "nav": calc.net_annual_value,
                "standard_deduction": calc.standard_deduction,
                "home_loan_interest": calc.home_loan_interest,
                "income_from_hp": calc.income_from_hp,
            })

        # Calculate loss setoff if applicable
        loss_setoff = Decimal("0")
        if total_hp_income < 0:
            loss_setoff = self.calculator.calculate_loss_setoff(total_hp_income)

        return {
            "financial_year": financial_year,
            "total_hp_income": total_hp_income,
            "loss_setoff_allowed": loss_setoff,
            "loss_carry_forward": max(Decimal("0"), abs(total_hp_income) - loss_setoff),
            "properties": property_breakdown,
        }

    def _row_to_property(self, row: sqlite3.Row) -> Property:
        """Convert database row to Property object."""
        return Property(
            id=row["id"],
            user_id=row["user_id"],
            property_type=PropertyType(row["property_type"]),
            address=row["address"],
            city=row["city"],
            pin_code=row["pin_code"],
            tenant_name=row["tenant_name"],
            acquisition_date=date.fromisoformat(row["acquisition_date"]) if row["acquisition_date"] else None,
            acquisition_cost=Decimal(str(row["acquisition_cost"])) if row["acquisition_cost"] else Decimal("0"),
            account_id=row["account_id"],
        )
