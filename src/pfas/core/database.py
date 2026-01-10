"""
SQLCipher database initialization and connection management.

Provides encrypted SQLite database with all schema tables for PFAS.
Uses singleton pattern for connection management.

Thread Safety Notes:
- Uses check_same_thread=False for multi-threaded access
- WAL mode is enabled for better concurrent read performance
- Use the transaction() context manager for atomic operations
- SQLite handles locking internally in WAL mode
"""

from pathlib import Path
from typing import Optional
from contextlib import contextmanager
import threading

try:
    import sqlcipher3 as sqlite3
    HAS_SQLCIPHER = True
except ImportError:
    import sqlite3
    HAS_SQLCIPHER = False

from pfas.core.exceptions import DatabaseError


# Schema SQL for all core tables
SCHEMA_SQL = """
-- Core Tables
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pan_encrypted BLOB NOT NULL,
    pan_salt BLOB NOT NULL,
    name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    account_type TEXT NOT NULL CHECK(account_type IN ('ASSET','LIABILITY','INCOME','EXPENSE','EQUITY')),
    parent_id INTEGER,
    currency TEXT DEFAULT 'INR',
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    user_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (parent_id) REFERENCES accounts(id) ON DELETE RESTRICT,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS journals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    description TEXT NOT NULL,
    reference_type TEXT,
    reference_id INTEGER,
    created_by INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_reversed BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS journal_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    journal_id INTEGER NOT NULL,
    account_id INTEGER NOT NULL,
    debit DECIMAL(15,2) DEFAULT 0,
    credit DECIMAL(15,2) DEFAULT 0,
    currency TEXT DEFAULT 'INR',
    exchange_rate DECIMAL(10,6) DEFAULT 1.0,
    narration TEXT,
    user_id INTEGER,
    FOREIGN KEY (journal_id) REFERENCES journals(id) ON DELETE CASCADE,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE RESTRICT,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS exchange_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    from_currency TEXT NOT NULL,
    to_currency TEXT NOT NULL DEFAULT 'INR',
    rate DECIMAL(10,6) NOT NULL,
    source TEXT DEFAULT 'SBI_TT_BUYING',
    user_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, from_currency, to_currency),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    record_id INTEGER NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('INSERT','UPDATE','DELETE')),
    old_values TEXT,
    new_values TEXT,
    user_id INTEGER,
    ip_address TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    token TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Indexes for core tables
CREATE INDEX IF NOT EXISTS idx_journal_entries_journal ON journal_entries(journal_id);
CREATE INDEX IF NOT EXISTS idx_journal_entries_account ON journal_entries(account_id);
CREATE INDEX IF NOT EXISTS idx_journal_entries_user ON journal_entries(user_id);
CREATE INDEX IF NOT EXISTS idx_journals_date ON journals(date);
CREATE INDEX IF NOT EXISTS idx_journals_created_by ON journals(created_by);
CREATE INDEX IF NOT EXISTS idx_audit_log_table ON audit_log(table_name, record_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_user ON audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_exchange_rates_date ON exchange_rates(date, from_currency);
CREATE INDEX IF NOT EXISTS idx_exchange_rates_user ON exchange_rates(user_id);
CREATE INDEX IF NOT EXISTS idx_accounts_code ON accounts(code);
CREATE INDEX IF NOT EXISTS idx_accounts_parent ON accounts(parent_id);
CREATE INDEX IF NOT EXISTS idx_accounts_user ON accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);

-- Bank Account Tables
CREATE TABLE IF NOT EXISTS bank_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_number_encrypted BLOB NOT NULL,
    account_number_salt BLOB NOT NULL,
    account_number_last4 TEXT NOT NULL,
    bank_name TEXT NOT NULL,
    branch TEXT,
    ifsc_code TEXT,
    account_type TEXT DEFAULT 'SAVINGS' CHECK(account_type IN ('SAVINGS', 'CURRENT', 'FD', 'RD')),
    opening_date DATE,
    user_id INTEGER,
    account_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS bank_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_account_id INTEGER NOT NULL,
    date DATE NOT NULL,
    value_date DATE,
    description TEXT NOT NULL,
    reference_number TEXT,
    debit DECIMAL(15,2) DEFAULT 0,
    credit DECIMAL(15,2) DEFAULT 0,
    balance DECIMAL(15,2),
    category TEXT,
    is_interest BOOLEAN DEFAULT FALSE,
    is_reconciled BOOLEAN DEFAULT FALSE,
    source_file TEXT,
    user_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(bank_account_id, date, description, debit, credit),
    FOREIGN KEY (bank_account_id) REFERENCES bank_accounts(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS bank_interest_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_account_id INTEGER NOT NULL,
    financial_year TEXT NOT NULL,
    total_interest DECIMAL(15,2) NOT NULL,
    tds_deducted DECIMAL(15,2) DEFAULT 0,
    section_80tta_eligible DECIMAL(15,2) DEFAULT 0,
    user_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(bank_account_id, financial_year),
    FOREIGN KEY (bank_account_id) REFERENCES bank_accounts(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- Indexes for bank tables
CREATE INDEX IF NOT EXISTS idx_bank_accounts_user ON bank_accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_bank_accounts_account ON bank_accounts(account_id);
CREATE INDEX IF NOT EXISTS idx_bank_txn_date ON bank_transactions(date);
CREATE INDEX IF NOT EXISTS idx_bank_txn_account ON bank_transactions(bank_account_id);
CREATE INDEX IF NOT EXISTS idx_bank_txn_user ON bank_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_bank_txn_interest ON bank_transactions(is_interest);
CREATE INDEX IF NOT EXISTS idx_bank_interest_user ON bank_interest_summary(user_id);

-- Automatic Audit Logging Triggers
-- These triggers automatically populate the audit_log table for all data changes

-- Journals table triggers
CREATE TRIGGER IF NOT EXISTS audit_journals_insert
AFTER INSERT ON journals
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, new_values, user_id, timestamp)
    VALUES (
        'journals',
        NEW.id,
        'INSERT',
        json_object(
            'id', NEW.id,
            'date', NEW.date,
            'description', NEW.description,
            'reference_type', NEW.reference_type,
            'reference_id', NEW.reference_id,
            'created_by', NEW.created_by,
            'is_reversed', NEW.is_reversed
        ),
        NEW.created_by,
        CURRENT_TIMESTAMP
    );
END;

CREATE TRIGGER IF NOT EXISTS audit_journals_update
AFTER UPDATE ON journals
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, old_values, new_values, user_id, timestamp)
    VALUES (
        'journals',
        NEW.id,
        'UPDATE',
        json_object(
            'date', OLD.date,
            'description', OLD.description,
            'reference_type', OLD.reference_type,
            'reference_id', OLD.reference_id,
            'is_reversed', OLD.is_reversed
        ),
        json_object(
            'date', NEW.date,
            'description', NEW.description,
            'reference_type', NEW.reference_type,
            'reference_id', NEW.reference_id,
            'is_reversed', NEW.is_reversed
        ),
        NEW.created_by,
        CURRENT_TIMESTAMP
    );
END;

CREATE TRIGGER IF NOT EXISTS audit_journals_delete
AFTER DELETE ON journals
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, old_values, user_id, timestamp)
    VALUES (
        'journals',
        OLD.id,
        'DELETE',
        json_object(
            'date', OLD.date,
            'description', OLD.description,
            'reference_type', OLD.reference_type,
            'reference_id', OLD.reference_id,
            'created_by', OLD.created_by,
            'is_reversed', OLD.is_reversed
        ),
        OLD.created_by,
        CURRENT_TIMESTAMP
    );
END;

-- Journal entries table triggers
CREATE TRIGGER IF NOT EXISTS audit_journal_entries_insert
AFTER INSERT ON journal_entries
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, new_values, user_id, timestamp)
    VALUES (
        'journal_entries',
        NEW.id,
        'INSERT',
        json_object(
            'id', NEW.id,
            'journal_id', NEW.journal_id,
            'account_id', NEW.account_id,
            'debit', NEW.debit,
            'credit', NEW.credit,
            'currency', NEW.currency,
            'exchange_rate', NEW.exchange_rate,
            'narration', NEW.narration
        ),
        NEW.user_id,
        CURRENT_TIMESTAMP
    );
END;

CREATE TRIGGER IF NOT EXISTS audit_journal_entries_update
AFTER UPDATE ON journal_entries
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, old_values, new_values, user_id, timestamp)
    VALUES (
        'journal_entries',
        NEW.id,
        'UPDATE',
        json_object(
            'account_id', OLD.account_id,
            'debit', OLD.debit,
            'credit', OLD.credit,
            'currency', OLD.currency,
            'exchange_rate', OLD.exchange_rate,
            'narration', OLD.narration
        ),
        json_object(
            'account_id', NEW.account_id,
            'debit', NEW.debit,
            'credit', NEW.credit,
            'currency', NEW.currency,
            'exchange_rate', NEW.exchange_rate,
            'narration', NEW.narration
        ),
        NEW.user_id,
        CURRENT_TIMESTAMP
    );
END;

CREATE TRIGGER IF NOT EXISTS audit_journal_entries_delete
AFTER DELETE ON journal_entries
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, old_values, user_id, timestamp)
    VALUES (
        'journal_entries',
        OLD.id,
        'DELETE',
        json_object(
            'journal_id', OLD.journal_id,
            'account_id', OLD.account_id,
            'debit', OLD.debit,
            'credit', OLD.credit,
            'currency', OLD.currency,
            'exchange_rate', OLD.exchange_rate,
            'narration', OLD.narration
        ),
        OLD.user_id,
        CURRENT_TIMESTAMP
    );
END;

-- Accounts table triggers
CREATE TRIGGER IF NOT EXISTS audit_accounts_insert
AFTER INSERT ON accounts
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, new_values, user_id, timestamp)
    VALUES (
        'accounts',
        NEW.id,
        'INSERT',
        json_object(
            'id', NEW.id,
            'code', NEW.code,
            'name', NEW.name,
            'account_type', NEW.account_type,
            'parent_id', NEW.parent_id,
            'currency', NEW.currency,
            'is_active', NEW.is_active
        ),
        NEW.user_id,
        CURRENT_TIMESTAMP
    );
END;

CREATE TRIGGER IF NOT EXISTS audit_accounts_update
AFTER UPDATE ON accounts
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, old_values, new_values, user_id, timestamp)
    VALUES (
        'accounts',
        NEW.id,
        'UPDATE',
        json_object(
            'code', OLD.code,
            'name', OLD.name,
            'account_type', OLD.account_type,
            'parent_id', OLD.parent_id,
            'currency', OLD.currency,
            'is_active', OLD.is_active
        ),
        json_object(
            'code', NEW.code,
            'name', NEW.name,
            'account_type', NEW.account_type,
            'parent_id', NEW.parent_id,
            'currency', NEW.currency,
            'is_active', NEW.is_active
        ),
        NEW.user_id,
        CURRENT_TIMESTAMP
    );
END;

CREATE TRIGGER IF NOT EXISTS audit_accounts_delete
AFTER DELETE ON accounts
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, old_values, user_id, timestamp)
    VALUES (
        'accounts',
        OLD.id,
        'DELETE',
        json_object(
            'code', OLD.code,
            'name', OLD.name,
            'account_type', OLD.account_type,
            'parent_id', OLD.parent_id,
            'currency', OLD.currency,
            'is_active', OLD.is_active
        ),
        OLD.user_id,
        CURRENT_TIMESTAMP
    );
END;

-- Exchange rates table triggers
CREATE TRIGGER IF NOT EXISTS audit_exchange_rates_insert
AFTER INSERT ON exchange_rates
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, new_values, user_id, timestamp)
    VALUES (
        'exchange_rates',
        NEW.id,
        'INSERT',
        json_object(
            'id', NEW.id,
            'date', NEW.date,
            'from_currency', NEW.from_currency,
            'to_currency', NEW.to_currency,
            'rate', NEW.rate,
            'source', NEW.source
        ),
        NEW.user_id,
        CURRENT_TIMESTAMP
    );
END;

CREATE TRIGGER IF NOT EXISTS audit_exchange_rates_update
AFTER UPDATE ON exchange_rates
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, old_values, new_values, user_id, timestamp)
    VALUES (
        'exchange_rates',
        NEW.id,
        'UPDATE',
        json_object(
            'date', OLD.date,
            'from_currency', OLD.from_currency,
            'to_currency', OLD.to_currency,
            'rate', OLD.rate,
            'source', OLD.source
        ),
        json_object(
            'date', NEW.date,
            'from_currency', NEW.from_currency,
            'to_currency', NEW.to_currency,
            'rate', NEW.rate,
            'source', NEW.source
        ),
        NEW.user_id,
        CURRENT_TIMESTAMP
    );
END;

CREATE TRIGGER IF NOT EXISTS audit_exchange_rates_delete
AFTER DELETE ON exchange_rates
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, old_values, user_id, timestamp)
    VALUES (
        'exchange_rates',
        OLD.id,
        'DELETE',
        json_object(
            'date', OLD.date,
            'from_currency', OLD.from_currency,
            'to_currency', OLD.to_currency,
            'rate', OLD.rate,
            'source', OLD.source
        ),
        OLD.user_id,
        CURRENT_TIMESTAMP
    );
END;

-- Bank accounts table triggers
CREATE TRIGGER IF NOT EXISTS audit_bank_accounts_insert
AFTER INSERT ON bank_accounts
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, new_values, user_id, timestamp)
    VALUES (
        'bank_accounts',
        NEW.id,
        'INSERT',
        json_object(
            'id', NEW.id,
            'account_number_last4', NEW.account_number_last4,
            'bank_name', NEW.bank_name,
            'branch', NEW.branch,
            'ifsc_code', NEW.ifsc_code,
            'account_type', NEW.account_type,
            'opening_date', NEW.opening_date
        ),
        NEW.user_id,
        CURRENT_TIMESTAMP
    );
END;

CREATE TRIGGER IF NOT EXISTS audit_bank_accounts_update
AFTER UPDATE ON bank_accounts
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, old_values, new_values, user_id, timestamp)
    VALUES (
        'bank_accounts',
        NEW.id,
        'UPDATE',
        json_object(
            'account_number_last4', OLD.account_number_last4,
            'bank_name', OLD.bank_name,
            'branch', OLD.branch,
            'ifsc_code', OLD.ifsc_code,
            'account_type', OLD.account_type,
            'opening_date', OLD.opening_date
        ),
        json_object(
            'account_number_last4', NEW.account_number_last4,
            'bank_name', NEW.bank_name,
            'branch', NEW.branch,
            'ifsc_code', NEW.ifsc_code,
            'account_type', NEW.account_type,
            'opening_date', NEW.opening_date
        ),
        NEW.user_id,
        CURRENT_TIMESTAMP
    );
END;

CREATE TRIGGER IF NOT EXISTS audit_bank_accounts_delete
AFTER DELETE ON bank_accounts
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, old_values, user_id, timestamp)
    VALUES (
        'bank_accounts',
        OLD.id,
        'DELETE',
        json_object(
            'account_number_last4', OLD.account_number_last4,
            'bank_name', OLD.bank_name,
            'branch', OLD.branch,
            'ifsc_code', OLD.ifsc_code,
            'account_type', OLD.account_type,
            'opening_date', OLD.opening_date
        ),
        OLD.user_id,
        CURRENT_TIMESTAMP
    );
END;

-- Bank transactions table triggers (limited fields to avoid sensitive data)
CREATE TRIGGER IF NOT EXISTS audit_bank_transactions_insert
AFTER INSERT ON bank_transactions
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, new_values, user_id, timestamp)
    VALUES (
        'bank_transactions',
        NEW.id,
        'INSERT',
        json_object(
            'id', NEW.id,
            'bank_account_id', NEW.bank_account_id,
            'date', NEW.date,
            'debit', NEW.debit,
            'credit', NEW.credit,
            'category', NEW.category,
            'is_reconciled', NEW.is_reconciled
        ),
        NEW.user_id,
        CURRENT_TIMESTAMP
    );
END;

CREATE TRIGGER IF NOT EXISTS audit_bank_transactions_update
AFTER UPDATE ON bank_transactions
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, old_values, new_values, user_id, timestamp)
    VALUES (
        'bank_transactions',
        NEW.id,
        'UPDATE',
        json_object(
            'category', OLD.category,
            'is_reconciled', OLD.is_reconciled
        ),
        json_object(
            'category', NEW.category,
            'is_reconciled', NEW.is_reconciled
        ),
        NEW.user_id,
        CURRENT_TIMESTAMP
    );
END;

CREATE TRIGGER IF NOT EXISTS audit_bank_transactions_delete
AFTER DELETE ON bank_transactions
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, old_values, user_id, timestamp)
    VALUES (
        'bank_transactions',
        OLD.id,
        'DELETE',
        json_object(
            'bank_account_id', OLD.bank_account_id,
            'date', OLD.date,
            'debit', OLD.debit,
            'credit', OLD.credit,
            'category', OLD.category
        ),
        OLD.user_id,
        CURRENT_TIMESTAMP
    );
END;

-- Mutual Fund Tables
CREATE TABLE IF NOT EXISTS mf_amcs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    short_name TEXT,
    website TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mf_schemes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    amc_id INTEGER,
    name TEXT NOT NULL,
    isin TEXT UNIQUE,
    asset_class TEXT NOT NULL CHECK(asset_class IN ('EQUITY', 'DEBT', 'HYBRID', 'OTHER')),
    scheme_type TEXT,
    nav_31jan2018 DECIMAL(15,4),
    user_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (amc_id) REFERENCES mf_amcs(id) ON DELETE RESTRICT,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS mf_folios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    scheme_id INTEGER,
    folio_number TEXT NOT NULL,
    status TEXT DEFAULT 'ACTIVE',
    opening_date DATE,
    account_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, scheme_id, folio_number),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (scheme_id) REFERENCES mf_schemes(id) ON DELETE RESTRICT,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS mf_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    folio_id INTEGER NOT NULL,
    transaction_type TEXT NOT NULL CHECK(transaction_type IN
        ('PURCHASE', 'REDEMPTION', 'SWITCH_IN', 'SWITCH_OUT', 'DIVIDEND', 'DIVIDEND_REINVEST')),
    date DATE NOT NULL,
    units DECIMAL(15,4) NOT NULL,
    nav DECIMAL(15,4) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    stt DECIMAL(15,2) DEFAULT 0,
    stamp_duty DECIMAL(15,2) DEFAULT 0,
    purchase_date DATE,
    purchase_units DECIMAL(15,4),
    purchase_nav DECIMAL(15,4),
    purchase_amount DECIMAL(15,2),
    grandfathered_units DECIMAL(15,4),
    grandfathered_nav DECIMAL(15,4),
    grandfathered_value DECIMAL(15,2),
    holding_period_days INTEGER,
    is_long_term BOOLEAN,
    cost_of_acquisition DECIMAL(15,2),
    indexed_cost DECIMAL(15,2),
    short_term_gain DECIMAL(15,2),
    long_term_gain DECIMAL(15,2),
    tax_percentage DECIMAL(5,2),
    user_id INTEGER,
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (folio_id) REFERENCES mf_folios(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS mf_capital_gains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    financial_year TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    stcg_amount DECIMAL(15,2) DEFAULT 0,
    ltcg_amount DECIMAL(15,2) DEFAULT 0,
    ltcg_exemption DECIMAL(15,2) DEFAULT 0,
    taxable_stcg DECIMAL(15,2) DEFAULT 0,
    taxable_ltcg DECIMAL(15,2) DEFAULT 0,
    stcg_tax_rate DECIMAL(5,2),
    ltcg_tax_rate DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, financial_year, asset_class),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- Indexes for MF tables
CREATE INDEX IF NOT EXISTS idx_mf_schemes_amc ON mf_schemes(amc_id);
CREATE INDEX IF NOT EXISTS idx_mf_schemes_isin ON mf_schemes(isin);
CREATE INDEX IF NOT EXISTS idx_mf_schemes_user ON mf_schemes(user_id);
CREATE INDEX IF NOT EXISTS idx_mf_folios_user ON mf_folios(user_id);
CREATE INDEX IF NOT EXISTS idx_mf_folios_scheme ON mf_folios(scheme_id);
CREATE INDEX IF NOT EXISTS idx_mf_txn_folio ON mf_transactions(folio_id);
CREATE INDEX IF NOT EXISTS idx_mf_txn_date ON mf_transactions(date);
CREATE INDEX IF NOT EXISTS idx_mf_txn_type ON mf_transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_mf_txn_user ON mf_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_mf_cg_user_fy ON mf_capital_gains(user_id, financial_year);

-- Stock Tables
CREATE TABLE IF NOT EXISTS stock_brokers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    broker_code TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stock_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    broker_id INTEGER,
    user_id INTEGER,
    symbol TEXT NOT NULL,
    isin TEXT,
    trade_date DATE NOT NULL,
    trade_type TEXT NOT NULL CHECK(trade_type IN ('BUY', 'SELL')),
    quantity INTEGER NOT NULL,
    price DECIMAL(15,2) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    brokerage DECIMAL(15,2) DEFAULT 0,
    stt DECIMAL(15,2) DEFAULT 0,
    exchange_charges DECIMAL(15,2) DEFAULT 0,
    gst DECIMAL(15,2) DEFAULT 0,
    sebi_charges DECIMAL(15,2) DEFAULT 0,
    stamp_duty DECIMAL(15,2) DEFAULT 0,
    net_amount DECIMAL(15,2) NOT NULL,
    trade_category TEXT CHECK(trade_category IN ('INTRADAY', 'DELIVERY', 'FNO')),
    buy_date DATE,
    buy_price DECIMAL(15,2),
    cost_of_acquisition DECIMAL(15,2),
    holding_period_days INTEGER,
    is_long_term BOOLEAN,
    capital_gain DECIMAL(15,2),
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (broker_id) REFERENCES stock_brokers(id) ON DELETE RESTRICT,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS stock_capital_gains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    financial_year TEXT NOT NULL,
    trade_category TEXT NOT NULL CHECK(trade_category IN ('INTRADAY', 'DELIVERY', 'FNO')),
    stcg_amount DECIMAL(15,2) DEFAULT 0,
    ltcg_amount DECIMAL(15,2) DEFAULT 0,
    ltcg_exemption DECIMAL(15,2) DEFAULT 0,
    taxable_stcg DECIMAL(15,2) DEFAULT 0,
    taxable_ltcg DECIMAL(15,2) DEFAULT 0,
    speculative_income DECIMAL(15,2) DEFAULT 0,
    stcg_tax_rate DECIMAL(5,2),
    ltcg_tax_rate DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, financial_year, trade_category),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- Indexes for stock tables
CREATE INDEX IF NOT EXISTS idx_stock_trades_user ON stock_trades(user_id);
CREATE INDEX IF NOT EXISTS idx_stock_trades_broker ON stock_trades(broker_id);
CREATE INDEX IF NOT EXISTS idx_stock_trades_date ON stock_trades(trade_date);
CREATE INDEX IF NOT EXISTS idx_stock_trades_symbol ON stock_trades(symbol);
CREATE INDEX IF NOT EXISTS idx_stock_trades_type ON stock_trades(trade_type);
CREATE INDEX IF NOT EXISTS idx_stock_cg_user_fy ON stock_capital_gains(user_id, financial_year);

-- EPF (Employee Provident Fund) Tables
CREATE TABLE IF NOT EXISTS epf_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    uan TEXT UNIQUE NOT NULL,
    establishment_id TEXT NOT NULL,
    establishment_name TEXT,
    member_id TEXT NOT NULL,
    member_name TEXT,
    date_of_joining DATE,
    account_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS epf_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    epf_account_id INTEGER NOT NULL,
    wage_month TEXT NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_type TEXT NOT NULL CHECK(transaction_type IN ('CR', 'DR', 'INT')),
    wages DECIMAL(15,2),
    eps_wages DECIMAL(15,2),
    employee_contribution DECIMAL(15,2) DEFAULT 0,
    employer_contribution DECIMAL(15,2) DEFAULT 0,
    pension_contribution DECIMAL(15,2) DEFAULT 0,
    vpf_contribution DECIMAL(15,2) DEFAULT 0,
    employee_balance DECIMAL(15,2),
    employer_balance DECIMAL(15,2),
    pension_balance DECIMAL(15,2),
    source_file TEXT,
    user_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(epf_account_id, wage_month, transaction_date),
    FOREIGN KEY (epf_account_id) REFERENCES epf_accounts(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS epf_interest (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    epf_account_id INTEGER NOT NULL,
    financial_year TEXT NOT NULL,
    employee_interest DECIMAL(15,2) DEFAULT 0,
    employer_interest DECIMAL(15,2) DEFAULT 0,
    taxable_interest DECIMAL(15,2) DEFAULT 0,
    tds_deducted DECIMAL(15,2) DEFAULT 0,
    user_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(epf_account_id, financial_year),
    FOREIGN KEY (epf_account_id) REFERENCES epf_accounts(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- Indexes for EPF tables
CREATE INDEX IF NOT EXISTS idx_epf_accounts_user ON epf_accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_epf_accounts_uan ON epf_accounts(uan);
CREATE INDEX IF NOT EXISTS idx_epf_txn_account ON epf_transactions(epf_account_id);
CREATE INDEX IF NOT EXISTS idx_epf_txn_date ON epf_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_epf_txn_user ON epf_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_epf_interest_account ON epf_interest(epf_account_id);
CREATE INDEX IF NOT EXISTS idx_epf_interest_user ON epf_interest(user_id);

-- PPF (Public Provident Fund) Tables
CREATE TABLE IF NOT EXISTS ppf_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    account_number TEXT UNIQUE NOT NULL,
    bank_name TEXT NOT NULL,
    branch TEXT,
    opening_date DATE NOT NULL,
    maturity_date DATE,
    account_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS ppf_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ppf_account_id INTEGER NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_type TEXT NOT NULL CHECK(transaction_type IN ('DEPOSIT', 'INTEREST', 'WITHDRAWAL')),
    amount DECIMAL(15,2) NOT NULL,
    balance DECIMAL(15,2),
    interest_rate DECIMAL(5,2),
    financial_year TEXT,
    source_file TEXT,
    user_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ppf_account_id, transaction_date, transaction_type, amount),
    FOREIGN KEY (ppf_account_id) REFERENCES ppf_accounts(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- Indexes for PPF tables
CREATE INDEX IF NOT EXISTS idx_ppf_accounts_user ON ppf_accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_ppf_txn_account ON ppf_transactions(ppf_account_id);
CREATE INDEX IF NOT EXISTS idx_ppf_txn_date ON ppf_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_ppf_txn_type ON ppf_transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_ppf_txn_user ON ppf_transactions(user_id);

-- NPS (National Pension System) Tables
CREATE TABLE IF NOT EXISTS nps_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    pran TEXT UNIQUE NOT NULL,
    nodal_office TEXT,
    scheme_preference TEXT,
    opening_date DATE,
    account_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS nps_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nps_account_id INTEGER NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_type TEXT NOT NULL CHECK(transaction_type IN ('CONTRIBUTION', 'REDEMPTION', 'SWITCH')),
    tier TEXT NOT NULL CHECK(tier IN ('I', 'II')),
    contribution_type TEXT CHECK(contribution_type IN ('EMPLOYEE', 'EMPLOYER')),
    amount DECIMAL(15,2) NOT NULL,
    units DECIMAL(15,4),
    nav DECIMAL(15,4),
    scheme TEXT,
    financial_year TEXT,
    source_file TEXT,
    user_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(nps_account_id, transaction_date, tier, amount),
    FOREIGN KEY (nps_account_id) REFERENCES nps_accounts(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- Indexes for NPS tables
CREATE INDEX IF NOT EXISTS idx_nps_accounts_user ON nps_accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_nps_accounts_pran ON nps_accounts(pran);
CREATE INDEX IF NOT EXISTS idx_nps_txn_account ON nps_transactions(nps_account_id);
CREATE INDEX IF NOT EXISTS idx_nps_txn_date ON nps_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_nps_txn_tier ON nps_transactions(tier);
CREATE INDEX IF NOT EXISTS idx_nps_txn_user ON nps_transactions(user_id);
"""


class DatabaseManager:
    """
    Singleton manager for SQLCipher encrypted database connections.

    Usage:
        db = DatabaseManager()
        conn = db.init("/path/to/db.sqlite", "password123")
        # Use connection...
        db.close()
    """

    _instance: Optional["DatabaseManager"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "DatabaseManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._connection = None
                    cls._instance._db_path = None
        return cls._instance

    @property
    def connection(self) -> sqlite3.Connection:
        """Get the current database connection."""
        if self._connection is None:
            raise DatabaseError("Database not initialized. Call init() first.")
        return self._connection

    def init(self, db_path: str, password: str) -> sqlite3.Connection:
        """
        Initialize encrypted database.

        Args:
            db_path: Path to database file or ":memory:" for in-memory database
            password: Encryption password for SQLCipher

        Returns:
            Database connection

        Raises:
            DatabaseError: If initialization fails
        """
        try:
            self._db_path = db_path

            # Create parent directory if needed (unless in-memory)
            if db_path != ":memory:":
                Path(db_path).parent.mkdir(parents=True, exist_ok=True)

            self._connection = sqlite3.connect(db_path, check_same_thread=False)
            self._connection.row_factory = sqlite3.Row

            # Configure SQLCipher encryption
            if HAS_SQLCIPHER:
                self._connection.execute(f"PRAGMA key = '{password}'")
                self._connection.execute("PRAGMA cipher_compatibility = 4")

            # Enable foreign keys
            self._connection.execute("PRAGMA foreign_keys = ON")

            # Enable WAL mode for better concurrent access
            # WAL mode allows multiple readers and one writer simultaneously
            if db_path != ":memory:":
                self._connection.execute("PRAGMA journal_mode = WAL")
                self._connection.execute("PRAGMA synchronous = NORMAL")

            # Execute schema
            self._execute_schema()

            return self._connection

        except Exception as e:
            raise DatabaseError(f"Failed to initialize database: {e}")

    def _execute_schema(self) -> None:
        """Create all tables if not exist."""
        try:
            self._connection.executescript(SCHEMA_SQL)
            self._connection.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to execute schema: {e}")

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        """Execute a SQL statement."""
        return self.connection.execute(sql, params)

    def executemany(self, sql: str, params_list: list) -> sqlite3.Cursor:
        """Execute a SQL statement with multiple parameter sets."""
        return self.connection.executemany(sql, params_list)

    def commit(self) -> None:
        """Commit the current transaction."""
        self.connection.commit()

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.connection.rollback()

    @contextmanager
    def transaction(self):
        """
        Context manager for atomic transactions.

        Usage:
            db = DatabaseManager()
            with db.transaction():
                db.execute("INSERT INTO accounts ...")
                db.execute("INSERT INTO journals ...")
            # Auto-commits on success, auto-rolls back on exception

        Raises:
            DatabaseError: If transaction fails
        """
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            yield self.connection
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise DatabaseError(f"Transaction failed: {e}") from e

    def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            self._db_path = None

    def get_tables(self) -> list[str]:
        """Get list of all tables in database."""
        cursor = self.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        return [row[0] for row in cursor.fetchall()]

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton instance (useful for testing)."""
        with cls._lock:
            if cls._instance and cls._instance._connection:
                cls._instance._connection.close()
            cls._instance = None


def get_connection() -> sqlite3.Connection:
    """Get the current database connection (convenience function)."""
    return DatabaseManager().connection
