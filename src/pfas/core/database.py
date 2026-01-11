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
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT,
    -- Unique constraint for duplicate detection
    -- Uses folio_id, date, transaction_type, amount, and purchase_date to identify unique transactions
    -- purchase_date differentiates multiple redemption lots on the same day
    UNIQUE(folio_id, date, transaction_type, amount, purchase_date)
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

-- Stock Dividends Table (REQ-STK-003)
CREATE TABLE IF NOT EXISTS stock_dividends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    broker_id INTEGER,
    symbol TEXT NOT NULL,
    isin TEXT,
    dividend_date DATE NOT NULL,
    quantity INTEGER NOT NULL,
    dividend_per_share DECIMAL(15,4) NOT NULL,
    gross_amount DECIMAL(15,2) NOT NULL,
    tds_amount DECIMAL(15,2) DEFAULT 0,
    net_amount DECIMAL(15,2) NOT NULL,
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, symbol, dividend_date, quantity),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (broker_id) REFERENCES stock_brokers(id) ON DELETE RESTRICT
);

-- Stock Dividend Summary Table
CREATE TABLE IF NOT EXISTS stock_dividend_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    financial_year TEXT NOT NULL,
    total_dividend DECIMAL(15,2) DEFAULT 0,
    total_tds DECIMAL(15,2) DEFAULT 0,
    net_dividend DECIMAL(15,2) DEFAULT 0,
    dividend_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, financial_year),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- STT Ledger Table (REQ-STK-006)
CREATE TABLE IF NOT EXISTS stock_stt_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    broker_id INTEGER,
    trade_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    trade_type TEXT NOT NULL CHECK(trade_type IN ('BUY', 'SELL')),
    trade_category TEXT NOT NULL CHECK(trade_category IN ('INTRADAY', 'DELIVERY', 'FNO')),
    trade_value DECIMAL(15,2) NOT NULL,
    stt_amount DECIMAL(15,2) NOT NULL,
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (broker_id) REFERENCES stock_brokers(id) ON DELETE RESTRICT
);

-- STT Summary Table
CREATE TABLE IF NOT EXISTS stock_stt_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    financial_year TEXT NOT NULL,
    delivery_stt DECIMAL(15,2) DEFAULT 0,
    intraday_stt DECIMAL(15,2) DEFAULT 0,
    fno_stt DECIMAL(15,2) DEFAULT 0,
    total_stt DECIMAL(15,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, financial_year),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- Indexes for dividend and STT tables
CREATE INDEX IF NOT EXISTS idx_stock_dividends_user ON stock_dividends(user_id);
CREATE INDEX IF NOT EXISTS idx_stock_dividends_date ON stock_dividends(dividend_date);
CREATE INDEX IF NOT EXISTS idx_stock_dividends_symbol ON stock_dividends(symbol);
CREATE INDEX IF NOT EXISTS idx_stock_stt_user ON stock_stt_ledger(user_id);
CREATE INDEX IF NOT EXISTS idx_stock_stt_date ON stock_stt_ledger(trade_date);
CREATE INDEX IF NOT EXISTS idx_stock_stt_category ON stock_stt_ledger(trade_category);

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

-- Salary and Form 16 Tables (REQ-SAL-001 to REQ-SAL-012)

-- Employers table
CREATE TABLE IF NOT EXISTS employers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    tan TEXT UNIQUE NOT NULL,
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Salary Records table (monthly payslips)
CREATE TABLE IF NOT EXISTS salary_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    employer_id INTEGER REFERENCES employers(id),
    pay_period TEXT NOT NULL,
    pay_date DATE,

    -- Earnings
    basic_salary DECIMAL(15,2) DEFAULT 0,
    hra DECIMAL(15,2) DEFAULT 0,
    special_allowance DECIMAL(15,2) DEFAULT 0,
    lta DECIMAL(15,2) DEFAULT 0,
    other_allowances DECIMAL(15,2) DEFAULT 0,
    gross_salary DECIMAL(15,2) DEFAULT 0,

    -- Deductions
    pf_employee DECIMAL(15,2) DEFAULT 0,
    pf_employer DECIMAL(15,2) DEFAULT 0,
    nps_employee DECIMAL(15,2) DEFAULT 0,
    nps_employer DECIMAL(15,2) DEFAULT 0,
    professional_tax DECIMAL(15,2) DEFAULT 0,
    income_tax_deducted DECIMAL(15,2) DEFAULT 0,
    espp_deduction DECIMAL(15,2) DEFAULT 0,
    tcs_on_espp DECIMAL(15,2) DEFAULT 0,
    other_deductions DECIMAL(15,2) DEFAULT 0,

    -- RSU Tax Credit (NEGATIVE deduction = credit)
    rsu_tax_credit DECIMAL(15,2) DEFAULT 0,

    -- Net
    total_deductions DECIMAL(15,2) DEFAULT 0,
    net_pay DECIMAL(15,2) DEFAULT 0,

    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, employer_id, pay_period)
);

-- RSU Tax Credits table (tracks credits for correlation)
CREATE TABLE IF NOT EXISTS rsu_tax_credits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    salary_record_id INTEGER REFERENCES salary_records(id),
    credit_amount DECIMAL(15,2) NOT NULL,
    credit_date DATE NOT NULL,
    vest_id INTEGER,
    correlation_status TEXT DEFAULT 'PENDING' CHECK(correlation_status IN ('PENDING', 'MATCHED', 'UNMATCHED')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Form 16 Records table (annual TDS certificate)
CREATE TABLE IF NOT EXISTS form16_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    employer_id INTEGER REFERENCES employers(id),
    assessment_year TEXT NOT NULL,

    -- Part A - TDS Summary
    q1_tds DECIMAL(15,2) DEFAULT 0,
    q2_tds DECIMAL(15,2) DEFAULT 0,
    q3_tds DECIMAL(15,2) DEFAULT 0,
    q4_tds DECIMAL(15,2) DEFAULT 0,
    total_tds DECIMAL(15,2) DEFAULT 0,

    -- Part B - Income under Section 17
    salary_17_1 DECIMAL(15,2) DEFAULT 0,
    perquisites_17_2 DECIMAL(15,2) DEFAULT 0,
    profits_17_3 DECIMAL(15,2) DEFAULT 0,
    gross_salary DECIMAL(15,2) DEFAULT 0,

    -- Exemptions under Section 10
    hra_exemption DECIMAL(15,2) DEFAULT 0,
    lta_exemption DECIMAL(15,2) DEFAULT 0,
    other_exemptions DECIMAL(15,2) DEFAULT 0,

    -- Deductions under Section 16
    standard_deduction DECIMAL(15,2) DEFAULT 0,
    professional_tax DECIMAL(15,2) DEFAULT 0,

    -- Chapter VI-A Deductions
    section_80c DECIMAL(15,2) DEFAULT 0,
    section_80ccc DECIMAL(15,2) DEFAULT 0,
    section_80ccd_1 DECIMAL(15,2) DEFAULT 0,
    section_80ccd_1b DECIMAL(15,2) DEFAULT 0,
    section_80ccd_2 DECIMAL(15,2) DEFAULT 0,
    section_80d DECIMAL(15,2) DEFAULT 0,
    section_80e DECIMAL(15,2) DEFAULT 0,
    section_80g DECIMAL(15,2) DEFAULT 0,

    -- Taxable Income and Tax
    taxable_income DECIMAL(15,2) DEFAULT 0,
    tax_payable DECIMAL(15,2) DEFAULT 0,

    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, employer_id, assessment_year)
);

-- Perquisites table (Form 12BA details)
CREATE TABLE IF NOT EXISTS perquisites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    form16_id INTEGER REFERENCES form16_records(id),
    perquisite_type TEXT NOT NULL CHECK(perquisite_type IN ('RSU', 'ESPP_DISCOUNT', 'EMPLOYER_PF', 'EMPLOYER_NPS', 'INTEREST_ACCRETION', 'OTHER')),
    description TEXT,
    gross_value DECIMAL(15,2) NOT NULL,
    recovered_from_employee DECIMAL(15,2) DEFAULT 0,
    taxable_value DECIMAL(15,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for salary tables
CREATE INDEX IF NOT EXISTS idx_salary_user ON salary_records(user_id);
CREATE INDEX IF NOT EXISTS idx_salary_employer ON salary_records(employer_id);
CREATE INDEX IF NOT EXISTS idx_salary_period ON salary_records(pay_period);
CREATE INDEX IF NOT EXISTS idx_salary_date ON salary_records(pay_date);
CREATE INDEX IF NOT EXISTS idx_rsu_credits_salary ON rsu_tax_credits(salary_record_id);
CREATE INDEX IF NOT EXISTS idx_rsu_credits_status ON rsu_tax_credits(correlation_status);
CREATE INDEX IF NOT EXISTS idx_form16_user ON form16_records(user_id);
CREATE INDEX IF NOT EXISTS idx_form16_employer ON form16_records(employer_id);
CREATE INDEX IF NOT EXISTS idx_form16_year ON form16_records(assessment_year);
CREATE INDEX IF NOT EXISTS idx_perquisites_form16 ON perquisites(form16_id);
CREATE INDEX IF NOT EXISTS idx_perquisites_type ON perquisites(perquisite_type);

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

-- Phase 2: Foreign Assets Tables

-- Foreign Accounts (broker accounts)
CREATE TABLE IF NOT EXISTS foreign_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    broker_name TEXT NOT NULL,
    account_number TEXT,
    country TEXT DEFAULT 'US',
    opening_date DATE,
    account_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE RESTRICT
);

-- Stock Plans (RSU/ESPP grants)
CREATE TABLE IF NOT EXISTS stock_plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    grant_number TEXT UNIQUE NOT NULL,
    grant_type TEXT NOT NULL CHECK(grant_type IN ('RSU', 'ESPP', 'ESOP', 'DRIP')),
    grant_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    company_name TEXT,
    potential_quantity DECIMAL(15,4),
    grant_price DECIMAL(15,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- RSU Vests
CREATE TABLE IF NOT EXISTS rsu_vests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    grant_number TEXT NOT NULL,
    vest_date DATE NOT NULL,
    shares_vested DECIMAL(15,4) NOT NULL,
    fmv_usd DECIMAL(15,4) NOT NULL,
    shares_withheld_for_tax DECIMAL(15,4) DEFAULT 0,
    net_shares DECIMAL(15,4) DEFAULT 0,
    tt_rate DECIMAL(10,4),
    perquisite_inr DECIMAL(15,2),
    salary_record_id INTEGER,
    correlation_status TEXT DEFAULT 'PENDING' CHECK(correlation_status IN ('PENDING', 'MATCHED', 'UNMATCHED')),
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, grant_number, vest_date),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (salary_record_id) REFERENCES salary_records(id) ON DELETE SET NULL
);

-- RSU Sales
CREATE TABLE IF NOT EXISTS rsu_sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    sale_date DATE NOT NULL,
    shares_sold DECIMAL(15,4) NOT NULL,
    sell_price_usd DECIMAL(15,4) NOT NULL,
    sell_value_usd DECIMAL(15,2) NOT NULL,
    sell_value_inr DECIMAL(15,2),
    cost_basis_usd DECIMAL(15,2) NOT NULL,
    cost_basis_inr DECIMAL(15,2),
    gain_usd DECIMAL(15,2),
    gain_inr DECIMAL(15,2),
    is_ltcg BOOLEAN DEFAULT FALSE,
    holding_period_days INTEGER,
    fees_usd DECIMAL(15,2) DEFAULT 0,
    fees_inr DECIMAL(15,2) DEFAULT 0,
    matched_lots TEXT,
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- ESPP Purchases
CREATE TABLE IF NOT EXISTS espp_purchases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    purchase_date DATE NOT NULL,
    shares_purchased DECIMAL(15,4) NOT NULL,
    purchase_price_usd DECIMAL(15,4) NOT NULL,
    market_price_usd DECIMAL(15,4) NOT NULL,
    discount_percentage DECIMAL(5,2),
    perquisite_per_share_usd DECIMAL(15,4),
    total_perquisite_usd DECIMAL(15,2),
    tt_rate DECIMAL(10,4),
    perquisite_inr DECIMAL(15,2),
    purchase_value_inr DECIMAL(15,2),
    lrs_amount_inr DECIMAL(15,2),
    tcs_collected DECIMAL(15,2) DEFAULT 0,
    financial_year TEXT,
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, purchase_date, shares_purchased),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- ESPP Sales
CREATE TABLE IF NOT EXISTS espp_sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    sale_date DATE NOT NULL,
    shares_sold DECIMAL(15,4) NOT NULL,
    sell_price_usd DECIMAL(15,4) NOT NULL,
    sell_value_usd DECIMAL(15,2) NOT NULL,
    sell_value_inr DECIMAL(15,2),
    cost_basis_usd DECIMAL(15,2) NOT NULL,
    cost_basis_inr DECIMAL(15,2),
    gain_usd DECIMAL(15,2),
    gain_inr DECIMAL(15,2),
    is_ltcg BOOLEAN DEFAULT FALSE,
    holding_period_days INTEGER,
    fees_usd DECIMAL(15,2) DEFAULT 0,
    fees_inr DECIMAL(15,2) DEFAULT 0,
    matched_lots TEXT,
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- Foreign Dividends
CREATE TABLE IF NOT EXISTS foreign_dividends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    dividend_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    shares_held DECIMAL(15,4),
    dividend_per_share_usd DECIMAL(15,6),
    gross_dividend_usd DECIMAL(15,2) NOT NULL,
    withholding_tax_usd DECIMAL(15,2) DEFAULT 0,
    net_dividend_usd DECIMAL(15,2),
    tt_rate DECIMAL(10,4),
    gross_dividend_inr DECIMAL(15,2),
    withholding_tax_inr DECIMAL(15,2),
    net_dividend_inr DECIMAL(15,2),
    is_reinvested BOOLEAN DEFAULT FALSE,
    shares_purchased DECIMAL(15,4) DEFAULT 0,
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, dividend_date, symbol, gross_dividend_usd),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- Foreign Holdings (for Schedule FA peak/closing values)
CREATE TABLE IF NOT EXISTS foreign_holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    account_number TEXT,
    valuation_date DATE NOT NULL,
    symbol TEXT,
    shares_held DECIMAL(15,4),
    price_usd DECIMAL(15,4),
    total_value_usd DECIMAL(15,2),
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- DTAA Credits
CREATE TABLE IF NOT EXISTS dtaa_credits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    income_type TEXT NOT NULL CHECK(income_type IN ('DIVIDEND', 'INTEREST', 'ROYALTY', 'CAPITAL_GAINS')),
    income_country TEXT NOT NULL DEFAULT 'US',
    income_date DATE NOT NULL,
    gross_income_usd DECIMAL(15,2) NOT NULL,
    tax_withheld_usd DECIMAL(15,2) NOT NULL,
    gross_income_inr DECIMAL(15,2),
    tax_withheld_inr DECIMAL(15,2),
    dtaa_article TEXT,
    max_dtaa_rate DECIMAL(5,4),
    indian_tax_on_income DECIMAL(15,2),
    credit_allowed DECIMAL(15,2),
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- Schedule FA (Foreign Assets disclosure)
CREATE TABLE IF NOT EXISTS schedule_fa (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    financial_year TEXT NOT NULL,
    assessment_year TEXT NOT NULL,
    total_peak_value_inr DECIMAL(15,2),
    total_closing_value_inr DECIMAL(15,2),
    total_income_inr DECIMAL(15,2),
    data_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, financial_year),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- Chapter VI-A Deductions (for ITR)
CREATE TABLE IF NOT EXISTS deductions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    financial_year TEXT NOT NULL,
    section_80c DECIMAL(15,2) DEFAULT 0,
    section_80ccc DECIMAL(15,2) DEFAULT 0,
    section_80ccd_1 DECIMAL(15,2) DEFAULT 0,
    section_80ccd_1b DECIMAL(15,2) DEFAULT 0,
    section_80ccd_2 DECIMAL(15,2) DEFAULT 0,
    section_80d DECIMAL(15,2) DEFAULT 0,
    section_80e DECIMAL(15,2) DEFAULT 0,
    section_80g DECIMAL(15,2) DEFAULT 0,
    section_80tta DECIMAL(15,2) DEFAULT 0,
    section_80ttb DECIMAL(15,2) DEFAULT 0,
    section_80u DECIMAL(15,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, financial_year),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- Capital Gains Summary (consolidated)
CREATE TABLE IF NOT EXISTS capital_gains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    financial_year TEXT NOT NULL,
    asset_type TEXT NOT NULL CHECK(asset_type IN ('EQUITY', 'MF_EQUITY', 'MF_DEBT', 'FOREIGN_EQUITY', 'PROPERTY', 'OTHER')),
    sell_date DATE,
    gain_type TEXT CHECK(gain_type IN ('STCG', 'LTCG')),
    realized_gain DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

-- Indexes for Phase 2 tables
CREATE INDEX IF NOT EXISTS idx_foreign_accounts_user ON foreign_accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_stock_plans_user ON stock_plans(user_id);
CREATE INDEX IF NOT EXISTS idx_stock_plans_grant ON stock_plans(grant_number);
CREATE INDEX IF NOT EXISTS idx_rsu_vests_user ON rsu_vests(user_id);
CREATE INDEX IF NOT EXISTS idx_rsu_vests_date ON rsu_vests(vest_date);
CREATE INDEX IF NOT EXISTS idx_rsu_vests_grant ON rsu_vests(grant_number);
CREATE INDEX IF NOT EXISTS idx_rsu_vests_status ON rsu_vests(correlation_status);
CREATE INDEX IF NOT EXISTS idx_rsu_sales_user ON rsu_sales(user_id);
CREATE INDEX IF NOT EXISTS idx_rsu_sales_date ON rsu_sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_espp_purchases_user ON espp_purchases(user_id);
CREATE INDEX IF NOT EXISTS idx_espp_purchases_date ON espp_purchases(purchase_date);
CREATE INDEX IF NOT EXISTS idx_espp_sales_user ON espp_sales(user_id);
CREATE INDEX IF NOT EXISTS idx_espp_sales_date ON espp_sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_foreign_dividends_user ON foreign_dividends(user_id);
CREATE INDEX IF NOT EXISTS idx_foreign_dividends_date ON foreign_dividends(dividend_date);
CREATE INDEX IF NOT EXISTS idx_foreign_dividends_symbol ON foreign_dividends(symbol);
CREATE INDEX IF NOT EXISTS idx_foreign_holdings_user ON foreign_holdings(user_id);
CREATE INDEX IF NOT EXISTS idx_foreign_holdings_date ON foreign_holdings(valuation_date);
CREATE INDEX IF NOT EXISTS idx_dtaa_credits_user ON dtaa_credits(user_id);
CREATE INDEX IF NOT EXISTS idx_dtaa_credits_date ON dtaa_credits(income_date);
CREATE INDEX IF NOT EXISTS idx_schedule_fa_user ON schedule_fa(user_id);
CREATE INDEX IF NOT EXISTS idx_schedule_fa_fy ON schedule_fa(financial_year);
CREATE INDEX IF NOT EXISTS idx_deductions_user_fy ON deductions(user_id, financial_year);
CREATE INDEX IF NOT EXISTS idx_capital_gains_user ON capital_gains(user_id);
CREATE INDEX IF NOT EXISTS idx_capital_gains_fy ON capital_gains(financial_year);
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
