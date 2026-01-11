"""Statement Tracker - Tracks which statements have been processed.

Prevents re-parsing of already processed files using SHA256 hash comparison.
Supports parser version tracking for automatic re-parsing when parsers are updated.
"""

import hashlib
from pathlib import Path
from datetime import datetime
from typing import Optional


class StatementTracker:
    """
    Tracks which statements have been processed.
    Prevents re-parsing of already processed files.
    """

    # Parser version - increment when parser logic changes
    PARSER_VERSIONS = {
        'ZERODHA_TAX_PNL': '1.0.0',
        'CAMS_CAS': '1.0.0',
        'KARVY_CG': '1.0.0',
        'ICICI_DIRECT': '1.0.0',
        'ETRADE_GL': '1.0.0',
        'ETRADE_RSU': '1.0.0',
        'ETRADE_ESPP': '1.0.0',
        'SALARY_PAYSLIP': '1.0.0',
        'FORM16': '1.0.0',
        'FORM_26AS': '1.0.0',
        'EPF_PASSBOOK': '1.0.0',
        'PPF_STATEMENT': '1.0.0',
        'NPS_STATEMENT': '1.0.0',
        'BANK_STATEMENT': '1.0.0',
        'OTHER': '1.0.0',
    }

    def __init__(self, db_connection):
        """
        Initialize with database connection.

        Args:
            db_connection: SQLite connection object
        """
        self.conn = db_connection

    def is_processed(self, user_id: int, file_path: Path) -> bool:
        """
        Check if a file has already been processed successfully.

        Args:
            user_id: User ID
            file_path: Path to the statement file

        Returns:
            True if file was processed successfully
        """
        file_hash = self._calculate_hash(file_path)

        cursor = self.conn.execute("""
            SELECT id, processing_status
            FROM statement_processing_log
            WHERE user_id = ? AND file_hash = ?
        """, (user_id, file_hash))

        row = cursor.fetchone()
        return row is not None and row[1] == 'COMPLETED'

    def needs_reprocessing(
        self,
        user_id: int,
        file_path: Path,
        statement_type: str
    ) -> bool:
        """
        Check if file needs reprocessing due to:
        - File content changed (hash mismatch)
        - Parser version updated
        - Previous processing failed

        Args:
            user_id: User ID
            file_path: Path to the statement file
            statement_type: Type of statement (e.g., 'ZERODHA_TAX_PNL')

        Returns:
            True if file should be reprocessed
        """
        file_hash = self._calculate_hash(file_path)
        parser_version = self.PARSER_VERSIONS.get(statement_type, '1.0.0')

        cursor = self.conn.execute("""
            SELECT file_hash, parser_version, processing_status
            FROM statement_processing_log
            WHERE user_id = ? AND file_path = ?
        """, (user_id, str(file_path)))

        row = cursor.fetchone()

        if not row:
            return True  # Never processed

        if row[0] != file_hash:
            return True  # File content changed

        if row[1] != parser_version:
            return True  # Parser updated

        if row[2] != 'COMPLETED':
            return True  # Previous processing failed

        return False

    def get_processing_status(
        self,
        user_id: int,
        file_path: Path
    ) -> Optional[dict]:
        """
        Get processing status for a file.

        Returns:
            Dict with status info or None if not found
        """
        file_hash = self._calculate_hash(file_path)

        cursor = self.conn.execute("""
            SELECT id, processing_status, records_extracted, error_message,
                   first_processed_at, last_processed_at
            FROM statement_processing_log
            WHERE user_id = ? AND file_hash = ?
        """, (user_id, file_hash))

        row = cursor.fetchone()
        if not row:
            return None

        return {
            'id': row[0],
            'status': row[1],
            'records_extracted': row[2],
            'error_message': row[3],
            'first_processed_at': row[4],
            'last_processed_at': row[5],
        }

    def mark_processing(
        self,
        user_id: int,
        file_path: Path,
        statement_type: str,
        financial_year: str
    ) -> int:
        """
        Mark file as being processed. Creates or updates record.

        Args:
            user_id: User ID
            file_path: Path to statement file
            statement_type: Type of statement
            financial_year: Financial year (e.g., '2024-25')

        Returns:
            Log ID for the record
        """
        file_hash = self._calculate_hash(file_path)
        file_stat = file_path.stat()
        parser_version = self.PARSER_VERSIONS.get(statement_type, '1.0.0')

        # Check if record exists
        cursor = self.conn.execute("""
            SELECT id FROM statement_processing_log
            WHERE user_id = ? AND file_hash = ?
        """, (user_id, file_hash))

        existing = cursor.fetchone()

        if existing:
            # Update existing record
            self.conn.execute("""
                UPDATE statement_processing_log
                SET processing_status = 'PROCESSING',
                    parser_version = ?,
                    last_processed_at = datetime('now'),
                    error_message = NULL
                WHERE id = ?
            """, (parser_version, existing[0]))
            self.conn.commit()
            return existing[0]
        else:
            # Insert new record
            cursor = self.conn.execute("""
                INSERT INTO statement_processing_log (
                    user_id, file_path, file_hash, file_size, file_modified_at,
                    statement_type, financial_year, parser_version,
                    processing_status, first_processed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PROCESSING', datetime('now'))
            """, (
                user_id, str(file_path), file_hash, file_stat.st_size,
                datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                statement_type, financial_year, parser_version
            ))
            self.conn.commit()
            return cursor.lastrowid

    def mark_completed(
        self,
        log_id: int,
        records_extracted: int
    ) -> None:
        """
        Mark processing as completed successfully.

        Args:
            log_id: ID from mark_processing()
            records_extracted: Number of records extracted
        """
        self.conn.execute("""
            UPDATE statement_processing_log
            SET processing_status = 'COMPLETED',
                records_extracted = ?,
                last_processed_at = datetime('now')
            WHERE id = ?
        """, (records_extracted, log_id))
        self.conn.commit()

    def mark_failed(self, log_id: int, error_message: str) -> None:
        """
        Mark processing as failed.

        Args:
            log_id: ID from mark_processing()
            error_message: Error description
        """
        self.conn.execute("""
            UPDATE statement_processing_log
            SET processing_status = 'FAILED',
                error_message = ?,
                last_processed_at = datetime('now')
            WHERE id = ?
        """, (error_message, log_id))
        self.conn.commit()

    def get_user_statements(
        self,
        user_id: int,
        financial_year: Optional[str] = None,
        status: Optional[str] = None
    ) -> list[dict]:
        """
        Get all statement processing records for a user.

        Args:
            user_id: User ID
            financial_year: Optional filter by FY
            status: Optional filter by status

        Returns:
            List of statement processing records
        """
        query = """
            SELECT id, file_path, file_hash, statement_type, financial_year,
                   processing_status, records_extracted, error_message,
                   first_processed_at, last_processed_at
            FROM statement_processing_log
            WHERE user_id = ?
        """
        params = [user_id]

        if financial_year:
            query += " AND financial_year = ?"
            params.append(financial_year)

        if status:
            query += " AND processing_status = ?"
            params.append(status)

        query += " ORDER BY last_processed_at DESC"

        cursor = self.conn.execute(query, params)

        return [
            {
                'id': row[0],
                'file_path': row[1],
                'file_hash': row[2],
                'statement_type': row[3],
                'financial_year': row[4],
                'status': row[5],
                'records_extracted': row[6],
                'error_message': row[7],
                'first_processed_at': row[8],
                'last_processed_at': row[9],
            }
            for row in cursor.fetchall()
        ]

    def _calculate_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file content."""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    @classmethod
    def get_parser_version(cls, statement_type: str) -> str:
        """Get current parser version for a statement type."""
        return cls.PARSER_VERSIONS.get(statement_type, '1.0.0')
