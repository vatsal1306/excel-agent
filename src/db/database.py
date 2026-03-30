"""
SQLite database layer for the email monitoring agent.

Provides a ``Database`` class that manages user records, OAuth2 tokens,
and email-processing bookmarks.  Uses WAL journal mode for safe concurrent
access from the FastAPI auth server and the long-running monitor process.
"""

import json
import os
import sqlite3
from typing import List, Optional

from src.Logging import logger
from src.db.models import User


class Database:
    """
    Manages a SQLite database storing monitored-mailbox users.

    Usage as a context manager::

        with Database("data/email_agent.db") as db:
            db.initialize()
            user = db.upsert_user("name@company.com", access, refresh)

    Or as a long-lived instance::

        db = Database("data/email_agent.db")
        db.connect()
        db.initialize()
        ...
        db.close()
    """

    _CREATE_USERS_TABLE = """
                          CREATE TABLE IF NOT EXISTS users
                          (
                              id
                              INTEGER
                              PRIMARY
                              KEY
                              AUTOINCREMENT,
                              email
                              TEXT
                              NOT
                              NULL
                              UNIQUE,
                              access_token
                              TEXT
                              NOT
                              NULL,
                              refresh_token
                              TEXT
                              NOT
                              NULL,
                              last_processed_email_id
                              TEXT,
                              last_processed_datetime
                              TEXT,
                              created_at
                              TEXT
                              NOT
                              NULL
                              DEFAULT
                              CURRENT_TIMESTAMP,
                              updated_at
                              TEXT
                              NOT
                              NULL
                              DEFAULT
                              CURRENT_TIMESTAMP
                          ); \
                          """

    _CREATE_UPDATED_AT_TRIGGER = """
                                 CREATE TRIGGER IF NOT EXISTS trg_users_updated_at
        AFTER
                                 UPDATE ON users
                                     FOR EACH ROW
                                 BEGIN
                                 UPDATE users
                                 SET updated_at = CURRENT_TIMESTAMP
                                 WHERE id = NEW.id;
                                 END; \
                                 """

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._connection: Optional[sqlite3.Connection] = None

    def __enter__(self) -> "Database":
        self.connect()
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def connect(self) -> None:
        """Establish the database connection with WAL mode and a generous busy-timeout."""
        os.makedirs(os.path.dirname(os.path.abspath(self._db_path)), exist_ok=True)
        self._connection = sqlite3.connect(self._db_path, timeout=30)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode = WAL;")
        self._connection.execute("PRAGMA busy_timeout = 30000;")
        self._connection.execute("PRAGMA recursive_triggers = OFF;")
        logger.info(f"Connected to database: {self._db_path}")

    def initialize(self) -> None:
        """Create tables and triggers if they do not already exist."""
        conn = self._get_connection()
        conn.executescript(self._CREATE_USERS_TABLE + self._CREATE_UPDATED_AT_TRIGGER)
        conn.commit()
        logger.info("Database schema initialized.")

    def close(self) -> None:
        """Close the database connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.info("Database connection closed.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError(
                "Database connection not established. Call connect() or use the context manager."
            )
        return self._connection

    @staticmethod
    def _row_to_user(row: sqlite3.Row) -> User:
        return User(
            id=row["id"],
            email=row["email"],
            access_token=row["access_token"],
            refresh_token=row["refresh_token"],
            last_processed_email_id=row["last_processed_email_id"],
            last_processed_datetime=row["last_processed_datetime"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    # ------------------------------------------------------------------
    # CRUD operations
    # ------------------------------------------------------------------

    def upsert_user(self, email: str, access_token: str, refresh_token: str) -> User:
        """
        Insert a new user or update tokens for an existing one.

        Returns:
            The ``User`` record after the upsert.
        Raises:
            RuntimeError: If the database connection is not open.
        """
        conn = self._get_connection()
        conn.execute(
            """
            INSERT INTO users (email, access_token, refresh_token)
            VALUES (?, ?, ?) ON CONFLICT(email) DO
            UPDATE SET
                access_token = excluded.access_token,
                refresh_token = excluded.refresh_token
            """,
            (email, access_token, refresh_token),
        )
        conn.commit()
        logger.info(f"Upserted user: {email}")
        return self.get_user_by_email(email)

    def get_all_users(self) -> List[User]:
        """Return every registered user, ordered by id."""
        conn = self._get_connection()
        rows = conn.execute("SELECT * FROM users ORDER BY id").fetchall()
        return [self._row_to_user(r) for r in rows]

    def get_user_by_email(self, email: str) -> Optional[User]:
        """Look up a user by email address (case-sensitive)."""
        conn = self._get_connection()
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        return self._row_to_user(row) if row else None

    def get_user_by_id(self, user_id: int) -> Optional[User]:
        """Look up a user by primary key."""
        conn = self._get_connection()
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        return self._row_to_user(row) if row else None

    def update_tokens(self, user_id: int, access_token: str, refresh_token: str) -> None:
        """
        Persist refreshed OAuth2 tokens for a user.

        The ``updated_at`` column is bumped automatically by the database trigger.
        """
        conn = self._get_connection()
        conn.execute(
            "UPDATE users SET access_token = ?, refresh_token = ? WHERE id = ?",
            (access_token, refresh_token, user_id),
        )
        conn.commit()
        logger.info(f"Updated tokens for user_id={user_id}.")

    def update_last_processed(
            self,
            user_id: int,
            email_id: str,
            processed_datetime: str,
    ) -> None:
        """
        Record the most-recent email that has been seen / processed.

        Args:
            user_id:              The user's primary key.
            email_id:             Microsoft Graph message id.
            processed_datetime:   ISO-8601 UTC string of the email's ``receivedDateTime``.
        """
        conn = self._get_connection()
        conn.execute(
            """
            UPDATE users
            SET last_processed_email_id = ?,
                last_processed_datetime = ?
            WHERE id = ?
            """,
            (email_id, processed_datetime, user_id),
        )
        conn.commit()
        logger.info(
            f"Updated last_processed for user_id={user_id}: "
            f"msg_id={email_id}, dt={processed_datetime}"
        )

    # ------------------------------------------------------------------
    # Token-cache import utility
    # ------------------------------------------------------------------

    def import_token_cache_file(self, bin_path: str) -> User:
        """
        Parse a serialised MSAL token-cache ``.bin`` file and upsert the
        contained user into the database.

        The ``.bin`` file is the JSON output of ``msal.SerializableTokenCache.serialize()``.

        Args:
            bin_path: Filesystem path to the ``.bin`` file.
        Returns:
            The ``User`` record created or updated.
        Raises:
            FileNotFoundError: If *bin_path* does not exist.
            ValueError:        If required fields cannot be extracted from the cache.
        """
        if not os.path.exists(bin_path):
            raise FileNotFoundError(f"Token cache file not found: {bin_path}")

        with open(bin_path, "r") as fh:
            cache_data = json.loads(fh.read())

        email = self._extract_field(cache_data, "Account", "username", "email")
        access_token = self._extract_field(cache_data, "AccessToken", "secret", "access token")
        refresh_token = self._extract_field(cache_data, "RefreshToken", "secret", "refresh token")

        logger.info(f"Imported token cache for {email} from {bin_path}")
        return self.upsert_user(email, access_token, refresh_token)

    @staticmethod
    def _extract_field(
            cache_data: dict,
            section: str,
            key: str,
            human_label: str,
    ) -> str:
        """
        Walk through a top-level section of the MSAL cache JSON and return
        the first non-empty value for *key*.
        """
        for entry in cache_data.get(section, {}).values():
            value = entry.get(key)
            if value:
                return value
        raise ValueError(
            f"Could not extract {human_label} from token cache "
            f"(section '{section}', key '{key}')."
        )
