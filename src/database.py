import json
import sqlite3
from src.config import DB_PATH
from src.Logging import logger


def get_db_connection():
    """
    Creates and returns a SQLite database connection with foreign keys enabled.
    """
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
        raise


def init_db():
    """
    Initialize database schema.
    """
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")

            cursor = conn.cursor()

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT,
                tenant_id TEXT,
                encrypted_access_token TEXT,
                encrypted_refresh_token TEXT,
                token_expires_at TEXT,
                inbox_delta_link TEXT,
                auth_status TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS inbound_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                graph_message_id TEXT,
                graph_attachment_id TEXT,
                sender_email TEXT,
                attachment_name TEXT,
                download_path TEXT,
                matched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                automation_job_id INTEGER,
                dispatch_job_id INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, graph_message_id, graph_attachment_id),
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                inbound_match_id INTEGER,
                job_type TEXT,
                status TEXT,
                run_status INTEGER,
                error_msg TEXT,
                input_json TEXT,
                output_json TEXT,
                started_at TEXT,
                finished_at TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(id),
                FOREIGN KEY(inbound_match_id) REFERENCES inbound_matches(id)     
            )
            """)
    except sqlite3.Error as e:
        logger.error(f"Database initialization failed: {e}")
        raise


def inbound_match_exists(user_id, message_id, attachment_name):

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT id
                FROM inbound_matches
                WHERE user_id = ?
                AND graph_message_id = ?
                AND attachment_name = ?
                """,
                (user_id, message_id, attachment_name)
            )

            return cursor.fetchone()

    except sqlite3.Error:
        logger.exception("DB error while checking inbound match")
        return None


def insert_inbound_match(user_id, message_id, attachment_name, file_path, job_id):

    try:

        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT INTO inbound_matches
                (user_id, graph_message_id, attachment_name, download_path, automation_job_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, message_id, attachment_name, file_path, job_id),
            )

    except sqlite3.Error:
        logger.exception("DB error while inserting inbound match")


def create_automation_job(user_id, file_path):

    try:

        with get_db_connection() as conn:
            cursor = conn.cursor()

            payload = json.dumps({"file_path": str(file_path)})

            cursor.execute(
                """
                INSERT INTO jobs (user_id, job_type, status, input_json, created_at)
                VALUES (?, 'automation', 'pending', ?, CURRENT_TIMESTAMP)
                """,
                (user_id, payload),
            )

            return cursor.lastrowid

    except sqlite3.Error:
        logger.exception("DB error while creating automation job")
        return None



def fetch_pending_jobs(limit: int = 5):
    """
    Fetch pending automation jobs.
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT id, user_id, inbound_match_id, input_json
                FROM jobs
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (limit,)
            )

            return cursor.fetchall()

    except sqlite3.Error:
        logger.exception("DB error while fetching pending jobs")
        return []


def mark_job_running(job_id: int):
    """
    Mark job as running.
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                UPDATE jobs
                SET status='running',
                    started_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (job_id,)
            )

    except sqlite3.Error:
        logger.exception(f"DB error while marking job {job_id} running")


def mark_job_completed(job_id: int, output: dict):
    """
    Mark job as completed and store output JSON.
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                UPDATE jobs
                SET status='completed',
                    output_json=?,
                    finished_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (json.dumps(output), job_id)
            )

    except sqlite3.Error:
        logger.exception(f"DB error while marking job {job_id} completed")


def mark_job_failed(job_id: int, error_msg: str):
    """
    Mark job as failed.
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                UPDATE jobs
                SET status='failed',
                    error_msg=?,
                    finished_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (error_msg, job_id)
            )

    except sqlite3.Error:
        logger.exception(f"DB error while marking job {job_id} failed")


def create_user_with_tokens(email, encrypted_access, encrypted_refresh, expiry_time):
    """
    Insert new authenticated user.
    """

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT INTO users (
                    email,
                    encrypted_access_token,
                    encrypted_refresh_token,
                    token_expires_at,
                    auth_status
                )
                VALUES (?, ?, ?, ?, 'active')
                """,
                (
                    email,
                    encrypted_access,
                    encrypted_refresh,
                    expiry_time,
                ),
            )

            return cursor.lastrowid

    except Exception:
        logger.exception("DB error while storing user tokens")
        raise

def update_user_delta_link(user_id, delta_link):
    """
    Save baseline delta link for inbox polling.
    """

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                UPDATE users
                SET inbox_delta_link = ?
                WHERE id = ?
                """,
                (delta_link, user_id),
            )

    except Exception:
        logger.exception("DB error while updating delta link")
        raise