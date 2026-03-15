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
                UNIQUE(user_id, graph_message_id, graph_attachment_id)
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
