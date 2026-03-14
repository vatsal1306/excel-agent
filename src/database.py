import sqlite3
from src.config import DB_PATH


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_connection()
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
        created_at TEXT,
        updated_at TEXT
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
        matched_at TEXT,
        automation_job_id INTEGER,
        dispatch_job_id INTEGER,
        created_at TEXT,
        UNIQUE(user_id, graph_message_id, graph_attachment_id)
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
        created_at TEXT,
        updated_at TEXT
    )
    """)

    conn.commit()
    conn.close()