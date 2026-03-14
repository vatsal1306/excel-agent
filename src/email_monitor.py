import time

from src.Logging import logger
from src.config import POLL_INTERVAL_SECONDS, TARGET_SENDER_EMAIL
from src.database import get_db_connection
from src.graph_api import get_inbox_delta
from src.utils import sender_matches
from src.attachment_handler import download_xlsx_attachments


def process_message(user_id, message):
    """
    Process a single message returned from Graph delta query.
    """

    message_id = message.get("id")

    sender = (
        message.get("from", {})
        .get("emailAddress", {})
        .get("address")
    )

    subject = message.get("subject")

    logger.info(f"Checking message {message_id} from {sender}")

    if not sender_matches(sender, TARGET_SENDER_EMAIL):
        logger.info("Sender does not match rule. Skipping.")
        return

    logger.info(f"Matched sender. Subject: {subject}")

    files = download_xlsx_attachments(message_id)

    if not files:
        logger.info("No XLSX attachments found.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    for file_path in files:

        logger.info(f"Creating automation job for file: {file_path}")

        cursor.execute(
            """
            INSERT INTO jobs (user_id, file_path, status, created_at)
            VALUES (?, ?, 'pending', CURRENT_TIMESTAMP)
            """,
            (user_id, file_path),
        )

    conn.commit()
    conn.close()


def run_poll_cycle():
    """
    Executes one inbox polling cycle.
    """

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id, inbox_delta_link FROM users WHERE auth_status='active'")

    users = cursor.fetchall()
    if not users:
        logger.info("No active users found. Skipping poll cycle.")
        return

    conn.close()

    for user in users:

        user_id = user["id"]
        delta_link = user["inbox_delta_link"]

        logger.info(f"Polling inbox for user {user_id}")

        try:

            response = get_inbox_delta(delta_link)

            messages = response.get("value", [])

            for message in messages:
                process_message(user_id, message)

            new_delta_link = response.get("@odata.deltaLink")

            if new_delta_link:

                conn = get_db_connection()
                cursor = conn.cursor()

                cursor.execute(
                    """
                    UPDATE users
                    SET inbox_delta_link = ?, last_poll_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (new_delta_link, user_id),
                )

                conn.commit()
                conn.close()

        except Exception as e:

            logger.error(f"Inbox poll failed for user {user_id}: {str(e)}")


def start_email_monitor():
    """
    Main polling loop.
    """

    logger.info("Email monitor started")

    while True:

        try:

            run_poll_cycle()

        except Exception as e:

            logger.error(f"Polling loop failure: {str(e)}")

        logger.info(f"Sleeping for {POLL_INTERVAL_SECONDS} seconds")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    start_email_monitor()