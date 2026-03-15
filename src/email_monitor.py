import os
import time

from src.Logging import logger
from src.config import POLL_INTERVAL_SECONDS, TARGET_SENDER_EMAIL
from src.database import (
    get_db_connection,
    inbound_match_exists,
    create_automation_job,
    insert_inbound_match
)
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

    for file_path in files:

        filename = os.path.basename(file_path)

        # Prevent duplicate automation jobs for same attachment
        existing = inbound_match_exists(user_id, message_id, filename)

        if existing:
            logger.info("Attachment already processed. Skipping.")
            continue

        logger.info(f"Creating automation job for file: {file_path}")

        job_id = create_automation_job(user_id, file_path)

        insert_inbound_match(
            user_id,
            message_id,
            filename,
            file_path,
            job_id
        )


def run_poll_cycle():
    """
    Executes one inbox polling cycle.
    """

    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT id, inbox_delta_link FROM users WHERE auth_status='active'"
        )

        users = cursor.fetchall()

    if not users:
        logger.info("No active users found. Skipping poll cycle.")
        return

    for user in users:

        user_id = user["id"]
        delta_link = user["inbox_delta_link"]

        logger.info(f"Polling inbox for user {user_id}")

        try:
            is_first_sync = delta_link is None
            response = get_inbox_delta(user_id, delta_link)

            # Handle Graph API pagination
            # Loop through paginated Graph API responses until deltaLink is reached
            while response:

                messages = response.get("value", [])
                # Do not process historical emails during first delta sync
                if not is_first_sync:
                    for message in messages:
                        process_message(user_id, message)

                next_link = response.get("@odata.nextLink")

                if next_link:
                    response = get_inbox_delta(user_id, next_link)
                    continue

                new_delta_link = response.get("@odata.deltaLink")

                if new_delta_link:

                    with get_db_connection() as conn:
                        cursor = conn.cursor()

                        cursor.execute(
                            """
                            UPDATE users
                            SET inbox_delta_link = ?, last_poll_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                            """,
                            (new_delta_link, user_id),
                        )

                break

        except Exception:
            logger.exception(f"Inbox poll failed for user {user_id}")


def start_email_monitor():
    """
    Main polling loop.
    """

    logger.info("Email monitor started")

    while True:

        try:

            run_poll_cycle()

        except Exception:
            logger.exception("Polling loop failure")

        logger.info(f"Sleeping for {POLL_INTERVAL_SECONDS // 60} mins")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    start_email_monitor()