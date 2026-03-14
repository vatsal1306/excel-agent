import os
import re
from datetime import datetime
from typing import List, Dict


def sender_matches(message_sender: str, expected_sender: str) -> bool:
    """
    Check if the sender matches the configured sender.
    Case-insensitive comparison.
    """

    if not message_sender:
        return False

    return message_sender.lower() == expected_sender.lower()


def find_xlsx_attachments(attachments: List[Dict]) -> List[Dict]:
    """
    Filter attachments and return only .xlsx files.
    """

    xlsx_files = []

    for attachment in attachments:

        name = attachment.get("name", "")

        if name.lower().endswith(".xlsx"):
            xlsx_files.append(attachment)

    return xlsx_files


def sanitize_filename(filename: str) -> str:
    """
    Remove unsafe characters from filenames.
    """

    filename = re.sub(r"[^\w\-. ]", "_", filename)

    return filename


def build_download_path(base_dir: str, message_id: str, attachment_name: str) -> str:
    """
    Build deterministic path for storing attachments.

    Example:
    data/downloads/{message_id}/{filename}
    """

    safe_name = sanitize_filename(attachment_name)

    directory = os.path.join(base_dir, message_id)

    os.makedirs(directory, exist_ok=True)

    return os.path.join(directory, safe_name)


def current_timestamp() -> str:
    """
    Return current timestamp string.
    """

    return datetime.utcnow().isoformat()