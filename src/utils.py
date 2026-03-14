import os
import re
from datetime import datetime
from typing import List, Dict
from pathlib import Path
from src.config import DOWNLOAD_DIR


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

    filename = re.sub(r"[^\w\-.]", "_", filename)

    return filename


def build_download_path(message_id: str, file_name: str) -> str:
    """
    Create deterministic download path for an attachment.
    """

    short_id = message_id[:12]

    folder = DOWNLOAD_DIR / f"msg_{short_id}"

    folder.mkdir(parents=True, exist_ok=True)

    return folder / file_name


def current_timestamp() -> str:
    """
    Return current timestamp string.
    """

    return datetime.utcnow().isoformat()