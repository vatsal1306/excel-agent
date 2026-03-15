import base64
import os
import uuid
from typing import List

from src.utils import find_xlsx_attachments, build_download_path
from src.Logging import logger


def download_xlsx_attachments(message: dict) -> List[str]:
    """
    Fetch attachments for a message and download only .xlsx files.
    Returns list of local file paths.
    """

    saved_files = []

    try:
        message_id = message.get("id")
        attachments = message.get("attachments", [])

        # ---------- FIX 1: Handle None response ----------
        if not attachments:
            logger.warning(f"No attachment payload returned for message {message_id}")
            return []

        attachments = attachments.get("value", [])

        xlsx_files = find_xlsx_attachments(attachments)

        for attachment in xlsx_files:

            attachment_name = attachment.get("name")

            # ---------- FIX 2: Prevent path traversal ----------
            safe_name = os.path.basename(attachment_name)
            name, ext = os.path.splitext(safe_name)
            unique_name = f"{name}_{uuid.uuid4().hex[:6]}{ext}"

            logger.info(
                f"Processing attachment: {safe_name} (message_id={message_id})"
            )
            content_bytes = attachment.get("contentBytes")

            if not content_bytes:
                logger.warning(f"No content for attachment {safe_name}")
                continue

            file_bytes = base64.b64decode(content_bytes)

            file_path = build_download_path(message_id, unique_name)

            with open(file_path, "wb") as f:
                f.write(file_bytes)

            logger.info(f"Saved attachment to {file_path}")

            saved_files.append(str(file_path))

    except Exception:
        logger.exception(
            f"Attachment download failed for message {message.get('id')}"
        )

        return saved_files
    return saved_files
