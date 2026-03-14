import base64
import os
from typing import List, Dict

from src.config import DOWNLOAD_DIR
from src.graph_api import get_attachments
from src.utils import find_xlsx_attachments, build_download_path
from src.Logging import logger


def download_xlsx_attachments(message_id: str) -> List[str]:
    """
    Fetch attachments for a message and download only .xlsx files.
    Returns list of local file paths.
    """

    saved_files = []

    try:
        attachments = get_attachments(message_id)

        attachments = attachments.get("value", [])

        xlsx_files = find_xlsx_attachments(attachments)

        for attachment in xlsx_files:

            attachment_id = attachment.get("id")
            attachment_name = attachment.get("name")

            logger.info(
                f"Processing attachment: {attachment_name} (message_id={message_id})"
            )

            content_bytes = attachment.get("contentBytes")

            if not content_bytes:
                logger.warning(f"No content for attachment {attachment_name}")
                continue

            file_bytes = base64.b64decode(content_bytes)

            file_path = build_download_path(
                DOWNLOAD_DIR,
                message_id,
                attachment_name
            )

            with open(file_path, "wb") as f:
                f.write(file_bytes)

            logger.info(f"Saved attachment to {file_path}")

            saved_files.append(file_path)

    except Exception as e:
        logger.error(f"Attachment download failed for message {message_id}: {str(e)}")
        raise

    return saved_files