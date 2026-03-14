from pathlib import Path

from src.Logging import logger
from src.config import EMAIL_RECIPIENTS, EMAIL_SUBJECT
from src.graph_api import send_email


def dispatch_result_email(job, automation_result):
    """
    Sends result email after automation finishes.
    """

    output_file = automation_result.get("output_file")

    if not output_file:
        raise Exception("No output file generated")

    file_path = Path(output_file)

    if not file_path.exists():
        raise Exception(f"Output file missing: {file_path}")

    logger.info(f"Preparing result email for job {job['id']}")

    email_payload = {
        "message": {
            "subject": EMAIL_SUBJECT or "Automation Result",
            "body": {
                "contentType": "Text",
                "content": "Automation completed successfully. See attached output."
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": recipient
                    }
                } for recipient in EMAIL_RECIPIENTS
            ],
            "attachments": []
        },
        "saveToSentItems": True
    }

    send_email(email_payload)

    logger.info(f"Result email sent for job {job['id']}")