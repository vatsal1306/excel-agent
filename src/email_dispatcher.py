import base64
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

    # read and encode file
    with open(file_path, "rb") as f:
        encoded_file = base64.b64encode(f.read()).decode("utf-8")

    attachment = {
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": file_path.name,
        "contentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "contentBytes": encoded_file,
    }

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
            "attachments": [attachment]
        },
        "saveToSentItems": True
    }

    send_email(email_payload)

    logger.info(f"Result email sent for job {job['id']}")