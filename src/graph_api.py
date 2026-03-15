import requests
from typing import Optional

from src.Logging import logger
from src.database import get_db_connection
from src.crypto import decrypt_token

GRAPH_BASE = "https://graph.microsoft.com/v1.0"


def get_user_access_token(user_id: int):
    """
    Fetch encrypted token from DB and decrypt it
    """
    try:

        with get_db_connection() as conn:

            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT encrypted_access_token
                FROM users
                WHERE id = ?
                """,
                (user_id,),
            )

            row = cursor.fetchone()

            if not row:
                raise Exception("User not found")

            encrypted_token = row["encrypted_access_token"]

            return decrypt_token(encrypted_token)

    except Exception as e:
        logger.error(f"Failed to fetch access token: {e}")
        return None

def get_inbox_delta(user_id: int, delta_link: Optional[str] = None):
    """
    Fetch inbox messages using Microsoft Graph delta query
    """

    try:

        token = get_user_access_token(user_id)

        if delta_link:
            url = delta_link
            params = None
        else:
            url = f"{GRAPH_BASE}/me/mailFolders/inbox/messages/delta"

            params = {
                "$select": "id,subject,from,hasAttachments",
                "$expand": "attachments"
            }

        headers = {
            "Authorization": f"Bearer {token}"
        }

        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30
        )

        if response.status_code != 200:
            logger.error(
                f"Graph inbox delta failed: {response.status_code} {response.text}"
            )
            return None

        return response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"Graph request error: {e}")
        return None

    except Exception as e:
        logger.exception(f"Unexpected error in get_inbox_delta: {e}")
        return None


def send_email(user_id: int, email_payload: dict):
    """
    Send email via Microsoft Graph
    """

    try:

        token = get_user_access_token(user_id)

        url = f"{GRAPH_BASE}/me/sendMail"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        response = requests.post(
            url,
            headers=headers,
            json=email_payload,
            timeout=30
        )

        if response.status_code not in [200, 202]:
            logger.error(
                f"Send email failed: {response.status_code} {response.text}"
            )
            return False

        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"Graph sendMail request error: {e}")
        return False

    except Exception as e:
        logger.exception(f"Unexpected error in send_email: {e}")
        return False
