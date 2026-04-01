"""
Microsoft Graph API client for mailbox operations.

Provides :class:`GraphClient` — a thin, typed wrapper around the Graph v1.0
``/me/messages`` and ``/me/messages/{id}/attachments`` endpoints.
All HTTP errors are surfaced as :class:`GraphAPIError` (or the more specific
:class:`TokenExpiredError` for HTTP 401).
"""

import base64
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import requests

from src.Logging import logger
from src.email_monitor.exceptions import GraphAPIError, TokenExpiredError


# ------------------------------------------------------------------
# Data transfer objects
# ------------------------------------------------------------------

@dataclass
class EmailMessage:
    """Lightweight representation of a Graph API mail message."""

    id: str
    subject: str
    sender_email: str
    received_datetime: str
    has_attachments: bool


@dataclass
class Attachment:
    """Represents a single file attachment downloaded from a message."""

    id: str
    name: str
    content_type: str
    content_bytes: bytes = field(repr=False)
    size: int


# ------------------------------------------------------------------
# Client
# ------------------------------------------------------------------

_GRAPH_BASE = "https://graph.microsoft.com/v1.0"

_MESSAGE_SELECT_FIELDS = "id,subject,from,receivedDateTime,hasAttachments"


class GraphClient:
    """
    Stateless HTTP client for Microsoft Graph mailbox endpoints.

    The caller is responsible for supplying a valid *access_token*.  If the token has expired the client raises
    :class:`TokenExpiredError` so the caller can refresh and retry.

    Args:
        access_token: A bearer token with ``Mail.ReadWrite`` and ``Mail.Send`` scopes.
    """

    def __init__(self, access_token: str):
        self._session = requests.Session()
        self._set_token(access_token)

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    def update_token(self, new_token: str) -> None:
        """Hot-swap the access token without creating a new client instance."""
        self._set_token(new_token)

    def _set_token(self, token: str) -> None:
        self._session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_latest_message(self) -> Optional[EmailMessage]:
        """
        Fetch the single most-recent email from the user's mailbox.

        Returns:
            An :class:`EmailMessage`, or ``None`` if the mailbox is empty.
        """
        data = self._get(
            f"{_GRAPH_BASE}/me/messages",
            params={
                "$top": "1",
                "$orderby": "receivedDateTime desc",
                "$select": _MESSAGE_SELECT_FIELDS,
            },
        )
        messages = data.get("value", [])
        if not messages:
            return None
        return self._parse_message(messages[0])

    def get_messages_since(self, since_datetime: str) -> List[EmailMessage]:
        """
        Fetch all messages received at-or-after *since_datetime*.

        Uses ``$filter=receivedDateTime ge …`` with ascending sort so the
        newest message is **last** in the returned list.  Pagination via
        ``@odata.nextLink`` is followed automatically.

        Args:
            since_datetime: ISO-8601 UTC string, e.g. ``2026-03-27T16:42:05Z``.

        Returns:
            A chronologically-ordered list of :class:`EmailMessage`.
        """
        messages: List[EmailMessage] = []
        data = self._get(
            f"{_GRAPH_BASE}/me/messages",
            params={
                "$filter": f"receivedDateTime ge {since_datetime}",
                "$orderby": "receivedDateTime asc",
                "$select": _MESSAGE_SELECT_FIELDS,
                "$top": "50",
            },
        )
        messages.extend(self._parse_message(item) for item in data.get("value", []))

        next_link = data.get("@odata.nextLink")
        while next_link:
            data = self._get(next_link)
            messages.extend(self._parse_message(item) for item in data.get("value", []))
            next_link = data.get("@odata.nextLink")

        logger.info(f"Fetched {len(messages)} message(s) since {since_datetime}.")
        return messages

    def get_xlsx_attachments(self, message_id: str) -> List[Attachment]:
        """
        Return all ``.xlsx`` file attachments for a given message.

        Only items whose ``@odata.type`` is ``#microsoft.graph.fileAttachment``
        and whose filename ends with ``.xlsx`` are included.

        Args:
            message_id: The Graph API message id.

        Returns:
            A list of :class:`Attachment` with decoded binary content.
        """
        data = self._get(f"{_GRAPH_BASE}/me/messages/{message_id}/attachments")

        attachments: List[Attachment] = []
        for item in data.get("value", []):
            if item.get("@odata.type") != "#microsoft.graph.fileAttachment":
                continue

            name = item.get("name", "")
            if not name.lower().endswith(".xlsx"):
                continue

            raw_b64 = item.get("contentBytes", "")
            content = base64.b64decode(raw_b64) if raw_b64 else b""

            attachments.append(
                Attachment(
                    id=item.get("id", ""),
                    name=name,
                    content_type=item.get("contentType", ""),
                    content_bytes=content,
                    size=item.get("size", 0),
                )
            )

        logger.info(
            f"Message {message_id}: found {len(attachments)} .xlsx attachment(s) "
            f"out of {len(data.get('value', []))} total."
        )
        return attachments

    def send_mail(
            self,
            *,
            subject: str,
            body_text: str,
            to_addresses: List[str],
            file_attachments: List[Tuple[str, str, bytes]],
    ) -> None:
        """
        Send an email from the signed-in user via ``POST /me/sendMail``.

        Args:
            subject: Message subject line.
            body_text: Plain-text body (may be empty).
            to_addresses: Recipient SMTP addresses.
            file_attachments: Sequence of ``(filename, content_type, content_bytes)``.

        Note:
            Requires ``Mail.Send`` (and typically ``Mail.ReadWrite``) on the token.
        """
        attachments_payload: List[Dict[str, Any]] = []
        for name, content_type, content_bytes in file_attachments:
            attachments_payload.append(
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": name,
                    "contentType": content_type,
                    "contentBytes": base64.b64encode(content_bytes).decode("ascii"),
                }
            )

        payload = {
            "message": {
                "subject": subject,
                "body": {"contentType": "Text", "content": body_text},
                "toRecipients": [
                    {"emailAddress": {"address": addr}} for addr in to_addresses
                ],
                "attachments": attachments_payload,
            },
            "saveToSentItems": True,
        }
        self._post(f"{_GRAPH_BASE}/me/sendMail", payload)

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        response = self._session.get(url, params=params, timeout=60)
        return self._handle_response(response)

    def _post(self, url: str, body: Dict[str, Any]) -> None:
        """POST JSON.  ``/me/sendMail`` returns **202 Accepted** with an empty body."""
        response = self._session.post(url, json=body, timeout=120)
        if response.status_code == 401:
            raise TokenExpiredError(
                status_code=401,
                message="Access token expired or invalid.",
                response=_safe_json(response),
            )
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "60")
            raise GraphAPIError(
                status_code=429,
                message=f"Rate limited. Retry-After: {retry_after}s.",
                response=_safe_json(response),
            )
        if not response.ok:
            raise GraphAPIError(
                status_code=response.status_code,
                message=response.text[:500],
                response=_safe_json(response),
            )

    @staticmethod
    def _handle_response(response: requests.Response) -> Dict[str, Any]:
        """Translate HTTP errors into typed exceptions."""
        if response.status_code == 401:
            raise TokenExpiredError(
                status_code=401,
                message="Access token expired or invalid.",
                response=_safe_json(response),
            )

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "60")
            raise GraphAPIError(
                status_code=429,
                message=f"Rate limited. Retry-After: {retry_after}s.",
                response=_safe_json(response),
            )

        if not response.ok:
            raise GraphAPIError(
                status_code=response.status_code,
                message=response.text[:500],
                response=_safe_json(response),
            )

        return _safe_json(response)

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_message(item: Dict[str, Any]) -> EmailMessage:
        sender_email = (
            item.get("from", {})
            .get("emailAddress", {})
            .get("address", "")
        )
        return EmailMessage(
            id=item.get("id", ""),
            subject=item.get("subject", ""),
            sender_email=sender_email,
            received_datetime=item.get("receivedDateTime", ""),
            has_attachments=bool(item.get("hasAttachments", False)),
        )


# ------------------------------------------------------------------
# Module-level utility
# ------------------------------------------------------------------

def _safe_json(response: requests.Response) -> Dict[str, Any]:
    try:
        return response.json()
    except (ValueError, requests.exceptions.JSONDecodeError):
        return {}
