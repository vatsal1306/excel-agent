"""Data models for the email agent database."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class User:
    """
    Represents a monitored mailbox user stored in the local SQLite database.

    Attributes:
        id:                         Auto-incremented primary key.
        email:                      The user's enterprise Outlook email address.
        access_token:               Current Microsoft Graph API access token.
        refresh_token:              Long-lived OAuth2 refresh token for silent renewal.
        last_processed_email_id:    Graph message-id of the last email that was seen.
        last_processed_datetime:    ISO-8601 UTC timestamp of that email's receivedDateTime.
        created_at:                 Row creation timestamp (auto-set by DB).
        updated_at:                 Row last-update timestamp (auto-set by DB trigger).
    """

    id: int
    email: str
    access_token: str
    refresh_token: str
    last_processed_email_id: Optional[str]
    last_processed_datetime: Optional[str]
    created_at: str
    updated_at: str

    @property
    def is_new(self) -> bool:
        """A user is considered 'new' if we have never recorded a baseline email."""
        return self.last_processed_datetime is None
