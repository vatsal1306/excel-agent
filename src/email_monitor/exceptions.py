"""Custom exceptions for the email monitoring agent."""

from typing import Any, Dict, Optional


class GraphAPIError(Exception):
    """
    Base exception for Microsoft Graph API errors.

    Attributes:
        status_code:  HTTP status code returned by the Graph API.
        response:     Parsed JSON body of the error response, if available.
    """

    def __init__(
            self,
            status_code: int,
            message: str,
            response: Optional[Dict[str, Any]] = None,
    ):
        self.status_code = status_code
        self.response = response or {}
        super().__init__(f"Graph API error (HTTP {status_code}): {message}")


class TokenExpiredError(GraphAPIError):
    """Raised when the access token has expired or been revoked (HTTP 401)."""


class TokenRefreshError(Exception):
    """Raised when an OAuth2 token refresh attempt fails."""
