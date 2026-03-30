"""
OAuth2 token refresh without MSAL.

Uses the raw Microsoft identity-platform token endpoint to exchange a
refresh token for a fresh access + refresh token pair.  This avoids
reconstructing an MSAL application object on every poll cycle and keeps
the monitoring script lightweight.
"""

from typing import Tuple

import requests

from src.Logging import logger
from src.email_monitor.exceptions import TokenRefreshError


class TokenManager:
    """
    Handles silent token renewal via the Microsoft OAuth2 v2.0 token endpoint.

    Args:
        client_id:     Azure AD application (client) ID.
        client_secret: Application client secret.
        tenant_id:     Azure AD directory (tenant) ID.
    """

    _TOKEN_URL_TEMPLATE = (
        "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    )
    _SCOPES = "Mail.ReadWrite Mail.Send offline_access"

    def __init__(self, client_id: str, client_secret: str, tenant_id: str):
        self._client_id = client_id
        self._client_secret = client_secret
        self._token_url = self._TOKEN_URL_TEMPLATE.format(tenant_id=tenant_id)

    def refresh_access_token(self, refresh_token: str) -> Tuple[str, str]:
        """
        Exchange a refresh token for a new access / refresh token pair.

        Args:
            refresh_token: The current OAuth2 refresh token.

        Returns:
            A ``(new_access_token, new_refresh_token)`` tuple.
            If Microsoft does not return a new refresh token the original
            *refresh_token* value is echoed back.

        Raises:
            TokenRefreshError:
                On network failure, non-200 response, or missing access token
                in the response payload.
        """
        payload = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "scope": self._SCOPES,
        }

        try:
            response = requests.post(self._token_url, data=payload, timeout=30)
        except requests.RequestException as exc:
            raise TokenRefreshError(
                f"Network error during token refresh: {exc}"
            ) from exc

        if not response.ok:
            error_body = self._safe_error_description(response)
            raise TokenRefreshError(
                f"Token refresh failed (HTTP {response.status_code}): {error_body}"
            )

        data = response.json()
        new_access_token = data.get("access_token")
        if not new_access_token:
            raise TokenRefreshError(
                "Token endpoint returned 200 but no access_token in payload."
            )

        new_refresh_token = data.get("refresh_token", refresh_token)

        logger.info("Tokens refreshed successfully.")
        return new_access_token, new_refresh_token

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_error_description(response: requests.Response) -> str:
        try:
            body = response.json()
            return body.get("error_description", response.text[:500])
        except (ValueError, requests.exceptions.JSONDecodeError):
            return response.text[:500]
