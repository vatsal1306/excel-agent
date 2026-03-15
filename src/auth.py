import requests
import datetime as dt

from src.config import (
    CLIENT_ID,
    CLIENT_SECRET,
    TENANT_ID,
    REDIRECT_URI
)

from src.crypto import encrypt_token
from src.graph_api import get_inbox_delta

from src.database import (
    create_user_with_tokens,
    update_user_delta_link
)


AUTH_BASE = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0"


def build_login_url():
    """
    Generates Microsoft login URL.
    """

    scope = "openid profile offline_access Mail.Read Mail.Send"

    url = (
        f"{AUTH_BASE}/authorize"
        f"?client_id={CLIENT_ID}"
        f"&response_type=code"
        f"&redirect_uri={REDIRECT_URI}"
        f"&response_mode=query"
        f"&scope={scope}"
    )

    return url


def exchange_code_for_token(code: str):
    """
    Exchange authorization code for access + refresh tokens.
    """

    token_url = f"{AUTH_BASE}/token"

    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    }

    response = requests.post(token_url, data=payload)

    if response.status_code != 200:
        raise Exception(f"Token exchange failed: {response.text}")

    return response.json()


def store_tokens(token_data):
    """
    Store encrypted tokens in DB.
    """

    access_token = token_data["access_token"]
    refresh_token = token_data["refresh_token"]
    expires_in = token_data["expires_in"]

    expiry_time = dt.datetime.utcnow() + dt.timedelta(seconds=expires_in)

    encrypted_access = encrypt_token(access_token)
    encrypted_refresh = encrypt_token(refresh_token)

    user_id = create_user_with_tokens(
        "connected_user",
        encrypted_access,
        encrypted_refresh,
        expiry_time.isoformat(),
    )

    return user_id


def baseline_inbox_sync(user_id):
    """
    First delta sync — do NOT process emails.
    Just store deltaLink checkpoint.
    """

    response = get_inbox_delta(user_id, None)

    delta_link = response.get("@odata.deltaLink")

    if not delta_link:
        raise Exception("Delta link missing during baseline sync")

    update_user_delta_link(user_id, delta_link)