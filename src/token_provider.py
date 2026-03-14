from src.oauth2.check_oauth2_token_valid import get_access_token


def fetch_access_token():
    """
    Returns a valid Microsoft Graph access token.
    The underlying MSAL cache will automatically refresh tokens if needed.
    """
    return get_access_token()