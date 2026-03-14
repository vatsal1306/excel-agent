import requests
from src.oauth2.headless_auth import get_access_token

GRAPH_BASE = "https://graph.microsoft.com/v1.0"


def get_inbox_messages():
    token = get_access_token()

    url = f"{GRAPH_BASE}/me/mailFolders/inbox/messages"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    params = {
        "$top": 10
    }

    response = requests.get(url, headers=headers, params=params)

    response.raise_for_status()

    return response.json()


def get_inbox_delta(delta_link=None):
    token = get_access_token()

    if delta_link:
        url = delta_link
    else:
        url = f"{GRAPH_BASE}/me/mailFolders/inbox/messages/delta"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(url, headers=headers)

    response.raise_for_status()

    return response.json()


def get_attachments(message_id: str):
    token = get_access_token()

    url = f"{GRAPH_BASE}/me/messages/{message_id}/attachments"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(url, headers=headers)

    response.raise_for_status()

    return response.json()


def send_email(email_payload: dict):
    token = get_access_token()

    url = f"{GRAPH_BASE}/me/sendMail"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, json=email_payload)

    response.raise_for_status()

    return True