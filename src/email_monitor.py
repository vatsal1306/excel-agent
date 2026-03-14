import time

import requests

from src.oauth2.headless_auth import get_access_token
from src.config import POLL_INTERVAL_SECONDS


GRAPH_ENDPOINT = "https://graph.microsoft.com/v1.0/me/messages"


def check_inbox():
    """
    Fetch latest emails from Outlook inbox
    """

    token = get_access_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    params = {
        "$select": "subject,from",
        "$top": 10
    }

    response = requests.get(GRAPH_ENDPOINT, headers=headers, params=params)

    if response.status_code != 200:
        print("Graph API error:", response.text)
        return

    emails = response.json().get("value", [])

    print(f"Found {len(emails)} emails\n")

    for email in emails:
        sender = email.get("from", {}).get("emailAddress", {}).get("address")
        subject = email.get("subject")

        print(f"Email from {sender} | Subject: {subject}")


def start_monitor():

    print("Starting email monitor...\n")

    while True:
        try:
            print("Checking inbox...\n")
            check_inbox()
        except Exception as e:
            print("Monitor error:", e)

        print(f"\nSleeping for {POLL_INTERVAL_SECONDS} seconds...\n")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    start_monitor()