import os
import msal
import requests

from src import envs

CLIENT_ID = envs["OUTLOOK_CLIENT_ID"]
AUTHORITY = "https://login.microsoftonline.com/consumers"
SCOPES = ["Mail.ReadWrite", "Mail.Send"]

CACHE_FILE = "token_cache.bin"


def get_access_token():
    cache = msal.SerializableTokenCache()

    if os.path.exists(CACHE_FILE):
        cache.deserialize(open(CACHE_FILE, "r").read())
    else:
        raise Exception("token_cache.bin not found.")

    app = msal.PublicClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        token_cache=cache
    )

    accounts = app.get_accounts()

    if not accounts:
        raise Exception("No accounts found in token cache.")

    result = app.acquire_token_silent(SCOPES, account=accounts[0])

    if result and "access_token" in result:

        if cache.has_state_changed:
            with open(CACHE_FILE, "w") as f:
                f.write(cache.serialize())

        return result["access_token"]

    raise Exception("Failed to acquire access token.")


def main():

    token = get_access_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    endpoint = "https://graph.microsoft.com/v1.0/me/messages?$select=subject,from&$top=3"

    response = requests.get(endpoint, headers=headers)

    if response.status_code == 200:

        emails = response.json().get("value", [])

        print("✔ Success! Latest emails:\n")

        for i, email in enumerate(emails, 1):
            sender = email.get("from", {}).get("emailAddress", {}).get("name", "Unknown")
            subject = email.get("subject", "No Subject")

            print(f"{i}. From: {sender} | Subject: {subject}")

    else:
        print("Graph API error:", response.text)


if __name__ == "__main__":
    main()