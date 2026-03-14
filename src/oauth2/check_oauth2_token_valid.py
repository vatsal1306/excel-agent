import os

import msal
import requests

from src import envs

# Use your exact Client ID
CLIENT_ID = envs['OUTLOOK_CLIENT_ID']
AUTHORITY = 'https://login.microsoftonline.com/consumers'
SCOPES = ['Mail.ReadWrite', 'Mail.Send']
CACHE_FILE = 'token_cache.bin'


def get_access_token():
    """Silently fetches a fresh access token using the local cache."""
    cache = msal.SerializableTokenCache()

    if os.path.exists(CACHE_FILE):
        cache.deserialize(open(CACHE_FILE, "r").read())
    else:
        raise Exception(f"'{CACHE_FILE}' not found. Please run the interactive login script first.")

    app = msal.PublicClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        token_cache=cache
    )

    accounts = app.get_accounts()
    if not accounts:
        raise Exception("No accounts found in cache. Please run the interactive login script again.")

    # This is the magic line. It grabs a valid token, and if it's expired,
    # it automatically uses the Refresh Token to get a new one.
    result = app.acquire_token_silent(SCOPES, account=accounts[0])

    if result and "access_token" in result:
        # If the MSAL library had to use the refresh token, it might give us a NEW refresh token.
        # This saves the updated cache back to the file so it never expires.
        if cache.has_state_changed:
            with open(CACHE_FILE, "w") as f:
                f.write(cache.serialize())

        return result['access_token']
    else:
        raise Exception(f"Failed to get token silently. You may need to log in again. Error: {result}")


def main():
    try:
        token = get_access_token()
        print("✔ Token acquired successfully!\n")

        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

        # We are hitting the mailbox endpoint instead of the profile endpoint
        print("Testing connection to Outlook Inbox...")

        # Gets the top 3 messages, selecting only the subject and sender to keep it clean
        endpoint = 'https://graph.microsoft.com/v1.0/me/messages?$select=subject,from&$top=3'
        response = requests.get(endpoint, headers=headers)

        if response.status_code == 200:
            emails = response.json().get('value', [])
            print("✔ Success! Here are your latest emails:\n")
            for i, email in enumerate(emails, 1):
                sender_name = email.get('from', {}).get('emailAddress', {}).get('name', 'Unknown')
                subject = email.get('subject', 'No Subject')
                print(f"{i}. From: {sender_name} | Subject: {subject}")
        else:
            print(f"❌ API Error: {response.status_code}")
            print(response.json())

    except Exception as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    main()
