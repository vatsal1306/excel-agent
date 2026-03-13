import os

import msal
from src import envs

# 1. Paste your Application (client) ID from Phase 2 here
CLIENT_ID = envs['OUTLOOK_CLIENT_ID']

# 2. Force consumer-only authentication
AUTHORITY = 'https://login.microsoftonline.com/consumers'
SCOPES = ['Mail.ReadWrite', 'Mail.Send']
CACHE_FILE = 'token_cache.bin'


def main():
    cache = msal.SerializableTokenCache()
    if os.path.exists(CACHE_FILE):
        cache.deserialize(open(CACHE_FILE, "r").read())

    app = msal.PublicClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        token_cache=cache
    )

    result = None
    accounts = app.get_accounts()

    if accounts:
        print("Found existing account. Attempting to refresh...")
        result = app.acquire_token_silent(SCOPES, account=accounts[0])

    if not result:
        print("Opening browser for authentication...")
        # This spins up the local server on http://localhost to catch the token
        result = app.acquire_token_interactive(scopes=SCOPES)

    if "access_token" in result:
        print("\n✔ Successfully authenticated!")

        if cache.has_state_changed:
            with open(CACHE_FILE, "w") as f:
                f.write(cache.serialize())

        print(f"Saved cache to '{CACHE_FILE}'. Copy this file to your headless server.")
    else:
        print("\n❌ Failed to acquire token.")
        print(f"Error: {result.get('error')}")
        print(f"Description: {result.get('error_description')}")


if __name__ == '__main__':
    main()
