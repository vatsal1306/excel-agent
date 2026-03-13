# Headless Outlook OAuth2 Integration

This guide details the process of setting up a Python application to programmatically read and send emails using a personal Microsoft Outlook account (`@outlook.com`).

Because the final script will run on a headless server (without a GUI or web browser), the setup involves a two-part flow:

1. **Local Authentication:** Using a local machine with a browser to log in once and generate a persistent token cache file.
2. **Headless Execution:** Moving that cache file to the server, where the script will silently use the embedded Refresh Token to continuously generate new Access Tokens without user intervention.

---

## Phase 1: Azure Portal Setup & App Registration

Microsoft requires all applications that interact with their APIs to be registered within a "Directory" (Tenant) in the Azure Portal, even if the app is strictly for personal use.

### Step 1: Claim Your Free Directory

Newly created personal `@outlook.com` accounts do not have a default Azure directory and will be blocked from creating applications. To force Microsoft to generate one for you:

1. Go to [azure.microsoft.com/free/](https://azure.microsoft.com/free/).
2. Click **Start free** and log in with your Outlook account.
3. Complete the sign-up profile (this requires a credit card for anti-bot identity verification, but you will not be charged for using App Registrations).
4. Once completed, you will be redirected to the Azure Portal, and a "Default Directory" will be automatically provisioned for you.

### Step 2: Register the Application

1. In the [Azure Portal](https://portal.azure.com/), use the top search bar to find and open **App registrations**.
2. Click **+ New registration**.
3. **Name:** Enter a descriptive name (e.g., `Outlook Headless Auth`).
4. **Supported account types (CRITICAL):** Select the bottom option: **"Personal Microsoft accounts only"**.
   *(Note: If that exact phrasing is missing, select "Accounts in any organizational directory and personal Microsoft accounts").*
5. Click **Register**.
6. On the resulting Overview page, copy the **Application (client) ID**. You will need this for your Python environment variables.
#### Save Client ID to Environment Variables

Once you have copied the **Application (client) ID** from the Azure Portal, you must save it securely so your Python scripts can access it dynamically.

Create a file named `.env` in the root directory of your Python project and add your Client ID like this:

```env
OUTLOOK_CLIENT_ID=your_copied_client_id_here
```

### Step 3: Configure Authentication (Redirect URI)

MSAL (Microsoft Authentication Library) needs a registered local endpoint to catch the token after you log in.

1. On the left-hand menu, click **Authentication**.
2. Click **+ Add a platform**, and select **Mobile and desktop applications**.
3. Under "Custom redirect URIs", type exactly: `http://localhost`
4. Click **Configure** (or **Save**).

### Step 4: Grant API Permissions

1. On the left-hand menu, click **API permissions**.
2. Click **+ Add a permission** -> **Microsoft Graph** -> **Delegated permissions**.
3. Search for and check the following two permissions:
   * `Mail.ReadWrite` (Allows reading, moving, and deleting emails)
   * `Mail.Send` (Allows sending emails)
4. Click **Add permissions**.

*(Developer Note: Do not explicitly request the `offline_access` scope in your Python code. The MSAL Python library automatically requests this behind the scenes to generate your refresh token. Adding it manually will throw a reserved scope `ValueError`).*

---

## Phase 2: Generating the Token Cache (Local Machine)

Before deploying to server, you must run an interactive script on a machine with a web browser. This script authenticates you and creates `token_cache.bin`, which contains both your Access Token and the crucial Refresh Token.

**Prerequisites:**
```bash
pip install msal
```

**Script:** `src/oauth2/get_tokens.py`

**Execution:** Run this script. A browser window will open asking you to log in and consent to the app. Once accepted, a token_cache.bin file will appear in your directory.

---

## Phase 3: Headless Execution (Server)

Move the `token_cache.bin` file generated to your headless server. Ensure this file is stored securely, as it grants full API access to your mailbox.

The following script reads the cache file. If the current access token is expired, acquire_token_silent() will automatically use the hidden Refresh Token to fetch a new one, update the .bin file, and return the valid token—all without triggering a browser prompt.

**Script:** `src/oauth2/headless_auth.py`

This script checks if a cached OAuth2 token for Microsoft Outlook is valid and uses it to fetch the latest emails from your inbox.
1. It loads a token cache from a file.
2. Uses MSAL to silently acquire a fresh access token (refreshes if expired).
3. If successful, it makes a request to the Microsoft Graph API to get the top 3 emails (subject and sender).
4. Prints the results or errors.

