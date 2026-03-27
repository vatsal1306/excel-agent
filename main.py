from datetime import datetime
from typing import Dict

import msal
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware

# It is highly recommended to store your REDIRECT_URI and a static SESSION_SECRET in your .env as well
from src import envs
from src.Logging import logger

CLIENT_ID = envs.get('OUTLOOK_CLIENT_ID')
CLIENT_SECRET = envs.get('OUTLOOK_CLIENT_SECRET')
TENANT_ID = envs.get('OUTLOOK_TENANT_ID')
REDIRECT_URI = envs.get('REDIRECT_URI')

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["Mail.ReadWrite", "Mail.Send"]

app = FastAPI(
    title="CRS Email Automation Service",
    description="Backend service handling Outlook automation and standard API endpoints.",
    version="1.0.0"
)

# Secure session middleware for OAuth state tracking
SESSION_SECRET = envs.get('SESSION_SECRET')
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)


def _build_msal_app(cache: msal.SerializableTokenCache = None) -> msal.ConfidentialClientApplication:
    """
    Constructs the MSAL Confidential Client Application.
    
    Args:
        cache (msal.SerializableTokenCache, optional): The token cache to use. Defaults to None.
    Returns:
        msal.ConfidentialClientApplication: The configured MSAL application instance.
    """
    return msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET,
        token_cache=cache
    )


@app.get("/health", tags=["System"])
async def health_check() -> Dict[str, str]:
    """
    Standard health check endpoint to verify the API status.
    """
    return {"status": "ok", "message": "FastAPI service is running securely."}


@app.get("/auth/login", tags=["Authentication"])
async def login(request: Request) -> RedirectResponse:
    """
    Initiates the OAuth2 flow. Instead of showing a link, this directly redirects
    the client to the Microsoft login page for a seamless experience.
    """
    try:
        msal_app = _build_msal_app()
        flow = msal_app.initiate_auth_code_flow(
            SCOPES,
            redirect_uri=REDIRECT_URI
        )

        # Save the MSAL flow state in the user's secure session
        request.session["flow"] = flow
        logger.info("Initiated MSAL auth flow and redirecting user to Microsoft.")

        return RedirectResponse(url=flow["auth_uri"])

    except Exception as e:
        logger.error(f"Failed to initiate login flow: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error during login initiation.")


@app.get("/callback", response_class=HTMLResponse, tags=["Authentication"])
async def callback(request: Request) -> HTMLResponse:
    """
    Catches the redirect from Microsoft, extracts the authorization code,
    fetches the tokens, and securely saves the token_cache.bin file.
    """
    try:
        # Retrieve the flow from the session
        flow = request.session.get("flow")
        if not flow:
            logger.warning("Callback accessed without a valid active session flow.")
            return HTMLResponse("<h3>Error:</h3> <p>Session expired or invalid flow. Please try logging in again.</p>",
                                status_code=400)

        cache = msal.SerializableTokenCache()
        msal_app = _build_msal_app(cache=cache)

        # Parse query parameters sent back by Microsoft
        query_params = dict(request.query_params)
        logger.info("Processing authorization code from Microsoft...")

        result = msal_app.acquire_token_by_auth_code_flow(flow, query_params)

        if "error" in result:
            error_msg = result.get('error_description', 'Unknown error occurred.')
            logger.error(f"MSAL Token Acquisition Error: {error_msg}")
            return HTMLResponse(f"<h3>Login failed:</h3> <p>{error_msg}</p>", status_code=401)

        # Generate a unique filename with a timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dynamic_cache_file = f"token_cache_{timestamp}.bin"

        # Write the successful token cache to the file system
        with open(dynamic_cache_file, "w") as f:
            f.write(cache.serialize())

        logger.info(f"Successfully authenticated and saved tokens to {dynamic_cache_file}")

        # Clear the session flow for security
        request.session.pop("flow", None)

        return HTMLResponse(
            "<h2>✔ Authentication successful!</h2>"
            "<p>Your backend is securely connected. You can safely close this window.</p>"
        )

    except Exception as e:
        logger.error(f"Error during OAuth callback processing: {str(e)}", exc_info=True)
        return HTMLResponse(f"<h3>An unexpected error occurred:</h3> <p>Please contact support.</p>", status_code=500)
