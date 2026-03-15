import threading

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from src.Logging import logger
from src.database import init_db
from src.auth import (
    build_login_url,
    exchange_code_for_token,
    store_tokens,
    baseline_inbox_sync
)

from src.email_monitor import start_email_monitor
from src.job_runner import start_job_runner


app = FastAPI(title="Email Automation Service")


@app.on_event("startup")
def startup_event():
    """
    Initialize database and background workers.
    """

    logger.info("Starting application")

    init_db()

    # Start email monitor thread
    threading.Thread(
        target=start_email_monitor,
        daemon=True
    ).start()

    # Start job runner thread
    threading.Thread(
        target=start_job_runner,
        daemon=True
    ).start()

    logger.info("Background workers started")


@app.get("/")
def root():
    return {"status": "service running"}


@app.get("/auth/login")
def login():
    """
    Redirect user to Microsoft login.
    """

    url = build_login_url()

    return RedirectResponse(url)


@app.get("/auth/callback")
def auth_callback(code: str):
    """
    Microsoft OAuth callback.
    """

    token_data = exchange_code_for_token(code)

    user_id = store_tokens(token_data)

    baseline_inbox_sync(user_id)

    return {"message": "Mailbox connected successfully"}