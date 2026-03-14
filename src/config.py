import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

CLIENT_ID = os.getenv("OUTLOOK_CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")

REDIRECT_URI = os.getenv("REDIRECT_URI")

TARGET_SENDER_EMAIL = os.getenv("TARGET_SENDER_EMAIL")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))

EMAIL_RECIPIENTS = [
    e.strip()
    for e in os.getenv("EMAIL_RECIPIENTS", "").split(",")
    if e.strip()
]

EMAIL_SUBJECT = os.getenv("EMAIL_SUBJECT")

TOKEN_ENCRYPTION_KEY = os.getenv("TOKEN_ENCRYPTION_KEY")

DB_PATH = Path(os.getenv("DB_PATH", str(BASE_DIR / "data/email_agent/app.db")))

DOWNLOAD_DIR = Path(os.getenv(
    "DOWNLOAD_DIR",
    str(BASE_DIR / "data/email_agent/downloads")
))

OUTPUT_DIR = Path(os.getenv(
    "OUTPUT_DIR",
    str(BASE_DIR / "data/email_agent/outputs")
))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Ensure required directories exist
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)