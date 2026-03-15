from pathlib import Path
from src import envs, ROOT_DIR

CLIENT_ID = envs.get("OUTLOOK_CLIENT_ID")
CLIENT_SECRET = envs.get("CLIENT_SECRET")
TENANT_ID = envs.get("TENANT_ID")

REDIRECT_URI = envs.get("REDIRECT_URI")

TARGET_SENDER_EMAIL = envs.get("TARGET_SENDER_EMAIL")

POLL_INTERVAL_SECONDS = int(envs.get("POLL_INTERVAL_SECONDS", "300"))

EMAIL_RECIPIENTS = [
    e.strip()
    for e in envs.get("EMAIL_RECIPIENTS", "").split(",")
    if e.strip()
]

EMAIL_SUBJECT = envs.get("EMAIL_SUBJECT")

TOKEN_ENCRYPTION_KEY = envs.get("TOKEN_ENCRYPTION_KEY")

DB_PATH = Path(envs.get("DB_PATH", str(ROOT_DIR / "data/email_agent/app.db")))

DOWNLOAD_DIR = Path(envs.get(
    "DOWNLOAD_DIR",
    str(ROOT_DIR / "data/email_agent/downloads")
))

OUTPUT_DIR = Path(envs.get(
    "OUTPUT_DIR",
    str(ROOT_DIR / "data/email_agent/outputs")
))

LOG_LEVEL = envs.get("LOG_LEVEL", "INFO")

# Ensure required directories exist
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
