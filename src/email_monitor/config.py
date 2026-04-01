"""
Configuration for the email monitoring agent.
Values are read from ``.env`` (if present) and overridden by process environment
variables (e.g. Docker Compose ``env_file`` / ``environment``).
"""

import os
from dataclasses import dataclass

from src import ROOT_DIR, envs
from src.Logging import logger


@dataclass(frozen=True)
class MonitorConfig:
    """
    Immutable configuration bundle for :class:`EmailMonitor`.
    """

    target_sender_email: str
    poll_interval_seconds: int
    db_path: str
    client_id: str
    client_secret: str
    tenant_id: str

    @classmethod
    def from_env(cls) -> "MonitorConfig":
        """
        Build a ``MonitorConfig`` from environment (``.env`` file plus ``os.environ``).

        Required keys:
            ``TARGET_SENDER_EMAIL``, ``OUTLOOK_CLIENT_ID``,
            ``OUTLOOK_CLIENT_SECRET``, ``OUTLOOK_TENANT_ID``

        Optional keys (with defaults):
            ``POLL_INTERVAL_SECONDS`` (default 300),
            ``DB_PATH`` (default ``data/email_agent.db`` relative to project root)

        Raises:
            ValueError: If any required key is missing or empty.
        """
        target_sender = cls._require("TARGET_SENDER_EMAIL")
        client_id = cls._require("OUTLOOK_CLIENT_ID")
        client_secret = cls._require("OUTLOOK_CLIENT_SECRET")
        tenant_id = cls._require("OUTLOOK_TENANT_ID")

        poll_interval = int(envs.get("POLL_INTERVAL_SECONDS", "300"))

        db_path = envs.get("DB_PATH", "")
        if not db_path:
            db_path = os.path.join(ROOT_DIR, "data", "email_agent.db")
        elif not os.path.isabs(db_path):
            db_path = os.path.join(ROOT_DIR, db_path)

        config = cls(
            target_sender_email=target_sender,
            poll_interval_seconds=poll_interval,
            db_path=db_path,
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
        )

        logger.info(
            f"MonitorConfig loaded: target_sender={target_sender}, poll_interval={poll_interval}s, db={db_path}"
        )
        return config

    @staticmethod
    def _require(key: str) -> str:
        """ Retrieve the value of a required environment variable. """
        value = envs.get(key, "")
        if not value:
            raise ValueError(
                f"Required environment variable '{key}' is not set (.env or environment)"
            )
        return value
