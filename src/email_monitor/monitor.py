"""
Core email monitoring loop.

:class:`EmailMonitor` polls each registered user's Outlook mailbox at a configurable interval. For *new* users
(no baseline yet) it records the latest message id and sleeps. For *existing* users it fetches every email received
since the last checkpoint, evaluates the trigger condition (sender match **and** at least one ``.xlsx`` attachment),
and — when matched — downloads the attachment and invokes ``run_pipeline``.
"""

import os
import signal
import time
from datetime import datetime
from typing import Optional

from src import ROOT_DIR
from src.Logging import logger
from src.db.database import Database
from src.db.models import User
from src.email_monitor.config import MonitorConfig
from src.email_monitor.exceptions import TokenExpiredError, TokenRefreshError
from src.email_monitor.graph_client import Attachment, EmailMessage, GraphClient
from src.email_monitor.token_manager import TokenManager
from src.run_transforms import run_pipeline


class EmailMonitor:
    """
    Long-running agent that watches registered mailboxes for matching emails.

    Args:
        config: A fully-populated :class:`MonitorConfig`.
    """

    _MAX_TOKEN_REFRESH_RETRIES = 1

    def __init__(self, config: MonitorConfig):
        self._config = config
        self._db = Database(config.db_path)
        self._token_manager = TokenManager(
            client_id=config.client_id,
            client_secret=config.client_secret,
            tenant_id=config.tenant_id,
        )
        self._running = True

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """
        Begin the monitor loop. Blocks until ``SIGINT`` / ``SIGTERM``.
        """
        self._register_signal_handlers()
        self._db.connect()
        self._db.initialize()

        logger.info(
            f"Email monitor started.  "
            f"Target sender: {self._config.target_sender_email}  |  "
            f"Poll interval: {self._config.poll_interval_seconds}s"
        )

        try:
            while self._running:
                self._poll_cycle()
                if self._running:
                    logger.info(
                        f"Cycle complete. Sleeping for "
                        f"{self._config.poll_interval_seconds}s ..."
                    )
                    self._interruptible_sleep(self._config.poll_interval_seconds)
        finally:
            self._db.close()
            logger.info("Email monitor stopped.")

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------

    def _register_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum: int, _frame) -> None:
        logger.info(f"Received signal {signum}. Initiating graceful shutdown …")
        self._running = False

    def _interruptible_sleep(self, seconds: int) -> None:
        """Sleep in small increments so we can react to shutdown signals quickly."""
        elapsed = 0
        while elapsed < seconds and self._running:
            time.sleep(min(1, seconds - elapsed))
            elapsed += 100

    # ------------------------------------------------------------------
    # Poll cycle
    # ------------------------------------------------------------------

    def _poll_cycle(self) -> None:
        """Execute one full pass over all registered users."""
        users = self._db.get_all_users()
        if not users:
            logger.info("No users registered. Waiting for user sign-in via /auth/login.")
            return

        logger.info(f"Processing {len(users)} user(s)")
        for user in users:
            if not self._running:
                break
            try:
                self._process_user(user)
            except Exception as e:
                logger.exception(f"Unhandled error while processing user {user.email}. {e}")

    # ------------------------------------------------------------------
    # Per-user processing
    # ------------------------------------------------------------------

    def _process_user(self, user: User) -> None:
        """
        Handle a single user. Automatically refreshes the access token on 401 and retries once.
        """
        client = GraphClient(user.access_token)

        for attempt in range(1 + self._MAX_TOKEN_REFRESH_RETRIES):
            try:
                if user.is_new:
                    self._handle_new_user(user, client)
                else:
                    self._handle_existing_user(user, client)
                return
            except TokenExpiredError:
                if attempt < self._MAX_TOKEN_REFRESH_RETRIES:
                    client = self._try_refresh_token(user, client)
                    if client is None:
                        return
                else:
                    logger.error(
                        f"Access token still invalid after refresh for {user.email}. "
                        f"User may need to re-authenticate."
                    )

    def _try_refresh_token(
            self, user: User, client: GraphClient
    ) -> Optional[GraphClient]:
        """
        Attempt a token refresh.  On success, persists new tokens and
        returns an updated :class:`GraphClient`.  On failure, logs the
        error and returns ``None``.
        """
        logger.info(f"Access token expired for {user.email}. Refreshing.")
        try:
            new_access, new_refresh = self._token_manager.refresh_access_token(
                user.refresh_token
            )
            self._db.update_tokens(user.id, new_access, new_refresh)
            client.update_token(new_access)
            logger.info(f"Token refreshed successfully for {user.email}.")
            return client
        except TokenRefreshError:
            logger.exception(
                f"Token refresh failed for {user.email}. "
                f"User must re-authenticate via /auth/login."
            )
            return None

    # ------------------------------------------------------------------
    # New-user flow (establish baseline)
    # ------------------------------------------------------------------

    def _handle_new_user(self, user: User, client: GraphClient) -> None:
        """
        For a brand-new user: read the single latest email, store its id
        and timestamp as the baseline, then return.  Nothing is processed.
        """
        latest = client.get_latest_message()
        if latest is None:
            logger.info(f"Mailbox for {user.email} is empty. Will check again next cycle.")
            return

        self._db.update_last_processed(
            user.id,
            email_id=latest.id,
            processed_datetime=latest.received_datetime,
        )
        logger.info(
            f"Baseline set for {user.email}: "
            f"msg_id={latest.id}, dt={latest.received_datetime}"
        )

    # ------------------------------------------------------------------
    # Existing-user flow (check for new emails)
    # ------------------------------------------------------------------

    def _handle_existing_user(self, user: User, client: GraphClient) -> None:
        """
        Fetch all emails received since the last checkpoint, evaluate the
        trigger condition, and process matches.
        """
        new_messages = client.get_messages_since(user.last_processed_datetime)

        # Deduplicate: the ``ge`` filter may re-include the checkpoint email
        if user.last_processed_email_id:
            new_messages = [
                m for m in new_messages if m.id != user.last_processed_email_id
            ]

        if not new_messages:
            logger.info(f"No new emails for {user.email}.")
            return

        logger.info(f"Found {len(new_messages)} new email(s) for {user.email}.")

        matching = 0
        for message in new_messages:
            if self._matches_condition(message):
                matching += 1
                self._process_matching_email(user, client, message)

        if matching == 0:
            logger.info(f"No emails matched the trigger condition for {user.email}.")

        # Advance the checkpoint to the newest email in this batch
        newest = new_messages[-1]
        self._db.update_last_processed(
            user.id,
            email_id=newest.id,
            processed_datetime=newest.received_datetime,
        )

    # ------------------------------------------------------------------
    # Condition evaluation
    # ------------------------------------------------------------------

    def _matches_condition(self, message: EmailMessage) -> bool:
        """
        An email matches when **both** conditions are true:

        1. The sender address equals the configured ``target_sender_email``
           (case-insensitive).
        2. The message has at least one attachment (``hasAttachments`` flag).

        .. note::
           The actual ``.xlsx`` extension check happens in
           :meth:`_process_matching_email` when attachments are fetched.
        """
        sender_ok = (
                message.sender_email.lower()
                == self._config.target_sender_email.lower()
        )
        return sender_ok and message.has_attachments

    # ------------------------------------------------------------------
    # Attachment download  +  pipeline execution
    # ------------------------------------------------------------------

    def _process_matching_email(
            self,
            user: User,
            client: GraphClient,
            message: EmailMessage,
    ) -> None:
        """
        Download every ``.xlsx`` attachment from *message* and run the
        transformation pipeline on each.
        """
        attachments = client.get_xlsx_attachments(message.id)
        if not attachments:
            logger.info(
                f"Email '{message.subject}' from {message.sender_email} "
                f"has attachments but none are .xlsx — skipping."
            )
            return

        for attachment in attachments:
            logger.info(
                f"Running pipeline for attachment '{attachment.name}' "
                f"from email '{message.subject}' (user={user.email})."
            )
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_root = os.path.join(
                    ROOT_DIR, "data", "runs", "email", timestamp
                )
                output_dir = run_pipeline(
                    attachment.content_bytes, output_root=output_root
                )
                self._handle_pipeline_output(user, message, attachment, output_dir)
            except Exception:
                logger.exception(
                    f"Pipeline failed for attachment '{attachment.name}' "
                    f"from email '{message.subject}'."
                )

    # ------------------------------------------------------------------
    # Post-pipeline placeholder
    # ------------------------------------------------------------------

    @staticmethod
    def _handle_pipeline_output(
            user: User,
            message: EmailMessage,
            attachment: Attachment,
            output_dir: str,
    ) -> None:
        """
        Placeholder hook for post-pipeline actions.

        TODO: Implement desired behaviour — e.g. email results back to the
        user, upload to a shared drive, send a webhook notification, etc.
        """
        logger.info(
            f"Pipeline output for user={user.email}, "
            f"email='{message.subject}', attachment='{attachment.name}' "
            f"saved to: {output_dir}"
        )
