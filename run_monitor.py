"""
CLI entry point for the email monitoring agent.

Usage:
    # Start the monitoring loop
    python run_monitor.py

    # Import an existing MSAL token-cache .bin file into the database
    python run_monitor.py --import-bin token_cache_20260327_164205.bin
"""

import argparse
import sys

from src.Logging import logger


def _import_bin(bin_path: str) -> None:
    """Import a .bin token-cache file and register the user in the DB."""
    from src.db.database import Database
    from src.email_monitor.config import MonitorConfig

    config = MonitorConfig.from_env()

    with Database(config.db_path) as db:
        user = db.import_token_cache_file(bin_path)
        logger.info(
            f"Successfully imported token cache. User registered: id={user.id}, email={user.email}"
        )
        print(f"User registered: id={user.id}, email={user.email}")


def _start_monitor() -> None:
    """Instantiate and start the email monitoring loop."""
    from src.email_monitor.config import MonitorConfig
    from src.email_monitor.monitor import EmailMonitor

    config = MonitorConfig.from_env()
    monitor = EmailMonitor(config)
    monitor.start()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CRS Email Monitoring Agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python run_monitor.py                              Start monitoring\n"
            "  python run_monitor.py --import-bin cache.bin        Import a token cache file\n"
        ),
    )

    parser.add_argument(
        "--import-bin",
        # default='token_cache_20260327_164205.bin',
        metavar="PATH",
        help="Import an MSAL token-cache .bin file into the database and exit.",
    )

    args = parser.parse_args()

    if args.import_bin:
        try:
            _import_bin(args.import_bin)
        except Exception:
            logger.exception("Failed to import token cache file.")
            sys.exit(1)
    else:
        _start_monitor()


if __name__ == "__main__":
    main()
