"""
Logging configuration for the application.
Creates a Logs directory if it doesn't exist and sets up a rotating file handler and a stream handler.
"""
import logging
import os
from logging.handlers import RotatingFileHandler

logger = logging.getLogger("CRS_Agent")
logger.setLevel(logging.INFO)

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_LOG_DIR = os.path.join(_PROJECT_ROOT, "Logs")
_LOG_FILE = os.path.join(_LOG_DIR, "app.log")

os.makedirs(_LOG_DIR, exist_ok=True)

formatter = logging.Formatter('%(asctime)s | %(filename)s | %(lineno)s | %(levelname)s | %(message)s')

file_handler = RotatingFileHandler(_LOG_FILE, maxBytes=10_000_000, backupCount=5)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)
