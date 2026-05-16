from cryptography.fernet import Fernet, InvalidToken
from src.config import TOKEN_ENCRYPTION_KEY
from src.Logging import logger

fernet = Fernet(TOKEN_ENCRYPTION_KEY.encode())


def encrypt_token(text: str) -> str:
    """
    Encrypt token before storing in DB
    """
    if not text:
        return None
    try:
        return fernet.encrypt(text.encode()).decode()
    except Exception as e:
        logger.error(f"Token encryption failed: {e}")
        raise


def decrypt_token(text: str) -> str:
    """
     Decrypt token before using in API calls
     """

    if not text:
        return None

    try:
        return fernet.decrypt(text.encode()).decode()
    except InvalidToken:
        logger.error("Invalid or corrupted token encountered during decryption")
        return None
