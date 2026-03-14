from cryptography.fernet import Fernet
from src.config import TOKEN_ENCRYPTION_KEY


fernet = Fernet(TOKEN_ENCRYPTION_KEY.encode())


def encrypt_token(text: str) -> str:
    """
    Encrypt token before storing in DB
    """
    return fernet.encrypt(text.encode()).decode()


def decrypt_token(text: str) -> str:
    """
     Decrypt token before using in API calls
     """
    return fernet.decrypt(text.encode()).decode()