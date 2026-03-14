from cryptography.fernet import Fernet
from src.config import TOKEN_ENCRYPTION_KEY


fernet = Fernet(TOKEN_ENCRYPTION_KEY.encode())


def encrypt(text: str) -> str:
    return fernet.encrypt(text.encode()).decode()


def decrypt(text: str) -> str:
    return fernet.decrypt(text.encode()).decode()