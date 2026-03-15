"""Fernet-based symmetric encryption for storing database credentials."""

from __future__ import annotations

import logging

from cryptography.fernet import Fernet, InvalidToken

from app.core.config import get_settings

logger = logging.getLogger(__name__)


def _get_fernet() -> Fernet:
    """Return a Fernet instance initialised with the app-wide encryption key."""
    key = get_settings().encryption_key
    if not key:
        raise RuntimeError(
            "ENCRYPTION_KEY is not set. "
            "Generate one with: python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'"
        )
    return Fernet(key.encode())


def encrypt_value(value: str) -> str:
    """Encrypt a plaintext string and return the base64-encoded ciphertext."""
    if not value:
        return ""
    return _get_fernet().encrypt(value.encode()).decode()


def decrypt_value(encrypted: str) -> str:
    """Decrypt a Fernet token back to plaintext.

    Returns an empty string if the input is empty or decryption fails.
    """
    if not encrypted:
        return ""
    try:
        return _get_fernet().decrypt(encrypted.encode()).decode()
    except InvalidToken:
        logger.error("Failed to decrypt value — token invalid or key mismatch.")
        raise ValueError("Decryption failed. Check that the ENCRYPTION_KEY has not changed.")
