"""
AES-256-GCM field-level encryption for sensitive data.

Provides encryption for PAN, Aadhaar, Bank Account numbers, etc.
Each field is encrypted with a unique salt for added security.
"""

import os
from typing import Tuple

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

from pfas.core.exceptions import EncryptionError


# Constants
SALT_LENGTH = 16
NONCE_LENGTH = 12
KEY_LENGTH = 32  # 256 bits
PBKDF2_ITERATIONS = 100000


def derive_key(master_key: bytes, salt: bytes) -> bytes:
    """
    Derive an encryption key from master key using PBKDF2.

    Args:
        master_key: The master key (password bytes)
        salt: Random salt for key derivation

    Returns:
        32-byte derived key for AES-256
    """
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=KEY_LENGTH,
        salt=salt,
        iterations=PBKDF2_ITERATIONS,
        backend=default_backend(),
    )
    return kdf.derive(master_key)


def encrypt_field(plaintext: str, master_key: bytes) -> Tuple[bytes, bytes]:
    """
    Encrypt a sensitive field with unique salt.

    Args:
        plaintext: The string to encrypt (e.g., PAN number)
        master_key: The master encryption key

    Returns:
        Tuple of (ciphertext, salt) where ciphertext includes the nonce

    Raises:
        EncryptionError: If encryption fails
    """
    try:
        # Generate unique salt for this field
        salt = os.urandom(SALT_LENGTH)

        # Generate random nonce
        nonce = os.urandom(NONCE_LENGTH)

        # Derive key from master key and salt
        key = derive_key(master_key, salt)

        # Encrypt using AES-256-GCM
        aesgcm = AESGCM(key)
        ciphertext = aesgcm.encrypt(nonce, plaintext.encode("utf-8"), None)

        # Return nonce + ciphertext, and salt
        return nonce + ciphertext, salt

    except Exception as e:
        raise EncryptionError(f"Encryption failed: {e}")


def decrypt_field(ciphertext: bytes, salt: bytes, master_key: bytes) -> str:
    """
    Decrypt a sensitive field.

    Args:
        ciphertext: The encrypted data (nonce + ciphertext)
        salt: The salt used during encryption
        master_key: The master encryption key

    Returns:
        Decrypted plaintext string

    Raises:
        EncryptionError: If decryption fails
    """
    try:
        # Derive key from master key and salt
        key = derive_key(master_key, salt)

        # Extract nonce and ciphertext
        nonce = ciphertext[:NONCE_LENGTH]
        ct = ciphertext[NONCE_LENGTH:]

        # Decrypt using AES-256-GCM
        aesgcm = AESGCM(key)
        plaintext = aesgcm.decrypt(nonce, ct, None)

        return plaintext.decode("utf-8")

    except Exception as e:
        raise EncryptionError(f"Decryption failed: {e}")


def generate_master_key(password: str, salt: bytes = None) -> Tuple[bytes, bytes]:
    """
    Generate a master key from a password.

    Args:
        password: User password
        salt: Optional salt (generated if not provided)

    Returns:
        Tuple of (master_key, salt)
    """
    if salt is None:
        salt = os.urandom(SALT_LENGTH)

    master_key = derive_key(password.encode("utf-8"), salt)
    return master_key, salt


def mask_sensitive(value: str, visible_chars: int = 4) -> str:
    """
    Mask a sensitive value for display (e.g., "AAPPS****R").

    Args:
        value: The sensitive value to mask
        visible_chars: Number of characters to show at start and end

    Returns:
        Masked string
    """
    if len(value) <= visible_chars * 2:
        return "*" * len(value)

    return value[:visible_chars] + "*" * (len(value) - visible_chars * 2) + value[-visible_chars:]
