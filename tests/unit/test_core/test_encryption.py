"""
Unit tests for encryption module.

Tests AES-256-GCM field-level encryption for sensitive data.
"""

import pytest
from pfas.core.encryption import (
    encrypt_field,
    decrypt_field,
    derive_key,
    generate_master_key,
    mask_sensitive,
    SALT_LENGTH,
    NONCE_LENGTH,
    KEY_LENGTH,
)
from pfas.core.exceptions import EncryptionError


class TestEncryption:
    """Tests for encryption functions."""

    def test_pan_encryption_roundtrip(self, master_key):
        """Test PAN encryption and decryption (TC-CORE-005)."""
        pan = "AAPPS0793R"

        # Encrypt
        ciphertext, salt = encrypt_field(pan, master_key)

        # Verify encrypted
        assert ciphertext != pan.encode()
        assert len(salt) == SALT_LENGTH

        # Decrypt
        decrypted = decrypt_field(ciphertext, salt, master_key)
        assert decrypted == pan

    def test_aadhaar_encryption_roundtrip(self, master_key):
        """Test Aadhaar encryption and decryption."""
        aadhaar = "123456789012"

        ciphertext, salt = encrypt_field(aadhaar, master_key)
        decrypted = decrypt_field(ciphertext, salt, master_key)

        assert decrypted == aadhaar

    def test_bank_account_encryption_roundtrip(self, master_key):
        """Test bank account number encryption and decryption."""
        bank_account = "1234567890123456"

        ciphertext, salt = encrypt_field(bank_account, master_key)
        decrypted = decrypt_field(ciphertext, salt, master_key)

        assert decrypted == bank_account

    def test_unique_salt_per_encryption(self, master_key):
        """Test that each encryption uses a unique salt."""
        plaintext = "SAME_VALUE"

        _, salt1 = encrypt_field(plaintext, master_key)
        _, salt2 = encrypt_field(plaintext, master_key)

        assert salt1 != salt2

    def test_unique_ciphertext_per_encryption(self, master_key):
        """Test that same plaintext produces different ciphertext each time."""
        plaintext = "SAME_VALUE"

        ciphertext1, _ = encrypt_field(plaintext, master_key)
        ciphertext2, _ = encrypt_field(plaintext, master_key)

        # Due to unique salt and nonce, ciphertext should differ
        assert ciphertext1 != ciphertext2

    def test_wrong_master_key_fails(self, master_key):
        """Test that decryption with wrong key fails."""
        plaintext = "SECRET"
        wrong_key = b"wrong_key_32_bytes_long_here!!"

        ciphertext, salt = encrypt_field(plaintext, master_key)

        with pytest.raises(EncryptionError):
            decrypt_field(ciphertext, salt, wrong_key)

    def test_wrong_salt_fails(self, master_key):
        """Test that decryption with wrong salt fails."""
        plaintext = "SECRET"

        ciphertext, salt = encrypt_field(plaintext, master_key)
        wrong_salt = b"x" * SALT_LENGTH

        with pytest.raises(EncryptionError):
            decrypt_field(ciphertext, wrong_salt, master_key)

    def test_tampered_ciphertext_fails(self, master_key):
        """Test that tampered ciphertext fails to decrypt."""
        plaintext = "SECRET"

        ciphertext, salt = encrypt_field(plaintext, master_key)

        # Tamper with ciphertext
        tampered = bytearray(ciphertext)
        tampered[-1] ^= 0xFF  # Flip bits in last byte
        tampered = bytes(tampered)

        with pytest.raises(EncryptionError):
            decrypt_field(tampered, salt, master_key)

    def test_ciphertext_structure(self, master_key):
        """Test that ciphertext contains nonce prefix."""
        plaintext = "TEST"

        ciphertext, _ = encrypt_field(plaintext, master_key)

        # Ciphertext should be: nonce (12 bytes) + encrypted data + tag (16 bytes)
        assert len(ciphertext) >= NONCE_LENGTH + len(plaintext) + 16

    def test_unicode_encryption(self, master_key):
        """Test encryption of unicode characters."""
        unicode_text = "नमस्ते 你好 مرحبا"

        ciphertext, salt = encrypt_field(unicode_text, master_key)
        decrypted = decrypt_field(ciphertext, salt, master_key)

        assert decrypted == unicode_text

    def test_empty_string_encryption(self, master_key):
        """Test encryption of empty string."""
        empty = ""

        ciphertext, salt = encrypt_field(empty, master_key)
        decrypted = decrypt_field(ciphertext, salt, master_key)

        assert decrypted == empty


class TestKeyDerivation:
    """Tests for key derivation functions."""

    def test_derive_key_length(self):
        """Test that derived key has correct length."""
        master_key = b"password"
        salt = b"x" * SALT_LENGTH

        derived = derive_key(master_key, salt)

        assert len(derived) == KEY_LENGTH

    def test_derive_key_deterministic(self):
        """Test that same inputs produce same key."""
        master_key = b"password"
        salt = b"x" * SALT_LENGTH

        key1 = derive_key(master_key, salt)
        key2 = derive_key(master_key, salt)

        assert key1 == key2

    def test_derive_key_different_salt(self):
        """Test that different salt produces different key."""
        master_key = b"password"
        salt1 = b"a" * SALT_LENGTH
        salt2 = b"b" * SALT_LENGTH

        key1 = derive_key(master_key, salt1)
        key2 = derive_key(master_key, salt2)

        assert key1 != key2

    def test_generate_master_key(self):
        """Test master key generation from password."""
        password = "user_password_123"

        key, salt = generate_master_key(password)

        assert len(key) == KEY_LENGTH
        assert len(salt) == SALT_LENGTH

    def test_generate_master_key_with_salt(self):
        """Test master key generation with provided salt."""
        password = "user_password_123"
        fixed_salt = b"fixed_salt_here!"

        key1, salt1 = generate_master_key(password, fixed_salt)
        key2, salt2 = generate_master_key(password, fixed_salt)

        assert key1 == key2
        assert salt1 == salt2 == fixed_salt


class TestMaskSensitive:
    """Tests for mask_sensitive function."""

    def test_mask_pan(self):
        """Test PAN masking."""
        pan = "AAPPS0793R"
        masked = mask_sensitive(pan)

        # 10 chars: 4 visible at start + 2 masked + 4 visible at end
        assert masked == "AAPP**793R"
        assert len(masked) == len(pan)

    def test_mask_aadhaar(self):
        """Test Aadhaar masking."""
        aadhaar = "123456789012"
        masked = mask_sensitive(aadhaar)

        assert masked == "1234****9012"

    def test_mask_short_value(self):
        """Test masking of short value."""
        short = "1234"
        masked = mask_sensitive(short, visible_chars=2)

        assert masked == "****"

    def test_mask_custom_visible_chars(self):
        """Test masking with custom visible chars."""
        value = "1234567890"
        masked = mask_sensitive(value, visible_chars=2)

        assert masked == "12******90"
