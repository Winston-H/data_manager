import base64
import hashlib
import hmac
import secrets
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import lru_cache

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


@dataclass
class EncryptedField:
    cipher: bytes
    nonce: bytes


def normalize_text(text: str, *, lower: bool = True) -> str:
    normalized = unicodedata.normalize("NFKC", text).strip()
    return normalized.lower() if lower else normalized


def build_ngrams(text: str, sizes: list[int]) -> list[str]:
    tokens: list[str] = []
    for size in sizes:
        if len(text) < size:
            continue
        for i in range(len(text) - size + 1):
            tokens.append(text[i : i + size])
    if not tokens and text:
        tokens.append(text)
    return sorted(set(tokens))


def blind_index_token(index_key: bytes, token: str) -> bytes:
    return hmac.new(index_key, token.encode("utf-8"), hashlib.sha256).digest()


@lru_cache(maxsize=32)
def _aesgcm_for_key(key: bytes) -> AESGCM:
    return AESGCM(key)


@lru_cache(maxsize=32)
def _fernet_for_key(key: bytes) -> Fernet:
    return Fernet(base64.urlsafe_b64encode(key))


def encrypt_value(key: bytes, plaintext: str) -> EncryptedField:
    nonce = secrets.token_bytes(12)
    aesgcm = _aesgcm_for_key(key)
    cipher = aesgcm.encrypt(nonce, plaintext.encode("utf-8"), None)
    return EncryptedField(cipher=cipher, nonce=nonce)


def decrypt_value(key: bytes, cipher: bytes, nonce: bytes) -> str:
    aesgcm = _aesgcm_for_key(key)
    plaintext = aesgcm.decrypt(nonce, cipher, None)
    return plaintext.decode("utf-8")


def encrypt_id_value(key: bytes, plaintext: str) -> str:
    return _fernet_for_key(key).encrypt(plaintext.encode("utf-8")).decode("utf-8")


def decrypt_id_value(key: bytes, cipher_text: str) -> str:
    return _fernet_for_key(key).decrypt(cipher_text.encode("utf-8")).decode("utf-8")


def encrypt_id_values(key: bytes, values: list[str], *, workers: int = 1) -> list[str]:
    if not values:
        return []
    if workers <= 1 or len(values) <= 1:
        return [encrypt_id_value(key, value) for value in values]

    worker_count = max(1, workers)
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="fernet-batch") as executor:
        return list(executor.map(lambda value: encrypt_id_value(key, value), values))
