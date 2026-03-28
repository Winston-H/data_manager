from datetime import datetime, timedelta, timezone
from uuid import uuid4

import jwt

try:
    from argon2 import PasswordHasher
    from argon2.exceptions import Argon2Error, VerifyMismatchError
except ImportError:
    PasswordHasher = None  # type: ignore[assignment]

    class Argon2Error(Exception):
        pass

    class VerifyMismatchError(Argon2Error):
        pass

try:
    import bcrypt
except ImportError:
    bcrypt = None  # type: ignore[assignment]

from app.core.config import get_settings
from app.core.errors import ApiError, ErrorCode
from app.core.error_reasons import ErrorReason

password_hasher = PasswordHasher() if PasswordHasher is not None else None


def hash_password(password: str) -> str:
    if password_hasher is not None:
        return password_hasher.hash(password)

    if bcrypt is not None:
        hashed = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())
        return hashed.decode("utf-8")

    raise RuntimeError("No supported password hashing backend is available")


def verify_password(password: str, password_hash: str) -> bool:
    if password_hash.startswith("$argon2"):
        if password_hasher is None:
            return False
        try:
            return password_hasher.verify(password_hash, password)
        except (VerifyMismatchError, Argon2Error):
            return False

    if password_hash.startswith(("$2a$", "$2b$", "$2x$", "$2y$")):
        if bcrypt is None:
            return False
        try:
            return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
        except ValueError:
            return False

    if password_hasher is not None:
        try:
            return password_hasher.verify(password_hash, password)
        except (VerifyMismatchError, Argon2Error):
            pass

    if bcrypt is not None:
        try:
            return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
        except ValueError:
            pass

    return False


def create_access_token(user_id: int, username: str, role: str) -> str:
    settings = get_settings()
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "username": username,
        "role": role,
        "jti": uuid4().hex,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=settings.jwt_expire_seconds)).timestamp()),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm="HS256")


def decode_access_token(token: str) -> dict:
    settings = get_settings()
    try:
        return jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])
    except jwt.PyJWTError as exc:
        raise ApiError(
            401,
            ErrorCode.UNAUTHORIZED,
            "Authentication failed",
            details={"reason": ErrorReason.TOKEN_INVALID.value, "context": {"error_type": type(exc).__name__}},
        ) from exc
