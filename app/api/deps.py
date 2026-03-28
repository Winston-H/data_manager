import sqlite3
from dataclasses import dataclass

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.config import get_settings
from app.core.errors import ApiError, ErrorCode
from app.core.error_reasons import ErrorReason
from app.core.security import decode_access_token
from app.db.sqlite import get_db_conn, is_locked_error
from app.services.token_revocation import cleanup_expired_revocations_if_due, is_token_revoked

bearer = HTTPBearer(auto_error=False)
settings = get_settings()


@dataclass
class CurrentUser:
    id: int
    username: str
    role: str


def get_conn() -> sqlite3.Connection:
    yield from get_db_conn()


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
    conn: sqlite3.Connection = Depends(get_conn),
) -> CurrentUser:
    if credentials is None:
        raise ApiError(
            401,
            ErrorCode.UNAUTHORIZED,
            "Authentication required",
            details={"reason": ErrorReason.AUTH_REQUIRED.value, "context": None},
        )
    payload = decode_access_token(credentials.credentials)
    try:
        user_id = int(payload["sub"])
        jti = str(payload["jti"])
    except (KeyError, TypeError, ValueError):
        raise ApiError(
            401,
            ErrorCode.UNAUTHORIZED,
            "Authentication failed",
            details={"reason": ErrorReason.TOKEN_INVALID.value, "context": {"missing_or_invalid": "sub_or_jti"}},
        )
    try:
        cleanup_expired_revocations_if_due(
            conn,
            min_interval_seconds=settings.token_revocation_cleanup_interval_seconds,
        )
    except sqlite3.OperationalError as exc:
        if not is_locked_error(exc):
            raise
    if is_token_revoked(conn, jti):
        raise ApiError(
            401,
            ErrorCode.UNAUTHORIZED,
            "Authentication failed",
            details={"reason": ErrorReason.TOKEN_REVOKED.value, "context": None},
        )

    row = conn.execute(
        "SELECT id, username, role, is_active FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    if row is None:
        raise ApiError(
            401,
            ErrorCode.UNAUTHORIZED,
            "Authentication failed",
            details={"reason": ErrorReason.TOKEN_INVALID.value, "context": {"user_id": user_id}},
        )
    if not bool(row["is_active"]):
        raise ApiError(
            403,
            ErrorCode.FORBIDDEN,
            "User is inactive",
            details={"reason": ErrorReason.USER_INACTIVE.value, "context": {"user_id": user_id}},
        )

    return CurrentUser(
        id=int(row["id"]),
        username=str(row["username"]),
        role=str(row["role"]),
    )


def require_roles(*roles: str):
    def checker(user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
        if user.role not in roles:
            raise ApiError(
                403,
                ErrorCode.FORBIDDEN,
                "Insufficient permissions",
                details={
                    "reason": ErrorReason.INSUFFICIENT_PERMISSIONS.value,
                    "context": {"required_roles": list(roles), "actual_role": user.role},
                },
            )
        return user

    return checker


def get_client_ip(request: Request) -> str | None:
    if request.client is None:
        return None
    return request.client.host
