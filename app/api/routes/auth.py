import sqlite3
import logging

from fastapi import APIRouter, Depends, Request
from fastapi.security import HTTPAuthorizationCredentials

from app.api.openapi_responses import RESP_400, RESP_401, RESP_403, RESP_404, RESP_500
from app.api.deps import CurrentUser, bearer, get_client_ip, get_conn, get_current_user
from app.core.config import get_settings
from app.core.errors import ApiError, ErrorCode
from app.core.error_reasons import ErrorReason
from app.core.security import create_access_token, decode_access_token, verify_password
from app.core.time import now_local_sql
from app.db.sqlite import is_locked_error
from app.schemas.auth import LoginRequest, LoginResponse, UserProfileResponse
from app.schemas.common import MessageResponse
from app.services.audit import write_audit
from app.services.quota import get_quota
from app.services.token_revocation import cleanup_expired_revocations, revoke_token
from app.services.users import get_user_by_username

router = APIRouter()
logger = logging.getLogger("app.auth")


@router.post(
    "/login",
    response_model=LoginResponse,
    responses={**RESP_400, **RESP_401, **RESP_403, **RESP_500},
)
def login(payload: LoginRequest, request: Request, conn: sqlite3.Connection = Depends(get_conn)):
    user = get_user_by_username(conn, payload.username)
    if user is None or not verify_password(payload.password, user["password_hash"]):
        write_audit(
            conn,
            user_id=None,
            username=payload.username,
            user_role=None,
            ip_address=get_client_ip(request),
            action_type="LOGIN",
            action_result="FAILED",
            detail={"reason": ErrorReason.INVALID_CREDENTIALS.value},
            trace_id=request.state.trace_id,
        )
        # Keep failed login audit even when request returns an error response.
        conn.commit()
        raise ApiError(
            401,
            ErrorCode.UNAUTHORIZED,
            "Authentication failed",
            details={"reason": ErrorReason.INVALID_CREDENTIALS.value, "context": {"username": payload.username}},
        )

    if not bool(user["is_active"]):
        raise ApiError(
            403,
            ErrorCode.FORBIDDEN,
            "User is inactive",
            details={"reason": ErrorReason.USER_INACTIVE.value, "context": {"user_id": user["id"]}},
        )

    try:
        conn.execute("UPDATE users SET last_login_at = ? WHERE id = ?", (now_local_sql(), user["id"]))
    except sqlite3.OperationalError as exc:
        if not is_locked_error(exc):
            raise
        logger.warning("skip_last_login_update_due_to_lock user_id=%s trace_id=%s", user["id"], request.state.trace_id)

    write_audit(
        conn,
        user_id=user["id"],
        username=user["username"],
        user_role=user["role"],
        ip_address=get_client_ip(request),
        action_type="LOGIN",
        action_result="SUCCESS",
        trace_id=request.state.trace_id,
    )

    token = create_access_token(user_id=user["id"], username=user["username"], role=user["role"])
    return LoginResponse(
        access_token=token,
        expires_in=get_settings().jwt_expire_seconds,
        user={
            "id": user["id"],
            "username": user["username"],
            "role": user["role"],
            "is_active": bool(user["is_active"]),
        },
    )


@router.post("/logout", response_model=MessageResponse, responses={**RESP_401, **RESP_500})
def logout(
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
    conn: sqlite3.Connection = Depends(get_conn),
):
    if credentials is None:
        raise ApiError(
            401,
            ErrorCode.UNAUTHORIZED,
            "Authentication required",
            details={"reason": ErrorReason.AUTH_REQUIRED.value, "context": None},
        )
    token_payload = decode_access_token(credentials.credentials)
    try:
        jti = str(token_payload["jti"])
        exp = int(token_payload["exp"])
    except (KeyError, TypeError, ValueError):
        raise ApiError(
            401,
            ErrorCode.UNAUTHORIZED,
            "Authentication failed",
            details={"reason": ErrorReason.TOKEN_INVALID.value, "context": {"missing_or_invalid": "jti_or_exp"}},
        )
    cleanup_expired_revocations(conn)
    revoke_token(conn, jti=jti, expires_at=exp, revoked_by=current_user.id)

    write_audit(
        conn,
        user_id=current_user.id,
        username=current_user.username,
        user_role=current_user.role,
        ip_address=get_client_ip(request),
        action_type="LOGOUT",
        action_result="SUCCESS",
        trace_id=request.state.trace_id,
    )
    return MessageResponse(message="success")


@router.post(
    "/refresh",
    response_model=LoginResponse,
    responses={**RESP_401, **RESP_403, **RESP_404, **RESP_500},
)
def refresh_token(
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    conn: sqlite3.Connection = Depends(get_conn),
):
    row = conn.execute(
        "SELECT id, username, role, is_active FROM users WHERE id = ?",
        (current_user.id,),
    ).fetchone()
    if row is None:
        raise ApiError(
            404,
            ErrorCode.NOT_FOUND,
            "User not found",
            details={"reason": ErrorReason.USER_NOT_FOUND.value, "context": {"user_id": current_user.id}},
        )
    if not bool(row["is_active"]):
        raise ApiError(
            403,
            ErrorCode.FORBIDDEN,
            "User is inactive",
            details={"reason": ErrorReason.USER_INACTIVE.value, "context": {"user_id": row["id"]}},
        )

    write_audit(
        conn,
        user_id=row["id"],
        username=row["username"],
        user_role=row["role"],
        ip_address=get_client_ip(request),
        action_type="TOKEN_REFRESH",
        action_result="SUCCESS",
        target_type="USER",
        target_id=str(row["id"]),
        trace_id=request.state.trace_id,
    )

    token = create_access_token(user_id=row["id"], username=row["username"], role=row["role"])
    return LoginResponse(
        access_token=token,
        expires_in=get_settings().jwt_expire_seconds,
        user={
            "id": row["id"],
            "username": row["username"],
            "role": row["role"],
            "is_active": bool(row["is_active"]),
        },
    )


@router.get("/me", response_model=UserProfileResponse, responses={**RESP_401, **RESP_404, **RESP_500})
def me(
    current_user: CurrentUser = Depends(get_current_user),
    conn: sqlite3.Connection = Depends(get_conn),
):
    row = conn.execute(
        "SELECT id, username, role, is_active, last_login_at FROM users WHERE id = ?", (current_user.id,)
    ).fetchone()
    if row is None:
        raise ApiError(
            404,
            ErrorCode.NOT_FOUND,
            "User not found",
            details={"reason": ErrorReason.USER_NOT_FOUND.value, "context": {"user_id": current_user.id}},
        )

    return UserProfileResponse(
        data={
            "id": row["id"],
            "username": row["username"],
            "role": row["role"],
            "is_active": bool(row["is_active"]),
            "last_login_at": row["last_login_at"],
            "quota": get_quota(conn, row["id"]),
        }
    )
