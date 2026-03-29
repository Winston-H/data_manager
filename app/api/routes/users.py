import sqlite3

from fastapi import APIRouter, Depends, Request

from app.api.deps import CurrentUser, get_client_ip, get_conn, require_roles
from app.api.openapi_responses import RESP_400, RESP_401, RESP_403, RESP_404, RESP_409, RESP_500
from app.core.errors import ApiError, ErrorCode
from app.core.error_reasons import ErrorReason
from app.schemas.common import MessageResponse
from app.schemas.users import (
    CreateUserRequest,
    QuotaResponse,
    QuotaUpdateRequest,
    UpdateUserRequest,
    UserListResponse,
    UserResponse,
)
from app.services.audit import write_audit
from app.services.quota import get_quota, update_quota
from app.services.users import create_user, delete_user, get_user_by_id, list_users, update_user
from app.services.visibility import filter_visible_user_rows

router = APIRouter()


@router.get("", response_model=UserListResponse, responses={**RESP_401, **RESP_403, **RESP_500})
def get_users(
    _: CurrentUser = Depends(require_roles("SUPER_ADMIN")),
    conn: sqlite3.Connection = Depends(get_conn),
):
    rows = filter_visible_user_rows(list_users(conn))
    data = []
    for r in rows:
        item = {
            "id": r["id"],
            "username": r["username"],
            "role": r["role"],
            "is_active": bool(r["is_active"]),
            "last_login_at": r["last_login_at"],
        }
        if r["role"] == "USER":
            item["quota"] = {
                "daily_limit": int(r["daily_limit"]),
                "daily_used": int(r["daily_used"]),
                "total_limit": int(r["total_limit"]),
                "total_used": int(r["total_used"]),
            }
        data.append(item)
    return UserListResponse(data=data)


@router.post(
    "",
    response_model=UserResponse,
    status_code=201,
    responses={**RESP_400, **RESP_401, **RESP_403, **RESP_409, **RESP_500},
)
def post_user(
    payload: CreateUserRequest,
    request: Request,
    current_user: CurrentUser = Depends(require_roles("SUPER_ADMIN")),
    conn: sqlite3.Connection = Depends(get_conn),
):
    if payload.role not in {"SUPER_ADMIN", "ADMIN", "USER"}:
        raise ApiError(
            400,
            ErrorCode.INVALID_ARGUMENT,
            "Invalid role",
            details={"reason": ErrorReason.INVALID_ROLE.value, "context": {"role": payload.role}},
        )

    row = create_user(conn, payload.username, payload.password, payload.role)
    write_audit(
        conn,
        user_id=current_user.id,
        username=current_user.username,
        user_role=current_user.role,
        ip_address=get_client_ip(request),
        action_type="USER_CREATE",
        action_result="SUCCESS",
        target_type="USER",
        target_id=str(row["id"]),
        detail={"created_username": row["username"], "role": row["role"]},
        trace_id=request.state.trace_id,
    )
    return UserResponse(
        data={
            "id": row["id"],
            "username": row["username"],
            "role": row["role"],
            "is_active": bool(row["is_active"]),
        }
    )


@router.patch(
    "/{user_id}",
    response_model=UserResponse,
    responses={**RESP_400, **RESP_401, **RESP_403, **RESP_404, **RESP_500},
)
def patch_user(
    user_id: int,
    payload: UpdateUserRequest,
    request: Request,
    current_user: CurrentUser = Depends(require_roles("SUPER_ADMIN")),
    conn: sqlite3.Connection = Depends(get_conn),
):
    if payload.role is not None and payload.role not in {"SUPER_ADMIN", "ADMIN", "USER"}:
        raise ApiError(
            400,
            ErrorCode.INVALID_ARGUMENT,
            "Invalid role",
            details={"reason": ErrorReason.INVALID_ROLE.value, "context": {"role": payload.role}},
        )

    row = update_user(conn, user_id, payload.role, payload.is_active)
    write_audit(
        conn,
        user_id=current_user.id,
        username=current_user.username,
        user_role=current_user.role,
        ip_address=get_client_ip(request),
        action_type="USER_UPDATE",
        action_result="SUCCESS",
        target_type="USER",
        target_id=str(user_id),
        detail={"role": row["role"], "is_active": bool(row["is_active"])} ,
        trace_id=request.state.trace_id,
    )

    return UserResponse(
        data={
            "id": row["id"],
            "username": row["username"],
            "role": row["role"],
            "is_active": bool(row["is_active"]),
        }
    )


@router.delete(
    "/{user_id}",
    response_model=MessageResponse,
    responses={**RESP_400, **RESP_401, **RESP_403, **RESP_404, **RESP_500},
)
def remove_user(
    user_id: int,
    request: Request,
    current_user: CurrentUser = Depends(require_roles("SUPER_ADMIN")),
    conn: sqlite3.Connection = Depends(get_conn),
):
    if user_id == current_user.id:
        raise ApiError(
            400,
            ErrorCode.INVALID_ARGUMENT,
            "Cannot delete current user",
            details={"reason": ErrorReason.CANNOT_DELETE_SELF.value, "context": {"user_id": user_id}},
        )
    delete_user(conn, user_id)
    write_audit(
        conn,
        user_id=current_user.id,
        username=current_user.username,
        user_role=current_user.role,
        ip_address=get_client_ip(request),
        action_type="USER_DELETE",
        action_result="SUCCESS",
        target_type="USER",
        target_id=str(user_id),
        trace_id=request.state.trace_id,
    )
    return MessageResponse(message="success")


@router.put(
    "/{user_id}/quota",
    response_model=QuotaResponse,
    responses={**RESP_400, **RESP_401, **RESP_403, **RESP_404, **RESP_500},
)
def put_quota(
    user_id: int,
    payload: QuotaUpdateRequest,
    request: Request,
    current_user: CurrentUser = Depends(require_roles("SUPER_ADMIN")),
    conn: sqlite3.Connection = Depends(get_conn),
):
    row = get_user_by_id(conn, user_id)
    if row is None:
        raise ApiError(
            404,
            ErrorCode.NOT_FOUND,
            "User not found",
            details={"reason": ErrorReason.USER_NOT_FOUND.value, "context": {"user_id": user_id}},
        )
    if row["role"] != "USER":
        raise ApiError(
            400,
            ErrorCode.INVALID_ARGUMENT,
            "Quota can only be set for USER role",
            details={
                "reason": ErrorReason.QUOTA_TARGET_ROLE_INVALID.value,
                "context": {"user_id": user_id, "role": row["role"]},
            },
        )

    quota = update_quota(conn, user_id, payload.daily_limit, payload.total_limit)
    write_audit(
        conn,
        user_id=current_user.id,
        username=current_user.username,
        user_role=current_user.role,
        ip_address=get_client_ip(request),
        action_type="QUOTA_UPDATE",
        action_result="SUCCESS",
        target_type="USER",
        target_id=str(user_id),
        detail={"quota": quota},
        trace_id=request.state.trace_id,
    )
    return QuotaResponse(data=quota)


@router.get(
    "/{user_id}/quota",
    response_model=QuotaResponse,
    responses={**RESP_400, **RESP_401, **RESP_403, **RESP_404, **RESP_500},
)
def get_user_quota(
    user_id: int,
    request: Request,
    current_user: CurrentUser = Depends(require_roles("SUPER_ADMIN")),
    conn: sqlite3.Connection = Depends(get_conn),
):
    row = get_user_by_id(conn, user_id)
    if row is None:
        raise ApiError(
            404,
            ErrorCode.NOT_FOUND,
            "User not found",
            details={"reason": ErrorReason.USER_NOT_FOUND.value, "context": {"user_id": user_id}},
        )
    if row["role"] != "USER":
        raise ApiError(
            400,
            ErrorCode.INVALID_ARGUMENT,
            "Quota can only be set for USER role",
            details={
                "reason": ErrorReason.QUOTA_TARGET_ROLE_INVALID.value,
                "context": {"user_id": user_id, "role": row["role"]},
            },
        )

    quota = get_quota(conn, user_id)
    write_audit(
        conn,
        user_id=current_user.id,
        username=current_user.username,
        user_role=current_user.role,
        ip_address=get_client_ip(request),
        action_type="QUOTA_VIEW",
        action_result="SUCCESS",
        target_type="USER",
        target_id=str(user_id),
        detail={"quota": quota},
        trace_id=request.state.trace_id,
    )
    return QuotaResponse(data=quota)
