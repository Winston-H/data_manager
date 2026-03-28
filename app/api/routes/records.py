import sqlite3

from fastapi import APIRouter, Depends, Request

from app.api.deps import CurrentUser, get_client_ip, get_conn, require_roles
from app.api.openapi_responses import RESP_401, RESP_403, RESP_404, RESP_500
from app.core.errors import ApiError, ErrorCode
from app.core.error_reasons import ErrorReason
from app.schemas.common import MessageResponse
from app.services.audit import write_audit
from app.services.records import delete_record

router = APIRouter()


@router.delete(
    "/{record_id}",
    response_model=MessageResponse,
    responses={**RESP_401, **RESP_403, **RESP_404, **RESP_500},
)
def remove_record(
    record_id: int,
    request: Request,
    current_user: CurrentUser = Depends(require_roles("SUPER_ADMIN", "ADMIN")),
    conn: sqlite3.Connection = Depends(get_conn),
):
    deleted = delete_record(conn, record_id)
    if deleted == 0:
        raise ApiError(
            404,
            ErrorCode.NOT_FOUND,
            "Record not found",
            details={"reason": ErrorReason.RECORD_NOT_FOUND.value, "context": {"record_id": record_id}},
        )

    write_audit(
        conn,
        user_id=current_user.id,
        username=current_user.username,
        user_role=current_user.role,
        ip_address=get_client_ip(request),
        action_type="DATA_DELETE",
        action_result="SUCCESS",
        target_type="RECORD",
        target_id=str(record_id),
        trace_id=request.state.trace_id,
    )
    return MessageResponse(message="success")

