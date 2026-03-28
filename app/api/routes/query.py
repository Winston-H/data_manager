import sqlite3

from fastapi import APIRouter, Depends, Request

from app.api.deps import CurrentUser, get_client_ip, get_conn, get_current_user
from app.api.openapi_responses import RESP_400, RESP_401, RESP_429, RESP_500
from app.schemas.query import QueryRequest, QueryResponse
from app.services.audit import write_audit
from app.services.quota import enforce_and_consume_quota, get_quota
from app.services.query import apply_role_mask, query_records

router = APIRouter()


@router.post("/query", response_model=QueryResponse, responses={**RESP_400, **RESP_401, **RESP_429, **RESP_500})
def query_api(
    payload: QueryRequest,
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    conn: sqlite3.Connection = Depends(get_conn),
):
    quota = None
    if current_user.role == "USER":
        quota = enforce_and_consume_quota(conn, current_user.id)
    else:
        quota = get_quota(conn, current_user.id)

    records, capped = query_records(conn, payload)
    records = apply_role_mask(records, current_user.role)
    write_audit(
        conn,
        user_id=current_user.id,
        username=current_user.username,
        user_role=current_user.role,
        ip_address=get_client_ip(request),
        action_type="DATA_QUERY",
        action_result="SUCCESS",
        detail={
            "name_keyword": payload.name_keyword,
            "id_no_keyword": payload.id_no_keyword,
            "year_prefix": payload.year_prefix,
            "year_start": payload.year_start,
            "year_end": payload.year_end,
            "returned": len(records),
        },
        trace_id=request.state.trace_id,
    )

    return QueryResponse(
        data=records,
        meta={
            "returned": len(records),
            "capped": capped,
            "quota": quota,
        },
    )
