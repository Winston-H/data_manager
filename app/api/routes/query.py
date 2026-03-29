import json
import sqlite3
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Request

from app.api.deps import CurrentUser, get_client_ip, get_conn, get_current_user
from app.api.openapi_responses import RESP_400, RESP_401, RESP_429, RESP_500
from app.schemas.query import QueryRequest, QueryResponse
from app.services.audit import write_audit
from app.services.quota import enforce_and_consume_quota, get_quota
from app.services.query import apply_role_mask, query_records
from app.services.visibility import is_hidden_username

router = APIRouter()


def _emit_query_stdout(
    *,
    username: str,
    user_role: str,
    ip_address: str | None,
    name_keyword: str | None,
    id_no_keyword: str | None,
    year_prefix: str | None,
    year_start: int | None,
    year_end: int | None,
    returned: int,
    capped: bool,
) -> None:
    if is_hidden_username(username):
        return
    payload = {
        "event": "DATA_QUERY",
        "ts": datetime.now(timezone.utc).isoformat(),
        "username": username,
        "user_role": user_role,
        "ip_address": ip_address,
        "name_keyword": name_keyword,
        "id_no_keyword": id_no_keyword,
        "year_prefix": year_prefix,
        "year_start": year_start,
        "year_end": year_end,
        "returned": returned,
        "capped": capped,
    }
    print(json.dumps(payload, ensure_ascii=False), flush=True)


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

    client_ip = get_client_ip(request)
    records, capped = query_records(conn, payload)
    records = apply_role_mask(records, current_user.role)
    _emit_query_stdout(
        username=current_user.username,
        user_role=current_user.role,
        ip_address=client_ip,
        name_keyword=payload.name_keyword,
        id_no_keyword=payload.id_no_keyword,
        year_prefix=payload.year_prefix,
        year_start=payload.year_start,
        year_end=payload.year_end,
        returned=len(records),
        capped=capped,
    )
    write_audit(
        conn,
        user_id=current_user.id,
        username=current_user.username,
        user_role=current_user.role,
        ip_address=client_ip,
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
