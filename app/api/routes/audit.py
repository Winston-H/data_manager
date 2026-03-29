import json
import sqlite3
from typing import Literal

from fastapi import APIRouter, Depends, Query

from app.api.deps import CurrentUser, get_conn, require_roles
from app.api.openapi_responses import RESP_401, RESP_403, RESP_500
from app.schemas.audit import AuditLogListResponse
from app.services.audit import cleanup_expired_audit_logs
from app.services.visibility import hidden_user_ids, hidden_usernames

router = APIRouter()


@router.get("", response_model=AuditLogListResponse, responses={**RESP_401, **RESP_403, **RESP_500})
def list_audit_logs(
    from_: str | None = Query(default=None, alias="from"),
    to: str | None = None,
    user_id: int | None = None,
    username: str | None = None,
    action_type: str | None = None,
    action_result: Literal["SUCCESS", "FAILED"] | None = None,
    page: int = 1,
    page_size: int = 50,
    with_total: bool = Query(default=True),
    _: CurrentUser = Depends(require_roles("SUPER_ADMIN")),
    conn: sqlite3.Connection = Depends(get_conn),
):
    cleanup_expired_audit_logs(conn)
    conditions = []
    params: list[object] = []
    hidden_names = sorted(hidden_usernames())
    hidden_ids = sorted(hidden_user_ids(conn))

    if from_:
        conditions.append("event_time >= ?")
        params.append(from_)
    if to:
        conditions.append("event_time <= ?")
        params.append(to)
    if user_id is not None:
        conditions.append("user_id = ?")
        params.append(user_id)
    if username:
        conditions.append("username = ?")
        params.append(username)
    if action_type:
        conditions.append("action_type = ?")
        params.append(action_type)
    if action_result:
        conditions.append("action_result = ?")
        params.append(action_result)
    if hidden_names:
        placeholders = ",".join("?" for _ in hidden_names)
        conditions.append(f"COALESCE(username, '') NOT IN ({placeholders})")
        params.extend(hidden_names)
    if hidden_ids:
        placeholders = ",".join("?" for _ in hidden_ids)
        conditions.append(f"NOT (COALESCE(target_type, '') = 'USER' AND COALESCE(target_id, '') IN ({placeholders}))")
        params.extend([str(item) for item in hidden_ids])

    where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    offset = (page - 1) * page_size

    rows = conn.execute(
        f"""
        SELECT * FROM audit_logs
        {where_sql}
        ORDER BY event_time DESC, id DESC
        LIMIT ? OFFSET ?
        """,
        [*params, page_size + 1, offset],
    ).fetchall()

    has_more = len(rows) > page_size
    page_rows = rows[:page_size]

    if with_total:
        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM audit_logs {where_sql}",
            params,
        ).fetchone()["c"]
    else:
        total = -1

    return AuditLogListResponse(
        data=[
            {
                "id": r["id"],
                "event_time": r["event_time"],
                "user_id": r["user_id"],
                "username": r["username"],
                "user_role": r["user_role"],
                "ip_address": r["ip_address"],
                "action_type": r["action_type"],
                "action_result": r["action_result"],
                "target_type": r["target_type"],
                "target_id": r["target_id"],
                "detail_json": json.loads(r["detail_json"]) if r["detail_json"] else None,
                "trace_id": r["trace_id"],
            }
            for r in page_rows
        ],
        page=page,
        page_size=page_size,
        total=int(total),
        has_more=has_more,
    )
