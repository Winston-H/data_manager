import sqlite3

from fastapi import APIRouter, Depends, Request

from app.api.deps import CurrentUser, get_client_ip, get_conn, get_current_user
from app.api.openapi_responses import RESP_401, RESP_403, RESP_500
from app.schemas.stats import StatsSummaryResponse
from app.services.audit import write_audit
from app.services.records import count_records

router = APIRouter()


@router.get(
    "/summary",
    response_model=StatsSummaryResponse,
    responses={**RESP_401, **RESP_403, **RESP_500},
)
def stats_summary(
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    conn: sqlite3.Connection = Depends(get_conn),
):
    total_records = count_records()
    total_import_jobs = int(conn.execute("SELECT COUNT(*) AS c FROM import_jobs").fetchone()["c"])
    running_import_jobs = int(
        conn.execute("SELECT COUNT(*) AS c FROM import_jobs WHERE status = 'RUNNING'").fetchone()["c"]
    )
    latest_import_finished = conn.execute(
        "SELECT MAX(finished_at) AS v FROM import_jobs WHERE finished_at IS NOT NULL"
    ).fetchone()["v"]
    latest_import_finished_at = str(latest_import_finished) if latest_import_finished is not None else None

    write_audit(
        conn,
        user_id=current_user.id,
        username=current_user.username,
        user_role=current_user.role,
        ip_address=get_client_ip(request),
        action_type="DATA_STATS",
        action_result="SUCCESS",
        detail={"total_records": total_records},
        trace_id=request.state.trace_id,
    )

    return StatsSummaryResponse(
        data={
            "total_records": total_records,
            "total_import_jobs": total_import_jobs,
            "running_import_jobs": running_import_jobs,
            "latest_import_finished_at": latest_import_finished_at,
        }
    )
