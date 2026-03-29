import json
import logging
import sqlite3
from typing import Any

from app.db.sqlite import is_locked_error
from app.services.visibility import is_hidden_user_id, is_hidden_username

logger = logging.getLogger("app.audit")

def write_audit(
    conn: sqlite3.Connection,
    *,
    user_id: int | None,
    username: str | None,
    user_role: str | None,
    ip_address: str | None,
    action_type: str,
    action_result: str,
    target_type: str | None = None,
    target_id: str | None = None,
    detail: dict[str, Any] | None = None,
    trace_id: str | None = None,
) -> None:
    if is_hidden_username(username):
        return
    if target_type == "USER" and is_hidden_user_id(conn, target_id):
        return
    try:
        conn.execute(
            """
            INSERT INTO audit_logs(
                user_id, username, user_role, ip_address,
                action_type, action_result, target_type, target_id,
                detail_json, trace_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                username,
                user_role,
                ip_address,
                action_type,
                action_result,
                target_type,
                target_id,
                json.dumps(detail, ensure_ascii=False) if detail else None,
                trace_id,
            ),
        )
    except sqlite3.OperationalError as exc:
        if not is_locked_error(exc):
            raise
        logger.warning(
            "skip_audit_write_due_to_lock action_type=%s action_result=%s trace_id=%s",
            action_type,
            action_result,
            trace_id,
        )
