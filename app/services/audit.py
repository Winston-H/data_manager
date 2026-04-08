import json
import logging
import sqlite3
import threading
import time
from typing import Any

from app.core.config import get_settings
from app.core.time import local_sql_days_ago, now_local_sql
from app.db.sqlite import is_locked_error
from app.services.visibility import is_hidden_user_id, is_hidden_username

logger = logging.getLogger("app.audit")
_cleanup_lock = threading.Lock()
_last_cleanup_monotonic = 0.0
_AUDIT_DETAIL_OMIT_KEYS = frozenset({"created_by", "filename_contains", "status"})


def _prune_audit_detail(detail: dict[str, Any] | None) -> dict[str, Any] | None:
    if not detail:
        return None
    pruned = {
        str(key): value
        for key, value in detail.items()
        if str(key) not in _AUDIT_DETAIL_OMIT_KEYS and value is not None and value != ""
    }
    return pruned or None


def cleanup_expired_audit_logs(conn: sqlite3.Connection, *, retention_days: int | None = None) -> int:
    settings = get_settings()
    days = int(retention_days if retention_days is not None else settings.audit_log_retention_days)
    if days <= 0:
        return 0
    cutoff = local_sql_days_ago(days)
    cur = conn.execute(
        "DELETE FROM audit_logs WHERE event_time < ?",
        (cutoff,),
    )
    return int(cur.rowcount)


def cleanup_expired_audit_logs_if_due(
    conn: sqlite3.Connection,
    *,
    min_interval_seconds: int | None = None,
    retention_days: int | None = None,
) -> int:
    global _last_cleanup_monotonic

    settings = get_settings()
    interval = int(
        min_interval_seconds
        if min_interval_seconds is not None
        else settings.audit_log_cleanup_interval_seconds
    )
    now = time.monotonic()
    with _cleanup_lock:
        if interval > 0 and now - _last_cleanup_monotonic < interval:
            return 0
        try:
            deleted = cleanup_expired_audit_logs(conn, retention_days=retention_days)
        except sqlite3.OperationalError as exc:
            if not is_locked_error(exc):
                raise
            logger.warning("skip_audit_cleanup_due_to_lock")
            deleted = 0
        _last_cleanup_monotonic = now
        return deleted

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
    cleanup_expired_audit_logs_if_due(conn)
    detail = _prune_audit_detail(detail)
    event_time = now_local_sql()
    try:
        conn.execute(
            """
            INSERT INTO audit_logs(
                event_time,
                user_id, username, user_role, ip_address,
                action_type, action_result, target_type, target_id,
                detail_json, trace_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_time,
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
