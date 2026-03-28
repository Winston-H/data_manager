import sqlite3
import threading
import time
from datetime import datetime, timezone

_cleanup_lock = threading.Lock()
_last_cleanup_monotonic = 0.0


def revoke_token(
    conn: sqlite3.Connection,
    *,
    jti: str,
    expires_at: int,
    revoked_by: int | None,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO token_revocations(jti, expires_at, revoked_by)
        VALUES (?, ?, ?)
        """,
        (jti, expires_at, revoked_by),
    )


def is_token_revoked(conn: sqlite3.Connection, jti: str) -> bool:
    row = conn.execute("SELECT 1 AS ok FROM token_revocations WHERE jti = ? LIMIT 1", (jti,)).fetchone()
    return row is not None


def cleanup_expired_revocations(conn: sqlite3.Connection) -> int:
    now_ts = int(datetime.now(timezone.utc).timestamp())
    cur = conn.execute("DELETE FROM token_revocations WHERE expires_at <= ?", (now_ts,))
    return int(cur.rowcount)


def cleanup_expired_revocations_if_due(conn: sqlite3.Connection, *, min_interval_seconds: int = 60) -> int:
    global _last_cleanup_monotonic

    now = time.monotonic()
    with _cleanup_lock:
        if now - _last_cleanup_monotonic < min_interval_seconds:
            return 0
        deleted = cleanup_expired_revocations(conn)
        _last_cleanup_monotonic = now
        return deleted
