import sqlite3

from app.core.errors import ApiError, ErrorCode
from app.core.error_reasons import ErrorReason
from app.core.time import local_today_isoformat, now_local_sql


class QuotaInfo(dict):
    pass


def get_quota(conn: sqlite3.Connection, user_id: int) -> QuotaInfo:
    quota = conn.execute(
        "SELECT daily_limit, total_limit, total_used FROM user_quotas WHERE user_id = ?", (user_id,)
    ).fetchone()
    if quota is None:
        conn.execute(
            "INSERT INTO user_quotas(user_id, daily_limit, total_limit, total_used) VALUES (?, 0, 0, 0)",
            (user_id,),
        )
        quota = conn.execute(
            "SELECT daily_limit, total_limit, total_used FROM user_quotas WHERE user_id = ?", (user_id,)
        ).fetchone()

    usage_date = local_today_isoformat()
    daily = conn.execute(
        "SELECT used_count FROM query_usage_daily WHERE user_id = ? AND usage_date = ?",
        (user_id, usage_date),
    ).fetchone()
    daily_used = daily["used_count"] if daily else 0

    return QuotaInfo(
        daily_limit=quota["daily_limit"],
        daily_used=daily_used,
        total_limit=quota["total_limit"],
        total_used=quota["total_used"],
    )


def update_quota(conn: sqlite3.Connection, user_id: int, daily_limit: int, total_limit: int) -> QuotaInfo:
    updated_at = now_local_sql()
    conn.execute(
        """
        INSERT INTO user_quotas(user_id, daily_limit, total_limit, total_used)
        VALUES(?, ?, ?, COALESCE((SELECT total_used FROM user_quotas WHERE user_id=?), 0))
        ON CONFLICT(user_id) DO UPDATE SET
          daily_limit=excluded.daily_limit,
          total_limit=excluded.total_limit,
          updated_at=?
        """,
        (user_id, daily_limit, total_limit, user_id, updated_at),
    )
    return get_quota(conn, user_id)


def enforce_and_consume_quota(conn: sqlite3.Connection, user_id: int) -> QuotaInfo:
    info = get_quota(conn, user_id)
    if info["daily_limit"] > 0 and info["daily_used"] >= info["daily_limit"]:
        raise ApiError(
            429,
            ErrorCode.QUOTA_EXCEEDED,
            "Daily query limit exceeded",
            details={
                "reason": ErrorReason.QUOTA_EXCEEDED_DAILY.value,
                "context": {
                    "user_id": user_id,
                    "daily_limit": info["daily_limit"],
                    "daily_used": info["daily_used"],
                },
            },
        )
    if info["total_limit"] > 0 and info["total_used"] >= info["total_limit"]:
        raise ApiError(
            429,
            ErrorCode.QUOTA_EXCEEDED,
            "Total query limit exceeded",
            details={
                "reason": ErrorReason.QUOTA_EXCEEDED_TOTAL.value,
                "context": {
                    "user_id": user_id,
                    "total_limit": info["total_limit"],
                    "total_used": info["total_used"],
                },
            },
        )

    usage_date = local_today_isoformat()
    conn.execute(
        """
        INSERT INTO query_usage_daily(user_id, usage_date, used_count)
        VALUES (?, ?, 1)
        ON CONFLICT(user_id, usage_date) DO UPDATE SET used_count = used_count + 1
        """,
        (user_id, usage_date),
    )
    conn.execute(
        "UPDATE user_quotas SET total_used = total_used + 1, updated_at = ? WHERE user_id = ?",
        (now_local_sql(), user_id),
    )
    return get_quota(conn, user_id)
