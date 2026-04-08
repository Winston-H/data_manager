import sqlite3

from app.core.errors import ApiError, ErrorCode
from app.core.error_reasons import ErrorReason
from app.core.security import hash_password, verify_password
from app.core.time import local_today_isoformat, now_local_sql


def get_user_by_username(conn: sqlite3.Connection, username: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()


def get_user_by_id(conn: sqlite3.Connection, user_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()


def verify_active_super_admin_password(conn: sqlite3.Connection, password: str) -> sqlite3.Row | None:
    rows = conn.execute(
        "SELECT id, username, password_hash FROM users WHERE role = 'SUPER_ADMIN' AND is_active = 1"
    ).fetchall()
    for row in rows:
        if verify_password(password, row["password_hash"]):
            return row
    return None


def list_users(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    usage_date = local_today_isoformat()
    return conn.execute(
        """
        SELECT
          u.id,
          u.username,
          u.role,
          u.is_active,
          u.last_login_at,
          COALESCE(q.daily_limit, 0) AS daily_limit,
          COALESCE(d.used_count, 0) AS daily_used,
          COALESCE(q.total_limit, 0) AS total_limit,
          COALESCE(q.total_used, 0) AS total_used
        FROM users u
        LEFT JOIN user_quotas q ON q.user_id = u.id
        LEFT JOIN query_usage_daily d
          ON d.user_id = u.id
         AND d.usage_date = ?
        ORDER BY u.id ASC
        """,
        (usage_date,),
    ).fetchall()


def create_user(conn: sqlite3.Connection, username: str, password: str, role: str) -> sqlite3.Row:
    now_text = now_local_sql()
    try:
        cur = conn.execute(
            """
            INSERT INTO users(username, password_hash, role, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (username, hash_password(password), role, now_text, now_text),
        )
    except sqlite3.IntegrityError as exc:
        raise ApiError(
            409,
            ErrorCode.CONFLICT,
            "Username already exists",
            details={"reason": ErrorReason.USERNAME_EXISTS.value, "context": {"username": username}},
        ) from exc
    user_id = cur.lastrowid
    conn.execute(
        "INSERT OR IGNORE INTO user_quotas(user_id, daily_limit, total_limit, total_used) VALUES (?, 0, 0, 0)",
        (user_id,),
    )
    row = get_user_by_id(conn, int(user_id))
    if row is None:
        raise ApiError(
            500,
            ErrorCode.INTERNAL_ERROR,
            "Failed to create user",
            details={"reason": ErrorReason.CREATE_USER_FAILED.value, "context": {"username": username}},
        )
    return row


def update_user(conn: sqlite3.Connection, user_id: int, role: str | None, is_active: bool | None) -> sqlite3.Row:
    row = get_user_by_id(conn, user_id)
    if row is None:
        raise ApiError(
            404,
            ErrorCode.NOT_FOUND,
            "User not found",
            details={"reason": ErrorReason.USER_NOT_FOUND.value, "context": {"user_id": user_id}},
        )

    new_role = role if role is not None else row["role"]
    new_active = int(is_active) if is_active is not None else row["is_active"]

    if new_role == "SUPER_ADMIN" and int(new_active) == 0:
        raise ApiError(
            400,
            ErrorCode.INVALID_ARGUMENT,
            "Super admin cannot be disabled",
            details={
                "reason": ErrorReason.SUPER_ADMIN_CANNOT_BE_DISABLED.value,
                "context": {"user_id": user_id},
            },
        )

    conn.execute(
        "UPDATE users SET role = ?, is_active = ?, updated_at = ? WHERE id = ?",
        (new_role, new_active, now_local_sql(), user_id),
    )
    return get_user_by_id(conn, user_id)  # type: ignore[return-value]


def delete_user(conn: sqlite3.Connection, user_id: int) -> None:
    cur = conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    if cur.rowcount == 0:
        raise ApiError(
            404,
            ErrorCode.NOT_FOUND,
            "User not found",
            details={"reason": ErrorReason.USER_NOT_FOUND.value, "context": {"user_id": user_id}},
        )
