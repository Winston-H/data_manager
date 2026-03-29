from __future__ import annotations

import sqlite3
from collections.abc import Iterable

from app.core.config import get_settings


def hidden_usernames() -> set[str]:
    raw = get_settings().hidden_usernames.strip()
    if not raw:
        return set()
    return {item.strip() for item in raw.split(",") if item.strip()}


def is_hidden_username(username: str | None) -> bool:
    if not username:
        return False
    return str(username).strip() in hidden_usernames()


def hidden_user_ids(conn: sqlite3.Connection) -> set[int]:
    names = sorted(hidden_usernames())
    if not names:
        return set()
    placeholders = ",".join("?" for _ in names)
    rows = conn.execute(f"SELECT id FROM users WHERE username IN ({placeholders})", names).fetchall()
    return {int(row["id"]) for row in rows}


def is_hidden_user_id(conn: sqlite3.Connection, user_id: int | str | None) -> bool:
    if user_id is None or str(user_id).strip() == "":
        return False
    try:
        normalized = int(user_id)
    except (TypeError, ValueError):
        return False
    return normalized in hidden_user_ids(conn)


def filter_visible_user_rows(rows: Iterable[sqlite3.Row]) -> list[sqlite3.Row]:
    hidden = hidden_usernames()
    if not hidden:
        return list(rows)
    return [row for row in rows if str(row["username"]) not in hidden]
