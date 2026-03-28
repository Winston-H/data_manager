import sqlite3

from app.core.config import get_settings
from app.schemas.query import QueryRequest
from app.services.clickhouse_records import query_clickhouse_records


def _mask_name(name: str) -> str:
    if not name:
        return name
    if len(name) == 1:
        return "*"
    return f"{name[0]}{'*' * (len(name) - 1)}"


def _mask_id_no(id_no: str) -> str:
    if not id_no:
        return id_no
    n = len(id_no)
    if n <= 4:
        return "*" * n
    if n <= 10:
        return f"{id_no[:2]}{'*' * (n - 4)}{id_no[-2:]}"
    return f"{id_no[:6]}{'*' * (n - 10)}{id_no[-4:]}"


def _mask_roles_config() -> set[str]:
    raw = get_settings().query_mask_roles.strip()
    if not raw:
        return set()
    if raw == "*":
        return {"SUPER_ADMIN", "ADMIN", "USER"}
    return {role.strip().upper() for role in raw.split(",") if role.strip()}


def apply_role_mask(records: list[dict], role: str) -> list[dict]:
    role_upper = role.upper()
    if role_upper not in _mask_roles_config():
        return records

    masked: list[dict] = []
    for rec in records:
        item = dict(rec)
        item["name"] = _mask_name(str(item.get("name", "")))
        item["id_no"] = _mask_id_no(str(item.get("id_no", "")))
        masked.append(item)
    return masked


def query_records(_conn: sqlite3.Connection, req: QueryRequest) -> tuple[list[dict], bool]:
    return query_clickhouse_records(req)
