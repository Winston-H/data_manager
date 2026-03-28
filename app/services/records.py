from __future__ import annotations

from typing import Any

from app.services.clickhouse_records import (
    count_clickhouse_records,
    delete_clickhouse_record,
    ensure_clickhouse_record_store,
    insert_clickhouse_records,
)


def ensure_record_store() -> None:
    ensure_clickhouse_record_store()


def count_records() -> int:
    return count_clickhouse_records()


def insert_record(
    _conn: object,
    *,
    name: str,
    id_no: str,
    year: str,
    created_by: int,
    **_: Any,
) -> int:
    inserted = insert_clickhouse_records(
        names=[name],
        id_nos=[id_no],
        birth_years=[int(year)],
        created_by=created_by,
    )
    return inserted[0]


def delete_record(_conn: object, record_id: int) -> int:
    return delete_clickhouse_record(record_id)
