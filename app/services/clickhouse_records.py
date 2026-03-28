from __future__ import annotations

from typing import Any, Iterable

from app.core.config import get_settings
from app.core.crypto import decrypt_id_value, encrypt_id_values, normalize_text
from app.core.id_cards import fingerprint_id_no, is_valid_id_no, normalize_id_no
from app.core.ids import new_record_id
from app.core.key_manager import load_keys
from app.db.clickhouse import clickhouse_command, clickhouse_insert_json_rows, clickhouse_query_rows, get_clickhouse_config, sql_quote
from app.schemas.query import QueryRequest

RESULT_LIMIT = 100


def ensure_clickhouse_record_store() -> None:
    cfg = get_clickhouse_config()
    if not cfg.base_url:
        raise RuntimeError("CLICKHOUSE_URL is required")

    clickhouse_command(f"CREATE DATABASE IF NOT EXISTS `{cfg.database}`", use_database=False)
    clickhouse_command(
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.records_table_sql} (
          id UInt64,
          name String,
          birth_year UInt16,
          id_no_cipher String,
          id_no_digest FixedString(64),
          created_by UInt64,
          created_at DateTime DEFAULT now(),
          INDEX idx_name_ngram name TYPE ngrambf_v1(2, 4096, 2, 0) GRANULARITY 4,
          INDEX idx_id_digest id_no_digest TYPE bloom_filter(0.01) GRANULARITY 4
        )
        ENGINE = MergeTree
        PARTITION BY intDiv(toUInt32(birth_year), 10)
        ORDER BY (birth_year, name, id)
        SETTINGS index_granularity = 8192
        """
    )


def _chunked(values: Iterable[str], chunk_size: int) -> Iterable[list[str]]:
    chunk: list[str] = []
    for value in values:
        chunk.append(value)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _chunked_in_values(values: Iterable[str], *, chunk_size: int, max_query_bytes: int) -> Iterable[list[str]]:
    # Keep each IN (...) clause well below ClickHouse max_query_size.
    max_bytes = max(1024, int(max_query_bytes))
    chunk: list[str] = []
    chunk_bytes = 0
    for value in values:
        value_bytes = len(str(value)) + 3
        if chunk and (len(chunk) >= chunk_size or chunk_bytes + value_bytes > max_bytes):
            yield chunk
            chunk = []
            chunk_bytes = 0
        chunk.append(value)
        chunk_bytes += value_bytes
    if chunk:
        yield chunk


def existing_id_fingerprints(digests: list[str], *, chunk_size: int | None = None) -> set[str]:
    if not digests:
        return set()

    settings = get_settings()
    chunk = max(1, chunk_size or settings.clickhouse_dedup_probe_batch_size)
    max_query_bytes = max(1024, settings.clickhouse_dedup_probe_max_query_bytes)
    cfg = get_clickhouse_config()
    existing: set[str] = set()
    for part in _chunked_in_values(digests, chunk_size=chunk, max_query_bytes=max_query_bytes):
        sql = ", ".join(sql_quote(item) for item in part)
        rows = clickhouse_query_rows(
            f"SELECT id_no_digest FROM {cfg.records_table_sql} WHERE id_no_digest IN ({sql})"
        )
        existing.update(str(row["id_no_digest"]) for row in rows)
    return existing


def insert_clickhouse_records(
    *,
    names: list[str],
    id_nos: list[str],
    birth_years: list[int],
    created_by: int,
) -> list[int]:
    if not names:
        return []

    settings = get_settings()
    keys = load_keys()
    data_key = keys.data_keys[keys.active_data_key_version]
    encrypted_ids = encrypt_id_values(
        data_key,
        [normalize_id_no(value) for value in id_nos],
        workers=max(1, settings.import_encrypt_workers),
    )

    rows: list[dict[str, Any]] = []
    for idx, enc_id in enumerate(encrypted_ids):
        rows.append(
            {
                "id": new_record_id(),
                "name": names[idx],
                "birth_year": int(birth_years[idx]),
                "id_no_cipher": enc_id,
                "id_no_digest": fingerprint_id_no(id_nos[idx]),
                "created_by": int(created_by),
            }
        )

    clickhouse_insert_json_rows(get_clickhouse_config().records_table_sql, rows)
    return [int(row["id"]) for row in rows]


def count_clickhouse_records() -> int:
    rows = clickhouse_query_rows(f"SELECT count() AS c FROM {get_clickhouse_config().records_table_sql}")
    return int(rows[0]["c"]) if rows else 0


def delete_clickhouse_record(record_id: int) -> int:
    cfg = get_clickhouse_config()
    exists = clickhouse_query_rows(f"SELECT count() AS c FROM {cfg.records_table_sql} WHERE id = {int(record_id)}")
    if not exists or int(exists[0]["c"]) == 0:
        return 0
    clickhouse_command(
        f"ALTER TABLE {cfg.records_table_sql} DELETE WHERE id = {int(record_id)}",
        settings={"mutations_sync": 1},
    )
    return 1


def _year_filters(req: QueryRequest) -> list[str]:
    clauses: list[str] = []
    if req.year_prefix:
        clauses.append(f"startsWith(toString(birth_year), {sql_quote(req.year_prefix)})")
    if req.year_start is not None:
        clauses.append(f"birth_year >= {int(req.year_start)}")
    if req.year_end is not None:
        clauses.append(f"birth_year <= {int(req.year_end)}")
    return clauses


def _decode_clickhouse_rows(rows: list[dict[str, Any]]) -> list[dict]:
    keys = load_keys()
    data_key = keys.data_keys[keys.active_data_key_version]
    output: list[dict] = []
    for row in rows:
        output.append(
            {
                "id": int(row["id"]),
                "name": str(row["name"]),
                "id_no": decrypt_id_value(data_key, str(row["id_no_cipher"])),
                "year": int(row["birth_year"]),
            }
        )
    return output


def _score_record(rec: dict, *, name_kw: str | None, id_kw: str | None, exact_id: bool) -> float:
    score = 1.0
    if exact_id:
        score += 100.0
    if name_kw:
        normalized_name = normalize_text(rec["name"])
        if normalized_name == name_kw:
            score += 20.0
        if name_kw in normalized_name:
            score += float(len(name_kw) * 5)
    if id_kw and id_kw in normalize_id_no(rec["id_no"]):
        score += float(len(id_kw) * 3)
    return score


def _base_select_sql(where_clauses: list[str], *, limit: int) -> str:
    where_sql = " AND ".join(where_clauses) if where_clauses else "1"
    return f"""
        SELECT id, name, birth_year, id_no_cipher
        FROM {get_clickhouse_config().records_table_sql}
        WHERE {where_sql}
        ORDER BY birth_year ASC, name ASC, id ASC
        LIMIT {int(limit)}
    """


def _query_by_name(req: QueryRequest) -> tuple[list[dict], bool]:
    settings = get_settings()
    name_kw_raw = req.name_keyword or ""
    where_clauses = [f"positionCaseInsensitiveUTF8(name, {sql_quote(name_kw_raw)}) > 0", *_year_filters(req)]
    rows = clickhouse_query_rows(_base_select_sql(where_clauses, limit=settings.clickhouse_query_candidate_limit))
    decoded = _decode_clickhouse_rows(rows)

    name_kw = normalize_text(name_kw_raw) if name_kw_raw else None
    id_kw = normalize_id_no(req.id_no_keyword) if req.id_no_keyword else None
    filtered: list[dict] = []
    for rec in decoded:
        if name_kw and name_kw not in normalize_text(rec["name"]):
            continue
        if id_kw and id_kw not in normalize_id_no(rec["id_no"]):
            continue
        rec["match_score"] = _score_record(rec, name_kw=name_kw, id_kw=id_kw, exact_id=False)
        filtered.append(rec)

    filtered.sort(key=lambda item: (-item["match_score"], item["year"], item["name"], item["id"]))
    capped = len(filtered) > RESULT_LIMIT or len(rows) >= settings.clickhouse_query_candidate_limit
    return filtered[:RESULT_LIMIT], capped


def _query_by_exact_id(req: QueryRequest) -> tuple[list[dict], bool]:
    id_kw = normalize_id_no(req.id_no_keyword or "")
    where_clauses = [f"id_no_digest = {sql_quote(fingerprint_id_no(id_kw))}", *_year_filters(req)]
    rows = clickhouse_query_rows(_base_select_sql(where_clauses, limit=RESULT_LIMIT))
    decoded = _decode_clickhouse_rows(rows)

    output: list[dict] = []
    for rec in decoded:
        if normalize_id_no(rec["id_no"]) != id_kw:
            continue
        rec["match_score"] = _score_record(rec, name_kw=None, id_kw=id_kw, exact_id=True)
        output.append(rec)
    output.sort(key=lambda item: (-item["match_score"], item["year"], item["name"], item["id"]))
    return output[:RESULT_LIMIT], len(output) > RESULT_LIMIT


def _scan_for_partial_id(req: QueryRequest) -> tuple[list[dict], bool]:
    settings = get_settings()
    where_clauses = _year_filters(req)
    scan_sql = f"SELECT count() AS c FROM {get_clickhouse_config().records_table_sql} WHERE {' AND '.join(where_clauses) if where_clauses else '1'}"
    count_rows = clickhouse_query_rows(scan_sql)
    total = int(count_rows[0]["c"]) if count_rows else 0
    if total > settings.clickhouse_partial_id_scan_limit:
        return [], False

    rows = clickhouse_query_rows(
        _base_select_sql(where_clauses, limit=settings.clickhouse_partial_id_scan_limit)
    )
    decoded = _decode_clickhouse_rows(rows)
    id_kw = normalize_id_no(req.id_no_keyword or "")
    output: list[dict] = []
    for rec in decoded:
        if id_kw not in normalize_id_no(rec["id_no"]):
            continue
        rec["match_score"] = _score_record(rec, name_kw=None, id_kw=id_kw, exact_id=False)
        output.append(rec)
    output.sort(key=lambda item: (-item["match_score"], item["year"], item["name"], item["id"]))
    return output[:RESULT_LIMIT], len(output) > RESULT_LIMIT


def query_clickhouse_records(req: QueryRequest) -> tuple[list[dict], bool]:
    if req.name_keyword:
        return _query_by_name(req)

    id_kw = normalize_id_no(req.id_no_keyword or "")
    if is_valid_id_no(id_kw):
        return _query_by_exact_id(req)
    return _scan_for_partial_id(req)
