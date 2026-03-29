from __future__ import annotations

from typing import Any, Iterable

from app.core.config import get_settings
from app.core.crypto import decrypt_id_value, encrypt_id_values, normalize_text
from app.core.id_cards import fingerprint_id_no, normalize_id_no
from app.core.ids import new_record_id
from app.core.key_manager import load_keys
from app.db.clickhouse import clickhouse_command, clickhouse_insert_json_rows, clickhouse_query_rows, get_clickhouse_config, sql_quote
from app.schemas.query import QueryRequest

def _name_keyword_raw(value: str | None) -> str | None:
    raw = str(value or "").strip()
    return raw or None


def _name_keyword(value: str | None) -> str | None:
    raw = _name_keyword_raw(value)
    return normalize_text(raw) if raw else None


def _surname_keyword_raw(value: str | None) -> str | None:
    raw = _name_keyword_raw(value)
    return raw[:1] if raw else None


def _surname_keyword(value: str | None) -> str | None:
    raw = _surname_keyword_raw(value)
    return normalize_text(raw) if raw else None


def _name_match_exact(value: str | None) -> bool:
    raw = _name_keyword_raw(value)
    return bool(raw and len(raw) > 1)


def _id_exact_keyword(value: str | None) -> str | None:
    normalized = normalize_id_no(value or "")
    if len(normalized) == 18:
        return normalized
    return None


def _id_prefix_keyword(value: str | None) -> str | None:
    normalized = normalize_id_no(value or "")
    if len(normalized) == 18:
        return None
    return normalized[:4] or None


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
          birth_year_raw String DEFAULT toString(birth_year),
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
    clickhouse_command(
        f"""
        ALTER TABLE {cfg.records_table_sql}
        ADD COLUMN IF NOT EXISTS birth_year_raw String DEFAULT toString(birth_year)
        AFTER birth_year
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
    birth_year_raws: list[str] | None = None,
    created_by: int,
) -> list[int]:
    if not names:
        return []

    settings = get_settings()
    keys = load_keys()
    data_key = keys.data_keys[keys.active_data_key_version]
    raw_ids = [str(value or "").strip() for value in id_nos]
    raw_years = (
        [str(value) for value in birth_year_raws]
        if birth_year_raws is not None
        else [str(int(value)) for value in birth_years]
    )
    encrypted_ids = encrypt_id_values(
        data_key,
        raw_ids,
        workers=max(1, settings.import_encrypt_workers),
    )

    rows: list[dict[str, Any]] = []
    for idx, enc_id in enumerate(encrypted_ids):
        rows.append(
            {
                "id": new_record_id(),
                "name": names[idx],
                "birth_year": int(birth_years[idx]),
                "birth_year_raw": raw_years[idx],
                "id_no_cipher": enc_id,
                "id_no_digest": fingerprint_id_no(raw_ids[idx]),
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


def _score_record(
    rec: dict,
    *,
    exact_name_kw: str | None,
    surname_kw: str | None,
    exact_id_kw: str | None,
    id_prefix: str | None,
) -> float:
    score = 1.0
    normalized_name = normalize_text(rec["name"])
    normalized_id = normalize_id_no(rec["id_no"])
    if exact_name_kw:
        if normalized_name == exact_name_kw:
            score += 30.0
    elif surname_kw:
        if normalized_name.startswith(surname_kw):
            score += 20.0
    if exact_id_kw:
        if normalized_id == exact_id_kw:
            score += 100.0
    elif id_prefix and normalized_id.startswith(id_prefix):
        score += float(len(id_prefix) * 3)
    return score


def _base_select_sql(where_clauses: list[str], *, limit: int, offset: int = 0) -> str:
    where_sql = " AND ".join(where_clauses) if where_clauses else "1"
    return f"""
        SELECT id, name, birth_year, id_no_cipher
        FROM {get_clickhouse_config().records_table_sql}
        WHERE {where_sql}
        ORDER BY birth_year ASC, name ASC, id ASC
        LIMIT {int(limit)}
        OFFSET {int(offset)}
    """


def _iter_decoded_candidates(where_clauses: list[str], *, batch_size: int) -> Iterable[dict[str, Any]]:
    offset = 0
    size = max(1, int(batch_size))
    while True:
        rows = clickhouse_query_rows(_base_select_sql(where_clauses, limit=size, offset=offset))
        if not rows:
            return
        decoded = _decode_clickhouse_rows(rows)
        for rec in decoded:
            yield rec
        if len(rows) < size:
            return
        offset += len(rows)


def _query_by_name(req: QueryRequest) -> tuple[list[dict], bool]:
    settings = get_settings()
    name_raw = _name_keyword_raw(req.name_keyword)
    if not name_raw:
        return [], False

    name_exact = _name_match_exact(req.name_keyword)
    exact_name_kw = _name_keyword(req.name_keyword) if name_exact else None
    surname_kw = None if name_exact else _surname_keyword(req.name_keyword)
    exact_id_kw = _id_exact_keyword(req.id_no_keyword)
    id_prefix = None if exact_id_kw else _id_prefix_keyword(req.id_no_keyword)

    where_clauses = [f"name = {sql_quote(name_raw)}" if name_exact else f"positionCaseInsensitiveUTF8(name, {sql_quote(name_raw[:1])}) = 1"]
    if exact_id_kw:
        where_clauses.append(f"id_no_digest = {sql_quote(fingerprint_id_no(exact_id_kw))}")
    where_clauses.extend(_year_filters(req))

    filtered: list[dict] = []
    for rec in _iter_decoded_candidates(where_clauses, batch_size=settings.clickhouse_query_candidate_limit):
        normalized_name = normalize_text(rec["name"])
        normalized_id = normalize_id_no(rec["id_no"])
        if exact_name_kw and normalized_name != exact_name_kw:
            continue
        if surname_kw and not normalized_name.startswith(surname_kw):
            continue
        if exact_id_kw and normalized_id != exact_id_kw:
            continue
        if id_prefix and not normalized_id.startswith(id_prefix):
            continue
        rec["match_score"] = _score_record(
            rec,
            exact_name_kw=exact_name_kw,
            surname_kw=surname_kw,
            exact_id_kw=exact_id_kw,
            id_prefix=id_prefix,
        )
        filtered.append(rec)

    filtered.sort(key=lambda item: (-item["match_score"], item["year"], item["name"], item["id"]))
    return filtered, False


def _query_by_exact_id(req: QueryRequest) -> tuple[list[dict], bool]:
    exact_id_kw = _id_exact_keyword(req.id_no_keyword)
    if not exact_id_kw:
        return [], False

    where_clauses = [f"id_no_digest = {sql_quote(fingerprint_id_no(exact_id_kw))}", *_year_filters(req)]
    output: list[dict] = []
    for rec in _iter_decoded_candidates(where_clauses, batch_size=max(1, get_settings().clickhouse_query_candidate_limit)):
        if normalize_id_no(rec["id_no"]) != exact_id_kw:
            continue
        rec["match_score"] = _score_record(
            rec,
            exact_name_kw=None,
            surname_kw=None,
            exact_id_kw=exact_id_kw,
            id_prefix=None,
        )
        output.append(rec)
    output.sort(key=lambda item: (-item["match_score"], item["year"], item["name"], item["id"]))
    return output, False


def _query_by_id_prefix(req: QueryRequest) -> tuple[list[dict], bool]:
    settings = get_settings()
    id_prefix = _id_prefix_keyword(req.id_no_keyword)
    if not id_prefix:
        return [], False

    where_clauses = _year_filters(req)
    output: list[dict] = []
    for rec in _iter_decoded_candidates(where_clauses, batch_size=settings.clickhouse_query_candidate_limit):
        if not normalize_id_no(rec["id_no"]).startswith(id_prefix):
            continue
        rec["match_score"] = _score_record(
            rec,
            exact_name_kw=None,
            surname_kw=None,
            exact_id_kw=None,
            id_prefix=id_prefix,
        )
        output.append(rec)
    output.sort(key=lambda item: (-item["match_score"], item["year"], item["name"], item["id"]))
    return output, False


def query_clickhouse_records(req: QueryRequest) -> tuple[list[dict], bool]:
    if req.name_keyword:
        return _query_by_name(req)
    if _id_exact_keyword(req.id_no_keyword):
        return _query_by_exact_id(req)
    return _query_by_id_prefix(req)
