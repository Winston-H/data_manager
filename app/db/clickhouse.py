from __future__ import annotations

import base64
import json
import shutil
import subprocess
from dataclasses import dataclass
from typing import Any
from urllib import error, parse, request

from app.core.config import get_settings


def _escape_sql_name(name: str) -> str:
    safe = str(name).strip().replace("`", "")
    return f"`{safe}`"


def sql_quote(text: object) -> str:
    value = str(text)
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


@dataclass(frozen=True)
class ClickHouseConfig:
    base_url: str
    database: str
    records_table: str
    username: str
    password: str
    timeout_seconds: float

    @property
    def records_table_sql(self) -> str:
        return f"{_escape_sql_name(self.database)}.{_escape_sql_name(self.records_table)}"


def get_clickhouse_config() -> ClickHouseConfig:
    settings = get_settings()
    return ClickHouseConfig(
        base_url=settings.clickhouse_url.strip().rstrip("/"),
        database=settings.clickhouse_database.strip(),
        records_table=settings.clickhouse_records_table.strip(),
        username=settings.clickhouse_username.strip(),
        password=settings.clickhouse_password,
        timeout_seconds=max(1.0, float(settings.clickhouse_timeout_seconds)),
    )


def _auth_header(cfg: ClickHouseConfig) -> dict[str, str]:
    if not cfg.username:
        return {}
    token = base64.b64encode(f"{cfg.username}:{cfg.password}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def _build_url(cfg: ClickHouseConfig, params: dict[str, Any] | None = None, *, use_database: bool = True) -> str:
    query = {"wait_end_of_query": "1"}
    if use_database and cfg.database:
        query["database"] = cfg.database
    if params:
        query.update({key: str(value) for key, value in params.items() if value is not None})
    return f"{cfg.base_url}/?{parse.urlencode(query)}"


def _http_body(sql: str, payload: bytes | None = None) -> bytes:
    sql_bytes = sql.encode("utf-8")
    if not payload:
        return sql_bytes
    return sql_bytes + b"\n" + payload


def clickhouse_command(
    sql: str,
    *,
    settings: dict[str, Any] | None = None,
    payload: bytes | None = None,
    use_database: bool = True,
) -> bytes:
    cfg = get_clickhouse_config()
    req = request.Request(
        _build_url(cfg, settings, use_database=use_database),
        data=_http_body(sql, payload),
        method="POST",
        headers={
            "Content-Type": "text/plain; charset=utf-8",
            **_auth_header(cfg),
        },
    )
    try:
        with request.urlopen(req, timeout=cfg.timeout_seconds) as resp:
            return resp.read()
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse HTTP {exc.code}: {body}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"ClickHouse connect failed: {exc.reason}") from exc


def clickhouse_query_rows(
    sql: str, *, settings: dict[str, Any] | None = None, use_database: bool = True
) -> list[dict[str, Any]]:
    payload = clickhouse_command(f"{sql}\nFORMAT JSON", settings=settings, use_database=use_database)
    data = json.loads(payload.decode("utf-8"))
    return list(data.get("data") or [])


def clickhouse_insert_json_rows(table_sql: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    lines = [json.dumps(row, ensure_ascii=False) for row in rows]
    payload = "\n".join(lines).encode("utf-8")
    settings = get_settings()
    cfg = get_clickhouse_config()
    native_error: RuntimeError | None = None
    if settings.clickhouse_prefer_native_client and shutil.which("clickhouse-client"):
        parts = parse.urlsplit(cfg.base_url)
        host = parts.hostname or "127.0.0.1"
        port = max(1, int(settings.clickhouse_native_port))
        cmd = [
            "clickhouse-client",
            f"--host={host}",
            f"--port={port}",
            f"--database={cfg.database}",
            f"--query=INSERT INTO {table_sql} FORMAT JSONEachRow",
        ]
        if cfg.username:
            cmd.append(f"--user={cfg.username}")
        if cfg.password:
            cmd.append(f"--password={cfg.password}")
        try:
            proc = subprocess.run(cmd, input=payload, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        except OSError as exc:
            native_error = RuntimeError(f"clickhouse-client insert failed: {exc}")
        else:
            if proc.returncode == 0:
                return
            stderr = proc.stderr.decode("utf-8", errors="replace").strip()
            native_error = RuntimeError(f"clickhouse-client insert failed: {stderr}")
    try:
        clickhouse_command(
            f"INSERT INTO {table_sql} FORMAT JSONEachRow",
            settings={"async_insert": 0},
            payload=payload,
        )
    except RuntimeError as exc:
        if native_error is not None:
            raise RuntimeError(f"{native_error}; http fallback failed: {exc}") from exc
        raise
