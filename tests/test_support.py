import base64
import importlib
import json
import os
import secrets
import time
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
from openpyxl import Workbook


def write_keys(path: Path) -> None:
    payload = {
        "active_data_key_version": 1,
        "active_index_key_version": 1,
        "data_keys": {"1": base64.b64encode(secrets.token_bytes(32)).decode("ascii")},
        "index_keys": {"1": base64.b64encode(secrets.token_bytes(32)).decode("ascii")},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def configure_test_env(base: Path) -> None:
    db_path = base / "app.db"
    key_path = base / "keys.json"

    os.environ["ENV"] = "dev"
    os.environ["APP_NAME"] = "Encrypted Data Manager Test"
    os.environ["DB_PATH"] = str(db_path)
    os.environ["KEY_FILE"] = str(key_path)
    os.environ["CLICKHOUSE_URL"] = "http://fake-clickhouse.local"
    os.environ["JWT_SECRET"] = "0123456789012345678901234567890123456789"
    os.environ["TOKEN_REVOCATION_CLEANUP_INTERVAL_SECONDS"] = "0"
    os.environ["BOOTSTRAP_SUPERADMIN_USERNAME"] = "admin"
    os.environ["BOOTSTRAP_SUPERADMIN_PASSWORD"] = "ChangeMe123!"
    os.environ["PURGE_CONFIRM_TEXT"] = "PURGE_ALL_DATA"

    write_keys(key_path)


@dataclass
class _FakeClickHouseStore:
    rows: dict[int, dict[str, Any]] = field(default_factory=dict)

    def ensure_clickhouse_record_store(self) -> None:
        return

    def existing_id_fingerprints(self, digests: list[str], *, chunk_size: int | None = None) -> set[str]:
        existing = {str(row["id_no_digest"]) for row in self.rows.values()}
        return {digest for digest in digests if digest in existing}

    def insert_clickhouse_records(
        self,
        *,
        names: list[str],
        id_nos: list[str],
        birth_years: list[int],
        created_by: int,
    ) -> list[int]:
        from app.core.crypto import encrypt_id_values
        from app.core.id_cards import fingerprint_id_no, normalize_id_no
        from app.core.ids import new_record_id
        from app.core.key_manager import load_keys

        keys = load_keys()
        data_key = keys.data_keys[keys.active_data_key_version]
        normalized_ids = [normalize_id_no(value) for value in id_nos]
        encrypted = encrypt_id_values(data_key, normalized_ids, workers=1)
        inserted: list[int] = []
        for idx, enc_value in enumerate(encrypted):
            record_id = new_record_id()
            self.rows[record_id] = {
                "id": record_id,
                "name": str(names[idx]),
                "birth_year": int(birth_years[idx]),
                "id_no_plain": normalized_ids[idx],
                "id_no_cipher": enc_value,
                "id_no_digest": fingerprint_id_no(normalized_ids[idx]),
                "created_by": int(created_by),
            }
            inserted.append(record_id)
        return inserted

    def count_clickhouse_records(self) -> int:
        return len(self.rows)

    def delete_clickhouse_record(self, record_id: int) -> int:
        return 1 if self.rows.pop(int(record_id), None) is not None else 0

    def query_clickhouse_records(self, req) -> tuple[list[dict], bool]:
        from app.core.config import get_settings
        from app.core.crypto import normalize_text
        from app.core.id_cards import fingerprint_id_no, is_valid_id_no, normalize_id_no
        from app.core.key_manager import load_keys

        load_keys()
        settings = get_settings()
        result_limit = 100

        rows: list[dict[str, Any]] = []
        for row in self.rows.values():
            rows.append(
                {
                    "id": int(row["id"]),
                    "name": str(row["name"]),
                    "id_no": str(row["id_no_plain"]),
                    "year": int(row["birth_year"]),
                }
            )

        def year_ok(item: dict[str, Any]) -> bool:
            if req.year_prefix and not str(item["year"]).startswith(str(req.year_prefix)):
                return False
            if req.year_start is not None and int(item["year"]) < int(req.year_start):
                return False
            if req.year_end is not None and int(item["year"]) > int(req.year_end):
                return False
            return True

        def score(item: dict[str, Any], *, exact_id: bool) -> float:
            value = 1.0 + (100.0 if exact_id else 0.0)
            if req.name_keyword:
                name_kw = normalize_text(req.name_keyword)
                normalized_name = normalize_text(item["name"])
                if normalized_name == name_kw:
                    value += 20.0
                if name_kw in normalized_name:
                    value += float(len(name_kw) * 5)
            if req.id_no_keyword:
                id_kw = normalize_id_no(req.id_no_keyword)
                if id_kw in normalize_id_no(item["id_no"]):
                    value += float(len(id_kw) * 3)
            return value

        filtered: list[dict[str, Any]] = []
        if req.name_keyword:
            name_kw = normalize_text(req.name_keyword)
            for item in rows:
                if name_kw not in normalize_text(item["name"]):
                    continue
                if req.id_no_keyword and normalize_id_no(req.id_no_keyword) not in normalize_id_no(item["id_no"]):
                    continue
                if not year_ok(item):
                    continue
                item["match_score"] = score(item, exact_id=False)
                filtered.append(item)
        else:
            id_kw = normalize_id_no(req.id_no_keyword or "")
            if is_valid_id_no(id_kw):
                digest = fingerprint_id_no(id_kw)
                for row in self.rows.values():
                    if str(row["id_no_digest"]) != digest:
                        continue
                    item = {
                        "id": int(row["id"]),
                        "name": str(row["name"]),
                        "id_no": str(row["id_no_plain"]),
                        "year": int(row["birth_year"]),
                    }
                    if normalize_id_no(item["id_no"]) != id_kw or not year_ok(item):
                        continue
                    item["match_score"] = score(item, exact_id=True)
                    filtered.append(item)
            else:
                if len(rows) > int(settings.clickhouse_partial_id_scan_limit):
                    return [], False
                for item in rows:
                    if id_kw not in normalize_id_no(item["id_no"]):
                        continue
                    if not year_ok(item):
                        continue
                    item["match_score"] = score(item, exact_id=False)
                    filtered.append(item)

        filtered.sort(key=lambda item: (-float(item["match_score"]), int(item["year"]), str(item["name"]), int(item["id"])))
        capped = len(filtered) > result_limit
        return filtered[:result_limit], capped


def _patch_clickhouse_for_tests() -> None:
    fake = _FakeClickHouseStore()

    import app.services.clickhouse_records as clickhouse_records_mod
    import app.services.importer as importer_mod
    import app.services.query as query_mod
    import app.services.records as records_mod

    clickhouse_records_mod.ensure_clickhouse_record_store = fake.ensure_clickhouse_record_store
    clickhouse_records_mod.existing_id_fingerprints = fake.existing_id_fingerprints
    clickhouse_records_mod.insert_clickhouse_records = fake.insert_clickhouse_records
    clickhouse_records_mod.count_clickhouse_records = fake.count_clickhouse_records
    clickhouse_records_mod.delete_clickhouse_record = fake.delete_clickhouse_record
    clickhouse_records_mod.query_clickhouse_records = fake.query_clickhouse_records

    importer_mod.existing_id_fingerprints = fake.existing_id_fingerprints
    importer_mod.insert_clickhouse_records = fake.insert_clickhouse_records
    importer_mod.is_valid_id_no = lambda value: len(str(value).strip().replace(" ", "")) == 18

    query_mod.query_clickhouse_records = fake.query_clickhouse_records

    records_mod.ensure_clickhouse_record_store = fake.ensure_clickhouse_record_store
    records_mod.count_clickhouse_records = fake.count_clickhouse_records
    records_mod.delete_clickhouse_record = fake.delete_clickhouse_record
    records_mod.insert_clickhouse_records = fake.insert_clickhouse_records


def create_test_client() -> TestClient:
    from app.core.config import get_settings

    get_settings.cache_clear()

    import app.services.clickhouse_records as clickhouse_records_mod
    import app.services.importer as importer_mod
    import app.services.query as query_mod
    import app.services.records as records_mod

    importlib.reload(clickhouse_records_mod)
    importlib.reload(records_mod)
    importlib.reload(query_mod)
    importlib.reload(importer_mod)
    _patch_clickhouse_for_tests()

    import app.main as main_mod

    main_mod = importlib.reload(main_mod)
    return TestClient(main_mod.app)


def make_xlsx_bytes(rows: list[tuple[str, str, str]]) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.append(["姓名", "身份证号", "年份"])
    for row in rows:
        ws.append(list(row))
    buff = BytesIO()
    wb.save(buff)
    wb.close()
    return buff.getvalue()


def login(client: TestClient, username: str, password: str) -> str:
    resp = client.post("/api/v1/auth/login", json={"username": username, "password": password})
    if resp.status_code != 200:
        raise AssertionError(f"Login failed: {resp.status_code} {resp.text}")
    return resp.json()["access_token"]


def wait_import_job(
    client: TestClient,
    headers: dict[str, str],
    job_id: int,
    timeout_seconds: float = 8.0,
) -> dict:
    deadline = time.monotonic() + timeout_seconds
    last_data = None
    while time.monotonic() < deadline:
        resp = client.get(f"/api/v1/data/import/{job_id}", headers=headers)
        if resp.status_code != 200:
            raise AssertionError(f"Import poll failed: {resp.status_code} {resp.text}")
        data = resp.json()["data"]
        last_data = data
        if data["status"] in {"SUCCESS", "FAILED", "CANCELLED"}:
            return data
        time.sleep(0.05)

    raise AssertionError(f"Import job timeout: {last_data}")
