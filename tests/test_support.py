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
        birth_year_raws: list[str] | None = None,
        created_by: int,
    ) -> list[int]:
        from app.core.crypto import encrypt_id_values
        from app.core.id_cards import fingerprint_id_no, normalize_id_no
        from app.core.ids import new_record_id
        from app.core.key_manager import load_keys

        keys = load_keys()
        data_key = keys.data_keys[keys.active_data_key_version]
        raw_ids = [str(value or "").strip() for value in id_nos]
        normalized_ids = [normalize_id_no(value) for value in raw_ids]
        raw_years = (
            [str(value) for value in birth_year_raws]
            if birth_year_raws is not None
            else [str(int(value)) for value in birth_years]
        )
        encrypted = encrypt_id_values(data_key, raw_ids, workers=1)
        inserted: list[int] = []
        for idx, enc_value in enumerate(encrypted):
            record_id = new_record_id()
            self.rows[record_id] = {
                "id": record_id,
                "name": str(names[idx]),
                "birth_year": int(birth_years[idx]),
                "birth_year_raw": raw_years[idx],
                "id_no_plain": raw_ids[idx],
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
        from app.core.crypto import normalize_text
        from app.core.id_cards import normalize_id_no
        from app.core.key_manager import load_keys

        load_keys()

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

        name_raw = str(req.name_keyword or "").strip()
        name_kw = normalize_text(name_raw) if name_raw else None
        name_exact = bool(name_raw and len(name_raw) > 1)
        surname_kw = None if name_exact else (normalize_text(name_raw[:1]) if name_raw else None)
        id_raw = normalize_id_no(req.id_no_keyword or "")
        exact_id_kw = id_raw if len(id_raw) == 18 else None
        id_prefix_kw = None if exact_id_kw else (id_raw[:4] or None)

        def score(item: dict[str, Any], *, exact_id: bool) -> float:
            value = 1.0
            normalized_name = normalize_text(item["name"])
            normalized_id = normalize_id_no(item["id_no"])
            if name_exact and name_kw and normalized_name == name_kw:
                value += 30.0
            elif surname_kw and normalized_name.startswith(surname_kw):
                value += 20.0
            if exact_id_kw and normalized_id == exact_id_kw:
                value += 100.0
            elif id_prefix_kw and normalized_id.startswith(id_prefix_kw):
                value += float(len(id_prefix_kw) * 3)
            return value

        filtered: list[dict[str, Any]] = []
        if req.name_keyword:
            for item in rows:
                normalized_name = normalize_text(item["name"])
                normalized_id = normalize_id_no(item["id_no"])
                if name_exact and name_kw and normalized_name != name_kw:
                    continue
                if surname_kw and not normalized_name.startswith(surname_kw):
                    continue
                if exact_id_kw and normalized_id != exact_id_kw:
                    continue
                if id_prefix_kw and not normalized_id.startswith(id_prefix_kw):
                    continue
                if not year_ok(item):
                    continue
                item["match_score"] = score(item, exact_id=False)
                filtered.append(item)
        else:
            for item in rows:
                normalized_id = normalize_id_no(item["id_no"])
                if exact_id_kw and normalized_id != exact_id_kw:
                    continue
                if id_prefix_kw and not normalized_id.startswith(id_prefix_kw):
                    continue
                if not year_ok(item):
                    continue
                item["match_score"] = score(item, exact_id=bool(exact_id_kw))
                filtered.append(item)

        filtered.sort(key=lambda item: (-float(item["match_score"]), int(item["year"]), str(item["name"]), int(item["id"])))
        return filtered, False


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
