import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.db import clickhouse as clickhouse_mod


class ClickHouseInsertFallbackTest(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = clickhouse_mod.ClickHouseConfig(
            base_url="http://127.0.0.1:8123",
            database="data_manager",
            records_table="person_records",
            username="default",
            password="",
            timeout_seconds=60.0,
        )
        self.settings = SimpleNamespace(
            clickhouse_prefer_native_client=True,
            clickhouse_native_port=9000,
        )
        self.rows = [{"id": 1, "name": "张三", "birth_year": 2023, "id_no_cipher": "cipher", "id_no_digest": "x" * 64}]

    def test_native_client_failure_falls_back_to_http(self) -> None:
        http_calls: list[tuple[str, dict | None, bytes | None]] = []

        def fake_clickhouse_command(sql: str, *, settings=None, payload=None, use_database=True):
            http_calls.append((sql, settings, payload))
            return b""

        proc = SimpleNamespace(returncode=1, stderr=b"native insert failed", stdout=b"")
        with (
            patch.object(clickhouse_mod, "get_settings", return_value=self.settings),
            patch.object(clickhouse_mod, "get_clickhouse_config", return_value=self.cfg),
            patch.object(clickhouse_mod.shutil, "which", return_value="/tmp/clickhouse-client"),
            patch.object(clickhouse_mod.subprocess, "run", return_value=proc),
            patch.object(clickhouse_mod, "clickhouse_command", side_effect=fake_clickhouse_command),
        ):
            clickhouse_mod.clickhouse_insert_json_rows("`data_manager`.`person_records`", self.rows)

        self.assertEqual(len(http_calls), 1)
        sql, settings, payload = http_calls[0]
        self.assertEqual(sql, "INSERT INTO `data_manager`.`person_records` FORMAT JSONEachRow")
        self.assertEqual(settings, {"async_insert": 0})
        self.assertEqual(payload, json.dumps(self.rows[0], ensure_ascii=False).encode("utf-8"))

    def test_combined_error_when_native_and_http_both_fail(self) -> None:
        proc = SimpleNamespace(returncode=1, stderr=b"native insert failed", stdout=b"")
        with (
            patch.object(clickhouse_mod, "get_settings", return_value=self.settings),
            patch.object(clickhouse_mod, "get_clickhouse_config", return_value=self.cfg),
            patch.object(clickhouse_mod.shutil, "which", return_value="/tmp/clickhouse-client"),
            patch.object(clickhouse_mod.subprocess, "run", return_value=proc),
            patch.object(clickhouse_mod, "clickhouse_command", side_effect=RuntimeError("ClickHouse HTTP 500: boom")),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                clickhouse_mod.clickhouse_insert_json_rows("`data_manager`.`person_records`", self.rows)

        message = str(ctx.exception)
        self.assertIn("clickhouse-client insert failed: native insert failed", message)
        self.assertIn("http fallback failed: ClickHouse HTTP 500: boom", message)


if __name__ == "__main__":
    unittest.main()
