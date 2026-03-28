import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.services import clickhouse_records as records_mod


class ClickHouseDedupProbeTest(unittest.TestCase):
    def test_existing_id_fingerprints_splits_large_in_clause(self) -> None:
        settings = SimpleNamespace(
            clickhouse_dedup_probe_batch_size=10_000,
            clickhouse_dedup_probe_max_query_bytes=160,
        )
        cfg = SimpleNamespace(records_table_sql="`data_manager`.`person_records`")
        digests = [f"{idx:064x}" for idx in range(20)]
        seen_sql: list[str] = []

        def fake_query(sql: str):
            seen_sql.append(sql)
            return [{"id_no_digest": value} for value in digests if value in sql]

        with (
            patch.object(records_mod, "get_settings", return_value=settings),
            patch.object(records_mod, "get_clickhouse_config", return_value=cfg),
            patch.object(records_mod, "clickhouse_query_rows", side_effect=fake_query),
        ):
            existing = records_mod.existing_id_fingerprints(digests)

        self.assertEqual(existing, set(digests))
        self.assertGreater(len(seen_sql), 1)
        self.assertTrue(all(len(sql) < 1200 for sql in seen_sql))


if __name__ == "__main__":
    unittest.main()
