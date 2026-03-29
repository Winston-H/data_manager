import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.schemas.query import QueryRequest
from app.services import clickhouse_records as records_mod


class ClickHouseQueryPagingTest(unittest.TestCase):
    def test_name_plus_id_prefix_scans_all_candidate_pages(self) -> None:
        settings = SimpleNamespace(clickhouse_query_candidate_limit=2)
        cfg = SimpleNamespace(records_table_sql="`data_manager`.`person_records`")
        raw_rows = [
            {"id": 1, "name": "陈甲", "birth_year": 1961, "id_no_cipher": "372900000000000001"},
            {"id": 2, "name": "陈乙", "birth_year": 1961, "id_no_cipher": "111100000000000002"},
            {"id": 3, "name": "陈丙", "birth_year": 1961, "id_no_cipher": "372900000000000003"},
            {"id": 4, "name": "陈丁", "birth_year": 1961, "id_no_cipher": "222200000000000004"},
        ]
        seen_offsets: list[int] = []

        def fake_query(sql: str):
            if "OFFSET 0" in sql:
                seen_offsets.append(0)
                return raw_rows[:2]
            if "OFFSET 2" in sql:
                seen_offsets.append(2)
                return raw_rows[2:]
            if "OFFSET 4" in sql:
                seen_offsets.append(4)
                return []
            return []

        def fake_decode(rows):
            return [
                {
                    "id": int(row["id"]),
                    "name": str(row["name"]),
                    "id_no": str(row["id_no_cipher"]),
                    "year": int(row["birth_year"]),
                }
                for row in rows
            ]

        with (
            patch.object(records_mod, "get_settings", return_value=settings),
            patch.object(records_mod, "get_clickhouse_config", return_value=cfg),
            patch.object(records_mod, "clickhouse_query_rows", side_effect=fake_query),
            patch.object(records_mod, "_decode_clickhouse_rows", side_effect=fake_decode),
        ):
            rows, capped = records_mod.query_clickhouse_records(
                QueryRequest(name_keyword="陈", id_no_keyword="3729", year_start=1961, year_end=1961)
            )

        self.assertFalse(capped)
        self.assertEqual(seen_offsets, [0, 2, 4])
        self.assertEqual([row["id"] for row in rows], [3, 1])


if __name__ == "__main__":
    unittest.main()
