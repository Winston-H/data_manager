import tempfile
import unittest
from pathlib import Path

from app.core.error_reasons import ErrorReason
from tests.test_support import (
    configure_test_env,
    create_test_client,
    login,
    make_xlsx_bytes,
    wait_import_job,
)


class ApiE2ETest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        base = Path(cls.tmp.name)
        configure_test_env(base)
        cls.client_cm = create_test_client()
        cls.client = cls.client_cm.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client_cm.__exit__(None, None, None)
        cls.tmp.cleanup()

    def test_01_end_to_end_flow(self) -> None:
        admin_token = login(self.client, "admin", "ChangeMe123!")
        admin_headers = {"Authorization": f"Bearer {admin_token}"}

        create_user_resp = self.client.post(
            "/api/v1/users",
            headers=admin_headers,
            json={"username": "user1", "password": "UserPass123!", "role": "USER"},
        )
        self.assertEqual(create_user_resp.status_code, 201, create_user_resp.text)
        user_id = create_user_resp.json()["data"]["id"]

        quota_resp = self.client.put(
            f"/api/v1/users/{user_id}/quota",
            headers=admin_headers,
            json={"daily_limit": 1, "total_limit": 1},
        )
        self.assertEqual(quota_resp.status_code, 200, quota_resp.text)

        xlsx_bytes = make_xlsx_bytes(
            [
                ("张三", "110101199001010011", "2020"),
                ("李四", "220202198812120022", "2019"),
                ("", "330303197701010033", "2022"),
            ]
        )
        import_resp = self.client.post(
            "/api/v1/data/import",
            headers=admin_headers,
            data={"super_admin_password": "ChangeMe123!"},
            files={
                "file": (
                    "sample.xlsx",
                    xlsx_bytes,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )
        self.assertEqual(import_resp.status_code, 202, import_resp.text)
        import_data = import_resp.json()["data"]
        self.assertIn(import_data["status"], {"PENDING", "RUNNING", "SUCCESS"})

        import_job = wait_import_job(self.client, admin_headers, int(import_data["id"]))
        self.assertEqual(import_job["status"], "SUCCESS")
        self.assertEqual(import_job["total_rows"], 3)
        self.assertEqual(import_job["success_rows"], 3)
        self.assertEqual(import_job["skipped_rows"], 0)
        self.assertEqual(import_job["failed_rows"], 0)
        self.assertEqual(
            import_job["total_rows"],
            import_job["success_rows"] + import_job["skipped_rows"] + import_job["failed_rows"],
        )

        query_admin = self.client.post("/api/v1/query", headers=admin_headers, json={"name_keyword": "张"})
        self.assertEqual(query_admin.status_code, 200, query_admin.text)
        query_data = query_admin.json()["data"]
        self.assertGreaterEqual(len(query_data), 1)
        record_id = query_data[0]["id"]

        delete_resp = self.client.delete(f"/api/v1/records/{record_id}", headers=admin_headers)
        self.assertEqual(delete_resp.status_code, 200, delete_resp.text)

        query_after_delete = self.client.post(
            "/api/v1/query", headers=admin_headers, json={"name_keyword": "张三"}
        )
        self.assertEqual(query_after_delete.status_code, 200, query_after_delete.text)
        self.assertEqual(query_after_delete.json()["meta"]["returned"], 0)

        user_token = login(self.client, "user1", "UserPass123!")
        user_headers = {"Authorization": f"Bearer {user_token}"}

        user_query_1 = self.client.post(
            "/api/v1/query", headers=user_headers, json={"id_no_keyword": "220299"}
        )
        self.assertEqual(user_query_1.status_code, 200, user_query_1.text)
        user_rows = user_query_1.json()["data"]
        self.assertGreaterEqual(len(user_rows), 1)
        self.assertNotEqual(user_rows[0]["name"], "李四")
        self.assertNotEqual(user_rows[0]["id_no"], "220202198812120022")
        self.assertTrue(user_rows[0]["id_no"].startswith("220202"))
        self.assertTrue(user_rows[0]["id_no"].endswith("0022"))

        user_query_2 = self.client.post(
            "/api/v1/query", headers=user_headers, json={"id_no_keyword": "220299"}
        )
        self.assertEqual(user_query_2.status_code, 429, user_query_2.text)
        self.assertEqual(user_query_2.json()["code"], "QUOTA_EXCEEDED")
        self.assertEqual(user_query_2.json()["message"], "Daily query limit exceeded")
        self.assertEqual(user_query_2.json()["details"]["reason"], ErrorReason.QUOTA_EXCEEDED_DAILY.value)

        audit_resp = self.client.get("/api/v1/audit-logs?page=1&page_size=50", headers=admin_headers)
        self.assertEqual(audit_resp.status_code, 200, audit_resp.text)
        self.assertGreater(audit_resp.json()["total"], 0)

    def test_02_year_range_requires_keyword(self) -> None:
        admin_token = login(self.client, "admin", "ChangeMe123!")
        admin_headers = {"Authorization": f"Bearer {admin_token}"}

        resp = self.client.post(
            "/api/v1/query",
            headers=admin_headers,
            json={"year_start": 2020, "year_end": 2021},
        )
        self.assertEqual(resp.status_code, 400, resp.text)
        self.assertEqual(resp.json()["code"], "INVALID_ARGUMENT")

    def test_03_import_csv_success(self) -> None:
        admin_token = login(self.client, "admin", "ChangeMe123!")
        admin_headers = {"Authorization": f"Bearer {admin_token}"}
        csv_bytes = (
            "姓名,身份证号,年份\n"
            "极速样本A,110101199001010011,2022\n"
            "极速样本B,220202199202020022,2023\n"
        ).encode("utf-8")

        resp = self.client.post(
            "/api/v1/data/import",
            headers=admin_headers,
            files={
                "file": (
                    "sample.csv",
                    csv_bytes,
                    "text/csv",
                )
            },
        )
        self.assertEqual(resp.status_code, 202, resp.text)
        job_id = int(resp.json()["data"]["id"])
        job = wait_import_job(self.client, admin_headers, job_id)
        self.assertEqual(job["status"], "SUCCESS")
        self.assertEqual(int(job["success_rows"]), 2)

    def test_04_query_year_prefix_filter(self) -> None:
        admin_token = login(self.client, "admin", "ChangeMe123!")
        admin_headers = {"Authorization": f"Bearer {admin_token}"}
        xlsx_bytes = make_xlsx_bytes(
            [
                ("年筛样本A", "110101196001010011", "1961"),
                ("年筛样本B", "220202197202020022", "1972"),
            ]
        )
        import_resp = self.client.post(
            "/api/v1/data/import",
            headers=admin_headers,
            files={
                "file": (
                    "year_prefix_case.xlsx",
                    xlsx_bytes,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )
        self.assertEqual(import_resp.status_code, 202, import_resp.text)
        wait_import_job(self.client, admin_headers, int(import_resp.json()["data"]["id"]))

        query_resp = self.client.post(
            "/api/v1/query",
            headers=admin_headers,
            json={"name_keyword": "年", "year_prefix": "196"},
        )
        self.assertEqual(query_resp.status_code, 200, query_resp.text)
        rows = query_resp.json()["data"]
        self.assertGreaterEqual(len(rows), 1)
        self.assertTrue(all(str(item["year"]).startswith("196") for item in rows))

    def test_05_import_keeps_duplicate_rows(self) -> None:
        admin_token = login(self.client, "admin", "ChangeMe123!")
        admin_headers = {"Authorization": f"Bearer {admin_token}"}
        duplicated = ("去重样本", "110101199001010199", "2024")
        xlsx_bytes = make_xlsx_bytes([duplicated, duplicated])
        import_resp = self.client.post(
            "/api/v1/data/import",
            headers=admin_headers,
            files={
                "file": (
                    "dedup_case.xlsx",
                    xlsx_bytes,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )
        self.assertEqual(import_resp.status_code, 202, import_resp.text)
        job = wait_import_job(self.client, admin_headers, int(import_resp.json()["data"]["id"]))
        self.assertEqual(job["status"], "SUCCESS")
        self.assertEqual(int(job["total_rows"]), 2)
        self.assertEqual(int(job["success_rows"]), 2)
        self.assertEqual(int(job["skipped_rows"]), 0)
        self.assertEqual(int(job["failed_rows"]), 0)


if __name__ == "__main__":
    unittest.main()
