import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import jwt
from openpyxl import Workbook

from app.core.error_reasons import ALL_ERROR_REASONS, ErrorReason
from app.core.config import get_settings
from app.db.sqlite import open_db_connection
from app.services.importer import create_import_job
from app.services.records import insert_record
from tests.test_support import (
    configure_test_env,
    create_test_client,
    login,
    make_xlsx_bytes,
    wait_import_job,
    write_keys,
)


class OpenApiContractTest(unittest.TestCase):
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

    def _admin_headers(self) -> dict[str, str]:
        token = login(self.client, "admin", "ChangeMe123!")
        return {"Authorization": f"Bearer {token}"}

    def test_01_openapi_required_paths_methods(self) -> None:
        schema = self.client.get("/openapi.json").json()
        paths = schema["paths"]

        required = {
            "/healthz": {"get"},
            "/api/v1/auth/login": {"post"},
            "/api/v1/auth/logout": {"post"},
            "/api/v1/auth/refresh": {"post"},
            "/api/v1/auth/me": {"get"},
            "/api/v1/users": {"get", "post"},
            "/api/v1/users/{user_id}": {"patch", "delete"},
            "/api/v1/users/{user_id}/quota": {"get", "put"},
            "/api/v1/data/import": {"get", "post"},
            "/api/v1/data/import/{job_id}": {"get"},
            "/api/v1/data/import/{job_id}/cancel": {"post"},
            "/api/v1/stats/summary": {"get"},
            "/api/v1/query": {"post"},
            "/api/v1/records/{record_id}": {"delete"},
            "/api/v1/audit-logs": {"get"},
        }

        for path, methods in required.items():
            self.assertIn(path, paths, f"missing path: {path}")
            for method in methods:
                self.assertIn(method, paths[path], f"missing method: {method} {path}")

    def test_02_openapi_declares_key_response_codes(self) -> None:
        schema = self.client.get("/openapi.json").json()
        paths = schema["paths"]

        def assert_responses(path: str, method: str, expected_codes: set[str]) -> None:
            actual_codes = set(paths[path][method]["responses"].keys())
            for code in expected_codes:
                self.assertIn(code, actual_codes, f"missing response code {code} for {method} {path}")

        assert_responses("/api/v1/query", "post", {"200", "400", "401", "429", "500"})
        assert_responses("/api/v1/data/import", "post", {"202", "400", "401", "403", "500"})
        assert_responses("/api/v1/data/import", "get", {"200", "401", "403", "500"})
        assert_responses("/api/v1/data/import/{job_id}/cancel", "post", {"200", "400", "401", "403", "404", "500"})
        assert_responses("/api/v1/stats/summary", "get", {"200", "401", "403", "500"})
        assert_responses("/api/v1/auth/refresh", "post", {"200", "401", "403", "404", "500"})
        assert_responses("/api/v1/users", "get", {"200", "401", "403", "500"})
        assert_responses("/api/v1/users/{user_id}/quota", "get", {"200", "400", "401", "403", "404", "500"})
        assert_responses("/api/v1/records/{record_id}", "delete", {"200", "401", "403", "404", "500"})

    def test_03_error_code_enum_in_openapi(self) -> None:
        schema = self.client.get("/openapi.json").json()
        error_code_enum = schema["components"]["schemas"]["ErrorCodeEnum"]["enum"]
        expected = {
            "INVALID_ARGUMENT",
            "UNAUTHORIZED",
            "FORBIDDEN",
            "NOT_FOUND",
            "CONFLICT",
            "QUOTA_EXCEEDED",
            "IMPORT_FAILED",
            "INTERNAL_ERROR",
        }
        self.assertEqual(set(error_code_enum), expected)

    def test_04_error_details_schema_in_openapi(self) -> None:
        schema = self.client.get("/openapi.json").json()
        details_props = schema["components"]["schemas"]["ErrorDetails"]["properties"]
        self.assertIn("reason", details_props)
        self.assertIn("context", details_props)
        reason_enum = schema["components"]["schemas"]["ErrorReason"]["enum"]
        self.assertEqual(set(reason_enum), ALL_ERROR_REASONS)

    def test_05_error_code_shape_unauthorized(self) -> None:
        resp = self.client.get("/api/v1/users")
        self.assertEqual(resp.status_code, 401, resp.text)
        body = resp.json()
        self.assertEqual(body["code"], "UNAUTHORIZED")
        self.assertEqual(body["message"], "Authentication required")
        self.assertIn("trace_id", body)
        self.assertIn("details", body)
        self.assertEqual(body["details"]["reason"], ErrorReason.AUTH_REQUIRED.value)
        self.assertIn("context", body["details"])

    def test_06_error_code_shape_forbidden(self) -> None:
        admin_headers = self._admin_headers()

        create_user_resp = self.client.post(
            "/api/v1/users",
            headers=admin_headers,
            json={"username": "u_forbidden", "password": "UserPass123!", "role": "USER"},
        )
        self.assertEqual(create_user_resp.status_code, 201, create_user_resp.text)

        user_token = login(self.client, "u_forbidden", "UserPass123!")
        user_headers = {"Authorization": f"Bearer {user_token}"}

        forbidden_resp = self.client.get("/api/v1/users", headers=user_headers)
        self.assertEqual(forbidden_resp.status_code, 403, forbidden_resp.text)
        self.assertEqual(forbidden_resp.json()["code"], "FORBIDDEN")
        self.assertEqual(forbidden_resp.json()["message"], "Insufficient permissions")
        self.assertEqual(
            forbidden_resp.json()["details"]["reason"],
            ErrorReason.INSUFFICIENT_PERMISSIONS.value,
        )

    def test_07_query_response_schema_and_quota_fields(self) -> None:
        admin_headers = self._admin_headers()

        xlsx_bytes = make_xlsx_bytes([("王五", "440404199901010044", "2021")])
        import_resp = self.client.post(
            "/api/v1/data/import",
            headers=admin_headers,
            data={"super_admin_password": "ChangeMe123!"},
            files={
                "file": (
                    "contract.xlsx",
                    xlsx_bytes,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )
        self.assertEqual(import_resp.status_code, 202, import_resp.text)
        job_data = wait_import_job(self.client, admin_headers, int(import_resp.json()["data"]["id"]))
        self.assertEqual(job_data["status"], "SUCCESS")

        query_resp = self.client.post("/api/v1/query", headers=admin_headers, json={"name_keyword": "王"})
        self.assertEqual(query_resp.status_code, 200, query_resp.text)
        body = query_resp.json()

        self.assertIn("data", body)
        self.assertIn("meta", body)
        self.assertIn("returned", body["meta"])
        self.assertIn("capped", body["meta"])
        self.assertIn("quota", body["meta"])

        if body["data"]:
            item = body["data"][0]
            for field in ["id", "name", "id_no", "year", "match_score"]:
                self.assertIn(field, item)

    def test_07b_stats_summary_shape(self) -> None:
        admin_headers = self._admin_headers()
        resp = self.client.get("/api/v1/stats/summary", headers=admin_headers)
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertIn("data", body)
        self.assertIn("total_records", body["data"])
        self.assertIn("total_import_jobs", body["data"])
        self.assertIn("running_import_jobs", body["data"])

    def test_08_query_year_range_without_keyword_rejected(self) -> None:
        admin_headers = self._admin_headers()
        resp = self.client.post(
            "/api/v1/query",
            headers=admin_headers,
            json={"year_start": 2020, "year_end": 2021},
        )
        self.assertEqual(resp.status_code, 400, resp.text)
        body = resp.json()
        self.assertEqual(body["code"], "INVALID_ARGUMENT")
        self.assertEqual(body["message"], "Request validation failed")
        self.assertEqual(body["details"]["reason"], ErrorReason.VALIDATION_ERROR.value)
        self.assertIn("context", body["details"])

    def test_09_invalid_role_reason(self) -> None:
        admin_headers = self._admin_headers()
        resp = self.client.post(
            "/api/v1/users",
            headers=admin_headers,
            json={"username": "u_bad_role", "password": "UserPass123!", "role": "BAD_ROLE"},
        )
        self.assertEqual(resp.status_code, 400, resp.text)
        body = resp.json()
        self.assertEqual(body["code"], "INVALID_ARGUMENT")
        self.assertEqual(body["message"], "Invalid role")
        self.assertEqual(body["details"]["reason"], ErrorReason.INVALID_ROLE.value)

    def test_10_import_empty_file_reason(self) -> None:
        admin_headers = self._admin_headers()
        resp = self.client.post(
            "/api/v1/data/import",
            headers=admin_headers,
            data={"super_admin_password": "ChangeMe123!"},
            files={"file": ("empty.xlsx", b"", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        )
        self.assertEqual(resp.status_code, 400, resp.text)
        body = resp.json()
        self.assertEqual(body["code"], "INVALID_ARGUMENT")
        self.assertEqual(body["message"], "Uploaded file is empty")
        self.assertEqual(body["details"]["reason"], ErrorReason.IMPORT_EMPTY_FILE.value)

    def test_10b_super_admin_import_without_password_success(self) -> None:
        admin_headers = self._admin_headers()
        xlsx_bytes = make_xlsx_bytes([("密码校验", "110101199001010001", "2026")])
        resp = self.client.post(
            "/api/v1/data/import",
            headers=admin_headers,
            files={
                "file": (
                    "password_check.xlsx",
                    xlsx_bytes,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )
        self.assertEqual(resp.status_code, 202, resp.text)
        job_id = int(resp.json()["data"]["id"])
        job_data = wait_import_job(self.client, admin_headers, job_id)
        self.assertEqual(job_data["status"], "SUCCESS")

    def test_10c_import_from_source_path_success(self) -> None:
        admin_headers = self._admin_headers()
        source_file = Path(self.tmp.name) / "source_path_case.xlsx"
        source_file.write_bytes(make_xlsx_bytes([("路径导入", "110101199001010188", "2025")]))
        resp = self.client.post(
            "/api/v1/data/import",
            headers=admin_headers,
            data={"super_admin_password": "ChangeMe123!", "source_path": str(source_file)},
        )
        self.assertEqual(resp.status_code, 202, resp.text)
        job_id = int(resp.json()["data"]["id"])
        job_data = wait_import_job(self.client, admin_headers, job_id)
        self.assertEqual(job_data["status"], "SUCCESS")
        self.assertGreaterEqual(int(job_data["success_rows"]), 1)

    def test_10d_import_merged_header_and_multi_sheet_success(self) -> None:
        admin_headers = self._admin_headers()
        wb = Workbook()
        ws1 = wb.active
        ws1.title = "合并数据_1"
        ws1.append(["source_file", "source_sheet", "姓名", "身份证号", "年份"])
        ws1.append(["老人数据.xlsx", "1961", "周洪", "320923196106290014", "1961"])

        ws2 = wb.create_sheet("合并数据_2")
        ws2.append(["source_file", "source_sheet", "姓名", "身份证号", "年份"])
        ws2.append(["老人数据.xlsx", "1962", "冯家洪", "422626196104110057", "1961"])

        buff = BytesIO()
        wb.save(buff)
        wb.close()
        payload = buff.getvalue()

        resp = self.client.post(
            "/api/v1/data/import",
            headers=admin_headers,
            files={
                "file": (
                    "merged_style.xlsx",
                    payload,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )
        self.assertEqual(resp.status_code, 202, resp.text)
        job_id = int(resp.json()["data"]["id"])
        job_data = wait_import_job(self.client, admin_headers, job_id)
        self.assertEqual(job_data["status"], "SUCCESS")
        self.assertEqual(int(job_data["success_rows"]), 2)

        query_resp = self.client.post("/api/v1/query", headers=admin_headers, json={"name_keyword": "周洪"})
        self.assertEqual(query_resp.status_code, 200, query_resp.text)
        self.assertGreaterEqual(len(query_resp.json()["data"]), 1)

    def test_10e_query_returns_inserted_record(self) -> None:
        admin_headers = self._admin_headers()
        conn = open_db_connection()
        try:
            insert_record(
                conn,
                name="正常记录",
                id_no="220202198812120022",
                year="1988",
                created_by=1,
            )
            conn.commit()
        finally:
            conn.close()

        query_resp = self.client.post("/api/v1/query", headers=admin_headers, json={"id_no_keyword": "220202"})
        self.assertEqual(query_resp.status_code, 200, query_resp.text)
        names = [item["name"] for item in query_resp.json()["data"]]
        self.assertIn("正常记录", names)

    def test_11_internal_error_reason_key_file_missing(self) -> None:
        from app.core.config import get_settings

        settings = get_settings()
        key_path = Path(settings.key_file)
        if key_path.exists():
            key_path.unlink()

        admin_headers = self._admin_headers()
        try:
            resp = self.client.post("/api/v1/query", headers=admin_headers, json={"name_keyword": "任意"})
            self.assertEqual(resp.status_code, 500, resp.text)
            body = resp.json()
            self.assertEqual(body["code"], "INTERNAL_ERROR")
            self.assertEqual(body["message"], "Key file not found")
            self.assertEqual(body["details"]["reason"], ErrorReason.KEY_FILE_NOT_FOUND.value)
            self.assertIn("context", body["details"])
        finally:
            write_keys(key_path)

    def test_12_error_reason_whitelist(self) -> None:
        admin_headers = self._admin_headers()
        responses = [
            self.client.get("/api/v1/users"),  # auth_required
            self.client.post(
                "/api/v1/query",
                headers=admin_headers,
                json={"year_start": 2020, "year_end": 2021},
            ),  # validation_error
            self.client.post(
                "/api/v1/data/import",
                headers=admin_headers,
                data={"super_admin_password": "ChangeMe123!"},
            files={
                    "file": (
                        "empty.xlsx",
                        b"",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                },
            ),  # import_empty_file
        ]
        for resp in responses:
            body = resp.json()
            reason = body["details"]["reason"]
            self.assertIn(reason, ALL_ERROR_REASONS)

    def test_13_audit_failed_reason_whitelist(self) -> None:
        bad_login_resp = self.client.post(
            "/api/v1/auth/login",
            json={"username": "admin", "password": "WrongPassword!"},
        )
        self.assertEqual(bad_login_resp.status_code, 401, bad_login_resp.text)

        admin_headers = self._admin_headers()
        logs_resp = self.client.get("/api/v1/audit-logs?action_type=LOGIN&page=1&page_size=100", headers=admin_headers)
        self.assertEqual(logs_resp.status_code, 200, logs_resp.text)
        logs = logs_resp.json()["data"]

        failed_logs = [item for item in logs if item["action_result"] == "FAILED"]
        self.assertGreaterEqual(len(failed_logs), 1)
        self.assertIn(
            ErrorReason.INVALID_CREDENTIALS.value,
            [(item.get("detail_json") or {}).get("reason") for item in failed_logs],
        )

        for item in failed_logs:
            detail = item.get("detail_json") or {}
            reason = detail.get("reason")
            if reason is not None:
                self.assertIn(reason, ALL_ERROR_REASONS)

    def test_14_audit_filter_by_username_and_action_result(self) -> None:
        self.client.post("/api/v1/auth/login", json={"username": "admin", "password": "WrongPassword!"})

        admin_headers = self._admin_headers()
        logs_resp = self.client.get(
            "/api/v1/audit-logs?action_type=LOGIN&username=admin&action_result=FAILED&page=1&page_size=100",
            headers=admin_headers,
        )
        self.assertEqual(logs_resp.status_code, 200, logs_resp.text)
        logs = logs_resp.json()["data"]

        self.assertGreaterEqual(len(logs), 1)
        for item in logs:
            self.assertEqual(item["action_type"], "LOGIN")
            self.assertEqual(item["username"], "admin")
            self.assertEqual(item["action_result"], "FAILED")

    def test_15_refresh_token_success(self) -> None:
        admin_headers = self._admin_headers()
        refresh_resp = self.client.post("/api/v1/auth/refresh", headers=admin_headers)
        self.assertEqual(refresh_resp.status_code, 200, refresh_resp.text)
        body = refresh_resp.json()
        self.assertIn("access_token", body)
        self.assertIn("expires_in", body)
        self.assertEqual(body["expires_in"], get_settings().jwt_expire_seconds)
        self.assertIn("user", body)

        me_resp = self.client.get(
            "/api/v1/auth/me",
            headers={"Authorization": f"Bearer {body['access_token']}"},
        )
        self.assertEqual(me_resp.status_code, 200, me_resp.text)

    def test_16_expired_token_rejected(self) -> None:
        settings = get_settings()
        now = datetime.now(timezone.utc)
        expired = jwt.encode(
            {
                "sub": "1",
                "username": "admin",
                "role": "SUPER_ADMIN",
                "iat": int((now - timedelta(hours=2)).timestamp()),
                "exp": int((now - timedelta(hours=1)).timestamp()),
            },
            settings.jwt_secret,
            algorithm="HS256",
        )
        resp = self.client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {expired}"})
        self.assertEqual(resp.status_code, 401, resp.text)
        body = resp.json()
        self.assertEqual(body["code"], "UNAUTHORIZED")
        self.assertEqual(body["message"], "Authentication failed")
        self.assertEqual(body["details"]["reason"], ErrorReason.TOKEN_INVALID.value)

    def test_17_audit_invalid_action_result_rejected(self) -> None:
        admin_headers = self._admin_headers()
        resp = self.client.get("/api/v1/audit-logs?action_result=BAD", headers=admin_headers)
        self.assertEqual(resp.status_code, 400, resp.text)
        body = resp.json()
        self.assertEqual(body["code"], "INVALID_ARGUMENT")
        self.assertEqual(body["message"], "Request validation failed")
        self.assertEqual(body["details"]["reason"], ErrorReason.VALIDATION_ERROR.value)

    def test_18_inactive_user_token_rejected(self) -> None:
        admin_headers = self._admin_headers()
        create_resp = self.client.post(
            "/api/v1/users",
            headers=admin_headers,
            json={"username": "u_inactive_token", "password": "UserPass123!", "role": "USER"},
        )
        self.assertEqual(create_resp.status_code, 201, create_resp.text)
        user_id = create_resp.json()["data"]["id"]

        user_token = login(self.client, "u_inactive_token", "UserPass123!")
        disable_resp = self.client.patch(
            f"/api/v1/users/{user_id}",
            headers=admin_headers,
            json={"is_active": False},
        )
        self.assertEqual(disable_resp.status_code, 200, disable_resp.text)

        query_resp = self.client.post(
            "/api/v1/query",
            headers={"Authorization": f"Bearer {user_token}"},
            json={"name_keyword": "任意"},
        )
        self.assertEqual(query_resp.status_code, 403, query_resp.text)
        body = query_resp.json()
        self.assertEqual(body["code"], "FORBIDDEN")
        self.assertEqual(body["message"], "User is inactive")
        self.assertEqual(body["details"]["reason"], ErrorReason.USER_INACTIVE.value)

    def test_18b_super_admin_cannot_be_disabled(self) -> None:
        admin_headers = self._admin_headers()
        users_resp = self.client.get("/api/v1/users", headers=admin_headers)
        self.assertEqual(users_resp.status_code, 200, users_resp.text)
        users = users_resp.json()["data"]
        super_admin = next((u for u in users if u.get("role") == "SUPER_ADMIN"), None)
        self.assertIsNotNone(super_admin)

        user_id = int(super_admin["id"])
        disable_resp = self.client.patch(
            f"/api/v1/users/{user_id}",
            headers=admin_headers,
            json={"is_active": False},
        )
        self.assertEqual(disable_resp.status_code, 400, disable_resp.text)
        body = disable_resp.json()
        self.assertEqual(body["code"], "INVALID_ARGUMENT")
        self.assertEqual(body["message"], "Super admin cannot be disabled")
        self.assertEqual(body["details"]["reason"], ErrorReason.SUPER_ADMIN_CANNOT_BE_DISABLED.value)

    def test_19_query_capped_flag_true_when_exceed_limit(self) -> None:
        admin_headers = self._admin_headers()
        rows = [
            (f"zzcap{i:03d}", f"99010119900101{i:04d}", "2022")
            for i in range(101)
        ]
        xlsx_bytes = make_xlsx_bytes(rows)
        import_resp = self.client.post(
            "/api/v1/data/import",
            headers=admin_headers,
            data={"super_admin_password": "ChangeMe123!"},
            files={
                "file": (
                    "capped_case.xlsx",
                    xlsx_bytes,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )
        self.assertEqual(import_resp.status_code, 202, import_resp.text)
        job_data = wait_import_job(self.client, admin_headers, int(import_resp.json()["data"]["id"]))
        self.assertEqual(job_data["status"], "SUCCESS")

        query_resp = self.client.post("/api/v1/query", headers=admin_headers, json={"name_keyword": "zzcap"})
        self.assertEqual(query_resp.status_code, 200, query_resp.text)
        body = query_resp.json()
        self.assertEqual(body["meta"]["returned"], 100)
        self.assertTrue(body["meta"]["capped"])

    def test_20_query_blank_keyword_rejected(self) -> None:
        admin_headers = self._admin_headers()
        resp = self.client.post("/api/v1/query", headers=admin_headers, json={"name_keyword": "   "})
        self.assertEqual(resp.status_code, 400, resp.text)
        body = resp.json()
        self.assertEqual(body["code"], "INVALID_ARGUMENT")
        self.assertEqual(body["message"], "Request validation failed")
        self.assertEqual(body["details"]["reason"], ErrorReason.VALIDATION_ERROR.value)

    def test_21_get_user_quota_success(self) -> None:
        admin_headers = self._admin_headers()
        create_resp = self.client.post(
            "/api/v1/users",
            headers=admin_headers,
            json={"username": "u_quota_view", "password": "UserPass123!", "role": "USER"},
        )
        self.assertEqual(create_resp.status_code, 201, create_resp.text)
        user_id = create_resp.json()["data"]["id"]

        set_resp = self.client.put(
            f"/api/v1/users/{user_id}/quota",
            headers=admin_headers,
            json={"daily_limit": 5, "total_limit": 20},
        )
        self.assertEqual(set_resp.status_code, 200, set_resp.text)

        get_resp = self.client.get(f"/api/v1/users/{user_id}/quota", headers=admin_headers)
        self.assertEqual(get_resp.status_code, 200, get_resp.text)
        quota = get_resp.json()["data"]
        self.assertEqual(quota["daily_limit"], 5)
        self.assertEqual(quota["total_limit"], 20)
        self.assertIn("daily_used", quota)
        self.assertIn("total_used", quota)

    def test_22_list_import_jobs_filter(self) -> None:
        admin_headers = self._admin_headers()
        filename = "list_filter_case.xlsx"
        xlsx_bytes = make_xlsx_bytes([("筛选样本", "310101199901010011", "2020")])
        create_resp = self.client.post(
            "/api/v1/data/import",
            headers=admin_headers,
            data={"super_admin_password": "ChangeMe123!"},
            files={
                "file": (
                    filename,
                    xlsx_bytes,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )
        self.assertEqual(create_resp.status_code, 202, create_resp.text)
        job_data = wait_import_job(self.client, admin_headers, int(create_resp.json()["data"]["id"]))
        self.assertEqual(job_data["status"], "SUCCESS")

        list_resp = self.client.get(
            "/api/v1/data/import?status=SUCCESS&filename_contains=list_filter_case&page=1&page_size=20",
            headers=admin_headers,
        )
        self.assertEqual(list_resp.status_code, 200, list_resp.text)
        body = list_resp.json()
        self.assertIn("data", body)
        self.assertIn("total", body)
        self.assertGreaterEqual(body["total"], 1)
        self.assertTrue(any(item["filename"] == filename for item in body["data"]))

    def test_23_cancel_pending_import_job_success(self) -> None:
        admin_headers = self._admin_headers()
        conn = open_db_connection()
        try:
            admin_id = conn.execute("SELECT id FROM users WHERE username = 'admin'").fetchone()["id"]
            job_id = create_import_job(conn, "pending_to_cancel.xlsx", 1, int(admin_id))
            conn.commit()
        finally:
            conn.close()

        cancel_resp = self.client.post(f"/api/v1/data/import/{job_id}/cancel", headers=admin_headers)
        self.assertEqual(cancel_resp.status_code, 200, cancel_resp.text)
        self.assertEqual(cancel_resp.json()["data"]["status"], "CANCELLED")

    def test_24_cancel_finished_import_job_rejected(self) -> None:
        admin_headers = self._admin_headers()
        xlsx_bytes = make_xlsx_bytes([("不可取消", "110101199001010099", "2024")])
        create_resp = self.client.post(
            "/api/v1/data/import",
            headers=admin_headers,
            data={"super_admin_password": "ChangeMe123!"},
            files={
                "file": (
                    "finished_then_cancel.xlsx",
                    xlsx_bytes,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )
        self.assertEqual(create_resp.status_code, 202, create_resp.text)
        job_data = wait_import_job(self.client, admin_headers, int(create_resp.json()["data"]["id"]))
        self.assertEqual(job_data["status"], "SUCCESS")

        cancel_resp = self.client.post(
            f"/api/v1/data/import/{job_data['id']}/cancel",
            headers=admin_headers,
        )
        self.assertEqual(cancel_resp.status_code, 400, cancel_resp.text)
        body = cancel_resp.json()
        self.assertEqual(body["code"], "INVALID_ARGUMENT")
        self.assertEqual(body["message"], "Import job is not cancellable")
        self.assertEqual(body["details"]["reason"], ErrorReason.IMPORT_JOB_NOT_CANCELLABLE.value)

    def test_25_logout_revokes_token(self) -> None:
        token = login(self.client, "admin", "ChangeMe123!")
        headers = {"Authorization": f"Bearer {token}"}
        logout_resp = self.client.post("/api/v1/auth/logout", headers=headers)
        self.assertEqual(logout_resp.status_code, 200, logout_resp.text)

        me_resp = self.client.get("/api/v1/auth/me", headers=headers)
        self.assertEqual(me_resp.status_code, 401, me_resp.text)
        body = me_resp.json()
        self.assertEqual(body["code"], "UNAUTHORIZED")
        self.assertEqual(body["message"], "Authentication failed")
        self.assertEqual(body["details"]["reason"], ErrorReason.TOKEN_REVOKED.value)

    def test_26_cleanup_expired_revocations_on_auth(self) -> None:
        conn = open_db_connection()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO token_revocations(jti, expires_at, revoked_by) VALUES (?, ?, ?)",
                ("expired-test-jti", 1, None),
            )
            conn.commit()
        finally:
            conn.close()

        admin_headers = self._admin_headers()
        me_resp = self.client.get("/api/v1/auth/me", headers=admin_headers)
        self.assertEqual(me_resp.status_code, 200, me_resp.text)

        conn = open_db_connection()
        try:
            row = conn.execute(
                "SELECT 1 AS ok FROM token_revocations WHERE jti = ?",
                ("expired-test-jti",),
            ).fetchone()
            self.assertIsNone(row)
        finally:
            conn.close()

    def test_27_admin_can_view_audit_logs(self) -> None:
        admin_headers = self._admin_headers()
        create_resp = self.client.post(
            "/api/v1/users",
            headers=admin_headers,
            json={"username": "u_audit_admin", "password": "AdminPass123!", "role": "ADMIN"},
        )
        self.assertEqual(create_resp.status_code, 201, create_resp.text)

        audit_admin_token = login(self.client, "u_audit_admin", "AdminPass123!")
        audit_admin_headers = {"Authorization": f"Bearer {audit_admin_token}"}
        logs_resp = self.client.get("/api/v1/audit-logs?page=1&page_size=20", headers=audit_admin_headers)
        self.assertEqual(logs_resp.status_code, 200, logs_resp.text)
        body = logs_resp.json()
        self.assertIn("data", body)
        self.assertIn("total", body)

    def test_28_user_cannot_import_and_cannot_view_others_job(self) -> None:
        admin_headers = self._admin_headers()
        create_resp = self.client.post(
            "/api/v1/users",
            headers=admin_headers,
            json={"username": "u_import_user", "password": "UserPass123!", "role": "USER"},
        )
        self.assertEqual(create_resp.status_code, 201, create_resp.text)

        user_token = login(self.client, "u_import_user", "UserPass123!")
        user_headers = {"Authorization": f"Bearer {user_token}"}
        user_xlsx = make_xlsx_bytes([("用户导入", "310101199901010088", "2023")])
        user_import_resp = self.client.post(
            "/api/v1/data/import",
            headers=user_headers,
            data={"super_admin_password": "ChangeMe123!"},
            files={
                "file": (
                    "user_import.xlsx",
                    user_xlsx,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )
        self.assertEqual(user_import_resp.status_code, 403, user_import_resp.text)
        self.assertEqual(user_import_resp.json()["code"], "FORBIDDEN")
        self.assertEqual(user_import_resp.json()["details"]["reason"], ErrorReason.INSUFFICIENT_PERMISSIONS.value)

        admin_xlsx = make_xlsx_bytes([("管理员导入", "310101199901010099", "2024")])
        admin_import_resp = self.client.post(
            "/api/v1/data/import",
            headers=admin_headers,
            data={"super_admin_password": "ChangeMe123!"},
            files={
                "file": (
                    "admin_import.xlsx",
                    admin_xlsx,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )
        self.assertEqual(admin_import_resp.status_code, 202, admin_import_resp.text)
        admin_job_id = int(admin_import_resp.json()["data"]["id"])

        other_job_resp = self.client.get(f"/api/v1/data/import/{admin_job_id}", headers=user_headers)
        self.assertEqual(other_job_resp.status_code, 403, other_job_resp.text)
        body = other_job_resp.json()
        self.assertEqual(body["code"], "FORBIDDEN")
        self.assertEqual(body["details"]["reason"], ErrorReason.INSUFFICIENT_PERMISSIONS.value)


if __name__ == "__main__":
    unittest.main()
