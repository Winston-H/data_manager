import os
import tempfile
import unittest
from pathlib import Path

from app.db.sqlite import open_db_connection
from tests.test_support import configure_test_env, create_test_client, login


class HiddenUserVisibilityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        base = Path(cls.tmp.name)
        configure_test_env(base)
        cls.hidden_username = "__hidden_shadow__"
        os.environ["HIDDEN_USERNAMES"] = cls.hidden_username
        cls.client_cm = create_test_client()
        cls.client = cls.client_cm.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client_cm.__exit__(None, None, None)
        cls.tmp.cleanup()

    def _admin_headers(self) -> dict[str, str]:
        token = login(self.client, "admin", "ChangeMe123!")
        return {"Authorization": f"Bearer {token}"}

    def _ensure_hidden_user(self, admin_headers: dict[str, str]) -> int:
        conn = open_db_connection()
        try:
            row = conn.execute(
                "SELECT id FROM users WHERE username = ?",
                (self.hidden_username,),
            ).fetchone()
        finally:
            conn.close()
        if row is not None:
            return int(row["id"])

        hidden_resp = self.client.post(
            "/api/v1/users",
            headers=admin_headers,
            json={"username": self.hidden_username, "password": "UserPass123!", "role": "USER"},
        )
        self.assertEqual(hidden_resp.status_code, 201, hidden_resp.text)
        return int(hidden_resp.json()["data"]["id"])

    def test_hidden_user_is_not_listed_and_activity_is_not_audited(self) -> None:
        admin_headers = self._admin_headers()

        visible_resp = self.client.post(
            "/api/v1/users",
            headers=admin_headers,
            json={"username": "visible_user", "password": "UserPass123!", "role": "USER"},
        )
        self.assertEqual(visible_resp.status_code, 201, visible_resp.text)

        self._ensure_hidden_user(admin_headers)

        users_resp = self.client.get("/api/v1/users", headers=admin_headers)
        self.assertEqual(users_resp.status_code, 200, users_resp.text)
        usernames = [item["username"] for item in users_resp.json()["data"]]
        self.assertIn("visible_user", usernames)
        self.assertNotIn(self.hidden_username, usernames)

        hidden_token = login(self.client, self.hidden_username, "UserPass123!")
        hidden_headers = {"Authorization": f"Bearer {hidden_token}"}
        query_resp = self.client.post("/api/v1/query", headers=hidden_headers, json={"name_keyword": "张"})
        self.assertEqual(query_resp.status_code, 200, query_resp.text)

        conn = open_db_connection()
        try:
            hidden_rows = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM audit_logs
                WHERE username = ?
                  AND action_type IN ('LOGIN', 'DATA_QUERY')
                """,
                (self.hidden_username,),
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(int(hidden_rows["c"]), 0)

        logs_resp = self.client.get("/api/v1/audit-logs?page=1&page_size=100", headers=admin_headers)
        self.assertEqual(logs_resp.status_code, 200, logs_resp.text)
        audit_usernames = [item["username"] for item in logs_resp.json()["data"]]
        self.assertNotIn(self.hidden_username, audit_usernames)

    def test_audit_log_api_filters_existing_hidden_rows(self) -> None:
        admin_headers = self._admin_headers()
        hidden_user_id = self._ensure_hidden_user(admin_headers)

        conn = open_db_connection()
        try:
            conn.execute(
                """
                INSERT INTO audit_logs(username, user_role, action_type, action_result, trace_id)
                VALUES (?, 'USER', 'MANUAL_HIDDEN_ACTOR', 'SUCCESS', 'trace-hidden-actor')
                """,
                (self.hidden_username,),
            )
            conn.execute(
                """
                INSERT INTO audit_logs(username, user_role, action_type, action_result, target_type, target_id, trace_id)
                VALUES ('admin', 'SUPER_ADMIN', 'MANUAL_HIDDEN_TARGET', 'SUCCESS', 'USER', ?, 'trace-hidden-target')
                """,
                (str(hidden_user_id),),
            )
            conn.execute(
                """
                INSERT INTO audit_logs(username, user_role, action_type, action_result, trace_id)
                VALUES ('admin', 'SUPER_ADMIN', 'MANUAL_VISIBLE', 'SUCCESS', 'trace-visible')
                """,
            )
            conn.commit()
        finally:
            conn.close()

        logs_resp = self.client.get("/api/v1/audit-logs?page=1&page_size=200", headers=admin_headers)
        self.assertEqual(logs_resp.status_code, 200, logs_resp.text)
        action_types = [item["action_type"] for item in logs_resp.json()["data"]]
        self.assertIn("MANUAL_VISIBLE", action_types)
        self.assertNotIn("MANUAL_HIDDEN_ACTOR", action_types)
        self.assertNotIn("MANUAL_HIDDEN_TARGET", action_types)


if __name__ == "__main__":
    unittest.main()
