import tempfile
import unittest
from pathlib import Path

from app.db.sqlite import open_db_connection
from app.services.importer import create_import_job, get_import_source_path, recover_pending_import_jobs
from tests.test_support import (
    configure_test_env,
    create_test_client,
    login,
    make_xlsx_bytes,
    wait_import_job,
)


class ImportRecoveryTest(unittest.TestCase):
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

    def test_recover_pending_import_job(self) -> None:
        admin_headers = self._admin_headers()
        conn = open_db_connection()
        try:
            admin_id = int(conn.execute("SELECT id FROM users WHERE username = 'admin'").fetchone()["id"])
            xlsx_bytes = make_xlsx_bytes([("恢复任务样本", "110101199001010066", "2023")])
            job_id = create_import_job(conn, "recover_pending.xlsx", len(xlsx_bytes), admin_id)
            get_import_source_path(job_id).write_bytes(xlsx_bytes)
            conn.commit()
        finally:
            conn.close()

        recovered = recover_pending_import_jobs()
        self.assertGreaterEqual(recovered, 1)

        job = wait_import_job(self.client, admin_headers, job_id)
        self.assertEqual(job["status"], "SUCCESS")
        self.assertEqual(job["success_rows"], 1)

    def test_recover_pending_import_job_file_missing_mark_failed(self) -> None:
        conn = open_db_connection()
        try:
            admin_id = int(conn.execute("SELECT id FROM users WHERE username = 'admin'").fetchone()["id"])
            job_id = create_import_job(conn, "recover_missing.xlsx", 123, admin_id)
            conn.commit()
        finally:
            conn.close()

        recovered = recover_pending_import_jobs()
        self.assertEqual(recovered, 0)

        conn = open_db_connection()
        try:
            row = conn.execute("SELECT status FROM import_jobs WHERE id = ?", (job_id,)).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["status"], "FAILED")
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
