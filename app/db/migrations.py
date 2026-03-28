from pathlib import Path

from app.db.sqlite import db_cursor


MIGRATION_FILES = [
    "001_init.sql",
    "002_token_revocations.sql",
    "004_drop_legacy_record_tables.sql",
]


def apply_migrations() -> None:
    sql_dir = Path(__file__).resolve().parents[2] / "sql"
    with db_cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
              version TEXT PRIMARY KEY,
              applied_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )

        for migration_name in MIGRATION_FILES:
            existed = cur.execute(
                "SELECT version FROM schema_migrations WHERE version = ?", (migration_name,)
            ).fetchone()
            if existed:
                continue

            migration_sql = (sql_dir / migration_name).read_text(encoding="utf-8")
            cur.executescript(migration_sql)
            cur.execute("INSERT INTO schema_migrations(version) VALUES (?)", (migration_name,))
