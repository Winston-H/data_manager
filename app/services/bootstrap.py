from app.core.config import get_settings
from app.core.security import hash_password
from app.core.time import now_local_sql
from app.db.migrations import apply_migrations
from app.db.sqlite import db_cursor


def bootstrap_super_admin() -> None:
    settings = get_settings()
    apply_migrations()

    with db_cursor() as cur:
        row = cur.execute(
            "SELECT id FROM users WHERE role = 'SUPER_ADMIN' ORDER BY id LIMIT 1"
        ).fetchone()
        if row:
            return

        now_text = now_local_sql()
        cur.execute(
            """
            INSERT INTO users(username, password_hash, role, is_active, created_at, updated_at)
            VALUES (?, ?, 'SUPER_ADMIN', 1, ?, ?)
            """,
            (
                settings.bootstrap_superadmin_username,
                hash_password(settings.bootstrap_superadmin_password),
                now_text,
                now_text,
            ),
        )
