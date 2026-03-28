import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from app.core.config import get_settings


def ensure_data_dir() -> None:
    settings = get_settings()
    db_parent = Path(settings.db_path).resolve().parent
    key_parent = Path(settings.key_file).resolve().parent
    db_parent.mkdir(parents=True, exist_ok=True)
    key_parent.mkdir(parents=True, exist_ok=True)


def _connect() -> sqlite3.Connection:
    settings = get_settings()
    timeout_seconds = max(1.0, float(settings.sqlite_busy_timeout_ms) / 1000.0)
    conn = sqlite3.connect(settings.db_path, check_same_thread=False, timeout=timeout_seconds)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute(f"PRAGMA busy_timeout={int(settings.sqlite_busy_timeout_ms)};")
    return conn


def open_db_connection() -> sqlite3.Connection:
    return _connect()


def is_locked_error(exc: BaseException) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and "locked" in str(exc).lower()


def ensure_wal_mode() -> None:
    settings = get_settings()
    timeout_seconds = max(1.0, float(settings.sqlite_busy_timeout_ms) / 1000.0)
    conn = sqlite3.connect(settings.db_path, check_same_thread=False, timeout=timeout_seconds)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except sqlite3.OperationalError as exc:
        # During heavy write windows we may not get an exclusive schema lock here.
        if "locked" not in str(exc).lower():
            raise
    finally:
        conn.close()


@contextmanager
def db_cursor() -> Generator[sqlite3.Cursor, None, None]:
    conn = _connect()
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def get_db_conn() -> Generator[sqlite3.Connection, None, None]:
    conn = _connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
