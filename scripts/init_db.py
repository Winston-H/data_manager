from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.migrations import apply_migrations
from app.db.sqlite import ensure_data_dir
from app.services.records import ensure_record_store


def main() -> None:
    ensure_data_dir()
    apply_migrations()
    ensure_record_store()
    print("Database initialized")


if __name__ == "__main__":
    main()
