import base64
import json
import os
import secrets
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import get_settings


def main() -> None:
    settings = get_settings()
    key_path = Path(settings.key_file)
    key_path.parent.mkdir(parents=True, exist_ok=True)
    if key_path.exists():
        # Existing key files may be chmod 400; temporarily relax to allow rotation/regeneration.
        os.chmod(key_path, 0o600)

    payload = {
        "active_data_key_version": 1,
        "active_index_key_version": 1,
        "data_keys": {
            "1": base64.b64encode(secrets.token_bytes(32)).decode("ascii"),
        },
        "index_keys": {
            "1": base64.b64encode(secrets.token_bytes(32)).decode("ascii"),
        },
    }

    with open(key_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    os.chmod(key_path, 0o400)
    print(f"Generated key file at: {key_path}")


if __name__ == "__main__":
    main()
