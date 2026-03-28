import base64
import json
import os
import stat
import threading
from dataclasses import dataclass

from app.core.config import get_settings
from app.core.errors import ApiError, ErrorCode
from app.core.error_reasons import ErrorReason


@dataclass
class KeyMaterial:
    active_data_key_version: int
    active_index_key_version: int
    data_keys: dict[int, bytes]
    index_keys: dict[int, bytes]


_cache_lock = threading.Lock()
_cached_path: str | None = None
_cached_stat_fingerprint: tuple[int, int, int] | None = None
_cached_keys: KeyMaterial | None = None


def _decode_map(raw: dict[str, str]) -> dict[int, bytes]:
    result: dict[int, bytes] = {}
    for version, encoded_key in raw.items():
        result[int(version)] = base64.b64decode(encoded_key)
    return result


def load_keys() -> KeyMaterial:
    global _cached_path, _cached_stat_fingerprint, _cached_keys

    settings = get_settings()
    path = settings.key_file
    try:
        st = os.stat(path)
    except FileNotFoundError:
        with _cache_lock:
            _cached_path = None
            _cached_stat_fingerprint = None
            _cached_keys = None
        raise ApiError(
            500,
            ErrorCode.INTERNAL_ERROR,
            "Key file not found",
            details={"reason": ErrorReason.KEY_FILE_NOT_FOUND.value, "context": {"key_file": path}},
        )

    mode = stat.S_IMODE(st.st_mode)
    if settings.env != "dev" and mode != 0o400:
        raise ApiError(
            500,
            ErrorCode.INTERNAL_ERROR,
            "Key file permission must be 400",
            details={
                "reason": ErrorReason.KEY_FILE_PERMISSION_INVALID.value,
                "context": {"key_file": path, "mode": mode},
            },
        )

    fingerprint = (st.st_mtime_ns, st.st_size, mode)
    with _cache_lock:
        if (
            _cached_keys is not None
            and _cached_path == path
            and _cached_stat_fingerprint == fingerprint
        ):
            return _cached_keys

        with open(path, "r", encoding="utf-8") as f:
            content = json.load(f)

        data_keys = _decode_map(content["data_keys"])
        index_keys = _decode_map(content["index_keys"])

        active_data_key_version = int(content["active_data_key_version"])
        active_index_key_version = int(content["active_index_key_version"])

        if active_data_key_version not in data_keys or active_index_key_version not in index_keys:
            raise ApiError(
                500,
                ErrorCode.INTERNAL_ERROR,
                "Active key version missing",
                details={
                    "reason": ErrorReason.ACTIVE_KEY_VERSION_MISSING.value,
                    "context": {
                        "active_data_key_version": active_data_key_version,
                        "active_index_key_version": active_index_key_version,
                    },
                },
            )

        material = KeyMaterial(
            active_data_key_version=active_data_key_version,
            active_index_key_version=active_index_key_version,
            data_keys=data_keys,
            index_keys=index_keys,
        )
        _cached_path = path
        _cached_stat_fingerprint = fingerprint
        _cached_keys = material
        return material
