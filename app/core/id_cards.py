from __future__ import annotations

import hashlib
import re
from datetime import datetime


_ID_NO_RE = re.compile(r"^\d{17}[\dXx]$")
_ID_NO_WEIGHTS = (7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2)
_ID_NO_CHECK_CODES = "10X98765432"


def normalize_id_no(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().replace(" ", "").upper()


def is_valid_id_no(id_no: str) -> bool:
    normalized = normalize_id_no(id_no)
    if not _ID_NO_RE.fullmatch(normalized):
        return False

    birth_text = normalized[6:14]
    try:
        datetime.strptime(birth_text, "%Y%m%d")
    except ValueError:
        return False

    checksum = sum(int(char) * weight for char, weight in zip(normalized[:17], _ID_NO_WEIGHTS, strict=True))
    return _ID_NO_CHECK_CODES[checksum % 11] == normalized[-1]


def fingerprint_id_no(id_no: str) -> str:
    normalized = normalize_id_no(id_no)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
