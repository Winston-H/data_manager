from dataclasses import dataclass


class ErrorCode:
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    IMPORT_FAILED = "IMPORT_FAILED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


@dataclass
class ApiError(Exception):
    status_code: int
    code: str
    message: str
    details: dict | None = None
