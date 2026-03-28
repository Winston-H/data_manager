from enum import Enum

from pydantic import BaseModel

from app.core.error_reasons import ErrorReason


class MessageResponse(BaseModel):
    message: str


class ErrorCodeEnum(str, Enum):
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    IMPORT_FAILED = "IMPORT_FAILED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class ErrorDetails(BaseModel):
    reason: ErrorReason
    context: dict | None = None


class ErrorResponse(BaseModel):
    code: ErrorCodeEnum
    message: str
    trace_id: str
    details: ErrorDetails | None = None
