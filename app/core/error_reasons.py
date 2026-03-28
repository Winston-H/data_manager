from enum import Enum


class ErrorReason(str, Enum):
    API_ERROR_UNSPECIFIED = "api_error_unspecified"
    VALIDATION_ERROR = "validation_error"
    INTERNAL_UNHANDLED_EXCEPTION = "internal_unhandled_exception"

    AUTH_REQUIRED = "auth_required"
    TOKEN_INVALID = "token_invalid"
    TOKEN_REVOKED = "token_revoked"
    INSUFFICIENT_PERMISSIONS = "insufficient_permissions"
    INVALID_CREDENTIALS = "invalid_credentials"
    USER_INACTIVE = "user_inactive"

    USER_NOT_FOUND = "user_not_found"
    INVALID_ROLE = "invalid_role"
    CANNOT_DELETE_SELF = "cannot_delete_self"
    SUPER_ADMIN_CANNOT_BE_DISABLED = "super_admin_cannot_be_disabled"
    QUOTA_TARGET_ROLE_INVALID = "quota_target_role_invalid"

    FILENAME_REQUIRED = "filename_required"
    IMPORT_SOURCE_REQUIRED = "import_source_required"
    IMPORT_SOURCE_CONFLICT = "import_source_conflict"
    IMPORT_SOURCE_PATH_NOT_FOUND = "import_source_path_not_found"
    IMPORT_SOURCE_PATH_NOT_FILE = "import_source_path_not_file"
    INVALID_FILE_EXTENSION = "invalid_file_extension"
    IMPORT_EMPTY_FILE = "import_empty_file"
    IMPORT_SUPER_ADMIN_PASSWORD_INVALID = "import_super_admin_password_invalid"
    IMPORT_JOB_MISSING_AFTER_CREATE = "import_job_missing_after_create"
    IMPORT_JOB_NOT_FOUND = "import_job_not_found"
    IMPORT_JOB_NOT_CANCELLABLE = "import_job_not_cancellable"
    IMPORT_JOB_FAILED = "import_job_failed"
    IMPORT_JOB_CANCELLED = "import_job_cancelled"
    IMPORT_WORKER_EXCEPTION = "import_worker_exception"

    RECORD_NOT_FOUND = "record_not_found"

    KEY_FILE_NOT_FOUND = "key_file_not_found"
    KEY_FILE_PERMISSION_INVALID = "key_file_permission_invalid"
    ACTIVE_KEY_VERSION_MISSING = "active_key_version_missing"

    QUOTA_EXCEEDED_DAILY = "quota_exceeded_daily"
    QUOTA_EXCEEDED_TOTAL = "quota_exceeded_total"

    USERNAME_EXISTS = "username_exists"
    CREATE_USER_FAILED = "create_user_failed"


ALL_ERROR_REASONS = {reason.value for reason in ErrorReason}

ERROR_REASON_MESSAGES: dict[str, str] = {
    ErrorReason.API_ERROR_UNSPECIFIED.value: "Request failed",
    ErrorReason.VALIDATION_ERROR.value: "Request validation failed",
    ErrorReason.INTERNAL_UNHANDLED_EXCEPTION.value: "Internal server error",
    ErrorReason.AUTH_REQUIRED.value: "Authentication required",
    ErrorReason.TOKEN_INVALID.value: "Authentication failed",
    ErrorReason.TOKEN_REVOKED.value: "Authentication failed",
    ErrorReason.INSUFFICIENT_PERMISSIONS.value: "Insufficient permissions",
    ErrorReason.INVALID_CREDENTIALS.value: "Authentication failed",
    ErrorReason.USER_INACTIVE.value: "User is inactive",
    ErrorReason.USER_NOT_FOUND.value: "User not found",
    ErrorReason.INVALID_ROLE.value: "Invalid role",
    ErrorReason.CANNOT_DELETE_SELF.value: "Cannot delete current user",
    ErrorReason.SUPER_ADMIN_CANNOT_BE_DISABLED.value: "Super admin cannot be disabled",
    ErrorReason.QUOTA_TARGET_ROLE_INVALID.value: "Quota can only be set for USER role",
    ErrorReason.FILENAME_REQUIRED.value: "Filename is required",
    ErrorReason.IMPORT_SOURCE_REQUIRED.value: "Either uploaded file or source path is required",
    ErrorReason.IMPORT_SOURCE_CONFLICT.value: "Only one import source is allowed",
    ErrorReason.IMPORT_SOURCE_PATH_NOT_FOUND.value: "Source path not found",
    ErrorReason.IMPORT_SOURCE_PATH_NOT_FILE.value: "Source path must be a file",
    ErrorReason.INVALID_FILE_EXTENSION.value: "Only .xlsx/.csv is supported",
    ErrorReason.IMPORT_EMPTY_FILE.value: "Uploaded file is empty",
    ErrorReason.IMPORT_SUPER_ADMIN_PASSWORD_INVALID.value: "Super admin password is invalid",
    ErrorReason.IMPORT_JOB_MISSING_AFTER_CREATE.value: "Import job not found after creation",
    ErrorReason.IMPORT_JOB_NOT_FOUND.value: "Import job not found",
    ErrorReason.IMPORT_JOB_NOT_CANCELLABLE.value: "Import job is not cancellable",
    ErrorReason.IMPORT_JOB_FAILED.value: "Import job failed",
    ErrorReason.IMPORT_JOB_CANCELLED.value: "Import job cancelled",
    ErrorReason.IMPORT_WORKER_EXCEPTION.value: "Import worker failed",
    ErrorReason.RECORD_NOT_FOUND.value: "Record not found",
    ErrorReason.KEY_FILE_NOT_FOUND.value: "Key file not found",
    ErrorReason.KEY_FILE_PERMISSION_INVALID.value: "Key file permission must be 400",
    ErrorReason.ACTIVE_KEY_VERSION_MISSING.value: "Active key version missing",
    ErrorReason.QUOTA_EXCEEDED_DAILY.value: "Daily query limit exceeded",
    ErrorReason.QUOTA_EXCEEDED_TOTAL.value: "Total query limit exceeded",
    ErrorReason.USERNAME_EXISTS.value: "Username already exists",
    ErrorReason.CREATE_USER_FAILED.value: "Failed to create user",
}


def message_for_reason(reason: str | None, fallback: str) -> str:
    if not reason:
        return fallback
    if reason == ErrorReason.API_ERROR_UNSPECIFIED.value:
        return fallback
    return ERROR_REASON_MESSAGES.get(reason, fallback)
