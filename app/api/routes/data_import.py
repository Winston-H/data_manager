import sqlite3
import shutil
import tempfile
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile

from app.api.deps import CurrentUser, get_client_ip, get_conn, require_roles
from app.api.openapi_responses import RESP_400, RESP_401, RESP_403, RESP_404, RESP_500
from app.core.config import get_settings
from app.core.errors import ApiError, ErrorCode
from app.core.error_reasons import ErrorReason
from app.schemas.import_job import ImportJobListResponse, ImportJobResponse
from app.services.audit import write_audit
from app.services.importer import (
    ImportAuditContext,
    cancel_import_job,
    create_import_job,
    get_import_job,
    get_live_import_progress,
    get_import_source_path,
    is_supported_import_filename,
    list_import_jobs,
    start_import_job_async,
    supported_import_extensions_text,
)

router = APIRouter()


@router.get(
    "/import",
    response_model=ImportJobListResponse,
    responses={**RESP_401, **RESP_403, **RESP_500},
)
def list_import(
    request: Request,
    status: Literal["PENDING", "RUNNING", "SUCCESS", "FAILED", "CANCELLED"] | None = None,
    created_by: int | None = None,
    filename_contains: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    current_user: CurrentUser = Depends(require_roles("SUPER_ADMIN", "ADMIN")),
    conn: sqlite3.Connection = Depends(get_conn),
):
    rows, total = list_import_jobs(
        conn,
        status=status,
        created_by=created_by,
        filename_contains=filename_contains,
        page=page,
        page_size=page_size,
    )
    write_audit(
        conn,
        user_id=current_user.id,
        username=current_user.username,
        user_role=current_user.role,
        ip_address=get_client_ip(request),
        action_type="DATA_IMPORT_LIST",
        action_result="SUCCESS",
        detail={
            "status": status,
            "created_by": created_by,
            "filename_contains": filename_contains,
            "page": page,
            "page_size": page_size,
            "returned": len(rows),
        },
        trace_id=request.state.trace_id,
    )
    return ImportJobListResponse(data=[dict(r) for r in rows], page=page, page_size=page_size, total=total)


@router.post(
    "/import",
    response_model=ImportJobResponse,
    status_code=202,
    responses={**RESP_400, **RESP_401, **RESP_403, **RESP_500},
)
def import_xlsx(
    request: Request,
    file: UploadFile | None = File(None),
    source_path: str | None = Form(default=None),
    super_admin_password: str | None = Form(default=None),
    current_user: CurrentUser = Depends(require_roles("SUPER_ADMIN")),
    conn: sqlite3.Connection = Depends(get_conn),
):
    tmp_path: Path | None = None
    file_size = 0
    source_path_text = (source_path or "").strip()
    has_file = bool(file and file.filename)
    has_source_path = bool(source_path_text)
    settings = get_settings()
    tmp_dir = Path(settings.db_path).resolve().parent / "tmp_uploads"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    allowed_ext_text = supported_import_extensions_text()

    try:
        if not has_file and not has_source_path:
            raise ApiError(
                400,
                ErrorCode.INVALID_ARGUMENT,
                "Either uploaded file or source path is required",
                details={"reason": ErrorReason.IMPORT_SOURCE_REQUIRED.value, "context": None},
            )
        if has_file and has_source_path:
            raise ApiError(
                400,
                ErrorCode.INVALID_ARGUMENT,
                "Only one import source is allowed",
                details={"reason": ErrorReason.IMPORT_SOURCE_CONFLICT.value, "context": None},
            )

        filename = ""
        target_source_path: Path

        if has_file:
            assert file is not None
            filename = file.filename or ""
            if not filename:
                raise ApiError(
                    400,
                    ErrorCode.INVALID_ARGUMENT,
                    "Filename is required",
                    details={"reason": ErrorReason.FILENAME_REQUIRED.value, "context": None},
                )
            if not is_supported_import_filename(filename):
                raise ApiError(
                    400,
                    ErrorCode.INVALID_ARGUMENT,
                    f"Only {allowed_ext_text} is supported",
                    details={"reason": ErrorReason.INVALID_FILE_EXTENSION.value, "context": {"filename": filename}},
                )
            file_suffix = Path(filename).suffix.lower() or ".tmp"
            with tempfile.NamedTemporaryFile(dir=tmp_dir, suffix=file_suffix, delete=False) as tmp:
                tmp_path = Path(tmp.name)
                while True:
                    chunk = file.file.read(1024 * 1024)
                    if not chunk:
                        break
                    tmp.write(chunk)
                    file_size += len(chunk)
            if file_size == 0:
                raise ApiError(
                    400,
                    ErrorCode.INVALID_ARGUMENT,
                    "Uploaded file is empty",
                    details={"reason": ErrorReason.IMPORT_EMPTY_FILE.value, "context": {"filename": filename}},
                )
            job_id = create_import_job(conn, filename, file_size, current_user.id)
            target_source_path = get_import_source_path(job_id, filename)
            tmp_path.replace(target_source_path)
            tmp_path = None
        else:
            raw_path = Path(source_path_text).expanduser()
            try:
                resolved_path = raw_path.resolve(strict=True)
            except FileNotFoundError as exc:
                raise ApiError(
                    400,
                    ErrorCode.INVALID_ARGUMENT,
                    "Source path not found",
                    details={
                        "reason": ErrorReason.IMPORT_SOURCE_PATH_NOT_FOUND.value,
                        "context": {"source_path": source_path_text},
                    },
                ) from exc
            if not resolved_path.is_file():
                raise ApiError(
                    400,
                    ErrorCode.INVALID_ARGUMENT,
                    "Source path must be a file",
                    details={
                        "reason": ErrorReason.IMPORT_SOURCE_PATH_NOT_FILE.value,
                        "context": {"source_path": str(resolved_path)},
                    },
                )
            if not is_supported_import_filename(resolved_path.name):
                raise ApiError(
                    400,
                    ErrorCode.INVALID_ARGUMENT,
                    f"Only {allowed_ext_text} is supported",
                    details={"reason": ErrorReason.INVALID_FILE_EXTENSION.value, "context": {"filename": resolved_path.name}},
                )
            file_size = int(resolved_path.stat().st_size)
            if file_size == 0:
                raise ApiError(
                    400,
                    ErrorCode.INVALID_ARGUMENT,
                    "Uploaded file is empty",
                    details={"reason": ErrorReason.IMPORT_EMPTY_FILE.value, "context": {"filename": resolved_path.name}},
                )
            filename = resolved_path.name
            job_id = create_import_job(conn, filename, file_size, current_user.id)
            target_source_path = get_import_source_path(job_id, filename)
            with resolved_path.open("rb") as src, target_source_path.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=4 * 1024 * 1024)

        start_import_job_async(
            job_id=job_id,
            file_path=target_source_path,
            created_by=current_user.id,
            audit_ctx=ImportAuditContext(
                user_id=current_user.id,
                username=current_user.username,
                user_role=current_user.role,
                ip_address=get_client_ip(request),
                trace_id=request.state.trace_id,
                filename=filename,
            ),
        )
        job_row = get_import_job(conn, job_id)
        if job_row is None:
            raise ApiError(
                500,
                ErrorCode.INTERNAL_ERROR,
                "Import job not found after creation",
                details={"reason": ErrorReason.IMPORT_JOB_MISSING_AFTER_CREATE.value, "context": {"job_id": job_id}},
            )
    finally:
        if file is not None:
            file.file.close()
        if tmp_path and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)

    return ImportJobResponse(data=dict(job_row))


@router.get(
    "/import/{job_id}",
    response_model=ImportJobResponse,
    responses={**RESP_401, **RESP_403, **RESP_404, **RESP_500},
)
def get_import(
    job_id: int,
    current_user: CurrentUser = Depends(require_roles("SUPER_ADMIN", "ADMIN", "USER")),
    conn: sqlite3.Connection = Depends(get_conn),
):
    row = get_import_job(conn, job_id)
    if row is None:
        raise ApiError(
            404,
            ErrorCode.NOT_FOUND,
            "Import job not found",
            details={"reason": ErrorReason.IMPORT_JOB_NOT_FOUND.value, "context": {"job_id": job_id}},
        )
    if current_user.role == "USER" and int(row["created_by"]) != current_user.id:
        raise ApiError(
            403,
            ErrorCode.FORBIDDEN,
            "Insufficient permissions",
            details={
                "reason": ErrorReason.INSUFFICIENT_PERMISSIONS.value,
                "context": {"job_id": job_id, "actual_role": current_user.role},
            },
        )
    data = dict(row)
    live = get_live_import_progress(job_id)
    if live and data.get("status") in {"PENDING", "RUNNING"}:
        for key in ("status", "total_rows", "success_rows", "skipped_rows", "failed_rows", "error_summary"):
            value = live.get(key)
            if value is not None:
                data[key] = value
    return ImportJobResponse(data=data)


@router.post(
    "/import/{job_id}/cancel",
    response_model=ImportJobResponse,
    responses={**RESP_400, **RESP_401, **RESP_403, **RESP_404, **RESP_500},
)
def cancel_import(
    job_id: int,
    request: Request,
    current_user: CurrentUser = Depends(require_roles("SUPER_ADMIN", "ADMIN")),
    conn: sqlite3.Connection = Depends(get_conn),
):
    row = cancel_import_job(conn, job_id, cancelled_by=current_user.username)
    if row is None:
        raise ApiError(
            404,
            ErrorCode.NOT_FOUND,
            "Import job not found",
            details={"reason": ErrorReason.IMPORT_JOB_NOT_FOUND.value, "context": {"job_id": job_id}},
        )
    if row["status"] in {"SUCCESS", "FAILED"}:
        raise ApiError(
            400,
            ErrorCode.INVALID_ARGUMENT,
            "Import job is not cancellable",
            details={
                "reason": ErrorReason.IMPORT_JOB_NOT_CANCELLABLE.value,
                "context": {"job_id": job_id, "status": row["status"]},
            },
        )

    write_audit(
        conn,
        user_id=current_user.id,
        username=current_user.username,
        user_role=current_user.role,
        ip_address=get_client_ip(request),
        action_type="DATA_IMPORT_CANCEL",
        action_result="SUCCESS",
        target_type="IMPORT_JOB",
        target_id=str(job_id),
        detail={"status": row["status"]},
        trace_id=request.state.trace_id,
    )
    return ImportJobResponse(data=dict(row))
