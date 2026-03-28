import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from time import perf_counter

from fastapi.encoders import jsonable_encoder
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.api.routes import router
from app.core.config import get_settings
from app.core.errors import ApiError, ErrorCode
from app.core.error_reasons import ErrorReason, message_for_reason
from app.core.ids import new_trace_id
from app.core.key_manager import load_keys
from app.db.sqlite import ensure_data_dir, ensure_wal_mode
from app.services.bootstrap import bootstrap_super_admin
from app.services.importer import recover_pending_import_jobs
from app.services.records import ensure_record_store

settings = get_settings()
logger = logging.getLogger("app.request")


def _normalize_error_details(details: object, *, fallback_reason: str) -> dict:
    if isinstance(details, dict):
        reason = details.get("reason")
        context = details.get("context")
        if isinstance(reason, str) and ("context" in details):
            return {"reason": reason, "context": context}
        return {"reason": fallback_reason, "context": details}
    return {"reason": fallback_reason, "context": None}


@asynccontextmanager
async def lifespan(_: FastAPI):
    ensure_data_dir()
    ensure_wal_mode()
    load_keys()
    bootstrap_super_admin()
    ensure_record_store()
    recover_pending_import_jobs()
    yield


app = FastAPI(title=settings.app_name, lifespan=lifespan)
web_dir = Path(__file__).resolve().parent / "web"
if web_dir.exists():
    app.mount("/web", StaticFiles(directory=web_dir), name="web")

    @app.get("/", include_in_schema=False)
    async def root() -> FileResponse:
        return FileResponse(web_dir / "index.html")


@app.middleware("http")
async def trace_id_middleware(request: Request, call_next):
    trace_id = request.headers.get("x-trace-id") or new_trace_id()
    request.state.trace_id = trace_id
    start = perf_counter()
    response = await call_next(request)
    duration_ms = round((perf_counter() - start) * 1000, 3)
    response.headers["x-trace-id"] = trace_id
    logger.info(
        json.dumps(
            {
                "trace_id": trace_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": duration_ms,
                "client_ip": request.client.host if request.client else None,
            },
            ensure_ascii=False,
        )
    )
    return response


@app.exception_handler(ApiError)
async def api_error_handler(request: Request, exc: ApiError):
    trace_id = getattr(request.state, "trace_id", new_trace_id())
    details = _normalize_error_details(
        exc.details,
        fallback_reason=ErrorReason.API_ERROR_UNSPECIFIED.value,
    )
    message = message_for_reason(details.get("reason"), exc.message)
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "code": exc.code,
            "message": message,
            "trace_id": trace_id,
            "details": details,
        },
    )


@app.exception_handler(RequestValidationError)
async def request_validation_error_handler(request: Request, exc: RequestValidationError):
    trace_id = getattr(request.state, "trace_id", new_trace_id())
    serialized_errors = jsonable_encoder(
        exc.errors(),
        custom_encoder={ValueError: lambda v: str(v)},
    )
    detail_payload = {
        "reason": ErrorReason.VALIDATION_ERROR.value,
        "context": {"errors": serialized_errors},
    }
    message = message_for_reason(ErrorReason.VALIDATION_ERROR.value, "Request validation failed")
    return JSONResponse(
        status_code=400,
        content={
            "code": ErrorCode.INVALID_ARGUMENT,
            "message": message,
            "trace_id": trace_id,
            "details": detail_payload,
        },
    )


@app.exception_handler(Exception)
async def unexpected_error_handler(request: Request, exc: Exception):
    trace_id = getattr(request.state, "trace_id", new_trace_id())
    message = message_for_reason(ErrorReason.INTERNAL_UNHANDLED_EXCEPTION.value, "Internal server error")
    return JSONResponse(
        status_code=500,
        content={
            "code": ErrorCode.INTERNAL_ERROR,
            "message": message,
            "trace_id": trace_id,
            "details": {"reason": ErrorReason.INTERNAL_UNHANDLED_EXCEPTION.value, "context": None},
        },
    )


app.include_router(router)
