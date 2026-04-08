from fastapi import APIRouter

from app.core.time import now_local_isoformat

router = APIRouter()


@router.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "time": now_local_isoformat()}
