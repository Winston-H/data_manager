from datetime import datetime, timezone

from fastapi import APIRouter

router = APIRouter()


@router.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}
