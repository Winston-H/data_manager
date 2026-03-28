from fastapi import APIRouter

from app.api.routes import audit, auth, data_import, health, query, records, stats, users

router = APIRouter()
router.include_router(health.router)
router.include_router(auth.router, prefix="/api/v1/auth", tags=["Auth"])
router.include_router(users.router, prefix="/api/v1/users", tags=["Users"])
router.include_router(data_import.router, prefix="/api/v1/data", tags=["DataImport"])
router.include_router(stats.router, prefix="/api/v1/stats", tags=["Stats"])
router.include_router(query.router, prefix="/api/v1", tags=["Query"])
router.include_router(records.router, prefix="/api/v1/records", tags=["Records"])
router.include_router(audit.router, prefix="/api/v1/audit-logs", tags=["Audit"])
