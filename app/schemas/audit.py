from pydantic import BaseModel


class AuditLogListResponse(BaseModel):
    data: list[dict]
    page: int
    page_size: int
    total: int
    has_more: bool = False
