from pydantic import BaseModel


class ImportJobResponse(BaseModel):
    data: dict


class ImportJobListResponse(BaseModel):
    data: list[dict]
    page: int
    page_size: int
    total: int
