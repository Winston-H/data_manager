from pydantic import BaseModel


class StatsSummary(BaseModel):
    total_records: int
    total_import_jobs: int
    running_import_jobs: int
    latest_import_finished_at: str | None = None


class StatsSummaryResponse(BaseModel):
    data: StatsSummary
