from pydantic import BaseModel, Field


class UserResponse(BaseModel):
    data: dict


class UserListResponse(BaseModel):
    data: list[dict]


class CreateUserRequest(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=8, max_length=128)
    role: str


class UpdateUserRequest(BaseModel):
    role: str | None = None
    is_active: bool | None = None


class QuotaUpdateRequest(BaseModel):
    daily_limit: int = Field(ge=0)
    total_limit: int = Field(ge=0)


class QuotaResponse(BaseModel):
    data: dict
