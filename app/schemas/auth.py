from pydantic import BaseModel


class UserSummary(BaseModel):
    id: int
    username: str
    role: str
    is_active: bool


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "Bearer"
    expires_in: int
    user: UserSummary


class UserProfileResponse(BaseModel):
    data: dict
