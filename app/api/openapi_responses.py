from app.schemas.common import ErrorResponse


RESP_400 = {400: {"model": ErrorResponse, "description": "Bad Request"}}
RESP_401 = {401: {"model": ErrorResponse, "description": "Unauthorized"}}
RESP_403 = {403: {"model": ErrorResponse, "description": "Forbidden"}}
RESP_404 = {404: {"model": ErrorResponse, "description": "Not Found"}}
RESP_409 = {409: {"model": ErrorResponse, "description": "Conflict"}}
RESP_429 = {429: {"model": ErrorResponse, "description": "Too Many Requests"}}
RESP_500 = {500: {"model": ErrorResponse, "description": "Internal Server Error"}}

