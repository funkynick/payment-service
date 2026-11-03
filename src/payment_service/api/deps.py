import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader

from payment_service.config import Settings, get_settings

# APIKeyHeader регистрирует схему безопасности в OpenAPI: в Swagger появляется кнопка Authorize
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(
    settings: Annotated[Settings, Depends(get_settings)],
    x_api_key: Annotated[str | None, Depends(api_key_header)],
) -> None:
    """Статический API-ключ. compare_digest - чтобы не утекало время сравнения."""
    if x_api_key is None or not secrets.compare_digest(x_api_key, settings.api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
