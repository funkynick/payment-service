from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Все настройки читаются из переменных окружения (см. .env.example)."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    api_key: str = "dev-secret-key"
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    webhook_secret: str = "dev-webhook-secret"

    # эмуляция шлюза
    gateway_delay_min: float = 2.0
    gateway_delay_max: float = 5.0
    gateway_success_rate: float = Field(default=0.9, ge=0.0, le=1.0)

    # consumer / retry / DLQ
    consumer_prefetch: int = Field(default=10, ge=1)
    max_attempts: int = Field(default=3, ge=1)
    retry_base_delay: float = Field(default=2.0, gt=0)
    webhook_timeout: float = 5.0

    # outbox relay
    outbox_poll_interval: float = 0.5
    outbox_batch_size: int = 100

    log_level: str = "INFO"

    def retry_delay(self, attempt: int) -> float:
        """Экспоненциальная задержка перед попыткой номер `attempt` (1-based).

        attempt=1 -> 0 (первая попытка сразу), attempt=2 -> base, attempt=3 -> base*2, ...
        """
        if attempt <= 1:
            return 0.0
        return self.retry_base_delay * (2 ** (attempt - 2))


@lru_cache
def get_settings() -> Settings:
    return Settings()
