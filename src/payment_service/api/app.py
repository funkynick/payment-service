from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text

from payment_service.api.routes.payments import router as payments_router
from payment_service.config import get_settings
from payment_service.db.session import engine, session_factory
from payment_service.logging import setup_logging


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    setup_logging(get_settings().log_level)
    yield
    await engine.dispose()


app = FastAPI(
    title="Payment Processing Service",
    version="0.1.0",
    description=(
        "Асинхронный процессинг платежей. Создание платежа возвращает 202, "
        "результат приходит на webhook_url. Все эндпоинты /api/v1 требуют X-API-Key."
    ),
    lifespan=lifespan,
)
app.include_router(payments_router)


@app.get("/health", tags=["service"], summary="Liveness/readiness для docker healthcheck")
async def health() -> dict[str, str]:
    async with session_factory() as session:
        await session.execute(text("SELECT 1"))
    return {"status": "ok"}
