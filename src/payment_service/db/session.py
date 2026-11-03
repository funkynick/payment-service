from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from payment_service.config import get_settings

# create_async_engine не подключается к БД, поэтому создание на импорте безопасно
engine = create_async_engine(get_settings().database_url, pool_pre_ping=True)
session_factory = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency: одна сессия на запрос."""
    async with session_factory() as session:
        yield session
