"""Async engine / session factory, plus the asyncpg ltree codec hook."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings


def _register_ltree_codec(engine: AsyncEngine) -> None:
    """Teach asyncpg to pass `ltree` values through as text.

    Without this, asyncpg raises on unknown OIDs. Registered per physical
    connection via the sync engine's ``connect`` event, which is the documented
    hook for asyncpg codecs.
    """

    @event.listens_for(engine.sync_engine, "connect")
    def _on_connect(dbapi_connection, _record):  # type: ignore[no-untyped-def]
        def _setup(connection):
            return connection.set_type_codec(
                "ltree",
                schema="public",
                encoder=str,
                decoder=str,
                format="text",
            )

        dbapi_connection.run_async(_setup)


def create_engine() -> AsyncEngine:
    engine = create_async_engine(
        settings.database_url,
        echo=settings.db_echo,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout,
        pool_pre_ping=True,
        future=True,
    )
    _register_ltree_codec(engine)
    return engine


engine: AsyncEngine = create_engine()

SessionFactory: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency: one session per request, rolled back on error."""
    async with SessionFactory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """Context manager for non-request callers (worker, scripts, tests)."""
    async with SessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def ping_database() -> bool:
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))
    return True


async def dispose_engine() -> None:
    await engine.dispose()
