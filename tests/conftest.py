"""Integration test harness.

These tests talk to real Postgres, real Redis and a real (LocalStack) S3. There
are no mocks: the whole point is to validate the presigned handshake, which only
means something against an actual S3 implementation.

The ASGI transport does not run the app's lifespan, so the bucket is created and
the schema migrated here instead.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Callable, Iterator

import pytest
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.session import SessionFactory, dispose_engine, engine
from app.main import create_app
from app.services.queue import JobQueue, get_queue
from app.services.s3 import S3Service, get_s3

TABLES = ("files", "storage_blobs", "folders")


def _alembic_config() -> Config:
    config = Config("alembic.ini")
    config.set_main_option("script_location", "migrations")
    config.set_main_option("sqlalchemy.url", settings.database_url.replace("%", "%%"))
    return config


@pytest.fixture(scope="session", autouse=True)
def migrated_database() -> Iterator[None]:
    """Idempotent - CI also runs `alembic upgrade head` explicitly."""
    command.upgrade(_alembic_config(), "head")
    yield


@pytest.fixture(scope="session")
async def s3() -> AsyncIterator[S3Service]:
    service = get_s3()
    await service.ensure_bucket()
    yield service


@pytest.fixture(scope="session")
async def queue() -> AsyncIterator[JobQueue]:
    q = get_queue()
    yield q
    await q.close()


@pytest.fixture(autouse=True)
async def clean_state(migrated_database: None, queue: JobQueue) -> AsyncIterator[None]:
    """Every test starts from an empty database and an empty queue."""
    async with engine.begin() as connection:
        await connection.execute(text(f"TRUNCATE {', '.join(TABLES)} CASCADE"))
    await queue.redis.delete(
        settings.queue_key,
        settings.processing_key,
        settings.delayed_key,
        settings.dead_letter_key,
    )
    yield


@pytest.fixture(scope="session", autouse=True)
async def _shutdown() -> AsyncIterator[None]:
    yield
    await dispose_engine()


@pytest.fixture
def owner_id() -> uuid.UUID:
    return settings.default_owner_id


@pytest.fixture
async def client(s3: S3Service) -> AsyncIterator[AsyncClient]:
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as http:
        yield http


@pytest.fixture
async def session() -> AsyncIterator[AsyncSession]:
    async with SessionFactory() as db:
        yield db


@pytest.fixture
def upload_bytes() -> Callable[..., bytes]:
    """Deterministic filler of an exact length, seeded by a marker."""

    def _make(size: int, marker: bytes = b"filestore") -> bytes:
        unit = marker * 64
        return (unit * (size // len(unit) + 1))[:size]

    return _make
