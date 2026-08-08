"""Liveness / readiness probes."""

from __future__ import annotations

import time

from fastapi import APIRouter
from sqlalchemy import text

from app import __version__
from app.api.deps import QueueDep, S3Dep, SessionDep
from app.config import settings
from app.schemas.common import ComponentHealth, HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse, summary="Liveness probe")
async def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        version=__version__,
        environment=settings.environment,
    )


@router.get("/health/ready", response_model=HealthResponse, summary="Readiness probe")
async def readiness(session: SessionDep, queue: QueueDep, s3: S3Dep) -> HealthResponse:
    components: list[ComponentHealth] = []

    started = time.perf_counter()
    try:
        await session.execute(text("SELECT 1"))
        components.append(
            ComponentHealth(
                name="postgres",
                healthy=True,
                latency_ms=round((time.perf_counter() - started) * 1000, 2),
            )
        )
    except Exception as exc:
        components.append(ComponentHealth(name="postgres", healthy=False, detail=str(exc)))

    started = time.perf_counter()
    try:
        await queue.ping()
        depth = await queue.depth()
        components.append(
            ComponentHealth(
                name="redis",
                healthy=True,
                detail=f"pending={depth['pending']} processing={depth['processing']}",
                latency_ms=round((time.perf_counter() - started) * 1000, 2),
            )
        )
    except Exception as exc:
        components.append(ComponentHealth(name="redis", healthy=False, detail=str(exc)))

    started = time.perf_counter()
    try:
        await s3.head_object("__readiness_probe__")
        s3_healthy = True
        detail = None
    except Exception as exc:
        s3_healthy = "not found" in str(exc).lower() or "does not exist" in str(exc).lower()
        detail = None if s3_healthy else str(exc)
    components.append(
        ComponentHealth(
            name="s3",
            healthy=s3_healthy,
            detail=detail,
            latency_ms=round((time.perf_counter() - started) * 1000, 2),
        )
    )

    healthy = all(component.healthy for component in components)
    return HealthResponse(
        status="ok" if healthy else "degraded",
        version=__version__,
        environment=settings.environment,
        components=components,
    )
