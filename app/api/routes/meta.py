"""Client bootstrap: upload tuning the SPA needs before its first request."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from app import __version__
from app.api.deps import QueueDep
from app.config import settings
from app.services.queue import JobQueue

router = APIRouter(tags=["meta"])


class ClientConfig(BaseModel):
    version: str
    environment: str
    multipart_threshold_bytes: int
    multipart_chunk_size_bytes: int
    max_file_size_bytes: int
    presign_expiry_seconds: int
    s3_bucket: str


class QueueDepthResponse(BaseModel):
    pending: int
    processing: int
    delayed: int
    dead: int


@router.get("/config", response_model=ClientConfig, summary="Upload tuning for clients")
async def client_config() -> ClientConfig:
    """The browser needs the threshold and chunk size to slice files locally."""
    return ClientConfig(
        version=__version__,
        environment=settings.environment,
        multipart_threshold_bytes=settings.multipart_threshold_bytes,
        multipart_chunk_size_bytes=settings.multipart_chunk_size_bytes,
        max_file_size_bytes=settings.max_file_size_bytes,
        presign_expiry_seconds=settings.presign_expiry_seconds,
        s3_bucket=settings.s3_bucket,
    )


@router.get("/queue", response_model=QueueDepthResponse, summary="Live queue depth")
async def queue_depth(queue: QueueDep) -> QueueDepthResponse:
    depth = await queue.depth()
    return QueueDepthResponse(**depth)


__all__ = ["ClientConfig", "JobQueue", "router"]
