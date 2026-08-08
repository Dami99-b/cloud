"""Job handlers, keyed by the job names on the wire."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.enums import JobName
from app.services.queue import JobQueue
from app.services.s3 import S3Service
from app.worker.jobs import dedup, metadata

JobHandler = Callable[[AsyncSession, S3Service, JobQueue, dict[str, Any]], Awaitable[None]]

HANDLERS: dict[str, JobHandler] = {
    JobName.FILE_UPLOADED.value: dedup.run,
    JobName.FILE_METADATA.value: metadata.run,
}

__all__ = ["HANDLERS", "JobHandler", "dedup", "metadata"]
