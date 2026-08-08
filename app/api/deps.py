"""Shared FastAPI dependencies."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import Depends, Header
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.core.errors import BadRequestError
from app.db.session import get_session
from app.services.files import FileService
from app.services.folders import FolderService
from app.services.queue import JobQueue, get_queue
from app.services.s3 import S3Service, get_s3

SessionDep = Annotated[AsyncSession, Depends(get_session)]


async def resolve_owner_id(
    x_user_id: Annotated[str | None, Header(alias="X-User-Id")] = None,
) -> uuid.UUID:
    """Resolve the calling principal.

    Deliberately minimal: a header-supplied UUID with a configured fallback so
    the demo UI works with no auth. Replace this one function with JWT/session
    verification and every route becomes multi-tenant unchanged.
    """
    if x_user_id is None or not x_user_id.strip():
        return settings.default_owner_id
    try:
        return uuid.UUID(x_user_id.strip())
    except ValueError as exc:
        raise BadRequestError("X-User-Id must be a UUID") from exc


OwnerDep = Annotated[uuid.UUID, Depends(resolve_owner_id)]


def get_s3_service() -> S3Service:
    return get_s3()


def get_job_queue() -> JobQueue:
    return get_queue()


S3Dep = Annotated[S3Service, Depends(get_s3_service)]
QueueDep = Annotated[JobQueue, Depends(get_job_queue)]


async def get_file_service(
    session: SessionDep,
    s3: S3Dep,
    queue: QueueDep,
) -> FileService:
    return FileService(session, s3, queue)


async def get_folder_service(session: SessionDep) -> FolderService:
    return FolderService(session)


FileServiceDep = Annotated[FileService, Depends(get_file_service)]
FolderServiceDep = Annotated[FolderService, Depends(get_folder_service)]
