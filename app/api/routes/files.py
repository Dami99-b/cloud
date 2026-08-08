"""File routes: the upload handshake, the explorer feed, downloads, deletes."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Query, Response, status

from app.api.deps import FileServiceDep, OwnerDep
from app.models.enums import FileStatus
from app.schemas.common import StatsResponse
from app.schemas.file import (
    CompleteUploadRequest,
    CompleteUploadResponse,
    DownloadResponse,
    FileListResponse,
    FileResponse,
    UploadIntentRequest,
    UploadIntentResponse,
)

router = APIRouter(prefix="/files", tags=["files"])


@router.post(
    "/upload-intent",
    response_model=UploadIntentResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Reserve a file record and hand back presigned upload targets",
)
async def create_upload_intent(
    payload: UploadIntentRequest,
    owner_id: OwnerDep,
    files: FileServiceDep,
) -> UploadIntentResponse:
    """Below the multipart threshold this returns a single presigned PUT.

    At or above it, an S3 multipart upload is initiated and one presigned URL
    per chunk is returned along with each chunk's byte range, so the browser can
    slice the file locally and upload the parts concurrently.
    """
    _, response = await files.create_upload_intent(owner_id, payload)
    return response


@router.post(
    "/complete-upload",
    response_model=CompleteUploadResponse,
    summary="Finalise the S3 object and hand the file to the async pipeline",
)
async def complete_upload(
    payload: CompleteUploadRequest,
    owner_id: OwnerDep,
    files: FileServiceDep,
) -> CompleteUploadResponse:
    """Idempotent: replaying a completion returns the record without re-queueing."""
    file, enqueued = await files.complete_upload(owner_id, payload.file_id, payload.parts)
    return CompleteUploadResponse(file=FileResponse.from_model(file), job_enqueued=enqueued)


@router.get("", response_model=FileListResponse, summary="List files")
async def list_files(
    owner_id: OwnerDep,
    files: FileServiceDep,
    folder_id: Annotated[uuid.UUID | None, Query()] = None,
    recursive: Annotated[bool, Query(description="Include every descendant folder")] = False,
    status_filter: Annotated[FileStatus | None, Query(alias="status")] = None,
    search: Annotated[str | None, Query(max_length=255)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> FileListResponse:
    items, total = await files.list(
        owner_id,
        folder_id=folder_id,
        recursive=recursive,
        status=status_filter,
        search=search,
        limit=limit,
        offset=offset,
    )
    return FileListResponse(
        items=[FileResponse.from_model(item) for item in items],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/stats", response_model=StatsResponse, summary="Storage + dedup accounting")
async def file_stats(owner_id: OwnerDep, files: FileServiceDep) -> StatsResponse:
    return StatsResponse(**await files.stats(owner_id))  # type: ignore[arg-type]


@router.get("/{file_id}", response_model=FileResponse, summary="Fetch one file")
async def get_file(
    file_id: uuid.UUID,
    owner_id: OwnerDep,
    files: FileServiceDep,
) -> FileResponse:
    return FileResponse.from_model(await files.get(file_id, owner_id))


@router.get(
    "/{file_id}/download",
    response_model=DownloadResponse,
    summary="Presigned GET URL for a READY file",
)
async def download_file(
    file_id: uuid.UUID,
    owner_id: OwnerDep,
    files: FileServiceDep,
) -> DownloadResponse:
    from app.config import settings

    file, url = await files.download_url(owner_id, file_id)
    return DownloadResponse(file_id=file.id, url=url, expires_in=settings.presign_expiry_seconds)


@router.post(
    "/{file_id}/abort",
    response_model=FileResponse,
    summary="Abort an in-flight upload and release the multipart reservation",
)
async def abort_upload(
    file_id: uuid.UUID,
    owner_id: OwnerDep,
    files: FileServiceDep,
) -> FileResponse:
    return FileResponse.from_model(await files.abort_upload(owner_id, file_id))


@router.delete(
    "/{file_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Soft-delete a file and release its blob reference",
)
async def delete_file(
    file_id: uuid.UUID,
    owner_id: OwnerDep,
    files: FileServiceDep,
) -> Response:
    await files.delete(owner_id, file_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
