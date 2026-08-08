"""File lifecycle: upload intent, completion, listing, deletion, accounting."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import String, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.core.errors import (
    BadRequestError,
    ConflictError,
    NotFoundError,
    PayloadTooLargeError,
)
from app.core.logging import get_logger
from app.models.blob import StorageBlob
from app.models.enums import FileStatus, JobName, UploadType
from app.models.file import File
from app.models.folder import Folder
from app.schemas.file import (
    CompletedPart,
    DirectUploadTarget,
    MultipartUploadTarget,
    PartTarget,
    UploadIntentRequest,
    UploadIntentResponse,
)
from app.services.folders import FolderService
from app.services.queue import JobQueue
from app.services.s3 import S3Service, guess_mime_type, plan_parts

logger = get_logger(__name__)

_UNSAFE_KEY_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(slots=True)
class DeletionResult:
    file_ids: list[uuid.UUID]
    released_objects: int


def sanitise_key_component(name: str) -> str:
    """Make a filename safe for an S3 key without losing readability."""
    cleaned = _UNSAFE_KEY_CHARS.sub("_", name.strip()).strip("._-")
    return (cleaned or "file")[:180]


def build_storage_key(owner_id: uuid.UUID, file_id: uuid.UUID, filename: str) -> str:
    """Date-sharded, collision-free object key.

    The date prefix keeps S3 listings and lifecycle rules manageable; the file
    UUID guarantees uniqueness even for identical names uploaded concurrently.
    """
    now = datetime.now(UTC)
    return f"{owner_id}/{now:%Y/%m/%d}/{file_id}/{sanitise_key_component(filename)}"


class FileService:
    def __init__(self, session: AsyncSession, s3: S3Service, queue: JobQueue) -> None:
        self.session = session
        self.s3 = s3
        self.queue = queue
        self.folders = FolderService(session)

    async def get(
        self,
        file_id: uuid.UUID,
        owner_id: uuid.UUID,
        *,
        include_deleted: bool = False,
    ) -> File:
        stmt = select(File).where(File.id == file_id, File.owner_id == owner_id)
        if not include_deleted:
            stmt = stmt.where(File.status != FileStatus.DELETED)
        file = await self.session.scalar(stmt)
        if file is None:
            raise NotFoundError("file not found", details={"file_id": str(file_id)})
        return file

    async def list(
        self,
        owner_id: uuid.UUID,
        *,
        folder_id: uuid.UUID | None = None,
        recursive: bool = False,
        status: FileStatus | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[File], int]:
        """Paginated listing. ``recursive`` walks the ltree sub-tree."""
        stmt = select(File).where(File.owner_id == owner_id, File.status != FileStatus.DELETED)
        count_stmt = select(func.count(File.id)).where(
            File.owner_id == owner_id, File.status != FileStatus.DELETED
        )

        if folder_id is not None:
            folder = await self.folders.get(folder_id, owner_id)
            if recursive:
                subtree = (
                    select(Folder.id)
                    .where(
                        Folder.owner_id == owner_id,
                        Folder.path.descendant_of(folder.path),
                    )
                    .scalar_subquery()
                )
                stmt = stmt.where(File.folder_id.in_(subtree))
                count_stmt = count_stmt.where(File.folder_id.in_(subtree))
            else:
                stmt = stmt.where(File.folder_id == folder.id)
                count_stmt = count_stmt.where(File.folder_id == folder.id)

        if status is not None:
            stmt = stmt.where(File.status == status)
            count_stmt = count_stmt.where(File.status == status)

        if search:
            pattern = f"%{search.lower()}%"
            stmt = stmt.where(func.lower(File.name).like(pattern))
            count_stmt = count_stmt.where(func.lower(File.name).like(pattern))

        total = int(await self.session.scalar(count_stmt) or 0)
        stmt = (
            stmt.order_by(File.created_at.desc(), cast(File.id, String)).limit(limit).offset(offset)
        )
        files = list((await self.session.scalars(stmt)).unique().all())
        return files, total

    async def create_upload_intent(
        self,
        owner_id: uuid.UUID,
        request: UploadIntentRequest,
    ) -> tuple[File, UploadIntentResponse]:
        if request.size <= 0:
            raise BadRequestError("size must be greater than zero")
        if request.size > settings.max_file_size_bytes:
            raise PayloadTooLargeError(
                "file exceeds the maximum allowed size",
                details={"max_bytes": settings.max_file_size_bytes, "size": request.size},
            )

        folder = await self.folders.resolve(owner_id, request.folder_id)
        file_id = uuid.uuid4()
        storage_key = build_storage_key(owner_id, file_id, request.name)
        mime_type = request.mime_type or guess_mime_type(request.name)

        file = File(
            id=file_id,
            owner_id=owner_id,
            folder_id=folder.id,
            name=request.name,
            mime_type=mime_type,
            size_bytes=request.size,
            storage_key=storage_key,
            original_storage_key=storage_key,
            status=FileStatus.UPLOADING,
            file_metadata={},
        )

        if request.size < settings.multipart_threshold_bytes:
            file.upload_type = UploadType.DIRECT
            target = DirectUploadTarget(
                url=self.s3.presign_put_object(storage_key, content_type=mime_type),
                method="PUT",
                headers={"Content-Type": mime_type},
            )
            response = UploadIntentResponse(
                file_id=file_id,
                upload_type=UploadType.DIRECT,
                storage_key=storage_key,
                folder_id=folder.id,
                expires_in=settings.presign_expiry_seconds,
                multipart_threshold=settings.multipart_threshold_bytes,
                direct=target,
            )
        else:
            try:
                plans = plan_parts(request.size, settings.multipart_chunk_size_bytes)
            except ValueError as exc:
                raise BadRequestError(str(exc)) from exc

            upload_id = await self.s3.create_multipart_upload(storage_key, content_type=mime_type)
            file.upload_type = UploadType.MULTIPART
            file.upload_id = upload_id
            file.part_count = len(plans)
            response = UploadIntentResponse(
                file_id=file_id,
                upload_type=UploadType.MULTIPART,
                storage_key=storage_key,
                folder_id=folder.id,
                expires_in=settings.presign_expiry_seconds,
                multipart_threshold=settings.multipart_threshold_bytes,
                multipart=MultipartUploadTarget(
                    upload_id=upload_id,
                    part_count=len(plans),
                    chunk_size=settings.multipart_chunk_size_bytes,
                    parts=[
                        PartTarget(
                            part_number=plan.part_number,
                            url=self.s3.presign_upload_part(
                                storage_key,
                                upload_id=upload_id,
                                part_number=plan.part_number,
                            ),
                            offset=plan.offset,
                            size=plan.size,
                        )
                        for plan in plans
                    ],
                ),
            )

        self.session.add(file)
        await self.session.commit()
        await self.session.refresh(file)
        logger.info(
            "upload intent created",
            extra={
                "file_id": str(file.id),
                "upload_type": str(file.upload_type),
                "size_bytes": file.size_bytes,
                "parts": file.part_count,
            },
        )
        return file, response

    async def complete_upload(
        self,
        owner_id: uuid.UUID,
        file_id: uuid.UUID,
        parts: list[CompletedPart],
    ) -> tuple[File, bool]:
        """Finalise the S3 object, flip to PROCESSING, enqueue the pipeline."""
        file = await self.get(file_id, owner_id)

        if file.status in {FileStatus.PROCESSING, FileStatus.READY}:
            return file, False
        if file.status is FileStatus.FAILED:
            raise ConflictError(
                "upload already failed; create a new upload intent",
                details={"file_id": str(file.id)},
            )
        if file.status is not FileStatus.UPLOADING:
            raise ConflictError(
                f"cannot complete an upload in state {file.status}",
                details={"status": str(file.status)},
            )

        if file.upload_type is UploadType.MULTIPART:
            if not file.upload_id:
                raise ConflictError("multipart upload id missing from the record")
            if not parts:
                raise BadRequestError("parts are required to complete a multipart upload")
            expected = file.part_count or 0
            invalid = [p.part_number for p in parts if p.part_number > expected]
            if invalid:
                raise BadRequestError(
                    "part_number out of range",
                    details={"invalid": invalid, "part_count": expected},
                )
            file.etag = await self.s3.complete_multipart_upload(
                file.storage_key,
                upload_id=file.upload_id,
                parts=[(p.part_number, p.etag) for p in parts],
            )
        else:
            try:
                stat = await self.s3.head_object(file.storage_key)
            except NotFoundError as exc:
                raise ConflictError(
                    "no object found at the presigned key; upload the bytes first",
                    details={"storage_key": file.storage_key},
                ) from exc
            file.etag = stat.etag

        stat = await self.s3.head_object(file.storage_key)
        if stat.size != file.size_bytes:
            logger.warning(
                "declared size differs from stored object; trusting S3",
                extra={
                    "file_id": str(file.id),
                    "declared": file.size_bytes,
                    "actual": stat.size,
                },
            )
            file.size_bytes = stat.size

        file.status = FileStatus.PROCESSING
        file.uploaded_at = datetime.now(UTC)
        file.upload_id = None
        await self.session.commit()

        await self.queue.enqueue(JobName.FILE_UPLOADED, {"file_id": str(file.id)})
        await self.session.refresh(file)
        logger.info("upload completed", extra={"file_id": str(file.id), "etag": file.etag})
        return file, True

    async def abort_upload(self, owner_id: uuid.UUID, file_id: uuid.UUID) -> File:
        file = await self.get(file_id, owner_id)
        if file.status not in {FileStatus.UPLOADING, FileStatus.PENDING}:
            raise ConflictError(
                f"cannot abort an upload in state {file.status}",
                details={"status": str(file.status)},
            )
        if file.upload_type is UploadType.MULTIPART and file.upload_id:
            await self.s3.abort_multipart_upload(file.storage_key, upload_id=file.upload_id)
        file.status = FileStatus.FAILED
        file.error_message = "upload aborted by client"
        file.upload_id = None
        await self.session.commit()
        await self.session.refresh(file)
        return file

    async def download_url(self, owner_id: uuid.UUID, file_id: uuid.UUID) -> tuple[File, str]:
        file = await self.get(file_id, owner_id)
        if file.status is not FileStatus.READY:
            raise ConflictError(
                "file is not ready for download",
                details={"status": str(file.status)},
            )
        url = self.s3.presign_get_object(file.storage_key, filename=file.name)
        return file, url

    async def _release_storage(self, file: File) -> bool:
        """Drop one reference to the file's bytes.

        Returns True when the physical S3 object was removed, which happens only
        when this was the last reference.
        """
        if file.blob_id is not None:
            blob = await self.session.get(StorageBlob, file.blob_id, with_for_update=True)
            if blob is None:
                return False
            blob.ref_count = max(0, blob.ref_count - 1)
            if blob.ref_count == 0:
                key = blob.storage_key
                await self.session.delete(blob)
                await self.session.flush()
                await self.s3.delete_object(key)
                return True
            return False

        claimed = await self.session.scalar(
            select(StorageBlob.id).where(StorageBlob.storage_key == file.storage_key)
        )
        if claimed is not None:
            return False
        if await self.s3.object_exists(file.storage_key):
            await self.s3.delete_object(file.storage_key)
            return True
        return False

    async def delete(self, owner_id: uuid.UUID, file_id: uuid.UUID) -> DeletionResult:
        file = await self.get(file_id, owner_id)

        if file.upload_type is UploadType.MULTIPART and file.upload_id:
            await self.s3.abort_multipart_upload(file.storage_key, upload_id=file.upload_id)
            file.upload_id = None

        released = await self._release_storage(file)
        file.status = FileStatus.DELETED
        file.deleted_at = datetime.now(UTC)
        file.blob_id = None
        await self.session.commit()
        logger.info(
            "file deleted",
            extra={"file_id": str(file.id), "object_released": released},
        )
        return DeletionResult(file_ids=[file.id], released_objects=int(released))

    async def delete_many(self, owner_id: uuid.UUID, file_ids: list[uuid.UUID]) -> DeletionResult:
        """Used by folder sub-tree deletion; releases every reference safely."""
        if not file_ids:
            return DeletionResult(file_ids=[], released_objects=0)

        files = list(
            (
                await self.session.scalars(
                    select(File).where(File.owner_id == owner_id, File.id.in_(file_ids))
                )
            )
            .unique()
            .all()
        )
        released = 0
        for file in files:
            if file.status is FileStatus.DELETED:
                continue
            if file.upload_type is UploadType.MULTIPART and file.upload_id:
                await self.s3.abort_multipart_upload(file.storage_key, upload_id=file.upload_id)
                file.upload_id = None
            released += int(await self._release_storage(file))
            file.status = FileStatus.DELETED
            file.deleted_at = datetime.now(UTC)
            file.blob_id = None
        await self.session.flush()
        return DeletionResult(file_ids=[f.id for f in files], released_objects=released)

    async def stats(self, owner_id: uuid.UUID) -> dict[str, object]:
        status_rows = (
            await self.session.execute(
                select(
                    File.status,
                    func.count(File.id),
                    func.coalesce(func.sum(File.size_bytes), 0),
                )
                .where(File.owner_id == owner_id, File.status != FileStatus.DELETED)
                .group_by(File.status)
            )
        ).all()

        files_by_status = {str(status): count for status, count, _ in status_rows}
        total_files = sum(count for _, count, _ in status_rows)
        logical_bytes = sum(int(size) for _, _, size in status_rows)

        physical_bytes = int(
            await self.session.scalar(
                select(func.coalesce(func.sum(StorageBlob.size_bytes), 0)).where(
                    StorageBlob.ref_count > 0
                )
            )
            or 0
        )
        unique_blobs = int(
            await self.session.scalar(
                select(func.count(StorageBlob.id)).where(StorageBlob.ref_count > 0)
            )
            or 0
        )
        duplicate_files = int(
            await self.session.scalar(
                select(func.count(File.id)).where(
                    File.owner_id == owner_id,
                    File.is_duplicate.is_(True),
                    File.status != FileStatus.DELETED,
                )
            )
            or 0
        )

        return {
            "total_files": total_files,
            "files_by_status": files_by_status,
            "logical_bytes": logical_bytes,
            "physical_bytes": physical_bytes,
            "deduplicated_bytes": max(0, logical_bytes - physical_bytes),
            "deduplicated_files": duplicate_files,
            "unique_blobs": unique_blobs,
        }
