"""Job 1 - streaming SHA-256 deduplication.

Streams the freshly uploaded object out of S3 one chunk at a time, hashes it,
and consults the content-addressed blob ledger:

* **hash already known** - the new record is soft-linked to the existing S3 key,
  the redundant object is deleted, and the blob's refcount goes up.
* **hash is new** - a blob row is created claiming this object as the canonical
  copy for that digest.

Memory stays flat regardless of object size, and the operation is idempotent:
re-running it for the same file converges to the same state.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import NotFoundError, PermanentJobError, RetryableJobError, StorageError
from app.core.logging import get_logger
from app.models.blob import StorageBlob
from app.models.enums import FileStatus, JobName
from app.models.file import File
from app.services.queue import JobQueue
from app.services.s3 import S3Service

logger = get_logger(__name__)


async def _claim_blob(
    session: AsyncSession,
    *,
    sha256: str,
    storage_key: str,
    size_bytes: int,
) -> tuple[StorageBlob, bool]:
    """Insert-or-fetch the blob for this digest.

    ``ON CONFLICT DO NOTHING`` makes two workers hashing identical bytes at the
    same instant safe: exactly one insert wins, the loser reads the winner's row.
    Returns ``(blob, created)``.
    """
    stmt = (
        pg_insert(StorageBlob)
        .values(
            id=uuid.uuid4(),
            sha256=sha256,
            storage_key=storage_key,
            size_bytes=size_bytes,
            ref_count=1,
        )
        .on_conflict_do_nothing(index_elements=[StorageBlob.sha256])
        .returning(StorageBlob.id)
    )
    inserted_id = await session.scalar(stmt)
    if inserted_id is not None:
        blob = await session.get(StorageBlob, inserted_id)
        if blob is None:
            raise RetryableJobError(f"blob row for {sha256[:12]} vanished after insert")
        return blob, True

    blob = await session.scalar(
        select(StorageBlob).where(StorageBlob.sha256 == sha256).with_for_update()
    )
    if blob is None:
        raise RetryableJobError(f"blob row for {sha256[:12]} disappeared during claim")
    return blob, False


async def run(
    session: AsyncSession,
    s3: S3Service,
    queue: JobQueue,
    payload: dict[str, object],
) -> None:
    raw_file_id = payload.get("file_id")
    if not raw_file_id:
        raise PermanentJobError("payload is missing file_id")
    try:
        file_id = uuid.UUID(str(raw_file_id))
    except ValueError as exc:
        raise PermanentJobError(f"file_id is not a UUID: {raw_file_id!r}") from exc

    file = await session.get(File, file_id, with_for_update=True)
    if file is None:
        raise PermanentJobError(f"file {file_id} no longer exists")

    if file.status in {FileStatus.READY, FileStatus.DELETED}:
        logger.info(
            "skipping dedup for terminal file",
            extra={"file_id": str(file.id), "status": str(file.status)},
        )
        return
    if file.status is not FileStatus.PROCESSING:
        raise PermanentJobError(f"file {file_id} is in state {file.status}, expected PROCESSING")

    file.processing_attempts += 1

    try:
        digest, streamed_size = await s3.compute_sha256(file.storage_key)
    except NotFoundError as exc:
        file.status = FileStatus.FAILED
        file.error_message = "uploaded object is missing from object storage"
        raise PermanentJobError(str(exc)) from exc
    except StorageError as exc:
        raise RetryableJobError(f"could not stream object for hashing: {exc}") from exc

    if streamed_size != file.size_bytes:
        logger.warning(
            "hashed byte count differs from recorded size; correcting",
            extra={
                "file_id": str(file.id),
                "recorded": file.size_bytes,
                "streamed": streamed_size,
            },
        )
        file.size_bytes = streamed_size

    blob, created = await _claim_blob(
        session,
        sha256=digest,
        storage_key=file.storage_key,
        size_bytes=streamed_size,
    )

    file.checksum_sha256 = digest
    file.blob_id = blob.id

    if created or blob.storage_key == file.storage_key:
        file.is_duplicate = False
        if not created:
            logger.info(
                "dedup job re-ran against its own canonical blob",
                extra={"file_id": str(file.id), "sha256": digest[:12]},
            )
    else:
        duplicate_key = file.storage_key
        file.storage_key = blob.storage_key
        file.is_duplicate = True
        await session.execute(
            update(StorageBlob)
            .where(StorageBlob.id == blob.id)
            .values(ref_count=StorageBlob.ref_count + 1)
        )
        await session.commit()
        try:
            await s3.delete_object(duplicate_key)
        except StorageError as exc:
            logger.error(
                "duplicate object left behind in S3",
                extra={"key": duplicate_key, "error": str(exc)},
            )
        logger.info(
            "duplicate detected and soft-linked",
            extra={
                "file_id": str(file.id),
                "sha256": digest[:12],
                "canonical_key": blob.storage_key,
                "reclaimed_bytes": streamed_size,
            },
        )

    file.file_metadata = {
        **(file.file_metadata or {}),
        "sha256": digest,
        "hashed_at": datetime.now(UTC).isoformat(),
        "deduplicated": file.is_duplicate,
        "blob_id": str(blob.id),
    }
    await session.commit()

    logger.info(
        "dedup job complete",
        extra={
            "file_id": str(file.id),
            "sha256": digest[:12],
            "duplicate": file.is_duplicate,
        },
    )

    await queue.enqueue(JobName.FILE_METADATA, {"file_id": str(file.id)})
