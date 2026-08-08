"""Job 2 - metadata extraction, then flip the file to READY.

Deliberately dependency-free: category and format facts are derived from the
MIME type, the name and a small header probe (PNG/JPEG/GIF dimensions), so the
worker image needs no native imaging libraries. The probe reads at most a few
hundred bytes via a ranged read, never the whole object.
"""

from __future__ import annotations

import struct
import uuid
from datetime import UTC, datetime
from pathlib import PurePosixPath

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import NotFoundError, PermanentJobError, RetryableJobError, StorageError
from app.core.logging import get_logger
from app.models.enums import FileStatus
from app.models.file import File
from app.services.queue import JobQueue
from app.services.s3 import S3Service

logger = get_logger(__name__)

HEADER_PROBE_BYTES = 64

_CATEGORIES = (
    ("image/", "image"),
    ("video/", "video"),
    ("audio/", "audio"),
    ("text/", "text"),
    ("application/pdf", "document"),
    ("application/zip", "archive"),
    ("application/x-tar", "archive"),
    ("application/gzip", "archive"),
    ("application/json", "data"),
)


def categorise(mime_type: str) -> str:
    lowered = mime_type.lower()
    for prefix, category in _CATEGORIES:
        if lowered.startswith(prefix):
            return category
    return "binary"


def probe_image_dimensions(header: bytes) -> tuple[int, int] | None:
    """Extract (width, height) from a PNG, GIF or JPEG header.

    Returns None for anything else - callers treat dimensions as optional.
    """
    if len(header) >= 24 and header.startswith(b"\x89PNG\r\n\x1a\n"):
        width, height = struct.unpack(">II", header[16:24])
        return int(width), int(height)

    if len(header) >= 10 and header[:6] in (b"GIF87a", b"GIF89a"):
        width, height = struct.unpack("<HH", header[6:10])
        return int(width), int(height)

    if header.startswith(b"\xff\xd8"):
        return None

    return None


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

    if file.status is FileStatus.READY:
        return
    if file.status is FileStatus.DELETED:
        logger.info("skipping metadata for deleted file", extra={"file_id": str(file.id)})
        return
    if file.status is not FileStatus.PROCESSING:
        raise PermanentJobError(f"file {file_id} is in state {file.status}, expected PROCESSING")

    suffix = PurePosixPath(file.name).suffix.lower().lstrip(".")
    metadata: dict[str, object] = {
        **(file.file_metadata or {}),
        "category": categorise(file.mime_type),
        "extension": suffix or None,
        "size_bytes": file.size_bytes,
        "content_type": file.mime_type,
        "upload_type": str(file.upload_type),
        "etag": file.etag,
    }

    try:
        stat = await s3.head_object(file.storage_key)
        metadata["stored_content_type"] = stat.content_type
        metadata["stored_size_bytes"] = stat.size
    except NotFoundError as exc:
        file.status = FileStatus.FAILED
        file.error_message = "object disappeared before metadata extraction"
        await session.commit()
        raise PermanentJobError(str(exc)) from exc
    except StorageError as exc:
        raise RetryableJobError(f"head_object failed: {exc}") from exc

    if metadata["category"] == "image" and file.size_bytes >= HEADER_PROBE_BYTES:
        try:
            header = await s3.get_object_range(file.storage_key, 0, HEADER_PROBE_BYTES - 1)
            dimensions = probe_image_dimensions(header)
            if dimensions is not None:
                metadata["width"], metadata["height"] = dimensions
        except (StorageError, NotFoundError) as exc:
            logger.warning("image probe failed", extra={"file_id": str(file.id), "error": str(exc)})

    metadata["processed_at"] = datetime.now(UTC).isoformat()

    file.file_metadata = metadata
    file.status = FileStatus.READY
    file.processed_at = datetime.now(UTC)
    file.error_message = None
    await session.commit()

    logger.info(
        "file ready",
        extra={
            "file_id": str(file.id),
            "category": metadata["category"],
            "duplicate": file.is_duplicate,
        },
    )
