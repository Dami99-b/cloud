"""Enumerations persisted as native PostgreSQL enum types."""

from __future__ import annotations

from enum import StrEnum


class FileStatus(StrEnum):
    """Lifecycle of a single logical file record."""

    PENDING = "PENDING"
    """Intent recorded, presigned URLs issued, no bytes committed yet."""

    UPLOADING = "UPLOADING"
    """Client is pushing bytes straight to S3."""

    PROCESSING = "PROCESSING"
    """Bytes committed to S3; the async pipeline owns the record."""

    READY = "READY"
    """Hashed, deduplicated, metadata extracted - safe to serve."""

    FAILED = "FAILED"
    """Upload aborted or the pipeline gave up after all retries."""

    DELETED = "DELETED"
    """Soft-deleted; blob refcount already decremented."""


class UploadType(StrEnum):
    DIRECT = "DIRECT"
    MULTIPART = "MULTIPART"


class JobName(StrEnum):
    """Queue job names. Values are part of the wire contract with the worker."""

    FILE_UPLOADED = "file:uploaded"
    FILE_METADATA = "file:metadata"


FILE_STATUS_ENUM_NAME = "file_status"
UPLOAD_TYPE_ENUM_NAME = "upload_type"

TERMINAL_STATUSES = frozenset({FileStatus.READY, FileStatus.FAILED, FileStatus.DELETED})
VISIBLE_STATUSES = frozenset(
    {
        FileStatus.PENDING,
        FileStatus.UPLOADING,
        FileStatus.PROCESSING,
        FileStatus.READY,
        FileStatus.FAILED,
    }
)
