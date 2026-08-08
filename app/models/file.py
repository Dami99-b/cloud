"""The logical file record - the unit the API and the UI talk about."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy import (
    Enum as SAEnum,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from app.models.enums import (
    FILE_STATUS_ENUM_NAME,
    UPLOAD_TYPE_ENUM_NAME,
    FileStatus,
    UploadType,
)

if TYPE_CHECKING:
    from app.models.blob import StorageBlob
    from app.models.folder import Folder


def _pg_enum(enum_cls: type, name: str) -> SAEnum:
    return SAEnum(
        enum_cls,
        name=name,
        native_enum=True,
        create_type=False,
        values_callable=lambda members: [member.value for member in members],
        validate_strings=True,
    )


class File(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """A user-visible file.

    ``storage_key`` is the S3 key the bytes actually live under. After
    deduplication it may point at a blob uploaded by a *different* file record,
    which is exactly what ``is_duplicate`` flags.
    """

    __tablename__ = "files"

    owner_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    folder_id: Mapped[uuid.UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("folders.id", ondelete="CASCADE"),
        nullable=False,
    )
    blob_id: Mapped[uuid.UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("storage_blobs.id", ondelete="SET NULL"),
        nullable=True,
    )

    name: Mapped[str] = mapped_column(String(512), nullable=False)
    mime_type: Mapped[str] = mapped_column(String(255), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)

    status: Mapped[FileStatus] = mapped_column(
        _pg_enum(FileStatus, FILE_STATUS_ENUM_NAME),
        nullable=False,
        default=FileStatus.PENDING,
    )
    upload_type: Mapped[UploadType] = mapped_column(
        _pg_enum(UploadType, UPLOAD_TYPE_ENUM_NAME),
        nullable=False,
        default=UploadType.DIRECT,
    )

    storage_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    original_storage_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    upload_id: Mapped[str | None] = mapped_column(String(512), nullable=True)
    part_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    etag: Mapped[str | None] = mapped_column(String(255), nullable=True)

    checksum_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_duplicate: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    file_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        default=dict,
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    processing_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    uploaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    folder: Mapped[Folder] = relationship(back_populates="files", lazy="joined")
    blob: Mapped[StorageBlob | None] = relationship(lazy="selectin")

    __table_args__ = (
        CheckConstraint("size_bytes >= 0", name="size_bytes_non_negative"),
        Index("ix_files_owner_id_status", "owner_id", "status"),
        Index("ix_files_folder_id", "folder_id"),
        Index("ix_files_checksum_sha256", "checksum_sha256"),
        Index("ix_files_storage_key", "storage_key"),
        Index("ix_files_created_at", "created_at"),
    )

    @property
    def is_deleted(self) -> bool:
        return self.status is FileStatus.DELETED or self.deleted_at is not None

    def __repr__(self) -> str:
        return f"<File id={self.id} name={self.name!r} status={self.status}>"
