"""Content-addressed storage blobs - the deduplication ledger."""

from __future__ import annotations

from sqlalchemy import BigInteger, CheckConstraint, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class StorageBlob(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """One row per unique byte-stream in the bucket.

    Many :class:`~app.models.file.File` records may reference the same blob;
    ``ref_count`` tracks how many, so the physical object can be reclaimed
    exactly when the last logical reference disappears.
    """

    __tablename__ = "storage_blobs"

    sha256: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    storage_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ref_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)

    __table_args__ = (
        CheckConstraint("ref_count >= 0", name="ref_count_non_negative"),
        CheckConstraint("size_bytes >= 0", name="size_bytes_non_negative"),
        CheckConstraint("char_length(sha256) = 64", name="sha256_length"),
        Index("ix_storage_blobs_storage_key", "storage_key"),
    )

    def __repr__(self) -> str:
        return f"<StorageBlob sha256={self.sha256[:12]}… refs={self.ref_count}>"
