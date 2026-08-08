"""Hierarchical folders, materialised with a PostgreSQL `ltree` path."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, ForeignKey, Index, String, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from app.db.types import LtreeType

if TYPE_CHECKING:
    from app.models.file import File

ROOT_LABEL = "root"


class Folder(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """A directory node.

    ``path`` is the full materialised ltree path (``root.documents.projects``).
    Sub-tree reads and deletes are single indexed operations against it, so
    depth costs nothing. ``parent_id`` is kept alongside for cheap, unambiguous
    single-level listings and FK integrity.
    """

    __tablename__ = "folders"

    owner_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    parent_id: Mapped[uuid.UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("folders.id", ondelete="CASCADE"),
        nullable=True,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    label: Mapped[str] = mapped_column(String(64), nullable=False)
    path: Mapped[str] = mapped_column(LtreeType(), nullable=False)
    depth: Mapped[int] = mapped_column(nullable=False, default=1)
    is_root: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    files: Mapped[list[File]] = relationship(
        back_populates="folder",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        UniqueConstraint("owner_id", "path", name="uq_folders_owner_id_path"),
        UniqueConstraint("parent_id", "label", name="uq_folders_parent_id_label"),
        Index("ix_folders_path_gist", "path", postgresql_using="gist"),
        Index("ix_folders_owner_id_parent_id", "owner_id", "parent_id"),
        Index(
            "uq_folders_owner_root",
            "owner_id",
            unique=True,
            postgresql_where=text("is_root"),
        ),
    )

    def __repr__(self) -> str:
        return f"<Folder id={self.id} path={self.path!r}>"
