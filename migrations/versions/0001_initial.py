"""initial schema: ltree folders, files, storage blobs

Revision ID: 0001_initial
Revises:
Create Date: 2026-08-08

Creates the `ltree` extension, the native enum types, the three core tables and
the GiST index that makes sub-tree reads sub-millisecond.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from app.db.types import LtreeType

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

FILE_STATUSES = ("PENDING", "UPLOADING", "PROCESSING", "READY", "FAILED", "DELETED")
UPLOAD_TYPES = ("DIRECT", "MULTIPART")


def upgrade() -> None:
    bind = op.get_bind()

    op.execute("CREATE EXTENSION IF NOT EXISTS ltree")

    file_status = postgresql.ENUM(*FILE_STATUSES, name="file_status", create_type=False)
    upload_type = postgresql.ENUM(*UPLOAD_TYPES, name="upload_type", create_type=False)
    file_status.create(bind, checkfirst=True)
    upload_type.create(bind, checkfirst=True)

    op.create_table(
        "folders",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("owner_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("parent_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("label", sa.String(length=64), nullable=False),
        sa.Column("path", LtreeType(), nullable=False),
        sa.Column("depth", sa.Integer(), nullable=False),
        sa.Column("is_root", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name="pk_folders"),
        sa.ForeignKeyConstraint(
            ["parent_id"],
            ["folders.id"],
            name="fk_folders_parent_id_folders",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("owner_id", "path", name="uq_folders_owner_id_path"),
        sa.UniqueConstraint("parent_id", "label", name="uq_folders_parent_id_label"),
    )
    op.create_index("ix_folders_path_gist", "folders", ["path"], postgresql_using="gist")
    op.create_index("ix_folders_owner_id_parent_id", "folders", ["owner_id", "parent_id"])
    op.create_index(
        "uq_folders_owner_root",
        "folders",
        ["owner_id"],
        unique=True,
        postgresql_where=sa.text("is_root"),
    )

    op.create_table(
        "storage_blobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("storage_key", sa.String(length=1024), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("ref_count", sa.BigInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name="pk_storage_blobs"),
        sa.UniqueConstraint("sha256", name="uq_storage_blobs_sha256"),
        sa.CheckConstraint("ref_count >= 0", name="ck_storage_blobs_ref_count_non_negative"),
        sa.CheckConstraint("size_bytes >= 0", name="ck_storage_blobs_size_bytes_non_negative"),
        sa.CheckConstraint("char_length(sha256) = 64", name="ck_storage_blobs_sha256_length"),
    )
    op.create_index("ix_storage_blobs_storage_key", "storage_blobs", ["storage_key"])

    op.create_table(
        "files",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("owner_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("folder_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("blob_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("name", sa.String(length=512), nullable=False),
        sa.Column("mime_type", sa.String(length=255), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("status", file_status, nullable=False, server_default="PENDING"),
        sa.Column("upload_type", upload_type, nullable=False, server_default="DIRECT"),
        sa.Column("storage_key", sa.String(length=1024), nullable=False),
        sa.Column("original_storage_key", sa.String(length=1024), nullable=False),
        sa.Column("upload_id", sa.String(length=512), nullable=True),
        sa.Column("part_count", sa.Integer(), nullable=True),
        sa.Column("etag", sa.String(length=255), nullable=True),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=True),
        sa.Column(
            "is_duplicate",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "processing_attempts",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("uploaded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name="pk_files"),
        sa.ForeignKeyConstraint(
            ["folder_id"],
            ["folders.id"],
            name="fk_files_folder_id_folders",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["blob_id"],
            ["storage_blobs.id"],
            name="fk_files_blob_id_storage_blobs",
            ondelete="SET NULL",
        ),
        sa.CheckConstraint("size_bytes >= 0", name="ck_files_size_bytes_non_negative"),
    )
    op.create_index("ix_files_owner_id_status", "files", ["owner_id", "status"])
    op.create_index("ix_files_folder_id", "files", ["folder_id"])
    op.create_index("ix_files_checksum_sha256", "files", ["checksum_sha256"])
    op.create_index("ix_files_storage_key", "files", ["storage_key"])
    op.create_index("ix_files_created_at", "files", ["created_at"])


def downgrade() -> None:
    bind = op.get_bind()

    op.drop_index("ix_files_created_at", table_name="files")
    op.drop_index("ix_files_storage_key", table_name="files")
    op.drop_index("ix_files_checksum_sha256", table_name="files")
    op.drop_index("ix_files_folder_id", table_name="files")
    op.drop_index("ix_files_owner_id_status", table_name="files")
    op.drop_table("files")

    op.drop_index("ix_storage_blobs_storage_key", table_name="storage_blobs")
    op.drop_table("storage_blobs")

    op.drop_index("uq_folders_owner_root", table_name="folders")
    op.drop_index("ix_folders_owner_id_parent_id", table_name="folders")
    op.drop_index("ix_folders_path_gist", table_name="folders")
    op.drop_table("folders")

    postgresql.ENUM(name="upload_type").drop(bind, checkfirst=True)
    postgresql.ENUM(name="file_status").drop(bind, checkfirst=True)
