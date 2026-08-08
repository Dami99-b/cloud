"""Folder request/response schemas."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator


class FolderCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    parent_id: uuid.UUID | None = Field(
        default=None,
        description="Defaults to the owner's root folder.",
    )

    @field_validator("name")
    @classmethod
    def _clean_name(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("name must not be blank")
        if "/" in cleaned or "\\" in cleaned:
            raise ValueError("name must not contain path separators")
        return cleaned


class FolderUpdateRequest(BaseModel):
    """Rename and/or re-parent. Both operations rewrite the ltree sub-tree."""

    name: str | None = Field(default=None, min_length=1, max_length=255)
    parent_id: uuid.UUID | None = None

    @field_validator("name")
    @classmethod
    def _clean_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("name must not be blank")
        if "/" in cleaned or "\\" in cleaned:
            raise ValueError("name must not contain path separators")
        return cleaned


class FolderResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    label: str
    path: str
    depth: int
    parent_id: uuid.UUID | None
    is_root: bool
    created_at: datetime
    updated_at: datetime


class FolderNode(FolderResponse):
    """A folder plus its descendants, nested for the UI tree."""

    children: list[FolderNode] = Field(default_factory=list)
    file_count: int = 0


FolderNode.model_rebuild()


class FolderListResponse(BaseModel):
    items: list[FolderResponse]
    total: int


class FolderDeleteResponse(BaseModel):
    deleted_folder_ids: list[uuid.UUID]
    deleted_file_ids: list[uuid.UUID]
    released_objects: int = Field(
        default=0,
        description="S3 objects physically removed because their last reference went away.",
    )
