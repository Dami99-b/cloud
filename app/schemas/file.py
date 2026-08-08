"""File + upload orchestration schemas."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.models.enums import FileStatus, UploadType


class UploadIntentRequest(BaseModel):
    """Client declares what it is about to upload; server picks the strategy."""

    name: str = Field(..., min_length=1, max_length=512)
    size: int = Field(..., ge=0, description="Exact byte length of the payload")
    mime_type: str | None = Field(default=None, max_length=255)
    folder_id: uuid.UUID | None = Field(
        default=None,
        description="Defaults to the owner's root folder.",
    )

    @field_validator("name")
    @classmethod
    def _clean_name(cls, value: str) -> str:
        cleaned = value.strip().replace("\\", "/").split("/")[-1]
        if not cleaned or cleaned in {".", ".."}:
            raise ValueError("name must be a valid file name")
        return cleaned

    @field_validator("mime_type")
    @classmethod
    def _clean_mime(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None


class DirectUploadTarget(BaseModel):
    """Single presigned PUT for payloads below the multipart threshold."""

    url: str
    method: str = "PUT"
    headers: dict[str, str] = Field(default_factory=dict)


class PartTarget(BaseModel):
    part_number: int = Field(..., ge=1, le=10_000)
    url: str
    offset: int = Field(..., ge=0, description="Byte offset of this chunk in the source file")
    size: int = Field(..., ge=1, description="Byte length of this chunk")


class MultipartUploadTarget(BaseModel):
    upload_id: str
    part_count: int
    chunk_size: int
    parts: list[PartTarget]


class UploadIntentResponse(BaseModel):
    file_id: uuid.UUID
    upload_type: UploadType
    storage_key: str
    folder_id: uuid.UUID
    expires_in: int
    multipart_threshold: int
    direct: DirectUploadTarget | None = None
    multipart: MultipartUploadTarget | None = None

    @model_validator(mode="after")
    def _exactly_one_target(self) -> UploadIntentResponse:
        if (self.direct is None) == (self.multipart is None):
            raise ValueError("exactly one of direct/multipart must be set")
        return self


class CompletedPart(BaseModel):
    part_number: int = Field(..., ge=1, le=10_000)
    etag: str = Field(..., min_length=1, max_length=255)

    @field_validator("etag")
    @classmethod
    def _normalise_etag(cls, value: str) -> str:
        return value.strip().strip('"')


class CompleteUploadRequest(BaseModel):
    file_id: uuid.UUID
    parts: list[CompletedPart] = Field(
        default_factory=list,
        description="Required for multipart uploads; ignored for direct uploads.",
    )

    @field_validator("parts")
    @classmethod
    def _unique_part_numbers(cls, value: list[CompletedPart]) -> list[CompletedPart]:
        numbers = [part.part_number for part in value]
        if len(numbers) != len(set(numbers)):
            raise ValueError("part_number values must be unique")
        return sorted(value, key=lambda part: part.part_number)


class FileResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    mime_type: str
    size_bytes: int
    status: FileStatus
    upload_type: UploadType
    folder_id: uuid.UUID
    folder_path: str | None = None
    storage_key: str
    checksum_sha256: str | None = None
    is_duplicate: bool
    metadata: dict[str, Any] = Field(default_factory=dict)
    error_message: str | None = None
    created_at: datetime
    updated_at: datetime
    uploaded_at: datetime | None = None
    processed_at: datetime | None = None

    @classmethod
    def from_model(cls, file: Any) -> FileResponse:
        return cls(
            id=file.id,
            name=file.name,
            mime_type=file.mime_type,
            size_bytes=file.size_bytes,
            status=file.status,
            upload_type=file.upload_type,
            folder_id=file.folder_id,
            folder_path=getattr(file.folder, "path", None),
            storage_key=file.storage_key,
            checksum_sha256=file.checksum_sha256,
            is_duplicate=file.is_duplicate,
            metadata=file.file_metadata or {},
            error_message=file.error_message,
            created_at=file.created_at,
            updated_at=file.updated_at,
            uploaded_at=file.uploaded_at,
            processed_at=file.processed_at,
        )


class FileListResponse(BaseModel):
    items: list[FileResponse]
    total: int
    limit: int
    offset: int


class DownloadResponse(BaseModel):
    file_id: uuid.UUID
    url: str
    expires_in: int


class CompleteUploadResponse(BaseModel):
    file: FileResponse
    job_enqueued: bool
