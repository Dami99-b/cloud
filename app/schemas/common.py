"""Shared response envelopes."""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    items: list[T]
    total: int = Field(..., description="Total rows matching the filter, ignoring pagination")
    limit: int
    offset: int

    @property
    def has_more(self) -> bool:
        return self.offset + len(self.items) < self.total


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorEnvelope(BaseModel):
    error: ErrorDetail


class ComponentHealth(BaseModel):
    name: str
    healthy: bool
    detail: str | None = None
    latency_ms: float | None = None


class HealthResponse(BaseModel):
    status: str
    version: str
    environment: str
    components: list[ComponentHealth] = Field(default_factory=list)


class StatsResponse(BaseModel):
    """Storage accounting, including what deduplication actually saved."""

    model_config = ConfigDict(from_attributes=True)

    total_files: int
    files_by_status: dict[str, int]
    logical_bytes: int = Field(..., description="Sum of every live file's size")
    physical_bytes: int = Field(..., description="Bytes actually stored in S3 after dedup")
    deduplicated_bytes: int = Field(..., description="logical_bytes - physical_bytes")
    deduplicated_files: int
    unique_blobs: int

    @property
    def dedup_ratio(self) -> float:
        return 0.0 if not self.logical_bytes else self.deduplicated_bytes / self.logical_bytes
