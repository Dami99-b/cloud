"""Application settings, resolved once from the environment."""

from __future__ import annotations

import uuid
from functools import lru_cache
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

KIB = 1024
MIB = 1024 * KIB
GIB = 1024 * MIB

S3_MIN_PART_SIZE = 5 * MIB
S3_MAX_PARTS = 10_000


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_name: str = "Distributed Cloud File Storage Engine"
    environment: Literal["local", "ci", "staging", "production"] = "local"
    log_level: str = "INFO"
    api_v1_prefix: str = "/api/v1"
    cors_origins: list[str] = Field(default_factory=lambda: ["*"])

    database_url: str = "postgresql+asyncpg://filestore:filestore@postgres:5432/filestore"
    db_pool_size: int = 10
    db_max_overflow: int = 20
    db_pool_timeout: int = 30
    db_echo: bool = False

    redis_url: str = "redis://redis:6379/0"
    queue_key: str = "filestore:jobs"
    processing_key: str = "filestore:jobs:processing"
    delayed_key: str = "filestore:jobs:delayed"
    dead_letter_key: str = "filestore:jobs:dead"

    s3_endpoint_url: str | None = "http://localstack:4566"
    s3_public_endpoint_url: str | None = "http://localhost:4566"
    s3_bucket: str = "user-uploads"
    s3_region: str = "us-east-1"
    s3_force_path_style: bool = True
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"

    multipart_threshold_bytes: int = 50 * MIB
    multipart_chunk_size_bytes: int = 5 * MIB
    presign_expiry_seconds: int = 3600
    max_file_size_bytes: int = 100 * GIB

    worker_concurrency: int = 4
    worker_block_timeout_seconds: int = 5
    job_max_attempts: int = 5
    job_backoff_base_seconds: float = 2.0
    job_backoff_max_seconds: float = 300.0
    hash_stream_chunk_bytes: int = 8 * MIB

    default_owner_id: uuid.UUID = uuid.UUID("00000000-0000-0000-0000-000000000001")

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _split_origins(cls, value: Any) -> Any:
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("["):
                return value
            return [item.strip() for item in stripped.split(",") if item.strip()]
        return value

    @field_validator("database_url")
    @classmethod
    def _require_async_driver(cls, value: str) -> str:
        if value.startswith("postgresql://"):
            return value.replace("postgresql://", "postgresql+asyncpg://", 1)
        if not value.startswith("postgresql+asyncpg://"):
            raise ValueError("database_url must use the postgresql+asyncpg driver")
        return value

    @field_validator("s3_endpoint_url", "s3_public_endpoint_url")
    @classmethod
    def _normalise_endpoint(cls, value: str | None) -> str | None:
        if value is None or not value.strip():
            return None
        return value.rstrip("/")

    @model_validator(mode="after")
    def _validate_upload_tuning(self) -> Settings:
        if self.multipart_chunk_size_bytes < S3_MIN_PART_SIZE:
            raise ValueError(
                f"multipart_chunk_size_bytes must be >= {S3_MIN_PART_SIZE} (S3 minimum part size)"
            )
        if self.multipart_threshold_bytes < self.multipart_chunk_size_bytes:
            raise ValueError("multipart_threshold_bytes must be >= multipart_chunk_size_bytes")
        if self.presign_expiry_seconds < 60 or self.presign_expiry_seconds > 604_800:
            raise ValueError("presign_expiry_seconds must be between 60 and 604800")
        return self

    @property
    def presign_endpoint_url(self) -> str | None:
        """Endpoint whose host is signed into browser-facing URLs."""
        return self.s3_public_endpoint_url or self.s3_endpoint_url

    @property
    def is_local_stack(self) -> bool:
        return self.s3_endpoint_url is not None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
