"""S3 access: presigned URLs, the multipart lifecycle, and streaming reads.

Two clients, deliberately:

* ``_internal`` talks to the endpoint reachable from *inside* the network
  (``http://localstack:4566`` under docker-compose) and performs server-side
  calls - create/complete/abort multipart, head, delete, streaming reads.
* ``_presign`` signs URLs against the endpoint the *browser* can reach
  (``http://localhost:4566``). SigV4 binds the host into the signature, so
  signing with the internal host would produce URLs that fail in the browser.

boto3 is synchronous, so every network call is dispatched to a worker thread.
Presign calls are pure local crypto and stay on the event loop.
"""

from __future__ import annotations

import hashlib
import mimetypes
from dataclasses import dataclass
from functools import partial
from typing import Any

import anyio.to_thread
import boto3
from botocore.client import BaseClient, Config
from botocore.exceptions import BotoCoreError, ClientError

from app.config import S3_MAX_PARTS, settings
from app.core.errors import ConflictError, NotFoundError, StorageError
from app.core.logging import get_logger

logger = get_logger(__name__)

_NOT_FOUND_CODES = {"404", "NoSuchKey", "NoSuchBucket", "NoSuchUpload", "NotFound"}


@dataclass(frozen=True, slots=True)
class PartPlan:
    """One chunk of a multipart upload."""

    part_number: int
    offset: int
    size: int


@dataclass(frozen=True, slots=True)
class ObjectStat:
    key: str
    size: int
    etag: str
    content_type: str | None


def plan_parts(total_size: int, chunk_size: int) -> list[PartPlan]:
    """Split ``total_size`` into S3-legal parts.

    Every part except the last is exactly ``chunk_size``; S3 rejects smaller
    non-final parts. Raises when the plan would exceed S3's 10 000-part cap.
    """
    if total_size <= 0:
        raise ValueError("total_size must be positive")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    part_count = -(-total_size // chunk_size)
    if part_count > S3_MAX_PARTS:
        raise ValueError(
            f"{part_count} parts exceeds the S3 maximum of {S3_MAX_PARTS}; "
            "increase multipart_chunk_size_bytes"
        )

    plans: list[PartPlan] = []
    offset = 0
    for part_number in range(1, part_count + 1):
        size = min(chunk_size, total_size - offset)
        plans.append(PartPlan(part_number=part_number, offset=offset, size=size))
        offset += size
    return plans


def guess_mime_type(filename: str, fallback: str = "application/octet-stream") -> str:
    guessed, _ = mimetypes.guess_type(filename)
    return guessed or fallback


class S3Service:
    def __init__(self, bucket: str | None = None) -> None:
        self.bucket = bucket or settings.s3_bucket
        self._internal: BaseClient | None = None
        self._presign: BaseClient | None = None

    def _build_client(self, endpoint_url: str | None) -> BaseClient:
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=settings.s3_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            config=Config(
                signature_version="s3v4",
                s3={
                    "addressing_style": "path" if settings.s3_force_path_style else "auto",
                },
                retries={"max_attempts": 3, "mode": "standard"},
                connect_timeout=10,
                read_timeout=120,
            ),
        )

    @property
    def client(self) -> BaseClient:
        if self._internal is None:
            self._internal = self._build_client(settings.s3_endpoint_url)
        return self._internal

    @property
    def presign_client(self) -> BaseClient:
        if self._presign is None:
            self._presign = self._build_client(settings.presign_endpoint_url)
        return self._presign

    @staticmethod
    def _error_code(error: ClientError) -> str:
        response = getattr(error, "response", {}) or {}
        code = str(response.get("Error", {}).get("Code", ""))
        status = str(response.get("ResponseMetadata", {}).get("HTTPStatusCode", ""))
        return code or status

    async def _call(self, operation: str, **kwargs: Any) -> dict[str, Any]:
        method = getattr(self.client, operation)
        try:
            return await anyio.to_thread.run_sync(partial(method, **kwargs))
        except ClientError as exc:
            code = self._error_code(exc)
            if code in _NOT_FOUND_CODES:
                raise NotFoundError(
                    f"S3 {operation} failed: object or upload does not exist",
                    details={"code": code, "operation": operation},
                ) from exc
            raise StorageError(
                f"S3 {operation} failed: {exc}",
                details={"code": code, "operation": operation},
            ) from exc
        except BotoCoreError as exc:
            raise StorageError(f"S3 {operation} failed: {exc}") from exc

    async def ensure_bucket(self) -> bool:
        """Create the bucket and its browser CORS policy if missing.

        Idempotent, and safe to run from every process on startup. Returns True
        when the bucket had to be created.
        """
        try:
            await self._call("head_bucket", Bucket=self.bucket)
            created = False
        except (NotFoundError, StorageError):
            create_kwargs: dict[str, Any] = {"Bucket": self.bucket}
            if settings.s3_region != "us-east-1":
                create_kwargs["CreateBucketConfiguration"] = {
                    "LocationConstraint": settings.s3_region
                }
            try:
                await self._call("create_bucket", **create_kwargs)
            except StorageError as exc:
                if "BucketAlreadyOwnedByYou" not in str(exc) and "BucketAlreadyExists" not in str(
                    exc
                ):
                    raise
            created = True

        await self.put_cors_policy()
        logger.info("s3 bucket ready", extra={"bucket": self.bucket, "created": created})
        return created

    async def put_cors_policy(self) -> None:
        """Allow browser PUTs and expose ETag so the SPA can complete multipart."""
        await self._call(
            "put_bucket_cors",
            Bucket=self.bucket,
            CORSConfiguration={
                "CORSRules": [
                    {
                        "AllowedHeaders": ["*"],
                        "AllowedMethods": ["GET", "PUT", "POST", "DELETE", "HEAD"],
                        "AllowedOrigins": ["*"],
                        "ExposeHeaders": ["ETag", "x-amz-request-id", "x-amz-version-id"],
                        "MaxAgeSeconds": 3000,
                    }
                ]
            },
        )

    def presign_put_object(
        self,
        key: str,
        *,
        content_type: str | None = None,
        expires_in: int | None = None,
    ) -> str:
        params: dict[str, Any] = {"Bucket": self.bucket, "Key": key}
        if content_type:
            params["ContentType"] = content_type
        return self.presign_client.generate_presigned_url(
            "put_object",
            Params=params,
            ExpiresIn=expires_in or settings.presign_expiry_seconds,
        )

    def presign_upload_part(
        self,
        key: str,
        *,
        upload_id: str,
        part_number: int,
        expires_in: int | None = None,
    ) -> str:
        return self.presign_client.generate_presigned_url(
            "upload_part",
            Params={
                "Bucket": self.bucket,
                "Key": key,
                "UploadId": upload_id,
                "PartNumber": part_number,
            },
            ExpiresIn=expires_in or settings.presign_expiry_seconds,
        )

    def presign_get_object(
        self,
        key: str,
        *,
        filename: str | None = None,
        expires_in: int | None = None,
    ) -> str:
        params: dict[str, Any] = {"Bucket": self.bucket, "Key": key}
        if filename:
            safe = filename.replace('"', "")
            params["ResponseContentDisposition"] = f'attachment; filename="{safe}"'
        return self.presign_client.generate_presigned_url(
            "get_object",
            Params=params,
            ExpiresIn=expires_in or settings.presign_expiry_seconds,
        )

    async def create_multipart_upload(self, key: str, *, content_type: str) -> str:
        response = await self._call(
            "create_multipart_upload",
            Bucket=self.bucket,
            Key=key,
            ContentType=content_type,
        )
        return str(response["UploadId"])

    async def complete_multipart_upload(
        self,
        key: str,
        *,
        upload_id: str,
        parts: list[tuple[int, str]],
    ) -> str:
        if not parts:
            raise ConflictError("cannot complete a multipart upload with zero parts")
        ordered = sorted(parts, key=lambda item: item[0])
        try:
            response = await self._call(
                "complete_multipart_upload",
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={
                    "Parts": [
                        {"PartNumber": number, "ETag": f'"{etag.strip(chr(34))}"'}
                        for number, etag in ordered
                    ]
                },
            )
        except StorageError as exc:
            code = str(exc.details.get("code", ""))
            if code.startswith("InvalidPart") or code == "EntityTooSmall":
                raise ConflictError(
                    f"multipart completion rejected by S3: {code}",
                    details={"code": code},
                ) from exc
            raise
        return str(response.get("ETag", ""))

    async def abort_multipart_upload(self, key: str, *, upload_id: str) -> None:
        try:
            await self._call(
                "abort_multipart_upload",
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
            )
        except NotFoundError:
            logger.info("multipart upload already gone", extra={"key": key})

    async def list_parts(self, key: str, *, upload_id: str) -> list[dict[str, Any]]:
        response = await self._call("list_parts", Bucket=self.bucket, Key=key, UploadId=upload_id)
        return list(response.get("Parts", []))

    async def head_object(self, key: str) -> ObjectStat:
        response = await self._call("head_object", Bucket=self.bucket, Key=key)
        return ObjectStat(
            key=key,
            size=int(response.get("ContentLength", 0)),
            etag=str(response.get("ETag", "")).strip('"'),
            content_type=response.get("ContentType"),
        )

    async def object_exists(self, key: str) -> bool:
        try:
            await self.head_object(key)
        except NotFoundError:
            return False
        return True

    async def put_object(self, key: str, body: bytes, *, content_type: str) -> str:
        response = await self._call(
            "put_object",
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
        )
        return str(response.get("ETag", "")).strip('"')

    async def get_object_bytes(self, key: str) -> bytes:
        response = await self._call("get_object", Bucket=self.bucket, Key=key)
        body = response["Body"]
        return await anyio.to_thread.run_sync(body.read)

    async def get_object_range(self, key: str, start: int, end: int) -> bytes:
        """Ranged read (inclusive bounds) - used for cheap header probes."""
        response = await self._call(
            "get_object", Bucket=self.bucket, Key=key, Range=f"bytes={start}-{end}"
        )
        body = response["Body"]
        return await anyio.to_thread.run_sync(body.read)

    async def delete_object(self, key: str) -> None:
        await self._call("delete_object", Bucket=self.bucket, Key=key)

    async def delete_objects(self, keys: list[str]) -> int:
        """Batch delete, chunked to S3's 1000-key limit. Returns keys removed."""
        removed = 0
        for start in range(0, len(keys), 1000):
            batch = keys[start : start + 1000]
            if not batch:
                continue
            await self._call(
                "delete_objects",
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
            )
            removed += len(batch)
        return removed

    def _hash_object_sync(self, key: str) -> tuple[str, int]:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            code = self._error_code(exc)
            if code in _NOT_FOUND_CODES:
                raise NotFoundError(
                    "cannot hash a missing object", details={"key": key, "code": code}
                ) from exc
            raise StorageError(f"S3 get_object failed: {exc}", details={"code": code}) from exc

        digest = hashlib.sha256()
        size = 0
        body = response["Body"]
        try:
            for chunk in body.iter_chunks(chunk_size=settings.hash_stream_chunk_bytes):
                if not chunk:
                    continue
                digest.update(chunk)
                size += len(chunk)
        finally:
            body.close()
        return digest.hexdigest(), size

    async def compute_sha256(self, key: str) -> tuple[str, int]:
        """Stream the object and return ``(hex_digest, byte_count)``.

        Never buffers the whole object: memory stays at one chunk regardless of
        whether the file is 1 KiB or 100 GiB.
        """
        return await anyio.to_thread.run_sync(partial(self._hash_object_sync, key))


_s3_service: S3Service | None = None


def get_s3() -> S3Service:
    """Process-wide S3 service singleton (boto3 clients are thread-safe)."""
    global _s3_service
    if _s3_service is None:
        _s3_service = S3Service()
    return _s3_service


def reset_s3_cache() -> None:
    """Drop the cached service - used by tests that swap settings."""
    global _s3_service
    _s3_service = None
