"""Worker pipeline: streaming SHA-256 dedup, then metadata extraction."""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy import select

from app.config import settings
from app.db.session import SessionFactory
from app.models.blob import StorageBlob
from app.services.queue import JobQueue
from app.services.s3 import S3Service
from app.worker.jobs import dedup, metadata

pytestmark = pytest.mark.integration

V1 = settings.api_v1_prefix


async def upload(
    client: AsyncClient, name: str, payload: bytes, folder_id: str | None = None
) -> dict:
    intent = (
        await client.post(
            f"{V1}/files/upload-intent",
            json={"name": name, "size": len(payload), "folder_id": folder_id},
        )
    ).json()

    async with httpx.AsyncClient(timeout=30.0) as raw:
        put = await raw.put(
            intent["direct"]["url"], content=payload, headers=intent["direct"]["headers"]
        )
    assert put.status_code == 200, put.text

    completed = await client.post(
        f"{V1}/files/complete-upload", json={"file_id": intent["file_id"], "parts": []}
    )
    assert completed.status_code == 200, completed.text
    return intent


async def drain(s3: S3Service, queue: JobQueue) -> int:
    handled = 0
    while True:
        claimed = await queue.claim(timeout=1)
        if claimed is None:
            return handled
        job, raw = claimed
        handler = {"file:uploaded": dedup.run, "file:metadata": metadata.run}[job.name]
        async with SessionFactory() as session:
            await handler(session, s3, queue, job.payload)
        await queue.acknowledge(raw)
        handled += 1


async def test_unique_file_becomes_ready_with_its_digest(
    client: AsyncClient,
    s3: S3Service,
    queue: JobQueue,
    upload_bytes: Callable[..., bytes],
) -> None:
    import hashlib

    payload = upload_bytes(32 * 1024, b"unique")
    intent = await upload(client, "unique.bin", payload)

    assert await drain(s3, queue) == 2

    row = (await client.get(f"{V1}/files/{intent['file_id']}")).json()
    assert row["status"] == "READY"
    assert row["checksum_sha256"] == hashlib.sha256(payload).hexdigest()
    assert row["is_duplicate"] is False
    assert row["storage_key"] == intent["storage_key"]
    assert row["metadata"]["deduplicated"] is False
    assert row["metadata"]["category"] == "binary"
    assert row["processed_at"] is not None


async def test_identical_content_is_soft_linked_and_the_copy_removed(
    client: AsyncClient,
    s3: S3Service,
    queue: JobQueue,
    session,
    upload_bytes: Callable[..., bytes],
) -> None:
    payload = upload_bytes(48 * 1024, b"twins")

    first = await upload(client, "original.bin", payload)
    await drain(s3, queue)
    second = await upload(client, "copy.bin", payload)
    await drain(s3, queue)

    original = (await client.get(f"{V1}/files/{first['file_id']}")).json()
    duplicate = (await client.get(f"{V1}/files/{second['file_id']}")).json()

    assert original["is_duplicate"] is False
    assert duplicate["is_duplicate"] is True
    assert duplicate["checksum_sha256"] == original["checksum_sha256"]
    assert duplicate["storage_key"] == original["storage_key"]
    assert duplicate["status"] == "READY"
    assert duplicate["metadata"]["deduplicated"] is True

    assert await s3.object_exists(first["storage_key"]) is True
    assert await s3.object_exists(second["storage_key"]) is False

    blobs = list((await session.scalars(select(StorageBlob))).all())
    assert len(blobs) == 1
    assert blobs[0].ref_count == 2
    assert blobs[0].storage_key == original["storage_key"]


async def test_dedup_reflected_in_storage_accounting(
    client: AsyncClient,
    s3: S3Service,
    queue: JobQueue,
    upload_bytes: Callable[..., bytes],
) -> None:
    payload = upload_bytes(40 * 1024, b"accounting")

    for name in ("a.bin", "b.bin", "c.bin"):
        await upload(client, name, payload)
        await drain(s3, queue)

    stats = (await client.get(f"{V1}/files/stats")).json()
    assert stats["total_files"] == 3
    assert stats["unique_blobs"] == 1
    assert stats["deduplicated_files"] == 2
    assert stats["logical_bytes"] == 3 * len(payload)
    assert stats["physical_bytes"] == len(payload)
    assert stats["deduplicated_bytes"] == 2 * len(payload)
    assert stats["files_by_status"] == {"READY": 3}


async def test_deleting_a_duplicate_keeps_the_shared_object(
    client: AsyncClient,
    s3: S3Service,
    queue: JobQueue,
    session,
    upload_bytes: Callable[..., bytes],
) -> None:
    payload = upload_bytes(16 * 1024, b"shared")

    first = await upload(client, "keep.bin", payload)
    await drain(s3, queue)
    second = await upload(client, "drop.bin", payload)
    await drain(s3, queue)

    assert (await client.delete(f"{V1}/files/{second['file_id']}")).status_code == 204

    assert await s3.object_exists(first["storage_key"]) is True
    blob = await session.scalar(select(StorageBlob))
    await session.refresh(blob)
    assert blob.ref_count == 1

    assert (await client.delete(f"{V1}/files/{first['file_id']}")).status_code == 204
    assert await s3.object_exists(first["storage_key"]) is False
    assert await session.scalar(select(StorageBlob)) is None


async def test_download_url_only_offered_once_ready(
    client: AsyncClient,
    s3: S3Service,
    queue: JobQueue,
    upload_bytes: Callable[..., bytes],
) -> None:
    payload = upload_bytes(8 * 1024, b"download")
    intent = await upload(client, "doc.pdf", payload)

    too_early = await client.get(f"{V1}/files/{intent['file_id']}/download")
    assert too_early.status_code == 409

    await drain(s3, queue)

    ready = await client.get(f"{V1}/files/{intent['file_id']}/download")
    assert ready.status_code == 200
    url = ready.json()["url"]
    assert str(settings.presign_endpoint_url) in url

    async with httpx.AsyncClient(timeout=30.0) as raw:
        fetched = await raw.get(url)
    assert fetched.status_code == 200
    assert fetched.content == payload


async def test_image_metadata_is_probed_from_the_header(
    client: AsyncClient, s3: S3Service, queue: JobQueue
) -> None:
    import struct
    import zlib

    ihdr = struct.pack(">II", 640, 480) + b"\x08\x02\x00\x00\x00"
    chunk = b"IHDR" + ihdr
    png = (
        b"\x89PNG\r\n\x1a\n"
        + struct.pack(">I", len(ihdr))
        + chunk
        + struct.pack(">I", zlib.crc32(chunk))
        + b"\x00" * 128
    )

    intent = await upload(client, "shot.png", png)
    await drain(s3, queue)

    row = (await client.get(f"{V1}/files/{intent['file_id']}")).json()
    assert row["status"] == "READY"
    assert row["metadata"]["category"] == "image"
    assert row["metadata"]["extension"] == "png"
    assert row["metadata"]["width"] == 640
    assert row["metadata"]["height"] == 480


async def test_malformed_payload_is_not_retried(s3: S3Service, queue: JobQueue, session) -> None:
    from app.core.errors import PermanentJobError

    with pytest.raises(PermanentJobError):
        await dedup.run(session, s3, queue, {})

    with pytest.raises(PermanentJobError):
        await dedup.run(session, s3, queue, {"file_id": "not-a-uuid"})


async def test_missing_file_row_is_permanent(s3: S3Service, queue: JobQueue, session) -> None:
    from app.core.errors import PermanentJobError

    with pytest.raises(PermanentJobError):
        await dedup.run(session, s3, queue, {"file_id": "00000000-0000-0000-0000-0000000000aa"})
