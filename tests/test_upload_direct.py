"""The direct (sub-threshold) upload flow, end to end through a real presigned PUT."""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest
from httpx import AsyncClient

from app.config import settings
from app.services.queue import JobQueue
from app.services.s3 import S3Service

pytestmark = pytest.mark.integration

V1 = settings.api_v1_prefix
SMALL = 64 * 1024


async def test_intent_below_threshold_returns_a_single_presigned_put(
    client: AsyncClient,
) -> None:
    response = await client.post(
        f"{V1}/files/upload-intent",
        json={"name": "notes.txt", "size": SMALL, "mime_type": "text/plain"},
    )
    assert response.status_code == 201, response.text

    body = response.json()
    assert body["upload_type"] == "DIRECT"
    assert body["multipart"] is None
    assert body["direct"]["method"] == "PUT"
    assert body["direct"]["headers"]["Content-Type"] == "text/plain"
    assert body["multipart_threshold"] == settings.multipart_threshold_bytes

    url = body["direct"]["url"]
    assert url.startswith(str(settings.presign_endpoint_url))
    assert "X-Amz-Signature=" in url
    assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in url
    assert body["storage_key"] in url


async def test_direct_upload_round_trip(
    client: AsyncClient,
    s3: S3Service,
    queue: JobQueue,
    upload_bytes: Callable[..., bytes],
) -> None:
    payload = upload_bytes(SMALL, b"direct")

    intent = (
        await client.post(
            f"{V1}/files/upload-intent",
            json={
                "name": "report.bin",
                "size": len(payload),
                "mime_type": "application/octet-stream",
            },
        )
    ).json()
    assert intent["upload_type"] == "DIRECT"

    async with httpx.AsyncClient(timeout=30.0) as raw:
        put = await raw.put(
            intent["direct"]["url"],
            content=payload,
            headers=intent["direct"]["headers"],
        )
    assert put.status_code == 200, put.text
    assert put.headers.get("ETag")

    completed = await client.post(
        f"{V1}/files/complete-upload", json={"file_id": intent["file_id"], "parts": []}
    )
    assert completed.status_code == 200, completed.text

    body = completed.json()
    assert body["job_enqueued"] is True
    assert body["file"]["status"] == "PROCESSING"
    assert body["file"]["size_bytes"] == len(payload)
    assert body["file"]["upload_type"] == "DIRECT"

    stat = await s3.head_object(intent["storage_key"])
    assert stat.size == len(payload)

    depth = await queue.depth()
    assert depth["pending"] == 1
    claimed = await queue.claim(timeout=2)
    assert claimed is not None
    job, _raw = claimed
    assert job.name == "file:uploaded"
    assert job.payload == {"file_id": intent["file_id"]}


async def test_complete_upload_is_idempotent(
    client: AsyncClient, queue: JobQueue, upload_bytes: Callable[..., bytes]
) -> None:
    payload = upload_bytes(2048, b"idem")
    intent = (
        await client.post(
            f"{V1}/files/upload-intent",
            json={"name": "idem.bin", "size": len(payload)},
        )
    ).json()

    async with httpx.AsyncClient(timeout=30.0) as raw:
        await raw.put(intent["direct"]["url"], content=payload, headers=intent["direct"]["headers"])

    first = await client.post(
        f"{V1}/files/complete-upload", json={"file_id": intent["file_id"], "parts": []}
    )
    second = await client.post(
        f"{V1}/files/complete-upload", json={"file_id": intent["file_id"], "parts": []}
    )

    assert first.json()["job_enqueued"] is True
    assert second.status_code == 200
    assert second.json()["job_enqueued"] is False
    assert (await queue.depth())["pending"] == 1


async def test_complete_without_uploading_the_bytes_conflicts(client: AsyncClient) -> None:
    intent = (
        await client.post(f"{V1}/files/upload-intent", json={"name": "ghost.bin", "size": 1024})
    ).json()

    response = await client.post(
        f"{V1}/files/complete-upload", json={"file_id": intent["file_id"], "parts": []}
    )
    assert response.status_code == 409


async def test_server_corrects_a_misdeclared_size(
    client: AsyncClient, upload_bytes: Callable[..., bytes]
) -> None:
    actual = upload_bytes(4096, b"lie")
    intent = (
        await client.post(f"{V1}/files/upload-intent", json={"name": "lie.bin", "size": 999_999})
    ).json()

    async with httpx.AsyncClient(timeout=30.0) as raw:
        await raw.put(intent["direct"]["url"], content=actual, headers=intent["direct"]["headers"])

    completed = await client.post(
        f"{V1}/files/complete-upload", json={"file_id": intent["file_id"], "parts": []}
    )
    assert completed.json()["file"]["size_bytes"] == len(actual)


async def test_upload_lands_in_the_requested_folder(client: AsyncClient) -> None:
    folder = (await client.post(f"{V1}/folders", json={"name": "Invoices"})).json()

    intent = (
        await client.post(
            f"{V1}/files/upload-intent",
            json={"name": "inv.pdf", "size": 4096, "folder_id": folder["id"]},
        )
    ).json()
    assert intent["folder_id"] == folder["id"]

    listed = await client.get(f"{V1}/files", params={"folder_id": folder["id"]})
    assert [item["name"] for item in listed.json()["items"]] == ["inv.pdf"]
    assert listed.json()["items"][0]["folder_path"] == "root.invoices"


async def test_mime_type_is_inferred_when_omitted(client: AsyncClient) -> None:
    intent = (
        await client.post(f"{V1}/files/upload-intent", json={"name": "photo.png", "size": 2048})
    ).json()
    assert intent["direct"]["headers"]["Content-Type"] == "image/png"


async def test_zero_and_oversized_declarations_are_rejected(client: AsyncClient) -> None:
    zero = await client.post(f"{V1}/files/upload-intent", json={"name": "e.bin", "size": 0})
    assert zero.status_code == 400

    huge = await client.post(
        f"{V1}/files/upload-intent",
        json={"name": "h.bin", "size": settings.max_file_size_bytes + 1},
    )
    assert huge.status_code == 413


async def test_abort_releases_the_reservation(client: AsyncClient) -> None:
    intent = (
        await client.post(f"{V1}/files/upload-intent", json={"name": "abort.bin", "size": 8192})
    ).json()

    aborted = await client.post(f"{V1}/files/{intent['file_id']}/abort")
    assert aborted.status_code == 200
    assert aborted.json()["status"] == "FAILED"

    response = await client.post(
        f"{V1}/files/complete-upload", json={"file_id": intent["file_id"], "parts": []}
    )
    assert response.status_code == 409
