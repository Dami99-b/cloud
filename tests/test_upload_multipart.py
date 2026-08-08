"""The multipart (at-or-above-threshold) flow.

CI never needs to ship 50 MB of test payload: `upload-intent` only looks at the
declared size to pick the strategy, so a 60 MB declaration produces a 5 MiB
chunk plan, of which this test uploads the first two real parts before calling
`complete-upload`. Completing with a contiguous 1..N prefix of genuinely
uploaded parts is exactly what S3 requires.
"""

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
TWO_PARTS_TOTAL = settings.multipart_chunk_size_bytes + 1024 * 1024


async def test_intent_at_threshold_plans_chunks_of_the_configured_size(
    client: AsyncClient,
) -> None:
    total = TWO_PARTS_TOTAL
    response = await client.post(
        f"{V1}/files/upload-intent",
        json={"name": "big.bin", "size": total},
    )
    assert response.status_code == 201, response.text

    body = response.json()
    assert body["upload_type"] == "MULTIPART"
    assert body["direct"] is None

    multipart = body["multipart"]
    chunk = settings.multipart_chunk_size_bytes
    assert multipart["chunk_size"] == chunk
    assert multipart["part_count"] == 2
    assert [part["part_number"] for part in multipart["parts"]] == [1, 2]
    assert [part["offset"] for part in multipart["parts"]] == [0, chunk]
    assert [part["size"] for part in multipart["parts"]] == [chunk, total - chunk]
    assert all("X-Amz-Signature=" in part["url"] for part in multipart["parts"])
    assert all(str(settings.presign_endpoint_url) in part["url"] for part in multipart["parts"])

    assert body["storage_key"] in multipart["parts"][0]["url"]


async def test_uploading_both_parts_and_completing(
    client: AsyncClient,
    s3: S3Service,
    queue: JobQueue,
    upload_bytes: Callable[..., bytes],
) -> None:
    total = TWO_PARTS_TOTAL
    payload = upload_bytes(total, b"multipart")

    intent = (
        await client.post(
            f"{V1}/files/upload-intent",
            json={"name": "archive.tar.gz", "size": total, "mime_type": "application/gzip"},
        )
    ).json()
    assert intent["upload_type"] == "MULTIPART"
    parts = intent["multipart"]["parts"]

    uploaded: list[dict[str, str | int]] = []
    async with httpx.AsyncClient(timeout=60.0) as raw:
        for part in parts:
            chunk = payload[part["offset"] : part["offset"] + part["size"]]
            put = await raw.put(part["url"], content=chunk)
            assert put.status_code == 200, put.text
            assert put.headers.get("ETag"), f"part {part['part_number']} returned no ETag"
            uploaded.append({"part_number": part["part_number"], "etag": put.headers["ETag"]})

    completed = await client.post(
        f"{V1}/files/complete-upload",
        json={"file_id": intent["file_id"], "parts": uploaded},
    )
    assert completed.status_code == 200, completed.text

    body = completed.json()
    assert body["job_enqueued"] is True
    assert body["file"]["status"] == "PROCESSING"
    assert body["file"]["size_bytes"] == total
    assert body["file"]["upload_type"] == "MULTIPART"

    stat = await s3.head_object(intent["storage_key"])
    assert stat.size == total

    assert await s3.get_object_bytes(intent["storage_key"]) == payload

    assert (await queue.depth())["pending"] == 1


async def test_complete_requires_parts(client: AsyncClient) -> None:
    intent = (
        await client.post(
            f"{V1}/files/upload-intent",
            json={"name": "unfinished.bin", "size": TWO_PARTS_TOTAL},
        )
    ).json()

    response = await client.post(
        f"{V1}/files/complete-upload",
        json={"file_id": intent["file_id"], "parts": []},
    )
    assert response.status_code == 400


async def test_part_numbers_must_be_in_range_and_unique(client: AsyncClient) -> None:
    intent = (
        await client.post(
            f"{V1}/files/upload-intent",
            json={"name": "bogus.bin", "size": TWO_PARTS_TOTAL},
        )
    ).json()

    out_of_range = await client.post(
        f"{V1}/files/complete-upload",
        json={
            "file_id": intent["file_id"],
            "parts": [{"part_number": 99, "etag": "quoted-etag"}],
        },
    )
    assert out_of_range.status_code == 400

    duplicate = await client.post(
        f"{V1}/files/complete-upload",
        json={
            "file_id": intent["file_id"],
            "parts": [
                {"part_number": 1, "etag": "a"},
                {"part_number": 1, "etag": "b"},
            ],
        },
    )
    assert duplicate.status_code == 422

    staged = await client.post(
        f"{V1}/files/complete-upload",
        json={
            "file_id": intent["file_id"],
            "parts": [{"part_number": 1, "etag": "garbage"}, {"part_number": 2, "etag": "garbage"}],
        },
    )
    assert staged.status_code == 409


async def test_abort_after_some_parts_discards_them(client: AsyncClient, s3: S3Service) -> None:
    intent = (
        await client.post(
            f"{V1}/files/upload-intent",
            json={"name": "giveup.bin", "size": TWO_PARTS_TOTAL},
        )
    ).json()

    part = intent["multipart"]["parts"][0]
    async with httpx.AsyncClient(timeout=60.0) as raw:
        put = await raw.put(part["url"], content=b"\x00" * part["size"])
    assert put.status_code == 200

    response = await client.post(f"{V1}/files/{intent['file_id']}/abort")
    assert response.status_code == 200
    assert response.json()["status"] == "FAILED"

    after = await client.post(
        f"{V1}/files/complete-upload",
        json={
            "file_id": intent["file_id"],
            "parts": [{"part_number": 1, "etag": put.headers["ETag"]}],
        },
    )
    assert after.status_code == 409


async def test_multipart_upload_into_a_subfolder(client: AsyncClient) -> None:
    folder = (await client.post(f"{V1}/folders", json={"name": "Media"})).json()
    intent = (
        await client.post(
            f"{V1}/files/upload-intent",
            json={"name": "movie.mov", "size": TWO_PARTS_TOTAL, "folder_id": folder["id"]},
        )
    ).json()

    assert intent["folder_id"] == folder["id"]
    assert intent["multipart"]["part_count"] == 2

    listed = await client.get(f"{V1}/files", params={"recursive": "true"})
    row = next(item for item in listed.json()["items"] if item["id"] == intent["file_id"])
    assert row["upload_type"] == "MULTIPART"
    assert row["status"] == "UPLOADING"
    assert row["folder_path"] == "root.media"
