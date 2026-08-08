"""Liveness, readiness, client bootstrap and the served SPA."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from app.config import settings

pytestmark = pytest.mark.integration


async def test_health_is_liveness_only(client: AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


async def test_readiness_probes_every_dependency(client: AsyncClient) -> None:
    response = await client.get("/health/ready")
    assert response.status_code == 200

    body = response.json()
    assert body["status"] == "ok"
    components = {component["name"]: component for component in body["components"]}
    assert {"postgres", "redis", "s3"} <= set(components)
    for name in ("postgres", "redis", "s3"):
        assert components[name]["healthy"] is True, components[name]


async def test_config_exposes_the_thresholds_the_browser_slices_with(client: AsyncClient) -> None:
    response = await client.get(f"{settings.api_v1_prefix}/config")
    assert response.status_code == 200

    body = response.json()
    assert body["multipart_threshold_bytes"] == settings.multipart_threshold_bytes
    assert body["multipart_chunk_size_bytes"] == settings.multipart_chunk_size_bytes
    assert body["s3_bucket"] == settings.s3_bucket
    assert body["multipart_chunk_size_bytes"] >= 5 * 1024 * 1024


async def test_queue_depth_endpoint(client: AsyncClient) -> None:
    response = await client.get(f"{settings.api_v1_prefix}/queue")
    assert response.status_code == 200
    assert response.json() == {"pending": 0, "processing": 0, "delayed": 0, "dead": 0}


async def test_root_serves_the_spa(client: AsyncClient) -> None:
    response = await client.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")

    html = response.text
    assert "<title>" in html
    assert "@@APP_SCRIPT@@" not in html
    assert "upload-intent" in html
    assert "complete-upload" in html


async def test_static_mount_serves_assets(client: AsyncClient) -> None:
    response = await client.get("/static/favicon.svg")
    assert response.status_code == 200
    assert "svg" in response.headers["content-type"]


async def test_unknown_route_returns_the_error_envelope(client: AsyncClient) -> None:
    response = await client.get(f"{settings.api_v1_prefix}/does-not-exist")
    assert response.status_code == 404
    assert "error" in response.json()
