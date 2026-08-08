"""FastAPI application factory.

Serves the JSON API under ``/api/v1``, the SPA at ``/`` and its assets from
``/static``.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from app import __version__
from app.api.router import api_router
from app.api.routes import health
from app.config import settings
from app.core.errors import AppError
from app.core.logging import configure_logging, get_logger
from app.db.session import dispose_engine, ping_database
from app.services.queue import close_queue, get_queue
from app.services.s3 import get_s3

logger = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"
INDEX_FILE = STATIC_DIR / "index.html"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging("api")
    logger.info(
        "starting api",
        extra={"version": __version__, "environment": settings.environment},
    )

    try:
        await ping_database()
        logger.info("postgres reachable")
    except Exception as exc:
        logger.error("postgres unreachable at startup", extra={"error": str(exc)})

    try:
        await get_s3().ensure_bucket()
    except Exception as exc:
        logger.error("could not ensure s3 bucket", extra={"error": str(exc)})

    try:
        await get_queue().ping()
        logger.info("redis reachable")
    except Exception as exc:
        logger.error("redis unreachable at startup", extra={"error": str(exc)})

    try:
        yield
    finally:
        await close_queue()
        await dispose_engine()
        logger.info("api stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version=__version__,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url=f"{settings.api_v1_prefix}/openapi.json",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Request-Id"],
    )

    @app.middleware("http")
    async def request_context(request: Request, call_next):  # type: ignore[no-untyped-def]
        request_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())
        started = time.perf_counter()
        response = await call_next(request)
        duration_ms = round((time.perf_counter() - started) * 1000, 2)
        response.headers["X-Request-Id"] = request_id
        if not request.url.path.startswith("/static"):
            logger.info(
                "request",
                extra={
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "status": response.status_code,
                    "duration_ms": duration_ms,
                },
            )
        return response

    @app.exception_handler(AppError)
    async def handle_app_error(_: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.to_payload())

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "validation_error",
                    "message": "request validation failed",
                    "details": {"errors": exc.errors()},
                }
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"code": "http_error", "message": str(exc.detail), "details": {}}},
        )

    @app.exception_handler(Exception)
    async def handle_unexpected(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled error", extra={"path": request.url.path, "error": str(exc)})
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "internal_error",
                    "message": "an unexpected error occurred",
                    "details": {},
                }
            },
        )

    app.include_router(health.router)
    app.include_router(api_router, prefix=settings.api_v1_prefix)

    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(
            INDEX_FILE,
            media_type="text/html",
            headers={"Cache-Control": "no-store"},
        )

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon() -> FileResponse:
        icon = STATIC_DIR / "favicon.svg"
        return FileResponse(icon, media_type="image/svg+xml")

    return app


app = create_app()
