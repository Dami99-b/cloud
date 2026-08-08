"""Structured JSON logging shared by the API and the worker."""

from __future__ import annotations

import logging
import sys
from typing import Any

from pythonjsonlogger import json as jsonlogger

from app.config import settings

_CONFIGURED = False


def configure_logging(service: str) -> None:
    """Install a single JSON handler on the root logger. Idempotent."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s",
            rename_fields={"asctime": "timestamp", "levelname": "level"},
            static_fields={"service": service, "environment": settings.environment},
        )
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(settings.log_level.upper())

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True

    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(
        logging.INFO if settings.db_echo else logging.WARNING
    )

    _CONFIGURED = True


def get_logger(name: str, **context: Any) -> logging.LoggerAdapter:
    return logging.LoggerAdapter(logging.getLogger(name), context)
