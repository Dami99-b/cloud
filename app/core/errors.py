"""Typed application errors mapped to a stable JSON error envelope."""

from __future__ import annotations

from typing import Any


class AppError(Exception):
    """Base class for every error the API deliberately surfaces."""

    status_code: int = 500
    code: str = "internal_error"

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details:
            payload["details"] = self.details
        return {"error": payload}


class BadRequestError(AppError):
    status_code = 400
    code = "bad_request"


class NotFoundError(AppError):
    status_code = 404
    code = "not_found"


class ConflictError(AppError):
    status_code = 409
    code = "conflict"


class PayloadTooLargeError(AppError):
    status_code = 413
    code = "payload_too_large"


class UnprocessableError(AppError):
    status_code = 422
    code = "unprocessable"


class StorageError(AppError):
    status_code = 502
    code = "storage_error"


class QueueError(AppError):
    status_code = 503
    code = "queue_error"


class RetryableJobError(Exception):
    """Raised by workers when a job should be retried with backoff."""


class PermanentJobError(Exception):
    """Raised by workers when retrying cannot help; job goes straight to the DLQ."""
