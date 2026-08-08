"""A reliable Redis job queue.

Delivery model
--------------
``BLMOVE main -> processing`` makes a claim atomic and crash-visible: if a
worker dies mid-job the payload is still sitting in the processing list and the
next worker to start reclaims it. Successful jobs are ``LREM``-ed from that
list; failed ones are re-scheduled into a delayed ZSET with exponential
backoff, and exhausted ones land in a dead-letter list for inspection.

This is intentionally a few dozen lines rather than a Celery dependency - the
semantics are explicit and the failure modes are visible.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.config import settings
from app.core.errors import QueueError
from app.core.logging import get_logger
from app.models.enums import JobName

logger = get_logger(__name__)


@dataclass(slots=True)
class Job:
    name: str
    payload: dict[str, Any]
    attempt: int = 1
    enqueued_at: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> Job:
        data = json.loads(raw)
        return cls(
            id=str(data.get("id", uuid.uuid4())),
            name=str(data["name"]),
            payload=dict(data.get("payload", {})),
            attempt=int(data.get("attempt", 1)),
            enqueued_at=float(data.get("enqueued_at", time.time())),
        )


class JobQueue:
    def __init__(self, redis: Redis | None = None) -> None:
        self._redis = redis

    @property
    def redis(self) -> Redis:
        if self._redis is None:
            self._redis = Redis.from_url(
                settings.redis_url,
                decode_responses=True,
                socket_keepalive=True,
                health_check_interval=30,
            )
        return self._redis

    async def enqueue(
        self,
        name: JobName | str,
        payload: dict[str, Any],
        *,
        attempt: int = 1,
    ) -> Job:
        job = Job(name=str(name), payload=payload, attempt=attempt)
        try:
            await self.redis.lpush(settings.queue_key, job.to_json())
        except RedisError as exc:
            raise QueueError(f"failed to enqueue {name}: {exc}") from exc
        logger.info("job enqueued", extra={"job": job.name, "job_id": job.id, **payload})
        return job

    async def enqueue_delayed(self, job: Job, *, delay_seconds: float) -> None:
        ready_at = time.time() + max(0.0, delay_seconds)
        try:
            await self.redis.zadd(settings.delayed_key, {job.to_json(): ready_at})
        except RedisError as exc:
            raise QueueError(f"failed to schedule retry for {job.name}: {exc}") from exc

    async def claim(self, *, timeout: int | None = None) -> tuple[Job, str] | None:
        """Atomically move one job to the processing list.

        Returns ``(job, raw_payload)``; the raw payload is the exact string that
        must be passed to :meth:`acknowledge` to remove it.
        """
        raw = await self.redis.blmove(
            settings.queue_key,
            settings.processing_key,
            timeout if timeout is not None else settings.worker_block_timeout_seconds,
            src="RIGHT",
            dest="LEFT",
        )
        if raw is None:
            return None
        try:
            return Job.from_json(raw), raw
        except (ValueError, KeyError, json.JSONDecodeError):
            logger.error("discarding malformed job payload", extra={"raw": raw[:500]})
            await self.redis.lrem(settings.processing_key, 1, raw)
            await self.redis.lpush(settings.dead_letter_key, raw)
            return None

    async def acknowledge(self, raw: str) -> None:
        await self.redis.lrem(settings.processing_key, 1, raw)

    async def retry(self, job: Job, raw: str, *, error: str) -> bool:
        """Re-schedule with exponential backoff. Returns False when exhausted."""
        await self.acknowledge(raw)
        if job.attempt >= settings.job_max_attempts:
            await self.dead_letter(job, error=error)
            return False

        delay = min(
            settings.job_backoff_base_seconds**job.attempt,
            settings.job_backoff_max_seconds,
        )
        retried = Job(
            id=job.id,
            name=job.name,
            payload=job.payload,
            attempt=job.attempt + 1,
            enqueued_at=job.enqueued_at,
        )
        await self.enqueue_delayed(retried, delay_seconds=delay)
        logger.warning(
            "job scheduled for retry",
            extra={
                "job": job.name,
                "job_id": job.id,
                "attempt": retried.attempt,
                "delay_seconds": delay,
                "error": error,
            },
        )
        return True

    async def dead_letter(self, job: Job, *, error: str) -> None:
        record = {
            "job": asdict(job),
            "error": error,
            "failed_at": time.time(),
        }
        await self.redis.lpush(settings.dead_letter_key, json.dumps(record, sort_keys=True))
        await self.redis.ltrim(settings.dead_letter_key, 0, 999)
        logger.error(
            "job moved to dead-letter queue",
            extra={"job": job.name, "job_id": job.id, "error": error},
        )

    async def promote_due_jobs(self, *, batch: int = 100) -> int:
        """Move delayed jobs whose backoff elapsed back onto the main queue."""
        now = time.time()
        due = await self.redis.zrangebyscore(
            settings.delayed_key, min="-inf", max=now, start=0, num=batch
        )
        promoted = 0
        for raw in due:
            if await self.redis.zrem(settings.delayed_key, raw):
                await self.redis.lpush(settings.queue_key, raw)
                promoted += 1
        if promoted:
            logger.info("promoted delayed jobs", extra={"count": promoted})
        return promoted

    async def reclaim_orphans(self) -> int:
        """Return every in-flight job to the main queue.

        Called once at worker start-up: anything still in the processing list
        belongs to a worker that is no longer running.
        """
        reclaimed = 0
        while True:
            raw = await self.redis.rpop(settings.processing_key)
            if raw is None:
                break
            await self.redis.lpush(settings.queue_key, raw)
            reclaimed += 1
        if reclaimed:
            logger.warning("reclaimed orphaned jobs", extra={"count": reclaimed})
        return reclaimed

    async def depth(self) -> dict[str, int]:
        return {
            "pending": await self.redis.llen(settings.queue_key),
            "processing": await self.redis.llen(settings.processing_key),
            "delayed": await self.redis.zcard(settings.delayed_key),
            "dead": await self.redis.llen(settings.dead_letter_key),
        }

    async def ping(self) -> bool:
        return bool(await self.redis.ping())

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None


_queue: JobQueue | None = None


def get_queue() -> JobQueue:
    global _queue
    if _queue is None:
        _queue = JobQueue()
    return _queue


async def close_queue() -> None:
    global _queue
    if _queue is not None:
        await _queue.close()
        _queue = None
