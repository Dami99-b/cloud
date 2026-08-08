"""The decoupled worker process.

Run it with ``python -m app.worker.main`` (docker-compose does exactly that).

Loop shape
----------
``N`` concurrent consumer tasks each claim a job with ``BLMOVE`` and process it
in its own database session, alongside one scheduler task that promotes delayed
retries back onto the main queue. SIGTERM/SIGINT flips a shared event so
in-flight jobs finish before the process exits.
"""

from __future__ import annotations

import asyncio
import signal
import sys
from typing import Any

from app.config import settings
from app.core.errors import PermanentJobError, RetryableJobError
from app.core.logging import configure_logging, get_logger
from app.db.session import SessionFactory, dispose_engine, ping_database
from app.services.queue import Job, JobQueue, get_queue
from app.services.s3 import S3Service, get_s3
from app.worker.jobs import HANDLERS

logger = get_logger(__name__)


class Worker:
    def __init__(self, queue: JobQueue | None = None, s3: S3Service | None = None) -> None:
        self.queue = queue or get_queue()
        self.s3 = s3 or get_s3()
        self.stopping = asyncio.Event()
        self.processed = 0
        self.failed = 0

    async def handle(self, job: Job) -> None:
        handler = HANDLERS.get(job.name)
        if handler is None:
            raise PermanentJobError(f"no handler registered for job {job.name!r}")

        async with SessionFactory() as session:
            try:
                await handler(session, self.s3, self.queue, job.payload)
            except Exception:
                await session.rollback()
                raise

    async def process_one(self, job: Job, raw: str) -> None:
        payload: dict[str, Any] = {"job": job.name, "job_id": job.id, "attempt": job.attempt}
        try:
            await self.handle(job)
        except PermanentJobError as exc:
            self.failed += 1
            logger.error("job failed permanently", extra={**payload, "error": str(exc)})
            await self.queue.acknowledge(raw)
            await self.queue.dead_letter(job, error=str(exc))
        except RetryableJobError as exc:
            self.failed += 1
            logger.warning("job failed, will retry", extra={**payload, "error": str(exc)})
            await self.queue.retry(job, raw, error=str(exc))
        except Exception as exc:
            self.failed += 1
            logger.exception("job raised an unexpected error", extra={**payload})
            await self.queue.retry(job, raw, error=repr(exc))
        else:
            self.processed += 1
            logger.info("job processed", extra=payload)
            await self.queue.acknowledge(raw)

    async def consumer(self, index: int) -> None:
        logger.info("consumer started", extra={"consumer": index})
        while not self.stopping.is_set():
            try:
                claimed = await self.queue.claim()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("claim failed", extra={"error": str(exc)})
                await asyncio.sleep(1.0)
                continue

            if claimed is None:
                continue
            job, raw = claimed
            await self.process_one(job, raw)
        logger.info("consumer stopped", extra={"consumer": index})

    async def scheduler(self) -> None:
        """Promote due retries; also the heartbeat that reports queue depth."""
        tick = 0
        while not self.stopping.is_set():
            try:
                await self.queue.promote_due_jobs()
                tick += 1
                if tick % 30 == 0:
                    logger.info(
                        "worker heartbeat",
                        extra={
                            "processed": self.processed,
                            "failed": self.failed,
                            **await self.queue.depth(),
                        },
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("scheduler tick failed", extra={"error": str(exc)})
            try:
                await asyncio.wait_for(self.stopping.wait(), timeout=1.0)
            except TimeoutError:
                continue

    async def wait_for_dependencies(self, *, attempts: int = 30, delay: float = 2.0) -> None:
        for attempt in range(1, attempts + 1):
            try:
                await ping_database()
                await self.queue.ping()
                logger.info("dependencies ready")
                return
            except Exception as exc:
                if attempt == attempts:
                    raise
                logger.warning(
                    "waiting for dependencies",
                    extra={"attempt": attempt, "error": str(exc)},
                )
                await asyncio.sleep(delay)

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop, sig.name)
            except NotImplementedError:
                signal.signal(sig, lambda *_, name=sig.name: self.request_stop(name))

    def request_stop(self, reason: str = "signal") -> None:
        if not self.stopping.is_set():
            logger.info("shutdown requested", extra={"reason": reason})
            self.stopping.set()

    async def run(self) -> None:
        configure_logging("worker")
        logger.info(
            "starting worker",
            extra={
                "concurrency": settings.worker_concurrency,
                "queue": settings.queue_key,
            },
        )
        self.install_signal_handlers()
        await self.wait_for_dependencies()

        try:
            await self.s3.ensure_bucket()
        except Exception as exc:
            logger.error("could not ensure s3 bucket", extra={"error": str(exc)})

        await self.queue.reclaim_orphans()

        tasks = [
            asyncio.create_task(self.consumer(index), name=f"consumer-{index}")
            for index in range(settings.worker_concurrency)
        ]
        tasks.append(asyncio.create_task(self.scheduler(), name="scheduler"))

        try:
            await self.stopping.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.queue.close()
            await dispose_engine()
            logger.info(
                "worker stopped",
                extra={"processed": self.processed, "failed": self.failed},
            )


async def main() -> int:
    worker = Worker()
    try:
        await worker.run()
    except Exception:
        logger.exception("worker crashed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
