from __future__ import annotations

import asyncio
import sys

from app.core.logging import configure_logging, get_logger
from app.services.s3 import get_s3

logger = get_logger(__name__)


async def main() -> int:
    configure_logging("bootstrap-s3")
    s3 = get_s3()
    created = await s3.ensure_bucket()
    logger.info(
        "bucket ready",
        extra={"bucket": s3.bucket, "created": created},
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
