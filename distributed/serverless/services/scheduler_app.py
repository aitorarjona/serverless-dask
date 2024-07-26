from __future__ import annotations

import asyncio
import logging
import os

from distributed._signals import wait_for_signals
from distributed.compatibility import asyncio_run
from distributed.config import get_loop_factory
from distributed.serverless.scheduler_service import ServerlessSchedulerService

logger = logging.getLogger(__name__)

SCHEDULER_SERVICE_ADDR = os.environ.get("SCHEDULER_SERVICE_ADDR", "ws://127.0.0.1:8786")


async def run():
    logger.info("-" * 47)
    ephemeral_scheduler = ServerlessSchedulerService(SCHEDULER_SERVICE_ADDR)
    logger.info("-" * 47)

    async def wait_for_dispatcher_to_finish():
        """Wait for the scheduler to initialize and finish"""
        await ephemeral_scheduler
        await ephemeral_scheduler.finished()

    async def wait_for_signals_and_close():
        """Wait for SIGINT or SIGTERM and close the scheduler upon receiving one of those signals"""
        signum = await wait_for_signals()
        await ephemeral_scheduler.close(reason=f"signal-{signum}")

    wait_for_signals_and_close_task = asyncio.create_task(
        wait_for_signals_and_close()
    )
    wait_for_dispatcher_to_finish_task = asyncio.create_task(
        wait_for_dispatcher_to_finish()
    )

    done, _ = await asyncio.wait(
        [wait_for_signals_and_close_task, wait_for_dispatcher_to_finish_task],
        return_when=asyncio.FIRST_COMPLETED,
    )
    # Re-raise exceptions from done tasks
    [task.result() for task in done]
    logger.info("Stopped ephemeral scheduler service at %r", ephemeral_scheduler.address)


if __name__ == "__main__":
    try:
        asyncio_run(run(), loop_factory=get_loop_factory())
    finally:
        logger.info("End ephemeral scheduler service")
