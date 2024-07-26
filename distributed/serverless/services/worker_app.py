from __future__ import annotations

import asyncio
import logging
import os

from distributed._signals import wait_for_signals
from distributed.compatibility import asyncio_run
from distributed.config import get_loop_factory
from distributed.serverless.worker_service import ServerlessWorkerService

logger = logging.getLogger(__name__)

PORT = os.environ.get("PORT", "8080")


async def run():
    logger.info("-" * 47)
    worker_service = ServerlessWorkerService(
        host="127.0.0.1",
        port=PORT,
        protocol="ws"
    )
    logger.info("-" * 47)

    async def wait_for_dispatcher_to_finish():
        """Wait for the scheduler to initialize and finish"""
        await worker_service
        await worker_service.finished()

    async def wait_for_signals_and_close():
        """Wait for SIGINT or SIGTERM and close the scheduler upon receiving one of those signals"""
        signum = await wait_for_signals()
        await worker_service.close(reason=f"signal-{signum}")

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
    logger.info("Stopped worker service at %r", worker_service.address)


if __name__ == "__main__":
    try:
        asyncio_run(run(), loop_factory=get_loop_factory())
    finally:
        logger.info("End worker service")






# import json
# import logging
# import os
#
# import dask
# import tornado.ioloop
# import tornado.web
#
# from distributed.versions import get_versions
# from distributed.serverless.lazy_worker import LazyWorker
#
# logger = logging.getLogger(__name__)
#
# HOST = os.environ.get("HOST", "127.0.0.1")
# PORT = os.environ.get("PORT", 8080)
#
#
# class KnativeWorkerService(tornado.web.RequestHandler):
#     async def post(self):
#         data = {}
#         try:
#             data = json.loads(self.request.body)
#         except json.JSONDecodeError:
#             self.set_status(400)
#             self.write({"status": "error", "message": "Invalid JSON"})
#
#         try:
#             print(data)
#             dask.config.set({"scheduler-address": data["scheduler_address"]})
#             logger.info("-" * 47)
#             worker = LazyWorker(
#                 # scheduler_ip="amqp://Scheduler-00000000-0000-0000-0000-000000000000:0",
#                 # scheduler_ip="ws://127.0.0.1",
#                 # scheduler_port=5555,
#                 nthreads=data["nthreads"],
#                 name=data["name"],
#                 contact_address=data["contact_address"],
#                 heartbeat_interval="25s",
#                 dashboard=False,
#                 # host="127.0.0.1",
#                 # port=8989,
#                 # protocol="tcp",
#                 nanny=None,
#                 validate=False,
#                 memory_limit=data["memory_limit"],
#                 connection_limit=1,
#             )
#             logger.info("-" * 47)
#
#             print("Starting worker...")
#             await worker
#             await worker.finished()
#
#             self.write({"status": "success", "data": data})
#         except KeyError as e:
#             self.set_status(400)
#             self.write({"status": "error", "message": str(e)})
#         except Exception as e:
#             self.set_status(500)
#             self.write({"status": "error", "message": str(e)})
#
#
# class WorkerMetadataHandler(tornado.web.RequestHandler):
#     versions = None
#
#     async def get(self):
#         if self.versions is None:
#             self.versions = get_versions()
#         print(self.versions)
#         self.write(self.versions)
#
#
# if __name__ == "__main__":
#     app = tornado.web.Application([
#         (r"/worker", KnativeWorkerService),
#         (r"/versions", WorkerMetadataHandler)
#     ])
#     app.listen(address=HOST, port=PORT)
#     print(f"Server is running on http://{HOST}:{PORT}")
#     tornado.ioloop.IOLoop.current().start()
