from __future__ import annotations

import logging
import random

from distributed import Status
from distributed.comm import Comm
from distributed.comm.addressing import addresses_from_user_args, parse_host_port
from distributed.node import ServerNode
from distributed.burst.worker import BurstableWorker
from distributed.shuffle import ShuffleWorkerPlugin
from distributed.versions import get_versions

logger = logging.getLogger(__name__)

WORKER_PLUGINS = {
    "shuffle": ShuffleWorkerPlugin,
}


class BurstableWorkerService(ServerNode):
    def __init__(self, host=None, port=None, interface=None, protocol=None):
        self.versions = get_versions()

        handlers = {
            "get-versions": self.get_versions,
            "bootstrap-worker": self.bootstrap_worker,
        }

        stream_handlers = {

        }

        addresses = addresses_from_user_args(
            host=host,
            port=port,
            interface=interface,
            protocol=protocol,
            security=None,
            default_port=self.default_port,
        )
        assert len(addresses) == 1
        self._start_address = addresses.pop()

        ServerNode.__init__(self, handlers=handlers, stream_handlers=stream_handlers)

    async def get_versions(self, comm: Comm):
        logger.debug("Get versions from %s", comm.peer_address)
        await comm.write(self.versions)

    async def bootstrap_worker(self, comm: Comm, name: str, nthreads: int, memory_limit: str,
                               scheduler_address: str):
        logger.info("======================= WORKER %s START =======================", name)
        scheduler_host, scheduler_port = parse_host_port(scheduler_address)
        logger.info("Will connect to scheduler at host=%s port=%d", scheduler_host, scheduler_port)

        worker = BurstableWorker(
            scheduler_ip=scheduler_host,
            scheduler_port=scheduler_port,
            nthreads=nthreads,
            name=name,
            heartbeat_interval="3s",
            dashboard=False,
            host="0.0.0.0",
            port=random.randint(49152, 65535),
            protocol="tcp",
            nanny=None,
            validate=False,
            memory_limit=memory_limit,
            connection_limit=1,
        )
        for plugin_name, plugin_cls in WORKER_PLUGINS.items():
            await worker.plugin_add(plugin_cls(), name=plugin_name)

        await worker
        await comm.write({"name": name, "contact_address": worker.address})

        worker.batched_stream.start(comm)
        worker.status = Status.running

        await worker.handle_scheduler(comm)
        worker.batched_stream.close()
        await worker.close()
        logger.info("======================= WORKER %s END =======================", name)

    async def start_unsafe(self):
        await self.listen(
            self._start_address,
            allow_offload=False,
            handshake_overrides={"pickle-protocol": 4, "compression": None},
        )

        for listener in self.listeners:
            logger.info("Worker service listening at %s", listener.contact_address)

        return self
