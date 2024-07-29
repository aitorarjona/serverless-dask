from __future__ import annotations

import logging
import os
import random
import textwrap
import time
from typing import Any

import dask
from dask.typing import Key

from distributed import Status
from distributed import versions as version_module
from distributed.batched import BatchedSend
from distributed.client import SourceCode
from distributed.comm import Comm
from distributed.core import error_message, Server
from distributed.protocol import deserialize
from distributed.scheduler import ClientState, _materialize_graph
from distributed.serverless.serverless_scheduler import ServerlessScheduler
from distributed.utils import is_python_shutting_down, offload
from distributed.versions import get_versions

logger = logging.getLogger(__name__)

DEFAULT_PORT = 8786
WORKER_SERVICE_ENDPOINT = os.environ.get("WORKER_SERVICE_ENDPOINT", "ws://127.0.0.1:8080")


class ServerlessSchedulerService(Server):
    schedulers = {}
    client_schedulers = {}
    client_comms = {}
    clients = {}
    client_plugins = {}
    worker_versions = {}

    def __init__(self, address):
        handlers = {
            "register-client": self.register_client,
            "gather": self.gather,
            "terminate": self.terminate,
            "register_scheduler_plugin": self.register_scheduler_plugin,
            "run_function": self.run_function
        }

        stream_handlers = {
            "update-graph": self.update_graph,
            # "client-desires-keys": self.client_desires_keys,
            # "update-data": self.update_data,
            # "report-key": self.report_on_key,
            "client-releases-keys": self.client_releases_keys,
            "heartbeat-client": self.client_heartbeat,
            "close-client": self.remove_client,
            "subscribe-topic": self.subscribe_topic,
            # "unsubscribe-topic": self.unsubscribe_topic,
            # "cancel-keys": self.stimulus_cancel,
        }

        self.connection_args = {"handshake_overrides": {  # common denominator
            "pickle-protocol": 4
        }}

        self._start_address = address

        self.scheduler_versions = get_versions()

        Server.__init__(self, handlers=handlers, stream_handlers=stream_handlers)

    # ----------------
    # Handling clients
    # ----------------

    async def register_client(self, comm: Comm, client: str, versions: dict[str, Any]):
        assert client is not None
        comm.name = "Scheduler->Client"
        logger.info("Receive client connection: %s", client)
        # self.log_event(["all", client], {"action": "add-client", "client": client})
        self.clients[client] = ClientState(client, versions=versions)

        # for plugin in list(self.plugins.values()):
        #     try:
        #         plugin.add_client(scheduler=self, client=client)
        #     except Exception as e:
        #         logger.exception(e)

        try:
            bcomm = BatchedSend(interval="2ms", loop=self.loop)
            bcomm.start(comm)
            self.client_comms[client] = bcomm
            msg = {"op": "stream-start"}
            version_warning = version_module.error_message(
                version_module.get_versions(),
                {},
                versions,
            )
            msg.update(version_warning)
            bcomm.send(msg)

            try:
                await self.handle_stream(comm=comm, extra={"client": client})
            finally:
                self.remove_client(client=client, stimulus_id=f"remove-client-{time.time()}")
                logger.debug("Finished handling client %s", client)
                # No need to close scheduler here, client will call terminate
        finally:
            if not comm.closed():
                self.client_comms[client].send({"op": "stream-closed"})
            try:
                if not is_python_shutting_down():
                    await self.client_comms[client].close()
                    del self.client_comms[client]
                    if self.status == Status.running:
                        logger.info("Close client connection: %s", client)
            except TypeError:  # comm becomes None during GC
                pass

    def remove_client(self, client: str, stimulus_id: str | None = None) -> None:
        if client not in self.clients:
            logger.info("Remove client: %s", client)
            self.client_schedulers[client].remove_client(client)
            del self.client_schedulers[client]
            del self.clients[client]

    def client_releases_keys(self, keys: set[Key], client: str):
        self.client_schedulers[client].client_releases_keys(keys, client)

    async def subscribe_topic(self, topic: str, client: str):
        logger.info("Client %s topic subscription: %s", client, topic)

    def client_heartbeat(self, client: str) -> None:
        cs = self.clients[client]
        cs.last_seen = time.time()

    # ---------------------
    # Handling schedulers
    # ---------------------

    async def update_graph(
            self,
            client: str,
            graph_header: dict,
            graph_frames: list[bytes],
            keys: set[Key],
            internal_priority: dict[Key, int] | None,
            submitting_task: Key | None,
            user_priority: int | dict[Key, int] = 0,
            actors: bool | list[Key] | None = None,
            fifo_timeout: float = 0.0,
            code: tuple[SourceCode, ...] = (),
            annotations: dict | None = None,
            stimulus_id: str | None = None,
    ):
        if client in self.client_schedulers:
            logger.error("Client %s already has a running scheduler, updating graph directly", client)
            await self.client_schedulers[client].update_graph(
                client, graph_header, graph_frames, keys, internal_priority,
                submitting_task, user_priority, actors, fifo_timeout, code, annotations, stimulus_id
            )
            return

        start = time.time()
        req_uuid = client.replace("Client-", "")
        scheduler_id = f"Scheduler-{req_uuid}"
        logger.info("======================= SCHEDULER START %s =======================", scheduler_id)
        # logger.info("Bootstrap scheduler for %s", scheduler_id)
        cs = self.clients[client]
        try:
            t0 = time.perf_counter()

            # TODO Setup these parameters based on num of CPUs and worker specs
            nworkers = int(os.environ.get("N_WORKERS", 3))
            nthreads = int(os.environ.get("N_THREADS", 1))
            # memory_limit = int(os.environ.get("MEMORY_LIMIT", 2147483648))  # 2GB
            memory_limit = int(os.environ.get("MEMORY_LIMIT", 4294967296))  # 4GB
            logger.info("Going to deploy %d workers with %d threads and %d memory limit",
                        nworkers, nthreads, memory_limit)

            # Bootstrap scheduler for this DAG run
            port = random.randint(49152, 65535)
            scheduler = ServerlessScheduler(
                client=cs,
                client_comm=self.client_comms[client],
                host="0.0.0.0",
                port=port,
                protocol="tcp",
                dashboard=False,

            )
            scheduler.client_comms[client] = self.client_comms[client]
            self.schedulers[scheduler_id] = scheduler
            self.client_schedulers[client] = scheduler
            await scheduler

            # Add plugins to scheduler
            if client in self.client_plugins:
                for plugin, name, idempotent in self.client_plugins[client]["scheduler"]:
                    logger.info("Registering scheduler plugin %s for running scheduler %s", name, scheduler_id)
                    await scheduler.register_scheduler_plugin(plugin, name, idempotent)
                for plugin, name, idempotent in self.client_plugins[client]["worker"]:
                    logger.info("Registering worker plugin %s for running scheduler %s", name, scheduler_id)
                    await scheduler.register_worker_plugin(plugin, name, idempotent)
                del self.client_plugins[client]

            # Materialize DAG
            # We do not call Scheduler.update_graph directly because we want to have the DAG here
            # in order to calculate the number of workers needed for this DAG
            try:
                graph = deserialize(graph_header, graph_frames).data
                del graph_header, graph_frames
            except Exception as e:
                msg = """\
                    Error during deserialization of the task graph. This frequently
                    occurs if the Scheduler and Client have different environments.
                    For more information, see
                    https://docs.dask.org/en/stable/deployment-considerations.html#consistent-software-environments
                """
                raise RuntimeError(textwrap.dedent(msg)) from e
            (
                dsk,
                dependencies,
                annotations_by_type,
            ) = await offload(
                _materialize_graph,
                graph=graph,
                global_annotations=annotations or {},
            )
            del graph
            if not internal_priority:
                # Removing all non-local keys before calling order()
                dsk_keys = set(
                    dsk
                )  # intersection() of sets is much faster than dict_keys
                stripped_deps = {
                    k: v.intersection(dsk_keys)
                    for k, v in dependencies.items()
                    if k in dsk_keys
                }
                internal_priority = await offload(
                    dask.order.order, dsk=dsk, dependencies=stripped_deps
                )

            # Add WorkerState to scheduler
            scheduler.bootstrap_workers(WORKER_SERVICE_ENDPOINT, nworkers,
                                        nthreads, memory_limit, self.scheduler_versions)

            # Enqueue tasks to scheduler
            scheduler._create_taskstate_from_graph(
                dsk=dsk,
                client=client,
                dependencies=dependencies,
                keys=set(keys),
                ordered=internal_priority or {},
                submitting_task=submitting_task,
                user_priority=user_priority,
                actors=actors,
                fifo_timeout=fifo_timeout,
                code=code,
                annotations_by_type=annotations_by_type,
                # FIXME: This is just used to attach to Computation
                # objects. This should be removed
                global_annotations=annotations,
                start=start,
                stimulus_id=stimulus_id or f"update-graph-{start}",
            )

            # logger.debug("Waiting for scheduler to finish")
            # await scheduler.finished()
        except RuntimeError as e:
            logger.error(str(e))
            err = error_message(e)
            for key in keys:
                self.report(
                    {
                        "op": "task-erred",
                        "key": key,
                        "exception": err["exception"],
                        "traceback": err["traceback"],
                    },
                    # This informs all clients in who_wants plus the current client
                    # (which may not have been added to who_wants yet)
                    client=client,
                )
        end = time.time()
        self.digest_metric("update-graph-duration", end - start)

    async def gather(self, keys, client, serializers=None):
        return await self.client_schedulers[client].gather(keys, serializers=serializers)

    async def terminate(self, client, reason=""):
        if client not in self.client_schedulers:
            logger.error("Client %s not found", client)
            return

        scheduler_id = self.client_schedulers[client].id
        logger.info("Client %s called terminate for scheduler %s", client, scheduler_id)
        await self.client_schedulers[client].close(reason=reason)
        logger.info("======================= SCHEDULER END %s =======================", scheduler_id)

    def register_scheduler_plugin(self, client, plugin, name=None, idempotent=None):
        if client not in self.clients:
            raise ValueError(f"Client {client} not found")
        if client in self.client_schedulers:
            logger.info("Registering plugin %s for running scheduler %s", name,
                        self.client_schedulers[client].id)
            self.client_schedulers[client].register_scheduler_plugin(plugin, name, idempotent)
        else:
            if client not in self.client_plugins:
                self.client_plugins[client] = {"scheduler": [], "worker": []}
            self.client_plugins[client]["scheduler"].append((plugin, name, idempotent))
            logger.info("Saved plugin %s for client %s, will register it later...", name, client)

    def run_function(self, client, function, args, kwargs, wait):
        return self.client_schedulers[client].run_function(
            comm=None, function=function, args=args, kwargs=kwargs, wait=wait
        )

    # ---------------------
    # Service management
    # ---------------------

    async def start_unsafe(self):
        await self.listen(
            self._start_address,
            allow_offload=False,
            handshake_overrides={"pickle-protocol": 4, "compression": None},
        )

        for listener in self.listeners:
            logger.info("Scheduler service listening at %s", listener.contact_address)

        return self
