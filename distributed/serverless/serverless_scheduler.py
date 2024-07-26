from __future__ import annotations

import asyncio
import copy
import logging
import time
from typing import TYPE_CHECKING

from distributed import Scheduler, Status
from distributed.batched import BatchedSend
from distributed.comm import Comm
from distributed.comm.core import connect_with_retry
from distributed.counter import Counter
from distributed.scheduler import ClientState, WorkerState

if TYPE_CHECKING:
    # TODO import from typing (requires Python >=3.10)
    # TODO import from typing (requires Python >=3.11)
    pass

logger = logging.getLogger(__name__)

INITIAL_HEARTBEAT_METRICS = {'task_counts': Counter(),
                             'bandwidth': {'total': 100000000, 'workers': {}, 'types': {}},
                             'digests_total_since_heartbeat': {'tick-duration': 0.0,
                                                               'latency': 0.0}, 'managed_bytes': 0,
                             'spilled_bytes': {'memory': 0, 'disk': 0},
                             'transfer': {'incoming_bytes': 0, 'incoming_count': 0,
                                          'incoming_count_total': 0,
                                          'outgoing_bytes': 0, 'outgoing_count': 0,
                                          'outgoing_count_total': 0},
                             'event_loop_interval': 0.0, 'cpu': 0.0, 'memory': 0,
                             'time': time.time(),
                             'host_net_io': {'read_bps': 0, 'write_bps': 0},
                             'host_disk_io': {'read_bps': 0.0, 'write_bps': 0.0}, 'num_fds': 0}


class ServerlessScheduler(Scheduler):
    def __init__(self,
                 client: ClientState,
                 client_comm: Comm,
                 **kwargs):
        super().__init__(**kwargs)

        self.clients[client.client_key] = client
        self.client_comms[client] = client_comm
        self.id = client.client_key.replace("Client", "Scheduler")

    def bootstrap_workers(self, worker_endpoint, n_workers, nthreads, memory_limit, versions):
        start = time.time()
        stimulus_id = f"bootstrap_workers-{start}"

        for i in range(n_workers):
            name = self.id.replace("Scheduler", f"Worker-{i}")
            address = f"{worker_endpoint}/{name}"

            # The comm object is not ready yet but with BatchedSend, the scheduler will be able to
            # enqueue messages to the worker even if it's not established yet.
            # The actual messages which will be sent as soon as the connection is established
            bs = BatchedSend(interval="5ms", loop=self.loop)
            self.stream_comms[address] = bs
            asyncio.create_task(self._spawn_worker_knative(worker_endpoint, address, name, nthreads, memory_limit))

            ws = WorkerState(
                address=address,
                status=Status.running,
                pid=0,
                name=name,
                nthreads=nthreads,
                memory_limit=memory_limit,
                local_directory=f"/tmp/dask-worker-space/{name}",
                nanny=None,
                server_id=name,
                services={},
                versions=versions,
                extra={},
                scheduler=self,
            )

            self.workers[address] = ws
            self.aliases[name] = address
            self.running.add(ws)
            self.total_nthreads += ws.nthreads

            # "Fake" worker heartbeat, needed to initialize some values in WorkerState
            self.heartbeat_worker(
                address=address,
                resolve_address=False,
                resources=None,
                host_info=None,
                metrics=copy.deepcopy(INITIAL_HEARTBEAT_METRICS),
                executing={},
                extensions={},
            )

            # Update idle state of the new worker
            self.check_idle_saturated(ws)

            # self.transitions(
            #     self.bulk_schedule_unrunnable_after_adding_worker(ws), stimulus_id
            # )

        self.total_nthreads_history.append((time.time(), self.total_nthreads))
        self.stimulus_queue_slots_maybe_opened(stimulus_id=stimulus_id)

    async def _spawn_worker_knative(self, worker_endpoint, address, name, nthreads, memory_limit):
        logger.debug("Spawn Knative worker %s", name)
        comm = await connect_with_retry(worker_endpoint, timeout="2s", max_retry=100,
                                        connection_args={"connect_timeout": 2.0,
                                                         "request_timeout": 0.0})
        logger.debug("Comm to %s worker successful", name)

        await comm.write({"op": "start-worker",
                          "address": address,
                          "name": name,
                          "nthreads": nthreads,
                          "memory_limit": memory_limit,
                          "scheduler_address": self.address,
                          "reply": False})
        res = await comm.read()

        self.workers[address].contact_address = res["contact_address"]

        batched_send = self.stream_comms[address]
        batched_send.start(comm)

        # This will keep running until the worker is removed
        await self.handle_worker(comm, address)

    async def update_worker_contact_address(self, worker):
        pass
