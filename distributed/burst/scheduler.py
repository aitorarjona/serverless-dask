from __future__ import annotations

import asyncio
import inspect
import logging
import time
from typing import TYPE_CHECKING

import tornado.ioloop
from kubernetes_asyncio import client, config, watch
from kubernetes_asyncio.client import ApiClient

from distributed import Scheduler, Status
from distributed.batched import BatchedSend
from distributed.comm import Comm, get_address_host
from distributed.comm.addressing import address_from_user_args
from distributed.comm.core import connect_with_retry, connect
from distributed.counter import Counter
from distributed.scheduler import ClientState, WorkerState

if TYPE_CHECKING:
    # TODO import from typing (requires Python >=3.10)
    # TODO import from typing (requires Python >=3.11)
    pass

logger = logging.getLogger(__name__)

WORKER_CONTAINER_IMAGE = "docker.io/aitorarjona/rapidask-k8s:dev"
K8S_NAMESPACE = "default"

WORKER_SERVICE_PORT = 8888
WORKER_SERVICE_PROTOCOL = "tcp"


def _get_initial_heartbeat_metrics(bandwidth=100000000,
                                   net_read_bps=0.0, net_write_bps=0.0,
                                   disk_read_bps=0.0, disk_write_bps=0.0):
    return {'task_counts': Counter(),
            'bandwidth': {'total': bandwidth, 'workers': {}, 'types': {}},
            'digests_total_since_heartbeat': {'tick-duration': 0.0,
                                              'latency': 0.0}, 'managed_bytes': 0,
            'spilled_bytes': {'memory': 0, 'disk': 0},
            'transfer': {'incoming_bytes': 0, 'incoming_count': 0,
                         'incoming_count_total': 0,
                         'outgoing_bytes': 0, 'outgoing_count': 0,
                         'outgoing_count_total': 0},
            'event_loop_interval': 0.0, 'cpu': 0.0, 'memory': 0,
            'time': time.time(),
            'host_net_io': {'read_bps': net_read_bps, 'write_bps': net_write_bps},
            'host_disk_io': {'read_bps': disk_read_bps, 'write_bps': disk_write_bps}, 'num_fds': 0}


async def _deploy_k8s_replicaset(cluster_name, replicas, cpu, memory):
    # await config.load_kube_config()
    config.load_incluster_config()

    metadata = client.V1ObjectMeta(name=cluster_name, labels={"app": cluster_name})

    worker_container = client.V1Container(
        name="worker",
        image=WORKER_CONTAINER_IMAGE,
        ports=[client.V1ContainerPort(container_port=8888)],
        resources=client.V1ResourceRequirements(
            limits={
                "cpu": cpu,
                "memory": memory,
            },
            requests={
                "cpu": cpu,
                "memory": memory,
            },
        ),
        image_pull_policy="Always",
        command=["python"],
        args=["-m", "distributed.burst.deploy.worker"]
    )

    template_metadata = client.V1ObjectMeta(labels={"app": cluster_name})
    template_spec = client.V1PodSpec(containers=[worker_container])
    pod_template = client.V1PodTemplateSpec(
        metadata=template_metadata,
        spec=template_spec
    )

    replicaset_spec = client.V1ReplicaSetSpec(
        replicas=replicas,
        selector={"matchLabels": {"app": cluster_name}},
        template=pod_template
    )
    replicaset = client.V1ReplicaSet(
        api_version="apps/v1",
        kind="ReplicaSet",
        metadata=metadata,
        spec=replicaset_spec
    )

    async with ApiClient() as api:
        apps_api = client.AppsV1Api(api)
        logger.debug("Creating replicaset %s...", cluster_name)
        await apps_api.create_namespaced_replica_set(namespace=K8S_NAMESPACE, body=replicaset)
        logger.debug("Replicaset created successfully")

    w = watch.Watch()
    core_api = client.CoreV1Api()

    replicas_ready = 0
    async for event in w.stream(core_api.list_namespaced_pod, namespace=K8S_NAMESPACE, label_selector=f"app={cluster_name}"):
        pod = event["object"]

        if pod.status.phase == "Failed" or pod.status.phase == "Unknown":
            logger.error(pod)
            raise Exception(f"Pod {pod.metadata.name} failed to schedule")

        if pod.status.phase == "Running" and pod.status.pod_ip:
            pod_name = pod.metadata.name
            pod_ip = pod.status.pod_ip
            logger.info("Worker %s running - IP: %s", pod_name, pod_ip)
            replicas_ready += 1
            yield pod_name, pod_ip
            if replicas_ready == replicas:
                logger.info("All workers running")
                w.stop()

    await w.close()
    # await core_api.close()


class K8sReplicasetBurstScheduler(Scheduler):
    def __init__(self,
                 client: ClientState,
                 client_comm: Comm,
                 **kwargs):
        super().__init__(**kwargs)

        self.clients[client.client_key] = client
        self.client_comms[client] = client_comm
        self.id = client.client_key.replace("Client", "Scheduler")
        self.replicaset_name = self.id.replace("Scheduler-", "rapidask-cluster-")

    async def bootstrap_workers(self, pool_size, nthreads, memory_limit, versions):
        # Bootstraps burstable workers deploying them all at once as a K8s replicaset
        start = time.time()
        stimulus_id = f"bootstrap_workers-{start}"

        tasks = []
        async for pod_name, pod_ip in _deploy_k8s_replicaset(self.replicaset_name, pool_size, 0.125, "256M"):
            worker_address = address_from_user_args(host=pod_ip, port=WORKER_SERVICE_PORT, protocol=WORKER_SERVICE_PROTOCOL)
            # Contact workers asynchronously
            task = asyncio.create_task(self._bootstrap_worker(worker_address, pod_name, nthreads, memory_limit))
            tasks.append(task)

        # worker_comms = await asyncio.gather(*tasks, return_exceptions=True)

        for task in asyncio.as_completed(tasks):
            # This is done sequentially (outside async function) to avoid race conditions when updating cluster state
            worker_name, worker_address, worker_comm = await task

            ws = WorkerState(
                address=worker_address,
                status=Status.running,
                pid=0,
                name=worker_name,
                nthreads=nthreads,
                memory_limit=memory_limit,
                local_directory=f"/tmp/dask-worker-space/{worker_name}",
                nanny=None,
                server_id=worker_name,
                services={},
                versions=versions,
                extra={},
                scheduler=self,
            )

            self.workers[worker_address] = ws
            self.aliases[worker_name] = worker_address
            self.running.add(ws)
            self.total_nthreads += ws.nthreads

            host = get_address_host(worker_address)
            dh = self.host_info.get(host)
            if dh is None:
                self.host_info[host] = dh = {}

            dh_addresses = dh.get("addresses")
            if dh_addresses is None:
                dh["addresses"] = dh_addresses = set()
                dh["nthreads"] = 0

            dh_addresses.add(worker_address)
            dh["nthreads"] += nthreads

            # "Fake" worker heartbeat, needed to initialize some values in WorkerState
            self.heartbeat_worker(
                address=worker_address,
                resolve_address=False,
                resources=None,
                host_info=None,
                metrics=_get_initial_heartbeat_metrics(),
                executing={},
                extensions={},
            )

            # Update idle state of the new worker
            self.check_idle_saturated(ws)

            # self.transitions(
            #     self.bulk_schedule_unrunnable_after_adding_worker(ws), stimulus_id
            # )

            # Update plugins
            for plugin in list(self.plugins.values()):
                try:
                    result = plugin.add_worker(scheduler=self, worker=worker_address)
                    if result is not None and inspect.isawaitable(result):
                        raise Exception("Plugin add_worker should not return awaitable")
                except Exception as e:
                    logger.exception(e)

            bs = BatchedSend(interval="5ms", loop=self.loop)
            bs.start(worker_comm)
            self.stream_comms[worker_address] = bs

            # Start handle_worker task, it will keep running listening for worker RPCs until the worker is removed
            tornado.ioloop.IOLoop.current().add_callback(self.handle_worker, worker_comm, worker_address)

        # Trigger cluster state update only once, when all workers are ready
        self.total_nthreads_history.append((time.time(), self.total_nthreads))
        self.stimulus_queue_slots_maybe_opened(stimulus_id=stimulus_id)

    async def _bootstrap_worker(self, address, name, nthreads, memory_limit):
        logger.debug("Contacting worker %s at %s", name, address)
        comm = await connect(address, timeout="10s")
        logger.debug("Comm to %s worker successful", name)

        await comm.write({"op": "bootstrap-worker",
                          "name": name,
                          "nthreads": nthreads,
                          "memory_limit": memory_limit,
                          "scheduler_address": self.address,
                          "reply": False})
        res = await comm.read()
        logger.debug(str(res))

        return res["name"], res["contact_address"], comm
