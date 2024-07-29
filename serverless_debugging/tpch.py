import json
import pickle
import time
from uuid import uuid4
from pprint import pprint
import s3fs
from dask.distributed import Client

from dask_queries import *
from distributed import SchedulerPlugin


class TaskWorkerMonitorPlugin(SchedulerPlugin):
    workers = {}
    task_story = {}
    start_time = time.time()

    def add_worker(self, scheduler, worker) -> None:
        if worker in self.workers:
            self.workers[worker]["add"] = time.time()
        else:
            self.workers[worker] = {"add": time.time()}

    def remove_worker(self, scheduler, worker, **kwargs) -> None:
        if worker in self.workers:
            self.workers[worker]["remove"] = time.time()
        else:
            self.workers[worker] = {"remove": time.time()}

    def transition(self, key, start, finish, *args, **kwargs) -> None:
        if key in self.task_story:
            self.task_story[key].append({"start": start, "finish": finish, "timestamp": time.time()})
        else:
            self.task_story[key] = [{"start": start, "finish": finish, "timestamp": time.time()}]

    def reset(self):
        self.task_story = {}


if __name__ == "__main__":
    timestamps = {}
    task_story = {}

    t0 = time.time()
    client = Client("ws://serverless-dask-scheduler-service.default.svc.cluster.local:8786", connection_limit=1)
    t1 = time.time()
    print(f"Time to create cluster: {t1 - t0:.2f} s")
    timestamps["cluster_creation"] = {"t0": t0, "t1": t1}

    s3 = s3fs.S3FileSystem(
        key="lab144",
        secret="astl1a4b4",
        endpoint_url="http://192.168.5.24:9000",
    )

    # Check that s3 is working
    dataset_path = "s3://dask-emartinez/tpch-data/scale-100/"
    s3.ls(dataset_path)

    plugin = TaskWorkerMonitorPlugin()
    client.register_plugin(plugin, name="TaskWorkerMonitorPlugin")


    def get_task_story(dask_scheduler):
        plugin = dask_scheduler.plugins["TaskWorkerMonitorPlugin"]
        data = {
            "task_story": plugin.task_story,
        }
        plugin.reset()
        return data


    def get_workers(dask_scheduler):
        plugin = dask_scheduler.plugins["TaskWorkerMonitorPlugin"]
        data = {
            "workers": plugin.workers,
        }
        return data


    query = query_1
    scale = 100

    t_init = time.time()
    print(f"Evaluating query {query.__name__} {dataset_path} {scale}...")
    q = query(dataset_path, s3, scale)
    print(f"Running query {query.__name__} {dataset_path} {scale}...")
    t0 = time.time()
    q.compute(client)
    t1 = time.time()
    timestamps[query.__name__] = {"t0": t0, "t1": t1}
    task_worker_monitor_data = client.run_on_scheduler(get_task_story)
    task_story[query.__name__] = task_worker_monitor_data
    print(f"Query {query.__name__} took {t1 - t0:.2f} s")
    t_end = time.time()

    # queries = [
    #     (query_3, "s3://tpch-benchmark-coiled/scale-300/", 300),
    #     (query_5, "s3://tpch-benchmark-coiled/scale-100/", 100),
    #     (query_10, "s3://tpch-benchmark-coiled/scale-200/", 200),
    #     (query_1, "s3://tpch-benchmark-coiled/scale-300/", 300),
    #     (query_15, "s3://tpch-benchmark-coiled/scale-300/", 100),
    # ]
    #
    # n_loops = 0
    # t_init = time.time()
    # for query, s3_path, scale in queries:
    #     n_loops += 1
    #     print(f"Evaluating query {query.__name__} {s3_path} {scale}...")
    #     q = query(s3_path, s3, scale)
    #     print(f"Running query {query.__name__} {s3_path} {scale}...")
    #     t0 = time.time()
    #     q.compute(client)
    #     t1 = time.time()
    #     timestamps[query.__name__] = {"t0": t0, "t1": t1}
    #     task_worker_monitor_data = client.run_on_scheduler(get_task_story)
    #     task_story[query.__name__] = task_worker_monitor_data
    #     print(f"Query {query.__name__} took {t1 - t0:.2f} s")
    #     if n_loops == len(queries):
    #         break
    #     print(f"Sleeping...")
    #     # cluster.scale(0)
    #     # cluster.wait_for_workers(0)
    #     time.sleep(30)
    #     # cluster.adapt(minimum=0, maximum=MAX_WORKERS)
    # t_end = time.time()

    print(f"Total time: {t_end - t_init:.2f} s")
    timestamps["total"] = {"t0": t_init, "t1": t_end}

    workers = client.run_on_scheduler(get_workers)

    pprint(task_story)
    pprint(workers)

    key = uuid4().hex[:4]
    print(f"Saving data with key {key}...")

    # with open(f"tasks_{key}.pickle", "wb") as f:
    #     pickle.dump(task_story, f)
    # with open(f"workers_{key}.pickle", "wb") as f:
    #     pickle.dump(workers, f)
    # with open(f"timestamps_{key}.json", "w") as f:
    #     json.dump(timestamps, f, indent=4)

    client.shutdown()
