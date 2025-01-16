import os
import time

from distributed import Client
import dask.bag as db

MAP_SIZE = 3


def sleepy(i):
    t0 = time.time()
    print("Going to sleep...")
    time.sleep(5)
    t1 = time.time()
    print("Done!")
    return (i, os.getenv("HOSTNAME", None), t0, t1)


if __name__ == "__main__":
    client = Client("ws://127.0.0.1:8786", connection_limit=1)
    t0 = time.perf_counter()
    b = db.from_sequence([(i,) for i in range(MAP_SIZE)], npartitions=MAP_SIZE).map(sleepy)
    res = client.compute(b).result()
    t1 = time.perf_counter()
    client.close()
    print(res)
    print("Exec time: ", t1 - t0)

    input("Press Enter to continue...")

    client = Client("ws://127.0.0.1:8786", connection_limit=1)
    t0 = time.perf_counter()
    b = db.from_sequence([(i,) for i in range(MAP_SIZE)], npartitions=MAP_SIZE).map(sleepy)
    res = client.compute(b).result()
    t1 = time.perf_counter()
    client.close()
    print(res)
    print("Exec time: ", t1 - t0)

    input("Press Enter to continue...")

    t0 = time.perf_counter()
    b = db.from_sequence([(i,) for i in range(MAP_SIZE)], npartitions=MAP_SIZE).map(sleepy)
    res = client.compute(b).result()
    t1 = time.perf_counter()
    print(res)
    print("Exec time: ", t1 - t0)

    input("Press Enter to continue...")


