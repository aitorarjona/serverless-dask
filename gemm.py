import time

import dask.array as da

from distributed import Client, get_task_stream


def matrix_multiply(size, chunk):
    A = da.random.random((size, size), chunks=(chunk, chunk))
    B = da.random.random((size, size), chunks=(chunk, chunk))
    return A.dot(B)


if __name__ == "__main__":
    # create dask client
    client = Client("ws://127.0.0.1:8786", connection_limit=1)

    x = matrix_multiply(1000, 250)
    with get_task_stream() as ts:
        x.compute()
