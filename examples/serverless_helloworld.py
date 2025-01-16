import time

from distributed import Client

if __name__ == "__main__":
    # create dask client
    client = Client("ws://127.0.0.1:8786", connection_limit=1)

    # input("Press Enter to continue...")

    def say_hello(name):
        print("Going to sleep...")
        time.sleep(5)
        print("Done!")
        return f"Hello {name}!"

    # submit a task to the scheduler
    future = client.submit(say_hello, "world")
    res = future.result()
    print(res)

    input("Press Enter to continue...")
