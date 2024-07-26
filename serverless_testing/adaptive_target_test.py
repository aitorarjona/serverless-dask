import math

TARGET_DURATION = 5
NUM_TASKS_QUEUED = 250
AVG_TASK_DURATION = 120
NUM_WORKERS = 3
TASKS_RUNNING_IN_WORKERS = 2
NTHREADS_PER_WORKER = 2


def adaptive_target():
    # CPU
    queued_occupancy = AVG_TASK_DURATION * (NUM_TASKS_QUEUED if NUM_TASKS_QUEUED < 100 else 100)
    print(f"Queued occupancy: {queued_occupancy}")

    tasks_ready = NUM_TASKS_QUEUED
    if tasks_ready > 100:
        queued_occupancy *= tasks_ready / 100
    print(f"Queued occupancy: {queued_occupancy}")

    cpu = math.ceil(queued_occupancy / TARGET_DURATION)

    # Avoid a few long tasks from asking for many cores
    for _ in range(NUM_WORKERS):
        if tasks_ready > cpu:
            break
        tasks_ready += TASKS_RUNNING_IN_WORKERS
    else:
        cpu = min(tasks_ready, cpu)

    # Divide by average nthreads per worker
    nthreads = NTHREADS_PER_WORKER * NUM_WORKERS
    cpu = math.ceil(cpu / nthreads * NUM_WORKERS)

    print(f"Workers: {cpu}")
    print(f"Available threads: {cpu * NTHREADS_PER_WORKER}")


if __name__ == "__main__":
    adaptive_target()