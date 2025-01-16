FROM python:3.12-slim

WORKDIR /rapidask

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt update && apt install -y \
    vim \
    && rm -rf /var/lib/apt/lists/*

COPY ../distributed /rapidask/distributed/
COPY ../pyproject.toml /rapidask/
# COPY ../serverless_debugging/ /rapidask/examples/
RUN pip install --upgrade pip
RUN pip install -e .
RUN pip install --no-cache-dir \
#    aio-pika==9.4.1 \
#    aioredis==2.0.1 \
#     websockets==12.0 \
#     s3fs==2024.6.1 \
    dask==2024.4.2
#     dask[array]==2024.4.2 \
#     dask[dataframe]==2024.4.2
