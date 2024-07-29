FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY ../distributed /app/distributed/
COPY ../pyproject.toml /app/
COPY ../serverless_debugging/ /app/serverless_debugging/
RUN pip install --upgrade pip
RUN pip install -e .
RUN pip install --no-cache-dir \
    aio-pika==9.4.1 \
    aioredis==2.0.1 \
    websockets==12.0 \
    s3fs==2024.6.1 \
    dask==2024.4.2 \
    dask[array]==2024.4.2 \
    dask[dataframe]==2024.4.2
