FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY distributed/ /app/distributed/
COPY pyproject.toml /app/
RUN pip install --upgrade pip
RUN pip install -e .
RUN pip install --no-cache-dir \
    aio-pika==9.4.1 \
    aioredis==2.0.1 \
    websockets==12.0
