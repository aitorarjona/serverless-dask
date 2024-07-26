from __future__ import annotations

import asyncio
import json
import logging
import struct
import uuid

import aioredis
import dask
import tornado

from distributed.comm.core import (
    BaseListener,
    Comm,
    Connector, CommClosedError,
)
from distributed.comm.registry import Backend, backends
from distributed.comm.utils import from_frames, to_frames

logger = logging.getLogger(__name__)

BIG_BYTES_SHARD_SIZE = dask.utils.parse_bytes(
    dask.config.get("distributed.comm.redis.shard")
)


def unparse_uri(uri: str):
    if uri.startswith("redis://"):
        uri = uri.split("://")[1]
    if "/" in uri:
        return uri.split("/", 1)
    raise ValueError(f"Invalid URI: {uri}")


def parse_uri(host, list_name):
    if not host.startswith("redis://"):
        host = f"redis://{host}"
    return f"{host}/{list_name}"


class Redis(Comm):
    """
    A Redis Comm to communicate two peers.
    For bidirectional communication, we create two lists: the consumer list, in which this client will
    consume messages sent from the remote peer, and the publisher list, in which the client will publish messages,
    which the remote peer will consume.
    """

    def __init__(self, redis_pool, consumer_key, publisher_key, deserialize):
        self._redis_pool = redis_pool
        self._consumer_key = consumer_key
        self._publisher_key = publisher_key
        Comm.__init__(self, deserialize)

    async def read(self, deserializers=None):
        n_frames = await self._redis_pool.blpop(self._consumer_key)
        if n_frames is None:
            # Connection is closed
            self.abort()
            raise CommClosedError()
        n_frames = struct.unpack("Q", n_frames)[0]

        frames = [(await self._redis_pool.blpop(self._consumer_key)) for _ in range(n_frames)]

        msg = await from_frames(
            frames,
            deserialize=self.deserialize,
            deserializers=deserializers,
            allow_offload=self.allow_offload,
        )
        return msg

    async def write(self, msg, serializers=None, on_error=None):
        frames = await to_frames(
            msg,
            allow_offload=self.allow_offload,
            serializers=serializers,
            on_error=on_error,
            context={
                "sender": self.local_info,
                "recipient": self.remote_info,
                **self.handshake_options,
            },
            frame_split_size=BIG_BYTES_SHARD_SIZE,
        )
        n = struct.pack("Q", len(frames))
        nbytes_frames = 0
        try:
            await self._redis_pool.lpush(self._publisher_key, n)
            for frame in frames:
                if type(frame) is not bytes:
                    frame = bytes(frame)
                await self._redis_pool.lpush(self._publisher_key, frame)
                nbytes_frames += len(frame)
        except Exception as e:
            raise CommClosedError(e)

        return nbytes_frames

    async def close(self):
        # TODO handle close calls to gracefully close the connection
        logger.debug("Closing Comm %s", self._consumer_key.name)
        self._closed = True

    def abort(self):
        # TODO handle abort calls to gracefully close the connection
        logger.debug("Abort Comm %s", self._consumer_key.name)

    def closed(self):
        return self._closed

    @property
    def local_address(self) -> str:
        return parse_uri(self._redis_pool.address, self._consumer_key)

    @property
    def peer_address(self) -> str:
        return parse_uri(self._redis_pool.address, self._publisher_key)


class RedisListener(BaseListener):
    prefix = "redis"

    def __init__(self, loc, handle_comm, deserialize, **connection_args):
        self._loc = loc
        self._handle_comm = handle_comm
        self._deserialize = deserialize
        self._connection_args = connection_args
        self._redis_pool = None

        BaseListener.__init__(self)

    async def start(self):
        list_key, host = unparse_uri(self._loc)
        logger.debug("Connecting to Redis server on %s", host)
        self._redis_pool = aioredis.from_url(f"redis://{host}")

        logger.debug(f"Starting Redis listener on queue {list_key}")
        tornado.ioloop.IOLoop.current().add_callback(self._consume_messages, list_key)

    def stop(self):
        # TODO handle stop calls to gracefully stop the listener
        logger.debug(f"Stopped Redis listener on list {self._loc}")

    @property
    def listen_address(self):
        return self._loc

    @property
    def contact_address(self):
        return self._loc

    async def _consume_messages(self, list_key):
        logger.debug("Consuming messages from Redis list %s", list_key)

        while True:
            raw_msg = await self._redis_pool.blpop(list_key)
            msg = json.loads(raw_msg)

            logger.debug(f'Received new listener connection request --> {msg["conn_id"]}')
            conn_id = msg["conn_id"]

            comm = Redis(self._redis_pool, conn_id + "-server", conn_id + "-client", self._deserialize)
            await self.on_connection(comm)
            await self._handle_comm(comm)


class RedisConnector(Connector):
    async def connect(self, address, deserialize=True, **connection_args):
        host, list_name = unparse_uri(address)
        logger.debug("Connecting to Redis server on %s", host)
        redis_pool = aioredis.from_url(f"redis://{host}")

        logger.debug("Sending connection request to listener on %s", list_name)
        conn_id = list_name + "-conn-" + uuid.uuid4().hex[:8]
        await redis_pool.lpush(list_name, json.dumps({"conn_id": conn_id}))

        comm = Redis(redis_pool, conn_id + "-client", conn_id + "-server", deserialize)
        return comm


class RedisBackend(Backend):
    def get_connector(self):
        return RedisConnector()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        listener_list, _ = unparse_uri(loc)
        return RedisListener(listener_list, handle_comm, deserialize, **connection_args)

    def get_address_host(self, loc):
        return loc

    def resolve_address(self, loc):
        return loc

    def get_local_address_for(self, loc):
        return loc

    def get_address_host_port(self, loc):
        if ":" in loc:
            host, port = loc.split(":")
            return host, int(port)
        else:
            return loc, 0


backends["redis"] = RedisBackend()
