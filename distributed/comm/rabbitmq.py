from __future__ import annotations

import logging
import uuid

import aio_pika
import dask
from aio_pika import ExchangeType, Message
from aio_pika.abc import AbstractIncomingMessage
from tornado.queues import Queue

from distributed.comm.core import (
    BaseListener,
    Comm,
    Connector, CommClosedError,
)
from distributed.comm.registry import Backend, backends
from distributed.comm.utils import from_frames, to_frames

logger = logging.getLogger(__name__)

BIG_BYTES_SHARD_SIZE = dask.utils.parse_bytes(
    dask.config.get("distributed.comm.rabbitmq.shard")
)
EXCHANGE = dask.config.get("distributed.comm.rabbitmq.exchange")
DURABLE = False
AUTO_DELETE = True
EXCLUSIVE = False


def address_to_queue_name(address: str):
    """Translate a "tcp-like address" address/uri (e.g. amqp://queuename:port) to a queue name, where the
    host is the queue name, the port is ignored"""
    if address.startswith("amqp://"):
        address = address.split("://")[1]
    if ":" in address:
        address = address.split(":")[0]
    return address


def queue_name_to_address(queue_name: str):
    """Translate a queue name to a "tcp-like address", where the queue name is the host,
     and the port is always 0 (e.g. amqp://queuename:0)"""
    return f"amqp://{queue_name}:0"


class RabbitMQ(Comm):
    """
    A RabbitMQ Comm to communicate two peers.
    For bidirectional communication, we create two queues: the consumer queue, in which this client will
    consume messages sent from the remote peer, and the publisher queue, in which the client will publish messages,
    which the remote peer will consume.
    """

    def __init__(self, consumer_queue, peer_routing_key, exchange, deserialize):
        self.buffer = Queue()
        self._consumer_queue = consumer_queue
        self._peer_routing_key = peer_routing_key
        self._consumer_tag = None
        self._exchange = exchange
        self._closed = False
        Comm.__init__(self, deserialize)

    async def init(self):
        # Declare the peer's queue
        await self._consumer_queue.bind(self._exchange)

        self._consumer_tag = await self._consumer_queue.consume(self._on_message)

    async def read(self, deserializers=None):
        try:
            msg: AbstractIncomingMessage = await self.buffer.get()
            if msg is None or not msg.properties.headers or "n_frames" not in msg.properties.headers:
                # Connection is closed
                self.abort()
                raise CommClosedError()
            n_frames = msg.properties.headers["n_frames"]
        except Exception as e:
            logger.error("Error reading message from queue %s", self._consumer_queue.name)
            raise CommClosedError(e)

        frames = [msg.body]
        if n_frames > 1:
            for _ in range(n_frames - 1):
                frames.append((await self.buffer.get()).body)
        msg = await from_frames(
            frames,
            deserialize=self.deserialize,
            deserializers=deserializers,
            allow_offload=self.allow_offload,
        )
        # logger.debug("Received message from queue %s", self._consumer_queue.name)
        return msg

    async def write(self, msg, serializers=None, on_error=None):
        # logger.debug("Sending message to queue %s --> %s", self._peer_routing_key, str(msg))
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
        nbytes_frames = 0
        n_frames = len(frames)
        try:
            for frame in frames:
                if type(frame) is not bytes:
                    frame = bytes(frame)
                message = Message(body=frame, headers={"n_frames": n_frames})
                await self._exchange.publish(message, routing_key=self._peer_routing_key)
                nbytes_frames += len(frame)
        except Exception as e:
            raise CommClosedError(e)

        # logger.debug("Sent %d messages to %s", len(frames), self._peer_routing_key)
        return nbytes_frames

    async def close(self):
        logger.debug("Closing Comm %s", self._consumer_queue.name)
        self._closed = True

    def abort(self):
        logger.debug("Abort Comm %s", self._consumer_queue.name)

    def closed(self):
        return self._closed

    @property
    def local_address(self) -> str:
        return queue_name_to_address(self._consumer_queue.name)

    @property
    def peer_address(self) -> str:
        return queue_name_to_address(self._peer_routing_key)

    async def _on_message(self, message: AbstractIncomingMessage):
        async with message.process():
            await self.buffer.put(message)


class RabbitMQListener(BaseListener):
    prefix = "amqp"

    def __init__(self, loc, handle_comm, deserialize, **connection_args):
        self._listener_queue = None
        self._consumer_tag = None
        self._exchange = None
        self._channel = None
        self._connection = None
        self._queue_name = loc
        self._comm_handler = handle_comm
        self._deserialize = deserialize
        self._connection_args = connection_args

        BaseListener.__init__(self)

    async def start(self):
        logger.debug(f"Starting RabbitMQ listener on queue {self._queue_name}")
        amqp_url = dask.config.get("distributed.comm.rabbitmq.amqp_url")
        # logger.debug("Connecting to RabbitMQ server on %s", amqp_url)
        self._connection = await aio_pika.connect(amqp_url)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=10)

        self._exchange = await self._channel.declare_exchange(
            EXCHANGE, ExchangeType.DIRECT,
        )

        self._listener_queue = await self._channel.declare_queue(self._queue_name, exclusive=EXCLUSIVE, durable=DURABLE,
                                                                 auto_delete=AUTO_DELETE)

        await self._listener_queue.bind(self._exchange)
        self._consumer_tag = await self._listener_queue.consume(self._on_message)

    def stop(self):
        pass
        # logger.debug(f"Stopping RabbitMQ listener on queue {self._queue_name}")
        # sync(IOLoop.current(), self._listener_queue.cancel, self._consumer_tag)
        # logger.debug(f"Stopped RabbitMQ listener on queue {self._queue_name}")

    @property
    def listen_address(self):
        return queue_name_to_address(self._queue_name)

    @property
    def contact_address(self):
        return queue_name_to_address(self._queue_name)

    async def _on_message(self, message: AbstractIncomingMessage):
        async with message.process():
            logger.debug(f"Received new listener connection request --> {message.reply_to}")
            conn_id = message.reply_to
            # Declare this server queue for the peer (client) to send messages to this server
            server_queue = await self._channel.declare_queue(conn_id + "-server", durable=DURABLE,
                                                             exclusive=EXCLUSIVE, auto_delete=AUTO_DELETE)
            await server_queue.bind(self._exchange)
            # Declare the peer's (client) queue for this server to send messages to the peer
            client_queue = await self._channel.declare_queue(conn_id + "-client", durable=DURABLE,
                                                             exclusive=EXCLUSIVE, auto_delete=AUTO_DELETE)
            await client_queue.bind(self._exchange)
            # message = Message(reply_to=consumer_queue, body=b"")
            # self._exchange.publish(message, routing_key=message.reply_to)

        comm = RabbitMQ(consumer_queue=server_queue, peer_routing_key=conn_id + "-client",
                        exchange=self._exchange, deserialize=self._deserialize)
        await comm.init()
        await self.on_connection(comm)
        await self._comm_handler(comm)


class RabbitMQConnector(Connector):
    async def connect(self, address, deserialize=True, **connection_args):
        amqp_url = dask.config.get("distributed.comm.rabbitmq.amqp_url")
        # logger.debug("Connecting to RabbitMQ server on %s", amqp_url)
        connection = await aio_pika.connect(amqp_url)
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=10)

        exchange = await channel.declare_exchange(
            EXCHANGE, ExchangeType.DIRECT,
        )

        listener_queue_name = address_to_queue_name(address)
        conn_id = listener_queue_name + "-conn-" + uuid.uuid4().hex[:8]

        # Declare and bind the peer queue
        logger.debug(f"Connecting to queue {listener_queue_name}")

        # Declare and the listener queue
        listener_queue = await channel.declare_queue(listener_queue_name, durable=DURABLE, exclusive=EXCLUSIVE,
                                                     auto_delete=AUTO_DELETE)
        await listener_queue.bind(exchange)

        # Declare and the peer's (server) queue
        server_queue = await channel.declare_queue(conn_id + "-server", durable=DURABLE, exclusive=EXCLUSIVE,
                                                   auto_delete=AUTO_DELETE)
        await server_queue.bind(exchange)

        # Declare and bind a queue for the peer (server) to reply to this client
        client_queue = await channel.declare_queue(conn_id + "-client", durable=DURABLE, exclusive=EXCLUSIVE,
                                                   auto_delete=AUTO_DELETE)
        await client_queue.bind(exchange)

        # Send a message to the peer with the connection id
        msg = Message(reply_to=conn_id, body=b"")
        await exchange.publish(msg, routing_key=listener_queue_name)

        comm = RabbitMQ(consumer_queue=client_queue, peer_routing_key=conn_id + "-server",
                        exchange=exchange, deserialize=deserialize)
        await comm.init()

        return comm


class RabbitMQBackend(Backend):
    def get_connector(self):
        return RabbitMQConnector()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        listener_queue = address_to_queue_name(loc)
        return RabbitMQListener(listener_queue, handle_comm, deserialize, **connection_args)

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


backends["amqp"] = RabbitMQBackend()
