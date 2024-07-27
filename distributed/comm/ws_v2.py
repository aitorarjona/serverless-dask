from __future__ import annotations

import asyncio
import struct
from functools import partial

from websockets.server import serve
from websockets.client import connect
from websockets.exceptions import ConnectionClosedOK

from distributed.comm import get_address_host_port, unparse_host_port
from distributed.comm.core import (
    BaseListener,
    Comm,
    Connector, CommClosedError, )
from distributed.comm.registry import Backend, backends
from distributed.comm.tcp import BaseTCPBackend
from distributed.comm.utils import from_frames, to_frames
from distributed.comm.ws import BIG_BYTES_SHARD_SIZE


class WSHandler:
    pass


async def server_handler(websocket, handler_comm, listener, deserialize, allow_offload):
    comm = WebSocketsV2(websocket, deserialize=deserialize, allow_offload=allow_offload)
    await listener.on_connection(comm)
    await handler_comm(comm)
    await asyncio.Future()


class WebSocketsV2(Comm):
    def __init__(self, websocket, deserialize, allow_offload):
        self.websocket = websocket
        self.deserialize = deserialize
        self.allow_offload = allow_offload
        Comm.__init__(self)

    async def read(self, deserializers=None):
        try:
            n_frames = await self.websocket.recv()
        except ConnectionClosedOK:
            raise CommClosedError()
        n_frames = struct.unpack("Q", n_frames)[0]
        frames = [(await self.websocket.recv()) for _ in range(n_frames)]
        return await from_frames(
            frames,
            deserialize=self.deserialize,
            deserializers=deserializers,
            allow_offload=self.allow_offload,
        )

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
        await self.websocket.send(n)
        for frame in frames:
            if type(frame) is not bytes:
                frame = bytes(frame)
            await self.websocket.send(frame)
            nbytes_frames += len(frame)

        return nbytes_frames

    async def close(self):
        await self.websocket.close()
        await self.websocket.wait_closed()

    def abort(self):
        raise NotImplementedError()

    def closed(self):
        return self.websocket.closed

    @property
    def local_address(self) -> str:
        addr, port = self.websocket.local_address
        return f"ws://{addr}:{port}"

    @property
    def peer_address(self) -> str:
        addr, port = self.websocket.remote_address
        return f"ws://{addr}:{port}"


class WebSocketV2Listener(BaseListener):
    prefix = "ws"

    def __init__(self, loc, handle_comm, deserialize, **connection_args):
        self.loc = loc
        self.handle_comm = handle_comm
        self.deserialize = deserialize
        self.connection_args = connection_args
        self.server = None

        BaseListener.__init__(self)

    async def start(self):
        host, port = get_address_host_port(self.loc, strict=False)
        handler = partial(server_handler, handler_comm=self.handle_comm, listener=self, deserialize=self.deserialize,
                          allow_offload=True)
        self.server = await serve(ws_handler=handler, host=host, port=port, open_timeout=None,
                                  close_timeout=None, max_size=None, max_queue=None)

    def stop(self):
        self.server.close()

    def get_host_port(self):
        return get_address_host_port(self.loc, strict=False)

    @property
    def listen_address(self):
        return self.prefix + unparse_host_port(*self.get_host_port())

    @property
    def contact_address(self):
        host, port = self.get_host_port()
        return self.prefix + unparse_host_port(host, port)


class WebSocketV2Connector(Connector):
    async def connect(self, address, deserialize=True, **connection_args):
        websocket = await connect(f"ws://{address}", open_timeout=None, close_timeout=None, max_size=None, max_queue=None)
        return WebSocketsV2(websocket, deserialize=deserialize, allow_offload=True)


class WebSocketV2Backend(BaseTCPBackend):
    def get_connector(self):
        return WebSocketV2Connector()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        return WebSocketV2Listener(loc, handle_comm, deserialize, **connection_args)


backends["ws"] = WebSocketV2Backend()
