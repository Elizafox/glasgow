"""Adapt Twisted WebSocket callbacks and flow control to asyncio coroutines."""

import asyncio
import json

from twisted.web.websocket import WebSocketResource as TwistedWebSocketResource


class WebSocketResource(TwistedWebSocketResource):
    def __init__(self, handler, logger):
        self._handler = handler
        self._logger = logger
        self._tasks = set()
        super().__init__(self)

    def buildProtocol(self, request):
        return _WebSocket(self, request)

    async def close(self):
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class _WebSocket:
    def __init__(self, resource, request):
        self._resource = resource
        self._request = request
        forwarded = request.getHeader("X-Forwarded-For")
        self.remote = (forwarded.split(",")[0].strip() if forwarded
                       else request.getClientAddress().host)
        self._messages = asyncio.Queue(maxsize=2)
        self._writable = asyncio.Event()
        self._writable.set()
        self._task = None
        self._closed = False

    def negotiationStarted(self, transport):
        self._transport = transport

    def negotiationFinished(self):
        # The HTTP channel remains registered as the producer after an upgrade.
        self._transport.detachProducer()
        self._transport.attachProducer(self)
        self._task = asyncio.create_task(self._run())
        self._resource._tasks.add(self._task)
        self._task.add_done_callback(self._resource._tasks.discard)

    async def _run(self):
        try:
            await self._resource._handler(self._request, self)
        except ConnectionError:
            pass
        except Exception:
            self._resource._logger.exception("web: WebSocket handler failed")
            await self.close(code=1011)
        finally:
            await self.close()

    def connectionLost(self, reason):
        self._closed = True
        self._writable.set()
        if self._task is not None:
            self._task.cancel()

    def _received(self, message):
        # Twisted delivers assembled messages; this limits accepted uploads, not its buffer.
        size = len(message.encode()) if isinstance(message, str) else len(message)
        if size > 4 * 1024 * 1024:
            self._transport.loseConnection(code=1009)
            return
        try:
            self._messages.put_nowait(message)
        except asyncio.QueueFull:
            self._transport.loseConnection(code=1009)

    def textMessageReceived(self, message):
        self._received(message)

    def bytesMessageReceived(self, message):
        self._received(bytes(message))

    def pongReceived(self, payload):
        pass

    def pauseProducing(self):
        self._writable.clear()

    def resumeProducing(self):
        self._writable.set()

    def stopProducing(self):
        self.connectionLost(None)

    async def receive(self):
        return await self._messages.get()

    async def send_json(self, data):
        await self._writable.wait()
        if self._closed:
            raise ConnectionError("WebSocket is closed")
        self._transport.sendTextMessage(json.dumps(data))

    async def send_bytes(self, data):
        await self._writable.wait()
        if self._closed:
            raise ConnectionError("WebSocket is closed")
        self._transport.sendBytesMessage(data)

    async def close(self, *, code=1000, message=""):
        if not self._closed:
            self._closed = True
            self._writable.set()
            self._transport.detachProducer()
            reason = message.encode()[:123].decode(errors="ignore")
            self._transport.loseConnection(code=code, reason=reason)
