"""Optional Twisted HTTP support on the caller's asyncio event loop."""

import asyncio
import base64
from io import BytesIO
from urllib.parse import unquote, urlsplit, urlunsplit

from hyperlink import URL
from twisted.internet.asyncioreactor import AsyncioSelectorReactor
from twisted.internet.base import ReactorBase
from twisted.internet.protocol import Protocol
from twisted.internet.task import Cooperator
from twisted.web.client import (
    Agent, BrowserLikeRedirectAgent, FileBodyProducer, GzipDecoder,
    ResponseDone,
)
from twisted.web.http_headers import Headers
from twisted.web.http import PotentialDataLoss


class _AsyncioReactor(AsyncioSelectorReactor):
    def installWaker(self):
        # asyncio handles thread wakeups; this reactor never installs signal handlers.
        pass

    def crash(self):
        # The caller owns the loop, including stopping it.
        ReactorBase.crash(self)


class HTTPContext:
    """Own Twisted connections and DNS workers on the caller's asyncio loop."""

    async def __aenter__(self):
        self.loop = asyncio.get_running_loop()
        self.reactor = _AsyncioReactor(self.loop)
        self.reactor.startRunning(installSignalHandlers=False)
        self.agent = BrowserLikeRedirectAgent(Agent(self.reactor))
        return self

    async def __aexit__(self, *exc):
        stopped = self.loop.create_future()
        self.reactor.addSystemEventTrigger("after", "shutdown", stopped.set_result, None)
        self.reactor.stop()
        try:
            await asyncio.shield(stopped)
        except asyncio.CancelledError:
            await stopped
            raise
        finally:
            for call in self.reactor.getDelayedCalls():
                call.cancel()

    async def request(self, method, url, *, data=None, headers=None):
        headers = dict(headers or {})
        headers.setdefault("Accept-Encoding", "gzip")
        parsed = urlsplit(url)
        if parsed.username is not None:
            credentials = f"{unquote(parsed.username)}:{unquote(parsed.password or '')}"
            headers.setdefault("Authorization",
                               "Basic " + base64.b64encode(credentials.encode()).decode())
            url = urlunsplit(parsed._replace(netloc=parsed.netloc.rsplit("@", 1)[1]))
        cooperator = Cooperator(scheduler=lambda call: self.reactor.callLater(0, call))
        body = None if data is None else FileBodyProducer(BytesIO(data), cooperator=cooperator)
        async with asyncio.timeout(300):
            return await self.agent.request(
                method.encode("ascii"), URL.from_text(url).to_uri().to_text().encode("ascii"),
                Headers({key: [value] for key, value in headers.items()}), body
            ).asFuture(self.loop)

    async def read(self, response):
        # readBody assumes an abortConnection-capable transport, but Agent responses expose
        # a producer proxy.
        # Use its public stopProducing method for cancellation instead
        body = _ResponseBody(self.loop)
        if response.headers.getRawHeaders(b"Content-Encoding") == [b"gzip"]:
            response = GzipDecoder(response)
        response.deliverBody(body)
        async with asyncio.timeout(300):
            try:
                return await body.complete
            finally:
                if body.complete.cancelled():
                    body.transport.stopProducing()


class _ResponseBody(Protocol):
    def __init__(self, loop):
        self.complete = loop.create_future()
        self.chunks = []

    def dataReceived(self, data):
        self.chunks.append(data)

    def connectionLost(self, reason):
        if self.complete.done():
            return
        # HTTP/1.0 can delimit a response by EOF rather than Content-Length
        if reason.check(ResponseDone, PotentialDataLoss):
            self.complete.set_result(b"".join(self.chunks))
        else:
            self.complete.set_exception(reason.value)
