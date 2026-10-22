import asyncio
from collections import deque
import importlib.util
import json
import logging
from types import SimpleNamespace
import unittest


@unittest.skipUnless(importlib.util.find_spec("twisted") and importlib.util.find_spec("wsproto"),
                     "HTTP dependencies not installed")
class YamahaWebTestCase(unittest.IsolatedAsyncioTestCase):
    loop_factory = asyncio.SelectorEventLoop

    async def asyncSetUp(self):
        from glasgow.applet.audio.yamaha_opx.web import YamahaOPxWebInterface
        from glasgow.support.http import HTTPContext
        from twisted.web.server import Site
        self.interface = YamahaOPxWebInterface(logging.getLogger(__name__),
            SimpleNamespace(chips=["YM3812"]), None, False)
        self.http = await self.enterAsyncContext(HTTPContext())
        self.root = self.interface.resource()
        self.listener = self.http.reactor.listenTCP(0, Site(self.root, reactor=self.http.reactor),
                                                   interface="127.0.0.1")
        self.port = self.listener.getHost().port
        self.addAsyncCleanup(self.stop)

    async def stop(self):
        await self.listener.stopListening().asFuture(asyncio.get_running_loop())
        for path in (b"queue", b"vgm"):
            await self.root.children[path].close()

    async def connect(self, path):
        from wsproto import WSConnection, ConnectionType
        from wsproto.events import Request, AcceptConnection
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        connection = WSConnection(ConnectionType.CLIENT)
        pending = deque()
        writer.write(connection.send(Request(host=f"127.0.0.1:{self.port}", target=path)))
        self.addAsyncCleanup(writer.wait_closed)
        self.addCleanup(writer.close)

        async def receive():
            while not pending:
                data = await asyncio.wait_for(reader.read(65536), timeout=5)
                if not data:
                    raise EOFError("WebSocket closed without expected message")
                connection.receive_data(data)
                pending.extend(connection.events())
            return pending.popleft()

        self.assertIsInstance(await receive(), AcceptConnection)
        return connection, writer, receive

    async def test_index(self):
        response = await self.http.request("GET", f"http://127.0.0.1:{self.port}/")
        body = await self.http.read(response)
        self.assertEqual(response.code, 200)
        self.assertIn(b"YM3812", body)
        self.assertNotIn(b"{{chip}}", body)

    async def test_queue_and_disconnect(self):
        _connection, writer, receive = await self.connect("/queue")
        self.assertEqual(json.loads((await receive()).data), {"len": 0})
        await self.interface._update_queue_len(1)
        self.assertEqual(json.loads((await receive()).data), {"len": 1})
        writer.close()
        await writer.wait_closed()
        resource = self.root.children[b"queue"]
        await asyncio.wait_for(asyncio.gather(*resource._tasks, return_exceptions=True), 5)
        self.assertEqual(resource._tasks, set())

    async def test_disabled_url(self):
        from wsproto.events import TextMessage, CloseConnection
        connection, writer, receive = await self.connect("/vgm")
        writer.write(connection.send(TextMessage(data=json.dumps({"Voltage": "5"}))))
        writer.write(connection.send(TextMessage(data="http://example.invalid/test.vgm")))
        event = await receive()
        self.assertIsInstance(event, CloseConnection)
        self.assertEqual(event.code, 1008)
        self.assertEqual(event.reason, "URL submissions not allowed")

    async def test_invalid_binary_upload(self):
        from wsproto.events import TextMessage, BytesMessage, CloseConnection
        connection, writer, receive = await self.connect("/vgm")
        writer.write(connection.send(TextMessage(data=json.dumps({"Voltage": "5"}))))
        writer.write(connection.send(BytesMessage(data=b"invalid")))
        event = await receive()
        self.assertIsInstance(event, CloseConnection)
        self.assertEqual(event.code, 1001)
        self.assertEqual(event.reason, "File is too short to be valid")
        self.assertEqual(self.interface._queue_len, 0)

    def prepare_playback(self):
        import struct
        from unittest.mock import AsyncMock
        interface = self.interface._opx_iface
        interface.sample_clocks = 72
        interface.channel_count = 1
        interface.filter = None
        interface.get_vgm_clock_rate = lambda reader: (reader.ym3812_clk, 1)
        for method in ("reset", "enable", "disable", "write_register", "wait_clocks"):
            setattr(interface, method, AsyncMock())
        interface.read_samples = AsyncMock(side_effect=lambda count: b"\x01\x02" * count)
        self.interface._set_voltage = AsyncMock()
        data = bytearray(0x80)
        data[:4] = b"Vgm "
        struct.pack_into("<I", data, 0x08, 0x151)
        struct.pack_into("<I", data, 0x18, 735)
        struct.pack_into("<I", data, 0x34, 0x80 - 0x34)
        struct.pack_into("<I", data, 0x50, 3579545)
        data.extend(b"\x5a\x20\x01\x62\x66")
        return bytes(data)

    async def upload(self, data):
        from wsproto.events import TextMessage, BytesMessage
        connection, writer, receive = await self.connect("/vgm")
        writer.write(connection.send(TextMessage(data=json.dumps({"Voltage": "5"}))))
        event = BytesMessage(data=data) if isinstance(data, bytes) else TextMessage(data=data)
        writer.write(connection.send(event))
        return writer, receive

    async def assert_playback(self, receive):
        from wsproto.events import BytesMessage, CloseConnection
        metadata = json.loads((await receive()).data)
        self.assertEqual(metadata["Chip"], "YM3812")
        pcm = bytearray()
        while True:
            event = await receive()
            if isinstance(event, CloseConnection):
                self.assertEqual(event.code, 1000)
                break
            self.assertIsInstance(event, BytesMessage)
            pcm.extend(event.data)
        self.assertEqual(pcm, b"\x01\x02" * metadata["Total-Samples"])
        self.interface._set_voltage.assert_awaited_once_with(5)
        self.interface._opx_iface.write_register.assert_awaited_once_with(0x20, 0x01)
        tasks = list(self.root.children[b"vgm"]._tasks)
        await asyncio.gather(*tasks, return_exceptions=True)
        self.assertEqual(self.interface._queue_len, 0)

    async def test_binary_playback(self):
        data = self.prepare_playback()
        _writer, receive = await self.upload(data)
        await self.assert_playback(receive)

    async def test_url_redirect_and_vgz_playback(self):
        import gzip
        from twisted.web.resource import Resource
        from twisted.web.static import Data
        data = self.prepare_playback()
        self.interface._allow_urls = True
        self.root.putChild(b"file.vgz", Data(gzip.compress(data + bytes(range(256))),
                                           "application/octet-stream"))

        class Redirect(Resource):
            def render_GET(self, request):
                request.setResponseCode(302)
                request.setHeader(b"Location", b"/file.vgz")
                return b""

        self.root.putChild(b"redirect", Redirect())
        _writer, receive = await self.upload(f"http://127.0.0.1:{self.port}/redirect")
        await self.assert_playback(receive)

    async def test_disconnect_while_queued(self):
        data = self.prepare_playback()
        async with self.interface._lock:
            writer, _receive = await self.upload(data)
            async with asyncio.timeout(5):
                async with self.interface._queue_cond:
                    await self.interface._queue_cond.wait_for(
                        lambda: self.interface._queue_len == 1)
            writer.close()
            await writer.wait_closed()
            tasks = list(self.root.children[b"vgm"]._tasks)
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 5)
        self.assertEqual(self.interface._queue_len, 0)
        self.interface._set_voltage.assert_not_awaited()

    async def test_oversized_upload(self):
        from wsproto.events import CloseConnection
        _writer, receive = await self.upload(b"\0" * (4 * 1024 * 1024 + 1))
        event = await receive()
        self.assertIsInstance(event, CloseConnection)
        self.assertEqual(event.code, 1009)

    async def test_fragmented_large_upload(self):
        from wsproto.events import TextMessage, BytesMessage
        data = self.prepare_playback() + bytes(range(256)) * 1024
        connection, writer, receive = await self.connect("/vgm")
        writer.write(connection.send(TextMessage(data=json.dumps({"Voltage": "5"}))))
        writer.write(connection.send(BytesMessage(data=data[:10], message_finished=False)))
        writer.write(connection.send(BytesMessage(data=data[10:], message_finished=True)))
        await self.assert_playback(receive)

    async def test_fragmented_text_headers(self):
        from wsproto.events import TextMessage, BytesMessage
        data = self.prepare_playback()
        connection, writer, receive = await self.connect("/vgm")
        writer.write(connection.send(TextMessage(data='{"Vol', message_finished=False)))
        writer.write(connection.send(TextMessage(data='tage":"5"}', message_finished=True)))
        writer.write(connection.send(BytesMessage(data=data)))
        await self.assert_playback(receive)
