import asyncio
import gzip
import hashlib
import json
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

from twisted.web.resource import Resource
from twisted.web.server import Site

from glasgow.protocol.vgm import VGMStreamReader
from glasgow.support.http import HTTPContext
from glasgow.support.websocket import WebSocketResource


class YamahaOPxWebInterface:
    def __init__(self, logger, opx_iface, set_voltage, allow_urls):
        self._logger    = logger
        self._opx_iface = opx_iface

        self._lock       = asyncio.Lock()
        self._queue_len  = 0
        self._queue_cond = asyncio.Condition()

        self._set_voltage = set_voltage
        self._allow_urls  = allow_urls

    def render_index(self):
        with Path(__file__).with_name("index.html").open() as f:
            index_html = f.read()
            index_html = index_html.replace("{{chip}}", self._opx_iface.chips[-1])
            index_html = index_html.replace("{{compat}}", ", ".join(self._opx_iface.chips))
            index_html = index_html.replace("{{url_display}}",
                                            "block" if self._allow_urls else "none")
            return index_html.encode()

    async def _update_queue_len(self, delta):
        await self._queue_cond.acquire()
        self._queue_len += delta
        self._queue_cond.notify_all()
        self._queue_cond.release()

    async def serve_queue(self, request, sock):
        try:
            while True:
                await self._queue_cond.acquire()
                try:
                    await sock.send_json({"len": self._queue_len})
                    await self._queue_cond.wait()
                finally:
                    self._queue_cond.release()

        except ConnectionError:
            pass

    async def serve_vgm(self, request, sock):
        from . import YamahaVGMStreamPlayer

        remote = sock.remote
        headers = json.loads(await sock.receive())
        vgm_msg = await sock.receive()

        if isinstance(vgm_msg, bytes):
            vgm_data = vgm_msg
        else:
            self._logger.info("web: URL %s submitted by %s", vgm_msg, remote)
            if not self._allow_urls:
                self._logger.warning("Received URL submission when disabled")
                await sock.close(code=1008, message="URL submissions not allowed")
                return

            async with HTTPContext() as http:
                response = await http.request("GET", vgm_msg)
                if response.code != 200:
                    await sock.close(code=2000 + response.code,
                                     message=response.phrase.decode(errors="replace"))
                    return
                length = response.length
                if not isinstance(length, int):
                    await sock.close(code=2999, message=
                        "Remote server did not specify Content-Length")
                    return
                if length > (1<<20):
                    await sock.close(code=2999, message=
                        f"File too large ({length} bytes) to be fetched")
                    return
                vgm_data = await http.read(response)

        digest = hashlib.sha256(vgm_data).hexdigest()[:16]
        self._logger.info("web: %s: submitted by %s",
                          digest, remote)

        try:
            if len(vgm_data) < 0x80:
                raise ValueError("File is too short to be valid")

            try:
                vgm_stream = BytesIO(vgm_data)
                if not vgm_data.startswith(b"Vgm "):
                    vgm_stream = gzip.GzipFile(fileobj=vgm_stream)

                vgm_reader = VGMStreamReader(vgm_stream)
            except OSError:
                raise ValueError("File is not in VGM or VGZ format")

            self._logger.info("web: %s: VGM has commands for %s",
                              digest, ", ".join(vgm_reader.chips()))

            clock_rate, clock_prescaler = self._opx_iface.get_vgm_clock_rate(vgm_reader)
            if clock_rate == 0:
                raise ValueError(
                    f"VGM file contains commands for {', '.join(vgm_reader.chips())}, "
                    f"which is not a supported chip")
            if clock_rate & 0xc0000000:
                raise ValueError("VGM file uses unsupported chip configuration")
            if len(vgm_reader.chips()) != 1:
                raise ValueError(
                    f"VGM file contains commands for {', '.join(vgm_reader.chips())}, "
                    f"but only playback of exactly one chip is supported")
            clock_rate *= clock_prescaler

            self._logger.info("web: %s: VGM is looped for %.2f/%.2f s",
                              digest, vgm_reader.loop_seconds, vgm_reader.total_seconds)

            vgm_player = YamahaVGMStreamPlayer(vgm_reader, self._opx_iface, clock_rate)
        except ValueError as e:
            self._logger.warning("web: %s: broken upload: %s",
                                 digest, str(e))
            await sock.close(code=1001, message=str(e))
            return

        sample_rate = 1 / vgm_player.sample_time
        self._logger.info("web: %s: sample rate %d", digest, sample_rate)

        await self._update_queue_len(+1)

        try:
            async with self._lock:
                await self._stream(sock, headers, digest, vgm_reader, vgm_player, sample_rate)
        finally:
            await self._update_queue_len(-1)

    async def _stream(self, sock, headers, digest, vgm_reader, vgm_player, sample_rate):
        try:
            voltage = float(headers["Voltage"])
            self._logger.info("web: %s: setting voltage to %.2f V", digest, voltage)
            await self._set_voltage(voltage)

        except Exception as error:
            await sock.close(code=2000, message=str(error))
            return

        self._logger.info("web: %s: start streaming", digest)

        await self._opx_iface.reset()
        if self._opx_iface.filter is not None:
            self._opx_iface.filter.sample_rate = sample_rate * self._opx_iface.sample_clocks
        # Soft reset does not clear all the state immediately, so wait a bit to make sure
        # all notes decay, etc.
        await vgm_player.wait_seconds(1)

        sample_queue = asyncio.Queue()
        record_fut = asyncio.ensure_future(vgm_player.record(sample_queue))
        play_fut   = asyncio.ensure_future(vgm_player.play())

        try:
            total_samples = int(vgm_reader.total_seconds * sample_rate)
            if vgm_reader.loop_samples in (0, vgm_reader.total_samples):
                # Either 0 or the entire VGM here means we'll loop the complete track.
                loop_skip_to = 0
            else:
                loop_skip_to = int((vgm_reader.total_seconds - vgm_reader.loop_seconds)
                                   * sample_rate)
            await sock.send_json({
                "Chip": vgm_reader.chips()[0],
                "Channel-Count": self._opx_iface.channel_count,
                "Sample-Rate": sample_rate,
                "Total-Samples": total_samples,
                "Loop-Skip-To": loop_skip_to,
            })

            while True:
                if play_fut.done() and play_fut.exception():
                    break

                samples = await asyncio.wait_for(sample_queue.get(), timeout=5.0)
                if not samples:
                    break
                await sock.send_bytes(samples)

            for fut in [play_fut, record_fut]:
                try:
                    await fut
                except NotImplementedError as e:
                    self._logger.exception("web: %s: error streaming",
                                           digest)
                    await sock.close(code=2000, message=str(e))
                    return

            self._logger.info("web: %s: done streaming",
                              digest)
            await sock.close()

        except TimeoutError:
            self._logger.info("web: %s: timeout streaming",
                              digest)
            await sock.close(code=1002, message="Streaming timeout (glitched too hard?)")

        except asyncio.CancelledError:
            self._logger.info("web: %s: cancel streaming",
                              digest)

            raise

        finally:
            for fut in (play_fut, record_fut):
                if not fut.done():
                    fut.cancel()
            await asyncio.gather(play_fut, record_fut, return_exceptions=True)

    def resource(self):
        interface = self

        class IndexResource(Resource):
            def render_GET(self, request):
                request.setHeader(b"Content-Type", b"text/html; charset=utf-8")
                return interface.render_index()

        root = Resource()
        root.putChild(b"", IndexResource())
        root.putChild(b"queue", WebSocketResource(self.serve_queue, self._logger))
        root.putChild(b"vgm", WebSocketResource(self.serve_vgm, self._logger))
        return root

    async def serve(self, endpoint):
        parsed_endpoint = urlparse(f"//{endpoint}")
        async with HTTPContext() as http:
            root = self.resource()
            listener = http.reactor.listenTCP(
                parsed_endpoint.port, Site(root, reactor=http.reactor),
                interface=parsed_endpoint.hostname or "")
            try:
                await asyncio.Future()
            finally:
                await listener.stopListening().asFuture(asyncio.get_running_loop())
                await root.children[b"queue"].close()
                await root.children[b"vgm"].close()
