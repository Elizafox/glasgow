import argparse
import asyncio
import importlib.util
import logging
import unittest
from unittest.mock import patch

from glasgow.support.data_logger import DataLogger, InfluxDBDataLogger, InfluxDB2DataLogger


@unittest.skipUnless(importlib.util.find_spec("twisted"), "HTTP dependencies not installed")
class HTTPTestCase(unittest.IsolatedAsyncioTestCase):
    loop_factory = asyncio.SelectorEventLoop

    async def asyncSetUp(self):
        self.requests = asyncio.Queue()
        self.status = 204
        self.body = b""
        self.send_length = True
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        self.endpoint = f"http://localhost:{self.server.sockets[0].getsockname()[1]}"
        self.addAsyncCleanup(self.server.wait_closed)
        self.addCleanup(self.server.close)

    async def handle(self, reader, writer):
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            lines = head.decode().split("\r\n")
            headers = dict(line.split(": ", 1) for line in lines[1:] if line)
            body = await reader.readexactly(int(headers.get("Content-Length", 0)))
            await self.requests.put((lines[0], headers, body))
            length = f"Content-Length: {len(self.body)}\r\n" if self.send_length else ""
            writer.write((f"HTTP/1.1 {self.status} Test\r\n{length}"
                          "Connection: close\r\n\r\n").encode() + self.body)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async def logger(self, name):
        args = argparse.Namespace(data_logger=name, endpoint=self.endpoint + "/old?old=true",
            database="test db", org="test org", bucket="bucket", token="secret", precision="s",
            retention_policy="policy", measurement="test", tags=[], batch_size=2)
        return await DataLogger(logging.getLogger(__name__), args, field_names={"value": "Value"})

    async def test_influxdb_batch(self):
        logger = await self.logger("influxdb")
        await logger.report_data({"value": 1}, timestamp=10)
        self.assertTrue(self.requests.empty())
        await logger.report_data({"value": 2}, timestamp=11)
        line, _headers, body = await self.requests.get()
        self.assertEqual(line, "POST /write?db=test+db&rp=policy&precision=s HTTP/1.1")
        self.assertEqual(body, b"test error=False,value=1 10\ntest error=False,value=2 11")
        self.assertEqual(logger._queue, [])

    async def test_influxdb2_authorization(self):
        logger = await self.logger("influxdb2")
        await logger.report_data({"value": 1}, timestamp=10)
        await logger.report_data({"value": 2}, timestamp=11)
        line, headers, body = await self.requests.get()
        self.assertEqual(line, "POST /api/v2/write?org=test+org&bucket=bucket&precision=s HTTP/1.1")
        self.assertEqual(headers["Authorization"], "Token secret")
        self.assertIn(b"value=2 11", body)

    async def test_http_error_clears_batch(self):
        self.status, self.body = 400, b"bad point"
        logger = await self.logger("influxdb")
        await logger.report_data({"value": 1}, timestamp=10)
        with self.assertLogs(__name__, level="ERROR") as logs:
            await logger.report_data({"value": 2}, timestamp=11)
        self.assertIn("write status=400 body=bad point", logs.output[0])
        self.assertEqual(logger._queue, [])

    async def test_network_error_retains_batch(self):
        self.server.close()
        await self.server.wait_closed()
        logger = await self.logger("influxdb2")
        await logger.report_data({"value": 1}, timestamp=10)
        with self.assertLogs(__name__, level="ERROR"):
            await logger.report_data({"value": 2}, timestamp=11)
        self.assertEqual(len(logger._queue), 2)

    async def test_context_leaves_loop_running(self):
        from glasgow.support.http import HTTPContext
        async with HTTPContext() as http:
            self.assertIs(http.loop, asyncio.get_running_loop())
            response = await http.request("GET", self.endpoint)
            self.assertEqual(await http.read(response), b"")
        self.assertFalse(http.reactor.running)
        self.assertEqual(http.reactor.getReaders(), [])
        self.assertEqual(http.reactor.getWriters(), [])
        self.assertEqual(http.reactor.getDelayedCalls(), [])
        self.assertIsNone(http.reactor.threadpool)
        self.assertEqual(await asyncio.create_task(asyncio.sleep(0, result=42)), 42)

    async def test_eof_delimited_response(self):
        from glasgow.support.http import HTTPContext
        self.status, self.body, self.send_length = 200, b"response", False
        async with HTTPContext() as http:
            response = await http.request("GET", self.endpoint)
            self.assertEqual(await http.read(response), b"response")

    async def test_basic_auth_endpoint(self):
        from glasgow.support.http import HTTPContext
        endpoint = self.endpoint.replace("http://", "http://user:pass@")
        async with HTTPContext() as http:
            response = await http.request("GET", endpoint)
            await http.read(response)
        _line, headers, _body = await self.requests.get()
        self.assertEqual(headers["Authorization"], "Basic dXNlcjpwYXNz")


class OptionalHTTPTestCase(unittest.TestCase):
    def test_loggers_hidden_without_twisted(self):
        with patch("glasgow.support.data_logger.find_spec", return_value=None):
            self.assertFalse(InfluxDBDataLogger.available())
            self.assertFalse(InfluxDB2DataLogger.available())
            parser = argparse.ArgumentParser()
            DataLogger.add_subparsers(parser)
            self.assertNotIn("influxdb", parser.format_help())
            self.assertIn("stdout", parser.format_help())


@unittest.skipUnless(importlib.util.find_spec("twisted"), "HTTP dependencies not installed")
class HTTPSTestCase(unittest.IsolatedAsyncioTestCase):
    loop_factory = asyncio.SelectorEventLoop

    async def test_untrusted_certificate_rejected(self):
        import datetime
        import ssl
        import tempfile
        from pathlib import Path
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from OpenSSL.SSL import Error
        from twisted.internet.error import ConnectError
        from twisted.web.client import ResponseNeverReceived
        from glasgow.support.http import HTTPContext

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, "localhost")])
        now = datetime.datetime.now(datetime.UTC)
        cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
            .sign(key, hashes.SHA256()))
        with tempfile.TemporaryDirectory() as directory:
            pem = Path(directory) / "server.pem"
            pem.write_bytes(cert.public_bytes(serialization.Encoding.PEM) + key.private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption()))
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(pem)

            async def handle(reader, writer):
                writer.close()
                await writer.wait_closed()

            async with await asyncio.start_server(handle, "127.0.0.1", 0, ssl=context) as server:
                async with HTTPContext() as http:
                    port = server.sockets[0].getsockname()[1]
                    with self.assertRaises((ConnectError, ResponseNeverReceived, Error)) as error:
                        await http.request("GET", f"https://localhost:{port}/")
                    self.assertIn("certificate verify failed", str(error.exception))


@unittest.skipUnless(importlib.util.find_spec("twisted"), "HTTP dependencies not installed")
class HTTPRuntimeTestCase(unittest.TestCase):
    def test_application_runtime(self):
        from glasgow.main import run_async
        from glasgow.support.http import HTTPContext

        @run_async
        async def main():
            async with HTTPContext() as http:
                self.assertIs(http.loop, asyncio.get_running_loop())
            return 0

        with patch("glasgow.main.TqdmProgressImpl"):
            with self.assertRaises(SystemExit) as exited:
                main()
        self.assertEqual(exited.exception.code, 0)
