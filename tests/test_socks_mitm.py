from __future__ import annotations

import asyncio
import ssl

import pytest

from codex_reset_proxy.certs import ensure_ca_certificate
from codex_reset_proxy.config import Settings
from codex_reset_proxy.outbound import open_socks5_socket
from codex_reset_proxy.socks_mitm import start_socks5_server


@pytest.mark.asyncio
async def test_socks5_tunnel_forwards_bytes():
    async def echo_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        data = await reader.read(1024)
        writer.write(b"echo:" + data)
        await writer.drain()
        writer.close()

    echo_server = await asyncio.start_server(echo_handler, "127.0.0.1", 0)
    echo_port = _server_port(echo_server)

    settings = Settings(
        upstream_base_url="https://chatgpt.test",
        listen_protocol="socks5_tunnel",
        listen_host="127.0.0.1",
        listen_port=0,
    )
    socks_server = await start_socks5_server(settings)
    socks_port = _server_port(socks_server)

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", socks_port)
        await _socks5_connect(reader, writer, "127.0.0.1", echo_port)
        writer.write(b"ping")
        await writer.drain()
        assert await reader.read(1024) == b"echo:ping"
        writer.close()
    finally:
        await _close_server(socks_server)
        await _close_server(echo_server)


@pytest.mark.asyncio
async def test_outbound_socks5_socket_uses_socks_proxy():
    async def echo_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        data = await reader.read(1024)
        writer.write(b"proxied:" + data)
        await writer.drain()
        writer.close()

    echo_server = await asyncio.start_server(echo_handler, "127.0.0.1", 0)
    echo_port = _server_port(echo_server)

    settings = Settings(
        upstream_base_url="https://chatgpt.test",
        listen_protocol="socks5_tunnel",
        listen_host="127.0.0.1",
        listen_port=0,
    )
    socks_server = await start_socks5_server(settings)
    socks_port = _server_port(socks_server)

    try:
        sock = await asyncio.to_thread(
            open_socks5_socket,
            f"socks5://127.0.0.1:{socks_port}",
            "127.0.0.1",
            echo_port,
            1,
        )
        reader, writer = await asyncio.open_connection(sock=sock)
        writer.write(b"ping")
        await writer.drain()
        assert await reader.read(1024) == b"proxied:ping"
        writer.close()
    finally:
        await _close_server(socks_server)
        await _close_server(echo_server)


@pytest.mark.asyncio
async def test_socks5_mitm_intercepts_https_and_forwards_http(tmp_path):
    upstream_requests: list[tuple[str, bytes]] = []

    async def upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        header_block = await reader.readuntil(b"\r\n\r\n")
        content_length = _content_length(header_block)
        body = await reader.readexactly(content_length) if content_length else b""
        upstream_requests.append((header_block.split(b"\r\n", 1)[0].decode("latin-1"), body))
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nok")
        await writer.drain()
        writer.close()

    upstream_server = await asyncio.start_server(upstream_handler, "127.0.0.1", 0)
    upstream_port = _server_port(upstream_server)

    settings = Settings(
        upstream_base_url=f"http://127.0.0.1:{upstream_port}",
        listen_protocol="socks5_mitm",
        listen_host="127.0.0.1",
        listen_port=0,
        intercept_host="chatgpt.test",
        mitm_cert_dir=str(tmp_path),
        response_header_timeout_seconds=1,
    )
    _, ca_cert_path = ensure_ca_certificate(settings)
    socks_server = await start_socks5_server(settings)
    socks_port = _server_port(socks_server)

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", socks_port)
        await _socks5_connect(reader, writer, "chatgpt.test", 443)

        context = ssl.create_default_context(cafile=str(ca_cert_path))
        await writer.start_tls(context, server_hostname="chatgpt.test")
        writer.write(
            b"POST /backend-api/codex/responses?x=1 HTTP/1.1\r\n"
            b"Host: chatgpt.test\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: 2\r\n"
            b"Connection: close\r\n"
            b"\r\n"
            b"{}"
        )
        await writer.drain()
        response = await reader.read()
        writer.close()
    finally:
        await _close_server(socks_server)
        await _close_server(upstream_server)

    assert b"HTTP/1.1 200 OK\r\n" in response
    assert b"transfer-encoding: chunked\r\n" in response.lower()
    assert b"\r\n2\r\nok\r\n0\r\n\r\n" in response
    assert upstream_requests == [("POST /backend-api/codex/responses?x=1 HTTP/1.1", b"{}")]


async def _socks5_connect(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    host: str,
    port: int,
) -> None:
    writer.write(b"\x05\x01\x00")
    await writer.drain()
    assert await reader.readexactly(2) == b"\x05\x00"

    host_bytes = host.encode("idna")
    writer.write(bytes([0x05, 0x01, 0x00, 0x03, len(host_bytes)]))
    writer.write(host_bytes)
    writer.write(port.to_bytes(2, "big"))
    await writer.drain()
    assert await reader.readexactly(10) == b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00"


def _server_port(server: asyncio.Server) -> int:
    sockets = server.sockets
    assert sockets
    return int(sockets[0].getsockname()[1])


def _content_length(header_block: bytes) -> int:
    for line in header_block.split(b"\r\n"):
        name, _, value = line.partition(b":")
        if name.lower() == b"content-length":
            return int(value.strip())
    return 0


async def _close_server(server: asyncio.Server) -> None:
    server.close()
    await server.wait_closed()
