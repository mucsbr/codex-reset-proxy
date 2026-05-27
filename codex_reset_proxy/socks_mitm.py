from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from http import HTTPStatus
from urllib.parse import urlsplit

from codex_reset_proxy.certs import ensure_ca_certificate, server_ssl_context_for_host
from codex_reset_proxy.config import Settings
from codex_reset_proxy.outbound import open_outbound_stream
from codex_reset_proxy.proxy import (
    ClientFactory,
    RequestBodyTooLarge,
    UpstreamOpenError,
    default_client_factory,
    filtered_request_headers_from_raw,
    filtered_response_headers,
    open_upstream_with_retries,
    stream_upstream_body,
)
from codex_reset_proxy.websocket_bridge import (
    WebSocketBridgeError,
    WebSocketConnectFactory,
    build_upstream_ws_url,
    default_websocket_connect_factory,
    open_websocket_bridge_with_retries,
    stream_websocket_as_sse,
    websocket_headers,
)

logger = logging.getLogger(__name__)

SOCKS_VERSION = 0x05
SOCKS_NO_AUTH = 0x00
SOCKS_NO_ACCEPTABLE_METHODS = 0xFF
SOCKS_CMD_CONNECT = 0x01
SOCKS_ATYP_IPV4 = 0x01
SOCKS_ATYP_DOMAIN = 0x03
SOCKS_ATYP_IPV6 = 0x04
SOCKS_REPLY_SUCCEEDED = 0x00
SOCKS_REPLY_GENERAL_FAILURE = 0x01
SOCKS_REPLY_COMMAND_NOT_SUPPORTED = 0x07

MAX_HTTP_HEADER_BYTES = 128 * 1024


@dataclass
class SocksTarget:
    host: str
    port: int


@dataclass
class HttpRequest:
    method: str
    target: str
    version: str
    headers: list[tuple[bytes, bytes]]
    body: bytes


async def serve_socks5(
    settings: Settings,
    *,
    client_factory: ClientFactory = default_client_factory,
    websocket_connect_factory: WebSocketConnectFactory = default_websocket_connect_factory,
) -> None:
    if settings.listen_protocol == "socks5_mitm":
        ca_key_path, ca_cert_path = ensure_ca_certificate(settings)
        logger.info("MITM CA certificate path: %s", ca_cert_path)
        logger.info("MITM CA key path: %s", ca_key_path)

    server = await start_socks5_server(
        settings,
        client_factory=client_factory,
        websocket_connect_factory=websocket_connect_factory,
    )
    sockets = ", ".join(str(sock.getsockname()) for sock in (server.sockets or []))
    logger.info("SOCKS5 server listening on %s", sockets)
    async with server:
        await server.serve_forever()


async def start_socks5_server(
    settings: Settings,
    *,
    client_factory: ClientFactory = default_client_factory,
    websocket_connect_factory: WebSocketConnectFactory = default_websocket_connect_factory,
) -> asyncio.Server:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await _handle_socks_client(
            settings,
            client_factory,
            websocket_connect_factory,
            reader,
            writer,
        )

    return await asyncio.start_server(handler, settings.listen_host, settings.listen_port)


async def _handle_socks_client(
    settings: Settings,
    client_factory: ClientFactory,
    websocket_connect_factory: WebSocketConnectFactory,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    target: SocksTarget | None = None
    try:
        target = await _socks5_handshake(reader, writer)
        if target is None:
            return

        if _should_mitm(settings, target):
            await _handle_mitm_connection(
                settings,
                client_factory,
                websocket_connect_factory,
                target,
                reader,
                writer,
            )
            return

        await _handle_tunnel(settings, target, reader, writer)
    except Exception as exc:
        logger.warning(
            "SOCKS5 client failed target=%s error=%s",
            f"{target.host}:{target.port}" if target else "<unknown>",
            exc.__class__.__name__,
        )
    finally:
        _close_writer(writer)


async def _socks5_handshake(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> SocksTarget | None:
    try:
        greeting = await reader.readexactly(2)
    except asyncio.IncompleteReadError:
        return None
    if greeting[0] != SOCKS_VERSION:
        return None

    methods = await reader.readexactly(greeting[1])
    if SOCKS_NO_AUTH not in methods:
        writer.write(bytes([SOCKS_VERSION, SOCKS_NO_ACCEPTABLE_METHODS]))
        await writer.drain()
        return None
    writer.write(bytes([SOCKS_VERSION, SOCKS_NO_AUTH]))
    await writer.drain()

    header = await reader.readexactly(4)
    if header[0] != SOCKS_VERSION:
        await _send_socks_reply(writer, SOCKS_REPLY_GENERAL_FAILURE)
        return None
    if header[1] != SOCKS_CMD_CONNECT:
        await _send_socks_reply(writer, SOCKS_REPLY_COMMAND_NOT_SUPPORTED)
        return None

    host = await _read_socks_address(reader, header[3])
    port = int.from_bytes(await reader.readexactly(2), "big")
    await _send_socks_reply(writer, SOCKS_REPLY_SUCCEEDED)
    return SocksTarget(host=host, port=port)


async def _read_socks_address(reader: asyncio.StreamReader, atyp: int) -> str:
    if atyp == SOCKS_ATYP_IPV4:
        return ".".join(str(part) for part in await reader.readexactly(4))
    if atyp == SOCKS_ATYP_DOMAIN:
        length = (await reader.readexactly(1))[0]
        return (await reader.readexactly(length)).decode("idna")
    if atyp == SOCKS_ATYP_IPV6:
        raw = await reader.readexactly(16)
        parts = [raw[index : index + 2].hex() for index in range(0, 16, 2)]
        return ":".join(parts)
    raise ValueError(f"unsupported SOCKS5 address type: {atyp}")


async def _send_socks_reply(writer: asyncio.StreamWriter, code: int) -> None:
    writer.write(bytes([SOCKS_VERSION, code, 0x00, SOCKS_ATYP_IPV4, 0, 0, 0, 0, 0, 0]))
    await writer.drain()


def _should_mitm(settings: Settings, target: SocksTarget) -> bool:
    if settings.listen_protocol != "socks5_mitm":
        return False
    return target.port == 443 and target.host.lower() == settings.intercept_host.lower()


async def _handle_tunnel(
    settings: Settings,
    target: SocksTarget,
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
) -> None:
    upstream_reader, upstream_writer = await open_outbound_stream(settings, target.host, target.port)
    logger.info("SOCKS5 tunnel target=%s:%s", target.host, target.port)
    try:
        await asyncio.gather(
            _pipe(client_reader, upstream_writer),
            _pipe(upstream_reader, client_writer),
        )
    finally:
        _close_writer(upstream_writer)


async def _handle_mitm_connection(
    settings: Settings,
    client_factory: ClientFactory,
    websocket_connect_factory: WebSocketConnectFactory,
    target: SocksTarget,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    ssl_context = server_ssl_context_for_host(settings, target.host)
    await writer.start_tls(
        ssl_context,
        ssl_handshake_timeout=settings.connect_timeout_seconds,
        ssl_shutdown_timeout=5,
    )
    request = await _read_http_request(reader, settings.max_request_body_bytes)
    if request is None:
        return

    path = _origin_path(request.target)
    intercept = _path_matches(settings, path)
    logger.info(
        "MITM HTTP request method=%s path=%s intercept=%s",
        request.method,
        path,
        intercept,
    )

    if intercept and settings.transport_mode == "websocket_per_request":
        await _write_websocket_bridge_response(settings, websocket_connect_factory, request, path, writer)
    else:
        await _write_http_proxy_response(settings, client_factory, request, path, writer)


async def _read_http_request(
    reader: asyncio.StreamReader,
    max_body_bytes: int,
) -> HttpRequest | None:
    try:
        header_block = await reader.readuntil(b"\r\n\r\n")
    except asyncio.IncompleteReadError:
        return None
    if len(header_block) > MAX_HTTP_HEADER_BYTES:
        raise RequestBodyTooLarge(f"request headers exceed {MAX_HTTP_HEADER_BYTES} bytes")

    header_lines = header_block[:-4].split(b"\r\n")
    request_line = header_lines[0].decode("latin-1")
    parts = request_line.split(" ")
    if len(parts) != 3:
        raise ValueError("invalid HTTP request line")
    method, target, version = parts

    headers: list[tuple[bytes, bytes]] = []
    content_length = 0
    chunked = False
    for line in header_lines[1:]:
        if not line:
            continue
        name, separator, value = line.partition(b":")
        if not separator:
            raise ValueError("invalid HTTP header line")
        stripped_value = value.lstrip()
        headers.append((name, stripped_value))
        lower_name = name.lower()
        if lower_name == b"content-length":
            content_length = int(stripped_value)
        elif lower_name == b"transfer-encoding" and b"chunked" in stripped_value.lower():
            chunked = True

    if chunked:
        body = await _read_chunked_body(reader, max_body_bytes)
    else:
        if content_length > max_body_bytes:
            raise RequestBodyTooLarge(f"request body exceeds {max_body_bytes} bytes")
        body = await reader.readexactly(content_length) if content_length else b""

    return HttpRequest(method=method, target=target, version=version, headers=headers, body=body)


async def _read_chunked_body(reader: asyncio.StreamReader, max_body_bytes: int) -> bytes:
    body = bytearray()
    while True:
        line = await reader.readline()
        size = int(line.split(b";", 1)[0].strip(), 16)
        if size == 0:
            while True:
                trailer = await reader.readline()
                if trailer in {b"\r\n", b"\n", b""}:
                    return bytes(body)
        body.extend(await reader.readexactly(size))
        if len(body) > max_body_bytes:
            raise RequestBodyTooLarge(f"request body exceeds {max_body_bytes} bytes")
        await reader.readexactly(2)


def _origin_path(target: str) -> str:
    parsed = urlsplit(target)
    if parsed.scheme and parsed.netloc:
        path = parsed.path or "/"
        return f"{path}?{parsed.query}" if parsed.query else path
    return target or "/"


def _path_matches(settings: Settings, path: str) -> bool:
    path_only = path.split("?", 1)[0]
    return path_only in settings.intercept_paths


async def _write_http_proxy_response(
    settings: Settings,
    client_factory: ClientFactory,
    request: HttpRequest,
    path: str,
    writer: asyncio.StreamWriter,
) -> None:
    try:
        opened = await open_upstream_with_retries(
            settings=settings,
            client_factory=client_factory,
            method=request.method,
            url=_upstream_url(settings, path),
            headers=filtered_request_headers_from_raw(settings, request.headers),
            body=request.body,
        )
    except UpstreamOpenError as exc:
        await _write_fixed_response(
            writer,
            exc.status_code,
            [(b"x-codex-reset-proxy-error", exc.error_code.encode("ascii"))],
            f"{exc.message} after {exc.attempts} attempt(s)\n".encode("utf-8"),
        )
        return

    headers = filtered_response_headers(opened.response)
    headers.append((b"x-codex-reset-proxy-attempts", str(opened.attempts).encode("ascii")))
    await _write_streaming_response(
        writer,
        opened.response.status_code,
        headers,
        stream_upstream_body(opened),
    )


async def _write_websocket_bridge_response(
    settings: Settings,
    websocket_connect_factory: WebSocketConnectFactory,
    request: HttpRequest,
    path: str,
    writer: asyncio.StreamWriter,
) -> None:
    if request.method != "POST":
        await _write_fixed_response(writer, 405, [], b"websocket_per_request transport only supports POST\n")
        return

    try:
        opened_ws = await open_websocket_bridge_with_retries(
            settings=settings,
            connect_factory=websocket_connect_factory,
            url=build_upstream_ws_url(_upstream_url(settings, path)),
            headers=websocket_headers(filtered_request_headers_from_raw(settings, request.headers)),
            body=request.body,
        )
    except WebSocketBridgeError as exc:
        await _write_fixed_response(
            writer,
            exc.status_code,
            [
                (b"x-codex-reset-proxy-error", exc.error_code.encode("ascii")),
                (b"x-codex-reset-proxy-attempts", str(exc.attempts).encode("ascii")),
            ],
            f"{exc.message}\n".encode("utf-8"),
        )
        return

    await _write_streaming_response(
        writer,
        200,
        [
            (b"content-type", b"text/event-stream; charset=utf-8"),
            (b"cache-control", b"no-cache"),
            (b"x-codex-reset-proxy-transport", b"websocket_per_request"),
            (b"x-codex-reset-proxy-attempts", str(opened_ws.attempts).encode("ascii")),
        ],
        stream_websocket_as_sse(opened_ws, settings),
    )


def _upstream_url(settings: Settings, path: str) -> str:
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{settings.upstream_base_url}{path}"


async def _write_fixed_response(
    writer: asyncio.StreamWriter,
    status_code: int,
    headers: list[tuple[bytes, bytes]],
    body: bytes,
) -> None:
    phrase = _reason_phrase(status_code)
    writer.write(f"HTTP/1.1 {status_code} {phrase}\r\n".encode("ascii"))
    for name, value in headers:
        writer.write(name + b": " + value + b"\r\n")
    writer.write(f"content-length: {len(body)}\r\n".encode("ascii"))
    writer.write(b"connection: close\r\n\r\n")
    writer.write(body)
    await writer.drain()


async def _write_streaming_response(
    writer: asyncio.StreamWriter,
    status_code: int,
    headers: list[tuple[bytes, bytes]],
    chunks: AsyncIterator[bytes],
) -> None:
    phrase = _reason_phrase(status_code)
    writer.write(f"HTTP/1.1 {status_code} {phrase}\r\n".encode("ascii"))
    for name, value in headers:
        if name.lower() in {b"content-length", b"transfer-encoding", b"connection"}:
            continue
        writer.write(name + b": " + value + b"\r\n")
    writer.write(b"transfer-encoding: chunked\r\n")
    writer.write(b"connection: close\r\n\r\n")
    await writer.drain()

    async for chunk in chunks:
        if not chunk:
            continue
        writer.write(f"{len(chunk):x}\r\n".encode("ascii"))
        writer.write(chunk)
        writer.write(b"\r\n")
        await writer.drain()
    writer.write(b"0\r\n\r\n")
    await writer.drain()


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while True:
            chunk = await reader.read(64 * 1024)
            if not chunk:
                break
            writer.write(chunk)
            await writer.drain()
    finally:
        _close_writer(writer)


def _reason_phrase(status_code: int) -> str:
    try:
        return HTTPStatus(status_code).phrase
    except ValueError:
        return "Unknown"


def _close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
