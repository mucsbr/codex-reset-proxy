from __future__ import annotations

import asyncio
import socket
from urllib.parse import unquote, urlsplit

from codex_reset_proxy.config import Settings


class SocksConnectError(Exception):
    pass


async def open_outbound_stream(
    settings: Settings,
    host: str,
    port: int,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    if not settings.outbound_proxy:
        return await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=settings.connect_timeout_seconds,
        )

    sock = await asyncio.to_thread(
        open_socks5_socket,
        settings.outbound_proxy,
        host,
        port,
        settings.connect_timeout_seconds,
    )
    return await asyncio.open_connection(sock=sock)


def open_socks5_socket(
    proxy_url: str,
    dest_host: str,
    dest_port: int,
    timeout: float,
) -> socket.socket:
    proxy = urlsplit(proxy_url)
    if proxy.scheme not in {"socks5", "socks5h"} or not proxy.hostname or not proxy.port:
        raise SocksConnectError("OUTBOUND_PROXY must be socks5://host:port")

    sock = socket.create_connection((proxy.hostname, proxy.port), timeout=timeout)
    sock.settimeout(timeout)
    try:
        _socks5_handshake(sock, proxy_url, dest_host, dest_port)
        sock.setblocking(False)
        return sock
    except Exception:
        sock.close()
        raise


def _socks5_handshake(sock: socket.socket, proxy_url: str, dest_host: str, dest_port: int) -> None:
    proxy = urlsplit(proxy_url)
    username = unquote(proxy.username or "")
    password = unquote(proxy.password or "")

    methods = [0x00]
    if username or password:
        methods.append(0x02)
    sock.sendall(bytes([0x05, len(methods), *methods]))

    selected = _recv_exact(sock, 2)
    if selected[0] != 0x05:
        raise SocksConnectError("invalid SOCKS5 greeting response")
    if selected[1] == 0xFF:
        raise SocksConnectError("SOCKS5 proxy rejected authentication methods")
    if selected[1] == 0x02:
        _socks5_username_password_auth(sock, username, password)
    elif selected[1] != 0x00:
        raise SocksConnectError(f"unsupported SOCKS5 authentication method: {selected[1]}")

    host_bytes = dest_host.encode("idna")
    if len(host_bytes) > 255:
        raise SocksConnectError("destination host is too long for SOCKS5")

    request = bytearray([0x05, 0x01, 0x00, 0x03, len(host_bytes)])
    request.extend(host_bytes)
    request.extend(dest_port.to_bytes(2, "big"))
    sock.sendall(request)

    header = _recv_exact(sock, 4)
    if header[0] != 0x05:
        raise SocksConnectError("invalid SOCKS5 connect response")
    if header[1] != 0x00:
        raise SocksConnectError(f"SOCKS5 connect failed with code {header[1]}")

    atyp = header[3]
    if atyp == 0x01:
        _recv_exact(sock, 4)
    elif atyp == 0x03:
        length = _recv_exact(sock, 1)[0]
        _recv_exact(sock, length)
    elif atyp == 0x04:
        _recv_exact(sock, 16)
    else:
        raise SocksConnectError("invalid SOCKS5 bound address type")
    _recv_exact(sock, 2)


def _socks5_username_password_auth(sock: socket.socket, username: str, password: str) -> None:
    username_bytes = username.encode("utf-8")
    password_bytes = password.encode("utf-8")
    if len(username_bytes) > 255 or len(password_bytes) > 255:
        raise SocksConnectError("SOCKS5 username/password is too long")

    sock.sendall(bytes([0x01, len(username_bytes)]))
    sock.sendall(username_bytes)
    sock.sendall(bytes([len(password_bytes)]))
    sock.sendall(password_bytes)

    response = _recv_exact(sock, 2)
    if response != b"\x01\x00":
        raise SocksConnectError("SOCKS5 username/password authentication failed")


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise SocksConnectError("SOCKS5 proxy closed the connection")
        chunks.extend(chunk)
    return bytes(chunks)
