from __future__ import annotations

import asyncio
import inspect
import json
import logging
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from json import JSONDecodeError
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from websockets.asyncio.client import connect as websockets_connect
from websockets.exceptions import WebSocketException

from codex_reset_proxy.config import Settings
from codex_reset_proxy.outbound import open_socks5_socket

logger = logging.getLogger(__name__)

WEBSOCKET_REQUEST_SKIP_HEADERS = {
    b"accept",
    b"accept-encoding",
    b"content-length",
    b"content-type",
}

WebSocketConnectFactory = Callable[[str, list[tuple[str, str]], Settings], Any]


class WebSocketBridgeError(Exception):
    def __init__(self, *, status_code: int, error_code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.message = message


@dataclass
class OpenedWebSocketBridge:
    websocket: Any
    context: Any
    url: str
    background_tasks: set[asyncio.Task[None]]
    close_owned_by_background: bool = False


async def default_websocket_connect_factory(
    url: str,
    headers: list[tuple[str, str]],
    settings: Settings,
) -> Any:
    kwargs: dict[str, Any] = {}
    if settings.outbound_proxy:
        parsed = urlsplit(url)
        host = parsed.hostname
        if not host:
            raise WebSocketBridgeError(
                status_code=500,
                error_code="invalid_websocket_url",
                message="websocket URL must include a host",
            )
        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        kwargs["sock"] = await asyncio.to_thread(
            open_socks5_socket,
            settings.outbound_proxy,
            host,
            port,
            settings.connect_timeout_seconds,
        )

    return websockets_connect(
        url,
        additional_headers=headers,
        open_timeout=settings.connect_timeout_seconds,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
        max_size=None,
        user_agent_header=None,
        **kwargs,
    )


def build_upstream_ws_url(upstream_http_url: str) -> str:
    parsed = urlsplit(upstream_http_url)
    if parsed.scheme == "http":
        scheme = "ws"
    elif parsed.scheme == "https":
        scheme = "wss"
    elif parsed.scheme in {"ws", "wss"}:
        scheme = parsed.scheme
    else:
        raise WebSocketBridgeError(
            status_code=500,
            error_code="invalid_websocket_url",
            message=f"cannot convert URL scheme to websocket: {parsed.scheme}",
        )
    return urlunsplit((scheme, parsed.netloc, parsed.path, parsed.query, parsed.fragment))


def build_response_create_request(body: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(body.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise WebSocketBridgeError(
            status_code=400,
            error_code="invalid_request_body",
            message="request body must be UTF-8 JSON",
        ) from exc
    except JSONDecodeError as exc:
        raise WebSocketBridgeError(
            status_code=400,
            error_code="invalid_request_json",
            message=f"request body must be a JSON object: {exc.msg}",
        ) from exc

    if not isinstance(payload, dict):
        raise WebSocketBridgeError(
            status_code=400,
            error_code="invalid_request_json",
            message="request body must be a JSON object",
        )

    payload.pop("type", None)
    return {"type": "response.create", **payload}


def websocket_headers(raw_headers: list[tuple[bytes, bytes]]) -> list[tuple[str, str]]:
    return [
        (name.decode("latin-1"), value.decode("latin-1"))
        for name, value in raw_headers
        if name.lower() not in WEBSOCKET_REQUEST_SKIP_HEADERS
    ]


async def open_websocket_bridge(
    *,
    settings: Settings,
    connect_factory: WebSocketConnectFactory,
    url: str,
    headers: list[tuple[str, str]],
    body: bytes,
) -> OpenedWebSocketBridge:
    request = build_response_create_request(body)
    context = connect_factory(url, headers, settings)
    if inspect.isawaitable(context):
        context = await context

    try:
        websocket = await context.__aenter__()
        await asyncio.wait_for(
            websocket.send(json.dumps(request, separators=(",", ":"))),
            timeout=settings.websocket_processed_timeout_seconds,
        )
    except asyncio.TimeoutError as exc:
        await _close_context_quietly(context)
        raise WebSocketBridgeError(
            status_code=504,
            error_code="websocket_open_timeout",
            message="timed out opening websocket or sending response.create",
        ) from exc
    except (OSError, WebSocketException) as exc:
        await _close_context_quietly(context)
        raise WebSocketBridgeError(
            status_code=502,
            error_code="websocket_open_error",
            message=f"failed to open websocket: {exc.__class__.__name__}",
        ) from exc
    except Exception as exc:
        await _close_context_quietly(context)
        raise WebSocketBridgeError(
            status_code=502,
            error_code="websocket_open_error",
            message=f"failed to open websocket: {exc.__class__.__name__}",
        ) from exc

    logger.info("websocket response.create sent url=%s", url)
    return OpenedWebSocketBridge(websocket=websocket, context=context, url=url, background_tasks=set())


async def stream_websocket_as_sse(
    opened: OpenedWebSocketBridge,
    settings: Settings,
) -> AsyncIterator[bytes]:
    try:
        while True:
            message = await asyncio.wait_for(
                opened.websocket.recv(),
                timeout=settings.websocket_idle_timeout_seconds,
            )
            text = _websocket_message_to_text(message)
            event_type = _event_type(text)
            completed_response_id = _completed_response_id(text)

            yield _format_sse(event_type, text)

            if completed_response_id:
                _schedule_response_processed(opened, settings, completed_response_id)
                return
    finally:
        if not opened.close_owned_by_background:
            await _close_context_quietly(opened.context)


def _schedule_response_processed(
    opened: OpenedWebSocketBridge,
    settings: Settings,
    response_id: str,
) -> None:
    opened.close_owned_by_background = True
    if not settings.websocket_send_response_processed:
        task = asyncio.create_task(_close_context_quietly(opened.context))
    else:
        task = asyncio.create_task(_send_response_processed_and_close(opened, settings, response_id))
    opened.background_tasks.add(task)
    task.add_done_callback(opened.background_tasks.discard)


async def _send_response_processed_and_close(
    opened: OpenedWebSocketBridge,
    settings: Settings,
    response_id: str,
) -> None:
    try:
        await _send_response_processed(opened, settings, response_id)
    finally:
        await _close_context_quietly(opened.context)


def _websocket_message_to_text(message: str | bytes) -> str:
    if isinstance(message, str):
        return message
    return message.decode("utf-8")


def _event_type(text: str) -> str:
    try:
        payload = json.loads(text)
    except JSONDecodeError:
        return "message"
    if isinstance(payload, dict) and isinstance(payload.get("type"), str):
        return payload["type"]
    return "message"


def _completed_response_id(text: str) -> str | None:
    try:
        payload = json.loads(text)
    except JSONDecodeError:
        return None
    if not isinstance(payload, dict) or payload.get("type") != "response.completed":
        return None

    response = payload.get("response")
    if isinstance(response, dict) and isinstance(response.get("id"), str):
        return response["id"]
    response_id = payload.get("response_id")
    if isinstance(response_id, str):
        return response_id
    return None


async def _send_response_processed(
    opened: OpenedWebSocketBridge,
    settings: Settings,
    response_id: str,
) -> None:
    if not settings.websocket_send_response_processed:
        return

    request = {"type": "response.processed", "response_id": response_id}
    try:
        await asyncio.wait_for(
            opened.websocket.send(json.dumps(request, separators=(",", ":"))),
            timeout=settings.websocket_processed_timeout_seconds,
        )
    except Exception as exc:
        logger.warning(
            "failed to send response.processed response_id=%s error=%s",
            response_id,
            exc.__class__.__name__,
        )


def _format_sse(event_type: str, data: str) -> bytes:
    lines = [f"event: {event_type}\n"]
    data_lines = data.splitlines() or [""]
    lines.extend(f"data: {line}\n" for line in data_lines)
    lines.append("\n")
    return "".join(lines).encode("utf-8")


async def _close_context_quietly(context: Any) -> None:
    try:
        await context.__aexit__(None, None, None)
    except Exception:
        logger.debug("failed to close websocket context after open error", exc_info=True)
