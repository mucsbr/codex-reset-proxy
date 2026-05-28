from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import time
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
WEBSOCKET_POOL_DRAIN_TIMEOUT_SECONDS = 0.01

WebSocketConnectFactory = Callable[[str, list[tuple[str, str]], Settings], Any]


class WebSocketBridgeError(Exception):
    def __init__(self, *, status_code: int, error_code: str, message: str, attempts: int = 1) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.message = message
        self.attempts = attempts


@dataclass(frozen=True)
class WebSocketPoolKey:
    url: str
    identity_digest: str


@dataclass
class IdleWebSocketBridge:
    websocket: Any
    context: Any
    key: WebSocketPoolKey
    created_at: float
    last_used_at: float


@dataclass
class OpenedWebSocketBridge:
    websocket: Any
    context: Any
    url: str
    background_tasks: set[asyncio.Task[None]]
    first_message: str | bytes | None = None
    attempts: int = 1
    close_owned_by_background: bool = False
    pool: WebSocketBridgePool | None = None
    pool_key: WebSocketPoolKey | None = None
    reused_connection: bool = False


class WebSocketBridgePool:
    def __init__(self, settings: Settings, connect_factory: WebSocketConnectFactory) -> None:
        self.settings = settings
        self.connect_factory = connect_factory
        self._idle: dict[WebSocketPoolKey, list[IdleWebSocketBridge]] = {}
        self._lock = asyncio.Lock()

    async def open_websocket_bridge(
        self,
        *,
        url: str,
        headers: list[tuple[str, str]],
        body: bytes,
    ) -> OpenedWebSocketBridge:
        request = build_response_create_request(body)
        key = websocket_pool_key(self.settings, url, headers)
        idle = await self._checkout(key)
        reused = idle is not None

        if idle is None:
            websocket, context = await _open_websocket_connection(
                settings=self.settings,
                connect_factory=self.connect_factory,
                url=url,
                headers=headers,
            )
        else:
            websocket = idle.websocket
            context = idle.context

        try:
            await _send_websocket_json(
                websocket,
                request,
                timeout=self.settings.websocket_processed_timeout_seconds,
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

        logger.info(
            "websocket response.create sent url=%s pooled=%s key=%s",
            url,
            reused,
            key.identity_digest[:12],
        )
        return OpenedWebSocketBridge(
            websocket=websocket,
            context=context,
            url=url,
            background_tasks=set(),
            pool=self,
            pool_key=key,
            reused_connection=reused,
        )

    async def release(self, opened: OpenedWebSocketBridge) -> None:
        if opened.pool_key is None:
            await _close_context_quietly(opened.context)
            return
        if self.settings.websocket_pool_max_idle <= 0 or self.settings.websocket_pool_idle_timeout_seconds <= 0:
            await _close_context_quietly(opened.context)
            return
        if not _websocket_appears_open(opened.websocket):
            await _close_context_quietly(opened.context)
            return
        if await _websocket_has_unexpected_pending_message(opened.websocket):
            await _close_context_quietly(opened.context)
            return

        now = time.monotonic()
        idle = IdleWebSocketBridge(
            websocket=opened.websocket,
            context=opened.context,
            key=opened.pool_key,
            created_at=now,
            last_used_at=now,
        )
        close_later: list[IdleWebSocketBridge] = []
        async with self._lock:
            bucket = self._idle.setdefault(opened.pool_key, [])
            while len(bucket) >= self.settings.websocket_pool_max_idle:
                close_later.append(bucket.pop(0))
            bucket.append(idle)
        for stale in close_later:
            await _close_context_quietly(stale.context)
        logger.info(
            "websocket returned to pool url=%s key=%s",
            opened.url,
            opened.pool_key.identity_digest[:12],
        )

    async def aclose(self) -> None:
        idle: list[IdleWebSocketBridge] = []
        async with self._lock:
            for bucket in self._idle.values():
                idle.extend(bucket)
            self._idle.clear()
        for entry in idle:
            await _close_context_quietly(entry.context)

    async def _checkout(self, key: WebSocketPoolKey) -> IdleWebSocketBridge | None:
        stale: list[IdleWebSocketBridge] = []
        selected: IdleWebSocketBridge | None = None
        now = time.monotonic()
        async with self._lock:
            bucket = self._idle.get(key)
            while bucket:
                candidate = bucket.pop()
                if self._is_idle_entry_usable(candidate, now):
                    selected = candidate
                    break
                stale.append(candidate)
            if bucket == []:
                self._idle.pop(key, None)

        for entry in stale:
            await _close_context_quietly(entry.context)

        if selected is not None:
            logger.info("websocket pool hit url=%s key=%s", key.url, key.identity_digest[:12])
        return selected

    def _is_idle_entry_usable(self, entry: IdleWebSocketBridge, now: float) -> bool:
        if now - entry.last_used_at > self.settings.websocket_pool_idle_timeout_seconds:
            return False
        return _websocket_appears_open(entry.websocket)


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


def websocket_pool_key(settings: Settings, url: str, headers: list[tuple[str, str]]) -> WebSocketPoolKey:
    key_header_names = {name.lower() for name in settings.websocket_pool_key_headers}
    configured_key_header = settings.upstream_api_key_header.lower()
    identity_headers = [
        (name.lower(), value)
        for name, value in headers
        if _is_websocket_pool_identity_header(name.lower(), key_header_names, configured_key_header)
    ]
    identity_headers.sort()
    digest_input = json.dumps(identity_headers, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return WebSocketPoolKey(url=url, identity_digest=hashlib.sha256(digest_input).hexdigest())


async def open_websocket_bridge(
    *,
    settings: Settings,
    connect_factory: WebSocketConnectFactory,
    url: str,
    headers: list[tuple[str, str]],
    body: bytes,
) -> OpenedWebSocketBridge:
    request = build_response_create_request(body)
    websocket, context = await _open_websocket_connection(
        settings=settings,
        connect_factory=connect_factory,
        url=url,
        headers=headers,
    )

    try:
        await _send_websocket_json(
            websocket,
            request,
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


async def open_websocket_bridge_with_retries(
    *,
    settings: Settings,
    connect_factory: WebSocketConnectFactory,
    url: str,
    headers: list[tuple[str, str]],
    body: bytes,
    pool: WebSocketBridgePool | None = None,
) -> OpenedWebSocketBridge:
    last_error_code = "websocket_first_message_timeout"
    last_status_code = 504
    last_message = (
        "upstream websocket did not return first message within "
        f"{settings.websocket_first_message_timeout_seconds:g}s"
    )

    for attempt in range(1, settings.upstream_max_attempts + 1):
        opened: OpenedWebSocketBridge | None = None
        try:
            if pool is None:
                opened = await open_websocket_bridge(
                    settings=settings,
                    connect_factory=connect_factory,
                    url=url,
                    headers=headers,
                    body=body,
                )
            else:
                opened = await pool.open_websocket_bridge(
                    url=url,
                    headers=headers,
                    body=body,
                )
            opened.first_message = await asyncio.wait_for(
                opened.websocket.recv(),
                timeout=settings.websocket_first_message_timeout_seconds,
            )
            opened.attempts = attempt
            logger.info(
                "websocket first message received attempt=%s/%s url=%s",
                attempt,
                settings.upstream_max_attempts,
                url,
            )
            return opened
        except asyncio.TimeoutError:
            last_error_code = "websocket_first_message_timeout"
            last_status_code = 504
            last_message = (
                "upstream websocket did not return first message within "
                f"{settings.websocket_first_message_timeout_seconds:g}s"
            )
            logger.warning(
                "websocket first message timeout attempt=%s/%s url=%s",
                attempt,
                settings.upstream_max_attempts,
                url,
            )
            if opened is not None:
                await _close_context_quietly(opened.context)
        except (OSError, WebSocketException) as exc:
            last_error_code = "websocket_first_message_error"
            last_status_code = 502
            last_message = f"upstream websocket failed before first message: {exc.__class__.__name__}"
            logger.warning(
                "websocket first message error attempt=%s/%s error=%s url=%s",
                attempt,
                settings.upstream_max_attempts,
                exc.__class__.__name__,
                url,
            )
            if opened is not None:
                await _close_context_quietly(opened.context)
        except WebSocketBridgeError as exc:
            if exc.status_code < 500:
                raise WebSocketBridgeError(
                    status_code=exc.status_code,
                    error_code=exc.error_code,
                    message=exc.message,
                    attempts=attempt,
                ) from exc
            last_error_code = exc.error_code
            last_status_code = exc.status_code
            last_message = exc.message
            if attempt >= settings.upstream_max_attempts:
                break
            logger.warning(
                "websocket open failed attempt=%s/%s error=%s url=%s",
                attempt,
                settings.upstream_max_attempts,
                exc.error_code,
                url,
            )

        if attempt < settings.upstream_max_attempts and settings.retry_backoff_seconds:
            await asyncio.sleep(settings.retry_backoff_seconds)

    raise WebSocketBridgeError(
        status_code=last_status_code,
        error_code=last_error_code,
        message=last_message,
        attempts=settings.upstream_max_attempts,
    )


async def stream_websocket_as_sse(
    opened: OpenedWebSocketBridge,
    settings: Settings,
) -> AsyncIterator[bytes]:
    try:
        while True:
            if opened.first_message is not None:
                message = opened.first_message
                opened.first_message = None
            else:
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
            await _discard_or_close(opened)


def _schedule_response_processed(
    opened: OpenedWebSocketBridge,
    settings: Settings,
    response_id: str,
) -> None:
    opened.close_owned_by_background = True
    if not settings.websocket_send_response_processed:
        task = asyncio.create_task(_release_or_close(opened))
    else:
        task = asyncio.create_task(_send_response_processed_and_release(opened, settings, response_id))
    opened.background_tasks.add(task)
    task.add_done_callback(opened.background_tasks.discard)


async def _send_response_processed_and_release(
    opened: OpenedWebSocketBridge,
    settings: Settings,
    response_id: str,
) -> None:
    processed_sent = await _send_response_processed(opened, settings, response_id)
    if processed_sent:
        await _release_or_close(opened)
    else:
        await _discard_or_close(opened)


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


async def _open_websocket_connection(
    *,
    settings: Settings,
    connect_factory: WebSocketConnectFactory,
    url: str,
    headers: list[tuple[str, str]],
) -> tuple[Any, Any]:
    context: Any | None = None
    try:
        context = connect_factory(url, headers, settings)
        if inspect.isawaitable(context):
            context = await context
        websocket = await context.__aenter__()
    except asyncio.TimeoutError as exc:
        if context is not None:
            await _close_context_quietly(context)
        raise WebSocketBridgeError(
            status_code=504,
            error_code="websocket_open_timeout",
            message="timed out opening websocket or sending response.create",
        ) from exc
    except (OSError, WebSocketException) as exc:
        if context is not None:
            await _close_context_quietly(context)
        raise WebSocketBridgeError(
            status_code=502,
            error_code="websocket_open_error",
            message=f"failed to open websocket: {exc.__class__.__name__}",
        ) from exc
    except Exception as exc:
        if context is not None:
            await _close_context_quietly(context)
        raise WebSocketBridgeError(
            status_code=502,
            error_code="websocket_open_error",
            message=f"failed to open websocket: {exc.__class__.__name__}",
        ) from exc
    return websocket, context


async def _send_websocket_json(websocket: Any, payload: dict[str, Any], *, timeout: float) -> None:
    await asyncio.wait_for(
        websocket.send(json.dumps(payload, separators=(",", ":"))),
        timeout=timeout,
    )


async def _release_or_close(opened: OpenedWebSocketBridge) -> None:
    if opened.pool is None:
        await _close_context_quietly(opened.context)
        return
    await opened.pool.release(opened)


async def _discard_or_close(opened: OpenedWebSocketBridge) -> None:
    await _close_context_quietly(opened.context)


def _is_websocket_pool_identity_header(
    name: str,
    configured_key_headers: set[str],
    configured_key_header: str,
) -> bool:
    if name == configured_key_header or name in configured_key_headers:
        return True
    return "auth" in name or "token" in name


def _websocket_appears_open(websocket: Any) -> bool:
    closed = getattr(websocket, "closed", None)
    if isinstance(closed, bool):
        return not closed

    state = getattr(websocket, "state", None)
    if state is None:
        return True
    state_name = getattr(state, "name", "")
    if isinstance(state_name, str) and state_name.upper() in {"CLOSING", "CLOSED"}:
        return False
    return str(state).upper() not in {"CLOSING", "CLOSED"}


async def _websocket_has_unexpected_pending_message(websocket: Any) -> bool:
    try:
        message = await asyncio.wait_for(websocket.recv(), timeout=WEBSOCKET_POOL_DRAIN_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        return False
    except Exception:
        return True

    text = _websocket_message_to_text(message)
    logger.warning(
        "discarding pooled websocket with unexpected pending message event=%s",
        _event_type(text),
    )
    return True


async def _send_response_processed(
    opened: OpenedWebSocketBridge,
    settings: Settings,
    response_id: str,
) -> bool:
    if not settings.websocket_send_response_processed:
        return True

    request = {"type": "response.processed", "response_id": response_id}
    try:
        await _send_websocket_json(
            opened.websocket,
            request,
            timeout=settings.websocket_processed_timeout_seconds,
        )
    except Exception as exc:
        logger.warning(
            "failed to send response.processed response_id=%s error=%s",
            response_id,
            exc.__class__.__name__,
        )
        return False
    return True


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
