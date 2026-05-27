from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass

import httpx
from starlette.requests import Request

from codex_reset_proxy.config import Settings

logger = logging.getLogger(__name__)

HOP_BY_HOP_HEADERS = {
    b"connection",
    b"keep-alive",
    b"proxy-authenticate",
    b"proxy-authorization",
    b"proxy-connection",
    b"te",
    b"trailer",
    b"transfer-encoding",
    b"upgrade",
}
REQUEST_ONLY_HEADERS = {
    b"host",
}

ClientFactory = Callable[[Settings], httpx.AsyncClient]


class RequestBodyTooLarge(Exception):
    pass


class UpstreamOpenError(Exception):
    def __init__(self, *, status_code: int, error_code: str, message: str, attempts: int) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.message = message
        self.attempts = attempts


@dataclass
class OpenedUpstream:
    response: httpx.Response
    stream_context: object
    client: httpx.AsyncClient
    attempts: int


def default_client_factory(settings: Settings) -> httpx.AsyncClient:
    timeout = httpx.Timeout(
        connect=settings.connect_timeout_seconds,
        read=None,
        write=settings.write_timeout_seconds,
        pool=settings.pool_timeout_seconds,
    )
    return httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=False,
        proxy=settings.outbound_proxy,
    )


async def read_limited_body(request: Request, limit: int) -> bytes:
    size = 0
    chunks: list[bytes] = []
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise RequestBodyTooLarge(f"request body exceeds {limit} bytes")
        chunks.append(chunk)
    return b"".join(chunks)


def build_upstream_url(settings: Settings, request: Request) -> str:
    raw_path = request.scope.get("raw_path", request.url.path.encode("ascii"))
    path = raw_path.decode("latin-1")
    if not path.startswith("/"):
        path = f"/{path}"

    query = request.scope.get("query_string", b"").decode("latin-1")
    url = f"{settings.upstream_base_url}{path}"
    if query:
        url = f"{url}?{query}"
    return url


def filtered_request_headers(settings: Settings, request: Request) -> list[tuple[bytes, bytes]]:
    return filtered_request_headers_from_raw(settings, request.headers.raw)


def filtered_request_headers_from_raw(
    settings: Settings,
    raw_headers: list[tuple[bytes, bytes]],
) -> list[tuple[bytes, bytes]]:
    headers = _filter_raw_headers(raw_headers, extra_skip=REQUEST_ONLY_HEADERS)
    if not settings.upstream_api_key:
        return headers

    api_key_header = settings.upstream_api_key_header.encode("latin-1")
    filtered = [(name, value) for name, value in headers if name.lower() != api_key_header.lower()]
    api_key_value = f"{settings.upstream_api_key_prefix}{settings.upstream_api_key}".encode("latin-1")
    filtered.append((api_key_header, api_key_value))
    return filtered


def filtered_response_headers(response: httpx.Response) -> list[tuple[bytes, bytes]]:
    return _filter_raw_headers(response.headers.raw)


def _filter_raw_headers(
    raw_headers: list[tuple[bytes, bytes]],
    *,
    extra_skip: set[bytes] | None = None,
) -> list[tuple[bytes, bytes]]:
    skip = set(HOP_BY_HOP_HEADERS)
    if extra_skip:
        skip.update(extra_skip)
    for name, value in raw_headers:
        if name.lower() == b"connection":
            skip.update(_split_header_tokens(value))

    return [(name, value) for name, value in raw_headers if name.lower() not in skip]


def _split_header_tokens(value: bytes) -> set[bytes]:
    return {part.strip().lower() for part in value.split(b",") if part.strip()}


async def open_upstream_with_retries(
    *,
    settings: Settings,
    client_factory: ClientFactory,
    method: str,
    url: str,
    headers: list[tuple[bytes, bytes]],
    body: bytes,
) -> OpenedUpstream:
    last_error_code = "upstream_request_failed"
    last_status_code = 502
    last_message = "upstream request failed before response headers were received"

    for attempt in range(1, settings.upstream_max_attempts + 1):
        client = client_factory(settings)
        stream_context = client.stream(
            method,
            url,
            headers=headers,
            content=body if body else None,
        )

        started_at = time.monotonic()
        try:
            response = await asyncio.wait_for(
                stream_context.__aenter__(),
                timeout=settings.response_header_timeout_seconds,
            )
            elapsed = time.monotonic() - started_at
            logger.info(
                "upstream headers received attempt=%s status=%s elapsed=%.3fs url=%s",
                attempt,
                response.status_code,
                elapsed,
                url,
            )
            return OpenedUpstream(
                response=response,
                stream_context=stream_context,
                client=client,
                attempts=attempt,
            )
        except TimeoutError:
            elapsed = time.monotonic() - started_at
            await client.aclose()
            last_error_code = "upstream_header_timeout"
            last_status_code = 504
            last_message = (
                "upstream did not return response headers within "
                f"{settings.response_header_timeout_seconds:g}s"
            )
            logger.warning(
                "upstream header timeout attempt=%s/%s elapsed=%.3fs url=%s",
                attempt,
                settings.upstream_max_attempts,
                elapsed,
                url,
            )
        except httpx.TimeoutException as exc:
            elapsed = time.monotonic() - started_at
            await client.aclose()
            last_error_code = "upstream_timeout"
            last_status_code = 504
            last_message = f"upstream timeout before response headers: {exc.__class__.__name__}"
            logger.warning(
                "upstream timeout attempt=%s/%s elapsed=%.3fs error=%s url=%s",
                attempt,
                settings.upstream_max_attempts,
                elapsed,
                exc.__class__.__name__,
                url,
            )
        except httpx.RequestError as exc:
            elapsed = time.monotonic() - started_at
            await client.aclose()
            last_error_code = "upstream_request_error"
            last_status_code = 502
            last_message = f"upstream request failed before response headers: {exc.__class__.__name__}"
            logger.warning(
                "upstream request error attempt=%s/%s elapsed=%.3fs error=%s url=%s",
                attempt,
                settings.upstream_max_attempts,
                elapsed,
                exc.__class__.__name__,
                url,
            )

        if attempt < settings.upstream_max_attempts and settings.retry_backoff_seconds:
            await asyncio.sleep(settings.retry_backoff_seconds)

    raise UpstreamOpenError(
        status_code=last_status_code,
        error_code=last_error_code,
        message=last_message,
        attempts=settings.upstream_max_attempts,
    )


async def stream_upstream_body(opened: OpenedUpstream) -> AsyncIterator[bytes]:
    try:
        async for chunk in opened.response.aiter_raw():
            yield chunk
    finally:
        await close_upstream(opened)


async def close_upstream(opened: OpenedUpstream) -> None:
    response_aclose = getattr(opened.response, "aclose", None)
    if response_aclose is not None:
        await response_aclose()

    try:
        await opened.stream_context.__aexit__(None, None, None)
    except RuntimeError as exc:
        if "asynchronous generator is already running" not in str(exc):
            raise
        logger.debug("httpx stream context was already closing", exc_info=True)
    finally:
        await opened.client.aclose()
