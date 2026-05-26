from __future__ import annotations

import asyncio
from collections.abc import Iterable

import httpx
import pytest

from codex_reset_proxy.app import create_app
from codex_reset_proxy.config import Settings


class FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        headers: Iterable[tuple[bytes, bytes]] | None = None,
        chunks: Iterable[bytes] = (b"ok",),
    ) -> None:
        self.status_code = status_code
        self.headers = httpx.Headers(headers or [(b"content-type", b"text/plain")])
        self._chunks = list(chunks)

    async def aiter_raw(self):
        for chunk in self._chunks:
            yield chunk


class FakeStreamContext:
    def __init__(self, *, response: FakeResponse | None = None, header_delay: float = 0.0) -> None:
        self.response = response or FakeResponse()
        self.header_delay = header_delay
        self.exited = False

    async def __aenter__(self):
        if self.header_delay:
            await asyncio.sleep(self.header_delay)
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        self.exited = True


class FakeClient:
    def __init__(self, context: FakeStreamContext, calls: list[dict]) -> None:
        self.context = context
        self.calls = calls
        self.closed = False

    def stream(self, method: str, url: str, *, headers, content):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                "content": content,
            }
        )
        return self.context

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_retries_when_upstream_headers_timeout_before_success():
    calls: list[dict] = []
    contexts = [
        FakeStreamContext(header_delay=0.05),
        FakeStreamContext(
            response=FakeResponse(
                headers=[
                    (b"content-type", b"text/plain"),
                    (b"connection", b"close"),
                    (b"x-upstream", b"yes"),
                ],
                chunks=(b"retried",),
            )
        ),
    ]

    def client_factory(_: Settings) -> FakeClient:
        return FakeClient(contexts.pop(0), calls)

    app = create_app(
        Settings(
            upstream_base_url="https://api.example.test/base",
            response_header_timeout_seconds=0.01,
            upstream_max_attempts=2,
            retry_backoff_seconds=0,
        ),
        client_factory=client_factory,
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://proxy") as client:
        response = await client.post("/backend-api/codex/responses?x=1", content=b'{"hello":"world"}')

    assert response.status_code == 200
    assert response.content == b"retried"
    assert response.headers["x-upstream"] == "yes"
    assert "connection" not in response.headers
    assert response.headers["x-codex-reset-proxy-attempts"] == "2"
    assert len(calls) == 2
    assert calls[0]["url"] == "https://api.example.test/base/backend-api/codex/responses?x=1"
    assert calls[1]["content"] == b'{"hello":"world"}'
    assert not any(name.lower() == b"host" for name, _ in calls[1]["headers"])


@pytest.mark.asyncio
async def test_exhausted_header_timeouts_return_504():
    calls: list[dict] = []
    contexts = [
        FakeStreamContext(header_delay=0.05),
        FakeStreamContext(header_delay=0.05),
    ]

    def client_factory(_: Settings) -> FakeClient:
        return FakeClient(contexts.pop(0), calls)

    app = create_app(
        Settings(
            upstream_base_url="https://api.example.test",
            response_header_timeout_seconds=0.01,
            upstream_max_attempts=2,
            retry_backoff_seconds=0,
        ),
        client_factory=client_factory,
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://proxy") as client:
        response = await client.post("/backend-api/codex/responses", content=b"{}")

    assert response.status_code == 504
    assert response.headers["x-codex-reset-proxy-error"] == "upstream_header_timeout"
    assert response.headers["x-codex-reset-proxy-attempts"] == "2"
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_rejects_request_body_larger_than_limit():
    app = create_app(
        Settings(
            upstream_base_url="https://api.example.test",
            max_request_body_bytes=3,
        )
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://proxy") as client:
        response = await client.post("/backend-api/codex/responses", content=b"1234")

    assert response.status_code == 413


@pytest.mark.asyncio
async def test_configured_api_key_overrides_authorization_header():
    calls: list[dict] = []
    contexts = [FakeStreamContext(response=FakeResponse(chunks=(b"ok",)))]

    def client_factory(_: Settings) -> FakeClient:
        return FakeClient(contexts.pop(0), calls)

    app = create_app(
        Settings(
            upstream_base_url="https://api.example.test",
            upstream_api_key="configured-key",
        ),
        client_factory=client_factory,
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://proxy") as client:
        response = await client.post(
            "/v1/chat/completions",
            headers={"authorization": "Bearer client-key"},
            content=b"{}",
        )

    assert response.status_code == 200
    auth_headers = [(name, value) for name, value in calls[0]["headers"] if name.lower() == b"authorization"]
    assert auth_headers == [(b"Authorization", b"Bearer configured-key")]


@pytest.mark.asyncio
async def test_client_api_key_header_is_preserved_when_proxy_key_is_not_configured():
    calls: list[dict] = []
    contexts = [FakeStreamContext(response=FakeResponse(chunks=(b"ok",)))]

    def client_factory(_: Settings) -> FakeClient:
        return FakeClient(contexts.pop(0), calls)

    app = create_app(
        Settings(
            upstream_base_url="https://api.example.test",
        ),
        client_factory=client_factory,
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://proxy") as client:
        response = await client.post(
            "/v1/chat/completions",
            headers={"authorization": "Bearer client-key"},
            content=b"{}",
        )

    assert response.status_code == 200
    auth_headers = [(name, value) for name, value in calls[0]["headers"] if name.lower() == b"authorization"]
    assert auth_headers == [(b"authorization", b"Bearer client-key")]
