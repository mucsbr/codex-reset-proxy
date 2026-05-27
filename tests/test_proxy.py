from __future__ import annotations

import asyncio
import json
from collections.abc import Iterable

import httpx
import pytest

from codex_reset_proxy.app import create_app
from codex_reset_proxy.config import Settings
from codex_reset_proxy.proxy import OpenedUpstream, stream_upstream_body


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
        self.closed = False

    async def aiter_raw(self):
        for chunk in self._chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class FakeStreamContext:
    def __init__(self, *, response: FakeResponse | None = None, header_delay: float = 0.0) -> None:
        self.response = response or FakeResponse()
        self.header_delay = header_delay
        self.exited = False
        self.exit_exc_type = None

    async def __aenter__(self):
        if self.header_delay:
            await asyncio.sleep(self.header_delay)
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        self.exited = True
        self.exit_exc_type = exc_type


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


class FakeWebSocket:
    def __init__(self, messages: Iterable[str | bytes]) -> None:
        self.messages = list(messages)
        self.sent: list[str] = []
        self.allow_send = asyncio.Event()
        self.allow_send.set()
        self.block_send_number: int | None = None
        self.recv_delay_seconds = 0.0

    async def send(self, message: str) -> None:
        if self.block_send_number == len(self.sent) + 1:
            await self.allow_send.wait()
        self.sent.append(message)

    async def recv(self) -> str | bytes:
        if self.recv_delay_seconds:
            await asyncio.sleep(self.recv_delay_seconds)
        if not self.messages:
            raise AssertionError("unexpected websocket recv")
        return self.messages.pop(0)


class FakeWebSocketContext:
    def __init__(self, websocket: FakeWebSocket) -> None:
        self.websocket = websocket
        self.exited = False

    async def __aenter__(self) -> FakeWebSocket:
        return self.websocket

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.exited = True


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


@pytest.mark.asyncio
async def test_stream_upstream_body_closes_without_throwing_generator_exit_into_context():
    calls: list[dict] = []
    response = FakeResponse(chunks=(b"one", b"two"))
    context = FakeStreamContext(response=response)
    client = FakeClient(context, calls)
    opened = OpenedUpstream(response=response, stream_context=context, client=client, attempts=1)

    stream = stream_upstream_body(opened)
    assert await anext(stream) == b"one"
    await stream.aclose()

    assert response.closed
    assert context.exited
    assert context.exit_exc_type is None
    assert client.closed


@pytest.mark.asyncio
async def test_websocket_per_request_wraps_request_streams_sse_and_sends_processed():
    websocket = FakeWebSocket(
        [
            json.dumps({"type": "response.created", "response": {"id": "resp-1"}}),
            json.dumps({"type": "response.completed", "response": {"id": "resp-1"}}),
        ]
    )
    context = FakeWebSocketContext(websocket)
    calls: list[dict] = []

    def websocket_connect_factory(url: str, headers: list[tuple[str, str]], settings: Settings):
        calls.append({"url": url, "headers": headers, "settings": settings})
        return context

    app = create_app(
        Settings(
            upstream_base_url="https://api.example.test/base",
            transport_mode="websocket_per_request",
        ),
        websocket_connect_factory=websocket_connect_factory,
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://proxy") as client:
        response = await client.post(
            "/responses?foo=bar",
            headers={
                "authorization": "Bearer client-key",
                "content-type": "application/json",
                "session-id": "session-1",
                "thread-id": "thread-1",
            },
            json={"model": "gpt-test", "input": [], "stream": True},
        )

    assert response.status_code == 200
    assert response.headers["x-codex-reset-proxy-transport"] == "websocket_per_request"
    assert response.headers["x-codex-reset-proxy-attempts"] == "1"
    assert calls[0]["url"] == "wss://api.example.test/base/responses?foo=bar"

    header_names = {name.lower() for name, _ in calls[0]["headers"]}
    assert "authorization" in header_names
    assert "session-id" in header_names
    assert "thread-id" in header_names
    assert "content-type" not in header_names
    assert "content-length" not in header_names

    sent_create = json.loads(websocket.sent[0])
    assert sent_create == {
        "type": "response.create",
        "model": "gpt-test",
        "input": [],
        "stream": True,
    }
    assert json.loads(websocket.sent[1]) == {
        "type": "response.processed",
        "response_id": "resp-1",
    }
    assert context.exited

    assert "event: response.created\n" in response.text
    assert "event: response.completed\n" in response.text
    assert 'data: {"type": "response.completed", "response": {"id": "resp-1"}}\n\n' in response.text


@pytest.mark.asyncio
async def test_websocket_per_request_finishes_client_stream_before_processed_ack():
    websocket = FakeWebSocket(
        [
            json.dumps({"type": "response.completed", "response": {"id": "resp-1"}}),
        ]
    )
    websocket.block_send_number = 2
    websocket.allow_send.clear()
    context = FakeWebSocketContext(websocket)

    def websocket_connect_factory(url: str, headers: list[tuple[str, str]], settings: Settings):
        return context

    app = create_app(
        Settings(
            upstream_base_url="https://api.example.test/base",
            transport_mode="websocket_per_request",
        ),
        websocket_connect_factory=websocket_connect_factory,
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://proxy") as client:
        response_task = asyncio.create_task(
            client.post(
                "/responses",
                json={"model": "gpt-test", "input": [], "stream": True},
            )
        )
        await asyncio.sleep(0)
        response = await asyncio.wait_for(response_task, timeout=1)

    assert response.status_code == 200
    assert "event: response.completed\n" in response.text
    assert len(websocket.sent) == 1
    assert not context.exited

    websocket.allow_send.set()
    for _ in range(10):
        if context.exited:
            break
        await asyncio.sleep(0.01)

    assert json.loads(websocket.sent[1]) == {
        "type": "response.processed",
        "response_id": "resp-1",
    }
    assert context.exited


@pytest.mark.asyncio
async def test_websocket_per_request_first_message_uses_first_message_timeout():
    websocket = FakeWebSocket(
        [
            json.dumps({"type": "response.created", "response": {"id": "resp-1"}}),
        ]
    )
    websocket.recv_delay_seconds = 0.05
    context = FakeWebSocketContext(websocket)
    calls: list[dict] = []

    def websocket_connect_factory(url: str, headers: list[tuple[str, str]], settings: Settings):
        calls.append({"url": url, "headers": headers, "settings": settings})
        return context

    app = create_app(
        Settings(
            upstream_base_url="https://api.example.test/base",
            transport_mode="websocket_per_request",
            websocket_first_message_timeout_seconds=0.01,
            websocket_idle_timeout_seconds=1,
            upstream_max_attempts=2,
            retry_backoff_seconds=0,
        ),
        websocket_connect_factory=websocket_connect_factory,
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://proxy") as client:
        response = await client.post(
            "/responses",
            json={"model": "gpt-test", "input": [], "stream": True},
        )

    assert response.status_code == 504
    assert response.headers["x-codex-reset-proxy-error"] == "websocket_first_message_timeout"
    assert response.headers["x-codex-reset-proxy-attempts"] == "2"
    assert len(calls) == 2
    assert context.exited


@pytest.mark.asyncio
async def test_websocket_per_request_retries_until_first_message_before_streaming():
    first_websocket = FakeWebSocket(
        [
            json.dumps({"type": "response.created", "response": {"id": "resp-timeout"}}),
        ]
    )
    first_websocket.recv_delay_seconds = 0.05
    second_websocket = FakeWebSocket(
        [
            json.dumps({"type": "response.created", "response": {"id": "resp-2"}}),
            json.dumps({"type": "response.completed", "response": {"id": "resp-2"}}),
        ]
    )
    contexts = [FakeWebSocketContext(first_websocket), FakeWebSocketContext(second_websocket)]
    calls: list[dict] = []

    def websocket_connect_factory(url: str, headers: list[tuple[str, str]], settings: Settings):
        calls.append({"url": url, "headers": headers, "settings": settings})
        return contexts[len(calls) - 1]

    app = create_app(
        Settings(
            upstream_base_url="https://api.example.test/base",
            transport_mode="websocket_per_request",
            websocket_first_message_timeout_seconds=0.01,
            upstream_max_attempts=2,
            retry_backoff_seconds=0,
        ),
        websocket_connect_factory=websocket_connect_factory,
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://proxy") as client:
        response = await client.post(
            "/responses",
            json={"model": "gpt-test", "input": [], "stream": True},
        )

    assert response.status_code == 200
    assert response.headers["x-codex-reset-proxy-attempts"] == "2"
    assert len(calls) == 2
    assert contexts[0].exited

    for _ in range(10):
        if contexts[1].exited:
            break
        await asyncio.sleep(0.01)

    assert contexts[1].exited
    assert json.loads(second_websocket.sent[1]) == {
        "type": "response.processed",
        "response_id": "resp-2",
    }
    assert "resp-timeout" not in response.text
    assert "resp-2" in response.text


@pytest.mark.asyncio
async def test_websocket_per_request_rejects_invalid_json_before_connecting():
    calls: list[dict] = []

    def websocket_connect_factory(url: str, headers: list[tuple[str, str]], settings: Settings):
        calls.append({"url": url, "headers": headers, "settings": settings})
        raise AssertionError("websocket should not be opened for invalid JSON")

    app = create_app(
        Settings(
            upstream_base_url="https://api.example.test",
            transport_mode="websocket_per_request",
        ),
        websocket_connect_factory=websocket_connect_factory,
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://proxy") as client:
        response = await client.post("/responses", content=b"{not-json")

    assert response.status_code == 400
    assert response.headers["x-codex-reset-proxy-error"] == "invalid_request_json"
    assert calls == []


@pytest.mark.asyncio
async def test_websocket_per_request_rejects_non_post_methods():
    app = create_app(
        Settings(
            upstream_base_url="https://api.example.test",
            transport_mode="websocket_per_request",
        )
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://proxy") as client:
        response = await client.get("/responses")

    assert response.status_code == 405
