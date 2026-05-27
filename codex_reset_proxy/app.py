from __future__ import annotations

import logging

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response, StreamingResponse
from starlette.routing import Route

from codex_reset_proxy.config import Settings
from codex_reset_proxy.proxy import (
    ClientFactory,
    RequestBodyTooLarge,
    UpstreamOpenError,
    build_upstream_url,
    default_client_factory,
    filtered_request_headers,
    filtered_response_headers,
    open_upstream_with_retries,
    read_limited_body,
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

PROXY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]


async def healthz(_: Request) -> Response:
    return JSONResponse({"ok": True})


async def proxy_endpoint(request: Request) -> Response:
    settings: Settings = request.app.state.settings
    client_factory: ClientFactory = request.app.state.client_factory
    websocket_connect_factory: WebSocketConnectFactory = request.app.state.websocket_connect_factory

    try:
        body = await read_limited_body(request, settings.max_request_body_bytes)
    except RequestBodyTooLarge as exc:
        return PlainTextResponse(str(exc), status_code=413)

    upstream_url = build_upstream_url(settings, request)
    request_headers = filtered_request_headers(settings, request)
    if settings.transport_mode == "websocket_per_request":
        if request.method != "POST":
            return PlainTextResponse("websocket_per_request transport only supports POST\n", status_code=405)
        try:
            opened_ws = await open_websocket_bridge_with_retries(
                settings=settings,
                connect_factory=websocket_connect_factory,
                url=build_upstream_ws_url(upstream_url),
                headers=websocket_headers(request_headers),
                body=body,
            )
        except WebSocketBridgeError as exc:
            return PlainTextResponse(
                f"{exc.message}\n",
                status_code=exc.status_code,
                headers={
                    "x-codex-reset-proxy-error": exc.error_code,
                    "x-codex-reset-proxy-attempts": str(exc.attempts),
                },
            )

        return StreamingResponse(
            stream_websocket_as_sse(opened_ws, settings),
            status_code=200,
            media_type="text/event-stream",
            headers={
                "x-codex-reset-proxy-transport": "websocket_per_request",
                "x-codex-reset-proxy-attempts": str(opened_ws.attempts),
            },
        )

    try:
        opened = await open_upstream_with_retries(
            settings=settings,
            client_factory=client_factory,
            method=request.method,
            url=upstream_url,
            headers=request_headers,
            body=body,
        )
    except UpstreamOpenError as exc:
        return PlainTextResponse(
            f"{exc.message} after {exc.attempts} attempt(s)\n",
            status_code=exc.status_code,
            headers={
                "x-codex-reset-proxy-error": exc.error_code,
                "x-codex-reset-proxy-attempts": str(exc.attempts),
            },
        )

    response = StreamingResponse(
        stream_upstream_body(opened),
        status_code=opened.response.status_code,
    )
    response.raw_headers = filtered_response_headers(opened.response)
    response.raw_headers.append((b"x-codex-reset-proxy-attempts", str(opened.attempts).encode("ascii")))
    return response


def create_app(
    settings: Settings | None = None,
    client_factory: ClientFactory = default_client_factory,
    websocket_connect_factory: WebSocketConnectFactory = default_websocket_connect_factory,
) -> Starlette:
    resolved_settings = settings or Settings.from_env()
    logging.basicConfig(
        level=getattr(logging, resolved_settings.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    app = Starlette(
        routes=[
            Route("/healthz", healthz, methods=["GET"]),
            Route("/{path:path}", proxy_endpoint, methods=PROXY_METHODS),
        ]
    )
    app.state.settings = resolved_settings
    app.state.client_factory = client_factory
    app.state.websocket_connect_factory = websocket_connect_factory
    return app
