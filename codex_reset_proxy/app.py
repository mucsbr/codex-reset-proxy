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

logger = logging.getLogger(__name__)

PROXY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]


async def healthz(_: Request) -> Response:
    return JSONResponse({"ok": True})


async def proxy_endpoint(request: Request) -> Response:
    settings: Settings = request.app.state.settings
    client_factory: ClientFactory = request.app.state.client_factory

    try:
        body = await read_limited_body(request, settings.max_request_body_bytes)
    except RequestBodyTooLarge as exc:
        return PlainTextResponse(str(exc), status_code=413)

    upstream_url = build_upstream_url(settings, request)
    request_headers = filtered_request_headers(settings, request)

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
    return app
