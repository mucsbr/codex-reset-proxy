# codex-reset-proxy

Small reverse proxy for Codex backend requests. It fails fast when the upstream server accepts a connection but does not return HTTP response headers, then retries only while no upstream headers have been received.

This is intentionally conservative:

- request bodies are buffered so a pre-header retry can replay the request;
- retries stop as soon as any upstream response headers are received;
- once streaming starts, the proxy only forwards bytes and does not retry mid-response.

## Run with Docker Compose

```bash
UPSTREAM_BASE_URL=https://api.openai.com docker compose up --build
```

With a proxy-managed API key:

```bash
UPSTREAM_BASE_URL=https://api.openai.com UPSTREAM_API_KEY=sk-... docker compose up --build
```

If you leave `UPSTREAM_API_KEY` unset, keep sending the key from the client headers and still set `UPSTREAM_BASE_URL`.

You can also copy `.env.example` to `.env` and edit the values before running Compose.

The proxy listens on `http://127.0.0.1:8788` by default. It forwards to `UPSTREAM_BASE_URL` and preserves the client request path and query string. For example, `http://127.0.0.1:8788/v1/chat/completions` becomes `${UPSTREAM_BASE_URL}/v1/chat/completions`.

## SOCKS5 MITM mode

For clients that hard-code `https://chatgpt.com/backend-api/codex/responses` but allow a global SOCKS5 proxy, run the service as a SOCKS5 MITM proxy:

```bash
UPSTREAM_BASE_URL=https://chatgpt.com \
LISTEN_PROTOCOL=socks5_mitm \
OUTBOUND_PROXY=socks5://10.255.200.17:7897 \
TRANSPORT_MODE=http \
docker compose up --build
```

Then point the client proxy setting at:

```text
socks5://127.0.0.1:8788
```

The proxy generates a local CA at `./certs/ca.crt` by default. Install that CA into the client trust store before using `socks5_mitm`; otherwise HTTPS certificate verification will fail. `socks5_mitm` only decrypts `INTERCEPT_HOST:443`. Within that host, only `INTERCEPT_PATHS` get the special HTTP fast-fail or WS bridge behavior; other paths are forwarded as ordinary HTTP. Other SOCKS5 CONNECT hosts are tunneled through `OUTBOUND_PROXY` unchanged.

`TRANSPORT_MODE=http` keeps the existing 30s response-header fast-fail and retry behavior after MITM decrypts the request. `TRANSPORT_MODE=websocket_per_request` converts intercepted Codex Responses HTTP requests into one-shot upstream WebSocket requests.

To reinstall the generated CA into a rebuilt Alpine client container:

```bash
./scripts/install-ca-into-container.sh --container cli-proxy-api --ca ./certs/ca.crt
```

If the client container is on another Docker host, run it from the machine that has `./certs/ca.crt`:

```bash
./scripts/install-ca-into-container.sh --host user@192.168.0.1 --container cli-proxy-api --ca ./certs/ca.crt
```

The script switches Alpine repositories to the Tsinghua mirror by default, installs `ca-certificates` if needed, runs `update-ca-certificates`, and verifies that the CA is present in the container bundle.

## Transport modes

The default `TRANSPORT_MODE=http` is a generic HTTP reverse proxy. It works for OpenAI-compatible endpoints such as `/v1/chat/completions` and for Codex HTTP streaming endpoints.

`TRANSPORT_MODE=websocket_per_request` is an experimental Codex Responses bridge:

- each incoming HTTP `POST` opens one upstream WebSocket connection;
- the upstream WebSocket URL is derived from `UPSTREAM_BASE_URL` plus the original path and query;
- the HTTP JSON request body is wrapped as a WebSocket `response.create` message;
- upstream WebSocket JSON messages are returned to the HTTP client as SSE events;
- after `response.completed` is returned to the HTTP client, the proxy ends the client stream and sends `response.processed` to the upstream WebSocket in the background.

This mode is only intended for the Codex Responses protocol. It is not a generic bridge for `/v1/chat/completions`. It also does not reduce connection count yet, because one HTTP request still creates one WebSocket connection. A later pooled mode would need to key long-lived WebSockets by auth/session headers and Codex thread/window headers.

## Configuration

Environment variables:

| Name | Default | Meaning |
| --- | --- | --- |
| `UPSTREAM_BASE_URL` | required | Base upstream URL. The incoming path and query are appended unchanged. |
| `UPSTREAM_API_KEY` | unset | Optional API key. When unset, the client's auth headers are forwarded unchanged. |
| `UPSTREAM_API_KEY_HEADER` | `Authorization` | Header to write when `UPSTREAM_API_KEY` is set. |
| `UPSTREAM_API_KEY_PREFIX` | `Bearer ` | Prefix used when writing `UPSTREAM_API_KEY`. OpenAI-compatible APIs usually use `Authorization: Bearer <key>`. |
| `PROXY_PORT` | `8788` | Host port used by Docker Compose. |
| `LISTEN_PROTOCOL` | `http_reverse` | `http_reverse`, `socks5_tunnel`, or `socks5_mitm`. |
| `LISTEN_HOST` | `0.0.0.0` | Address to bind inside the container or local process. |
| `LISTEN_PORT` | `8000` | Port to bind inside the container or local process. Compose maps `PROXY_PORT` to this. |
| `OUTBOUND_PROXY` | unset | Optional upstream network exit. Currently supports `socks5://host:port` or `socks5h://host:port`. |
| `TRANSPORT_MODE` | `http` | `http` for normal reverse proxying, or `websocket_per_request` for the one-shot Codex Responses WS bridge. |
| `INTERCEPT_HOST` | upstream host | Host intercepted by `socks5_mitm`, usually `chatgpt.com`. |
| `INTERCEPT_PATHS` | `/backend-api/codex/responses` | Comma-separated origin paths intercepted by `socks5_mitm`. |
| `MITM_CERT_DIR` | `/data/certs` | Directory inside the container where the generated CA and leaf certificates are stored. |
| `MITM_CERT_DIR_HOST` | `./certs` | Host directory mounted to `MITM_CERT_DIR` by Docker Compose. |
| `RESPONSE_HEADER_TIMEOUT_SECONDS` | `30` | Per-attempt time limit for receiving upstream response headers. |
| `UPSTREAM_MAX_ATTEMPTS` | `2` | Total attempts, including the first request. |
| `CONNECT_TIMEOUT_SECONDS` | `10` | TCP/TLS connection timeout per attempt. |
| `WRITE_TIMEOUT_SECONDS` | `30` | Timeout while sending the buffered request to upstream. |
| `POOL_TIMEOUT_SECONDS` | `10` | httpx connection-pool acquisition timeout. |
| `WEBSOCKET_FIRST_MESSAGE_TIMEOUT_SECONDS` | `30` | In `websocket_per_request` mode, maximum wait for the first upstream WebSocket message after `response.create` is sent. |
| `WEBSOCKET_IDLE_TIMEOUT_SECONDS` | `600` | Maximum idle wait for the next upstream WebSocket message in `websocket_per_request` mode. |
| `WEBSOCKET_PROCESSED_TIMEOUT_SECONDS` | `10` | Timeout for sending `response.create` and `response.processed` WebSocket messages. |
| `WEBSOCKET_SEND_RESPONSE_PROCESSED` | `true` | Whether to send `response.processed` after `response.completed` in `websocket_per_request` mode. |
| `MAX_REQUEST_BODY_BYTES` | `33554432` | Maximum buffered request body size. |
| `RETRY_BACKOFF_SECONDS` | `0.25` | Delay between failed pre-header attempts. |
| `LOG_LEVEL` | `INFO` | Python logging level. |

Build-time variables:

| Name | Default | Meaning |
| --- | --- | --- |
| `PYTHON_IMAGE` | `python:3.12-slim` | Base image used by Docker. Override this if Docker Hub access is slow or blocked. |
| `PIP_INDEX_URL` | `https://pypi.tuna.tsinghua.edu.cn/simple` | pip package index used during image build. |
| `PIP_TRUSTED_HOST` | `pypi.tuna.tsinghua.edu.cn` | pip trusted host for the configured index. |
| `PIP_DEFAULT_TIMEOUT` | `120` | pip network timeout during image build. |

## Important deployment note

This service is a reverse proxy. Point the client API base URL at this service, for example `http://127.0.0.1:8788`. If the client requests `/v1/chat/completions`, the proxy requests `${UPSTREAM_BASE_URL}/v1/chat/completions`.

For most OpenAI-compatible APIs the key header is `Authorization: Bearer <key>`, which is the default when `UPSTREAM_API_KEY` is set. If an upstream expects a different header, configure it explicitly, for example `UPSTREAM_API_KEY_HEADER=x-api-key UPSTREAM_API_KEY_PREFIX=`.

A normal `HTTPS_PROXY` forward proxy receives a CONNECT tunnel. Without TLS interception it cannot see HTTP response headers and cannot replay an encrypted request, so it cannot safely implement this header-timeout retry behavior.

## China Network Notes

Docker builds default to the Tsinghua PyPI mirror for Python dependencies. If you want the official PyPI index instead:

```bash
PIP_INDEX_URL=https://pypi.org/simple PIP_TRUSTED_HOST= docker compose build
```

If pulling `python:3.12-slim` from Docker Hub is slow, set `PYTHON_IMAGE` to an image your server can pull, then rebuild:

```bash
PYTHON_IMAGE=<your-mirror>/python:3.12-slim docker compose build
```

## Local development

```bash
python -m pip install -e ".[dev]"
pytest
UPSTREAM_BASE_URL=https://api.openai.com LISTEN_HOST=127.0.0.1 LISTEN_PORT=8788 python -m codex_reset_proxy.server
```

##  Thanks


<p>
  <a href="https://linux.do">
    <img src="https://img.shields.io/badge/LinuxDo-community-1f6feb" alt="LinuxDo">
  </a>
</p>

## License

Apache License 2.0.
