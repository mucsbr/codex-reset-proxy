from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Settings:
    upstream_base_url: str = ""
    upstream_api_key: str | None = None
    upstream_api_key_header: str = "Authorization"
    upstream_api_key_prefix: str = "Bearer "
    transport_mode: str = "http"
    response_header_timeout_seconds: float = 30.0
    upstream_max_attempts: int = 2
    connect_timeout_seconds: float = 10.0
    write_timeout_seconds: float = 30.0
    pool_timeout_seconds: float = 10.0
    websocket_idle_timeout_seconds: float = 600.0
    websocket_processed_timeout_seconds: float = 10.0
    websocket_send_response_processed: bool = True
    max_request_body_bytes: int = 32 * 1024 * 1024
    retry_backoff_seconds: float = 0.25
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Settings":
        upstream_base_url = _env_required("UPSTREAM_BASE_URL").rstrip("/")
        _validate_upstream_base_url(upstream_base_url)
        upstream_api_key_header = _env_str("UPSTREAM_API_KEY_HEADER", cls.upstream_api_key_header).strip()
        if not upstream_api_key_header:
            raise ValueError("UPSTREAM_API_KEY_HEADER cannot be empty")
        transport_mode = _env_str("TRANSPORT_MODE", cls.transport_mode)
        _validate_transport_mode(transport_mode)

        return cls(
            upstream_base_url=upstream_base_url,
            upstream_api_key=_env_optional("UPSTREAM_API_KEY"),
            upstream_api_key_header=upstream_api_key_header,
            upstream_api_key_prefix=_env_str("UPSTREAM_API_KEY_PREFIX", cls.upstream_api_key_prefix),
            transport_mode=transport_mode,
            response_header_timeout_seconds=_env_float(
                "RESPONSE_HEADER_TIMEOUT_SECONDS",
                cls.response_header_timeout_seconds,
            ),
            upstream_max_attempts=max(1, _env_int("UPSTREAM_MAX_ATTEMPTS", cls.upstream_max_attempts)),
            connect_timeout_seconds=_env_float("CONNECT_TIMEOUT_SECONDS", cls.connect_timeout_seconds),
            write_timeout_seconds=_env_float("WRITE_TIMEOUT_SECONDS", cls.write_timeout_seconds),
            pool_timeout_seconds=_env_float("POOL_TIMEOUT_SECONDS", cls.pool_timeout_seconds),
            websocket_idle_timeout_seconds=_env_float(
                "WEBSOCKET_IDLE_TIMEOUT_SECONDS",
                cls.websocket_idle_timeout_seconds,
            ),
            websocket_processed_timeout_seconds=_env_float(
                "WEBSOCKET_PROCESSED_TIMEOUT_SECONDS",
                cls.websocket_processed_timeout_seconds,
            ),
            websocket_send_response_processed=_env_bool(
                "WEBSOCKET_SEND_RESPONSE_PROCESSED",
                cls.websocket_send_response_processed,
            ),
            max_request_body_bytes=max(1, _env_int("MAX_REQUEST_BODY_BYTES", cls.max_request_body_bytes)),
            retry_backoff_seconds=max(0.0, _env_float("RETRY_BACKOFF_SECONDS", cls.retry_backoff_seconds)),
            log_level=_env_str("LOG_LEVEL", cls.log_level).upper(),
        )


def _env_required(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        raise ValueError(f"{name} is required")
    return value


def _env_optional(name: str) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return None
    return value


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None or value == "" else value


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return float(value)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def _validate_upstream_base_url(value: str) -> None:
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("UPSTREAM_BASE_URL must be an absolute http:// or https:// URL")


def _validate_transport_mode(value: str) -> None:
    if value not in {"http", "websocket_per_request"}:
        raise ValueError("TRANSPORT_MODE must be one of: http, websocket_per_request")
