from __future__ import annotations

import asyncio
import logging

import uvicorn

from codex_reset_proxy.config import Settings
from codex_reset_proxy.socks_mitm import serve_socks5


def main() -> None:
    settings = Settings.from_env()
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if settings.listen_protocol == "http_reverse":
        uvicorn.run(
            "codex_reset_proxy.app:create_app",
            factory=True,
            host=settings.listen_host,
            port=settings.listen_port,
            proxy_headers=True,
        )
        return

    asyncio.run(serve_socks5(settings))


if __name__ == "__main__":
    main()
