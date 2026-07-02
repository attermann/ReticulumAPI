"""aiohttp app factory and lifecycle for rnsapid.

Phase 1 exposes:
- GET  /health  — liveness probe
- GET  /version — version + protocol info
- GET  /ws     — echo WebSocket

Later phases add REST + WS handlers by wiring service classes onto app[...] and
registering routes here. Both HTTPS and (optional) HTTP listeners share this
same app factory so every endpoint is reachable on both listeners identically.
"""
from __future__ import annotations

import asyncio
import logging
import ssl
from pathlib import Path

from aiohttp import WSMsgType, web

from . import __version__
from .config import Config
from .paths import StoragePaths
from .tls import build_ssl_context, cert_fingerprint_sha256, ensure_self_signed


log = logging.getLogger(__name__)


async def _health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def _version(request: web.Request) -> web.Response:
    return web.json_response(
        {
            "name": "rnsapid",
            "version": __version__,
            "protocol": {"rest": 1, "ws": 1},
        }
    )


async def _ws_echo(request: web.Request) -> web.WebSocketResponse:
    """Phase 1 placeholder WS handler — echoes text/JSON frames back.

    Replaced in Phase 2 with the auth + router pipeline.
    """
    ws = web.WebSocketResponse(heartbeat=30, receive_timeout=90)
    await ws.prepare(request)
    log.info("ws client connected: %s", request.remote)
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                await ws.send_str(msg.data)
            elif msg.type == WSMsgType.BINARY:
                await ws.send_bytes(msg.data)
            elif msg.type == WSMsgType.ERROR:
                log.warning("ws error: %s", ws.exception())
                break
    finally:
        log.info("ws client disconnected: %s", request.remote)
    return ws


def build_app(config: Config, storage: StoragePaths) -> web.Application:
    app = web.Application(client_max_size=config.limits.max_ws_message_bytes)
    app["config"] = config
    app["storage"] = storage
    app.router.add_get("/health", _health)
    app.router.add_get("/version", _version)
    app.router.add_get("/ws", _ws_echo)
    return app


def build_ssl(config: Config, storage: StoragePaths) -> ssl.SSLContext | None:
    if not config.network.tls:
        return None
    if config.tls.mode == "disabled":
        return None
    if config.tls.mode == "user_provided":
        cert = Path(config.tls.cert_file).expanduser()
        key = Path(config.tls.key_file).expanduser()
        return build_ssl_context(cert, key)
    paths = ensure_self_signed(storage.certs_dir, config.tls.self_signed_cn)
    log.info(
        "self-signed cert SHA-256 fingerprint: %s",
        cert_fingerprint_sha256(paths.cert),
    )
    return build_ssl_context(paths.cert, paths.key)


async def run(config: Config, storage: StoragePaths) -> None:
    """Run the daemon until cancelled. Serves TLS and (optional) plaintext ports."""
    app = build_app(config, storage)

    runner = web.AppRunner(app)
    await runner.setup()
    sites: list[web.BaseSite] = []

    if config.network.tls:
        ssl_ctx = build_ssl(config, storage)
        tls_site = web.TCPSite(
            runner, config.network.bind_host, config.network.bind_port, ssl_context=ssl_ctx
        )
        await tls_site.start()
        sites.append(tls_site)
        log.info(
            "listening on https://%s:%d",
            config.network.bind_host,
            config.network.bind_port,
        )

    if config.network.allow_http or not config.network.tls:
        plain_port = (
            config.network.bind_port if not config.network.tls else config.network.http_port
        )
        plain_site = web.TCPSite(runner, config.network.bind_host, plain_port)
        await plain_site.start()
        sites.append(plain_site)
        log.info(
            "listening on http://%s:%d", config.network.bind_host, plain_port
        )

    try:
        stop = asyncio.Event()
        await stop.wait()
    finally:
        for site in sites:
            await site.stop()
        await runner.cleanup()
