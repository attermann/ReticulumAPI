"""aiohttp app factory and lifecycle for rnsapid.

The single aiohttp Application here serves REST + WebSocket on the same port.
Phase-specific handler modules register their routes and their WS message
handlers with the app during `build_app`. The WS entry point (`/ws`) runs the
first-frame auth handshake and then routes inbound frames via `ws_router`.
"""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
from pathlib import Path

from aiohttp import WSMsgType, web

from . import __version__
from .async_bridge import AsyncBridge
from .auth.middleware import auth_middleware
from .auth.session import SessionRegistry
from .config import Config
from .handlers import phase2_auth, phase3_identity, phase4_announce
from .paths import StoragePaths
from .rns.announces import AnnounceService
from .rns.destinations import DestinationService
from .rns.identities import IdentityService
from .rns.service import RNSService
from .tls import build_ssl_context, cert_fingerprint_sha256, ensure_self_signed
from .ws.connection import WSConnection
from .ws.hub import WSHub
from .ws.router import WSRouter


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


async def _ws_handler(request: web.Request) -> web.WebSocketResponse:
    app = request.app
    config: Config = app["config"]
    registry: SessionRegistry = app["sessions"]
    hub: WSHub = app["hub"]
    router: WSRouter = app["ws_router"]

    ws = web.WebSocketResponse(heartbeat=30, receive_timeout=90)
    await ws.prepare(request)
    conn = WSConnection(ws, app=app)
    hub.register(conn)
    log.info("ws client connected: %s (conn=%s)", request.remote, conn.id)

    async def _reject(code: int, reason: str) -> None:
        await conn.send_json({"type": "auth.session.rejected", "reason": reason})
        await ws.close(code=code, message=reason.encode())

    try:
        session = None
        if config.auth.enabled:
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=config.auth.ws_auth_frame_timeout)
            except asyncio.TimeoutError:
                await _reject(4001, "auth_timeout")
                return ws

            if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.CLOSING, WSMsgType.ERROR):
                return ws
            if msg.type != WSMsgType.TEXT:
                await _reject(4001, "auth_required")
                return ws
            try:
                data = json.loads(msg.data)
            except Exception:
                await _reject(4001, "invalid_json")
                return ws
            if data.get("type") != "auth" or not isinstance(data.get("token"), str):
                await _reject(4001, "auth_required")
                return ws

            session = registry.get_by_token(data["token"])
            if session is None:
                await _reject(4001, "invalid_token")
                return ws
        else:
            # Auth disabled: attach to the shared anonymous session unconditionally.
            # An optional first-frame auth with a real token still works; if the
            # first message is not an auth frame, it is dispatched normally.
            session = registry.anonymous()

        conn.attach(session)
        await conn.send_json(
            {
                "type": "auth.session.attached",
                "session_id": session.id,
                "is_anonymous": session.is_anonymous,
            }
        )
        await hub.send_session(
            session.id,
            {"type": "auth.session.connected", "session_id": session.id, "connection_id": conn.id},
        )

        async for frame in ws:
            if frame.type == WSMsgType.TEXT:
                try:
                    data = json.loads(frame.data)
                except Exception:
                    await conn.send_json({"type": "error", "error": "invalid_json"})
                    continue
                if not isinstance(data, dict):
                    await conn.send_json({"type": "error", "error": "invalid_message"})
                    continue
                if conn.session is not None:
                    conn.session.touch()
                await router.dispatch(conn, data)
            elif frame.type == WSMsgType.ERROR:
                log.warning("ws error: %s", ws.exception())
                break
    finally:
        if conn.session is not None:
            await hub.send_session(
                conn.session.id,
                {"type": "auth.session.disconnected", "session_id": conn.session.id, "connection_id": conn.id},
            )
        conn.detach()
        hub.unregister(conn)
        log.info("ws client disconnected: %s (conn=%s)", request.remote, conn.id)
    return ws


def build_app(
    config: Config,
    storage: StoragePaths,
    *,
    start_rns: bool = True,
    rns_service: RNSService | None = None,
) -> web.Application:
    hub = WSHub()
    router = WSRouter()
    sessions = SessionRegistry(config, hub)
    identities = IdentityService(storage)
    destinations = DestinationService()
    announces = AnnounceService(hub)

    app = web.Application(
        client_max_size=config.limits.max_ws_message_bytes,
        middlewares=[auth_middleware],
    )
    app["config"] = config
    app["storage"] = storage
    app["hub"] = hub
    app["ws_router"] = router
    app["sessions"] = sessions
    app["identities"] = identities
    app["destinations"] = destinations
    app["announces"] = announces
    app["rns_service"] = rns_service if rns_service is not None else (RNSService(config) if start_rns else None)
    app["_start_rns"] = start_rns and app["rns_service"] is not None

    # Session cleanup teardowns owned destinations for the ending session.
    sessions.register_cleanup(destinations.cleanup_session)

    app.router.add_get("/health", _health)
    app.router.add_get("/version", _version)
    app.router.add_get("/ws", _ws_handler)

    phase2_auth.register(app)
    phase3_identity.register(app)
    phase4_announce.register(app)

    async def _on_startup(_app):
        AsyncBridge.set_main_loop(asyncio.get_running_loop())
        # RNS.Reticulum initialisation installs signal handlers, so it must
        # run on the main thread. Callers pass `start_rns=False` and call
        # `rns_service.start()` themselves before starting the loop (see cli.py
        # for the daemon path and conftest.py for tests).
        announces.start()
        sessions.start_reaper()

    async def _on_cleanup(_app):
        await sessions.stop_reaper()
        announces.stop()
        if _app["_start_rns"] and _app["rns_service"] is not None:
            _app["rns_service"].stop()
        AsyncBridge.clear_main_loop()

    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
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


async def run(
    config: Config,
    storage: StoragePaths,
    *,
    rns_service: RNSService | None = None,
) -> None:
    """Run the daemon until cancelled. Serves TLS and (optional) plaintext ports."""
    # If the caller started RNS on the main thread already, pass it in and we
    # won't try to start it ourselves.
    start_rns = rns_service is None
    app = build_app(config, storage, start_rns=start_rns, rns_service=rns_service)

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
