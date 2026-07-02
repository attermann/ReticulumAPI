"""Phase 1 smoke test: HTTP + HTTPS listeners serving /health, /version, /ws.

Runs end-to-end against a real aiohttp server. Uses plaintext for HTTP checks
and a self-signed cert for HTTPS checks (with cert verification disabled — this
is a smoke test, not a TLS conformance test).
"""
from __future__ import annotations

import asyncio
import ssl

import aiohttp
import pytest

from rnsapi.config import Config, NetworkConfig, TlsConfig
from rnsapi.server import build_app, build_ssl
from rnsapi.tls import ensure_self_signed


async def _read_attach(ws):
    while True:
        msg = await asyncio.wait_for(ws.receive(), timeout=5)
        if msg.type.name != "TEXT":
            continue
        ev = msg.json()
        if ev.get("type") == "auth.session.attached":
            return ev


async def _find_free_port() -> int:
    import socket

    s = socket.socket()
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


@pytest.mark.asyncio
async def test_plaintext_health_version_ws(rnsapi_home):
    cfg = Config()
    cfg.network = NetworkConfig(bind_host="127.0.0.1", bind_port=0, tls=False, allow_http=False)
    cfg.tls = TlsConfig(mode="disabled")

    app = build_app(cfg, rnsapi_home)

    from aiohttp.test_utils import TestClient, TestServer

    server = TestServer(app)
    await server.start_server()
    try:
        async with TestClient(server) as client:
            r = await client.get("/health")
            assert r.status == 200
            body = await r.json()
            assert body == {"status": "ok"}

            r = await client.get("/version")
            assert r.status == 200
            body = await r.json()
            assert body["name"] == "rnsapid"
            assert body["protocol"] == {"rest": 1, "ws": 1}

            async with client.ws_connect("/ws") as ws:
                # In auth-disabled mode the server auto-attaches an anonymous session
                attach = await _read_attach(ws)
                assert attach["is_anonymous"] is True
                await ws.close()
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_https_and_wss_with_self_signed_cert(rnsapi_home):
    cfg = Config()
    cfg.network = NetworkConfig(bind_host="127.0.0.1", bind_port=0, tls=True, allow_http=False)
    cfg.tls = TlsConfig(mode="self_signed", self_signed_cn="localhost")

    ssl_ctx = build_ssl(cfg, rnsapi_home)
    assert ssl_ctx is not None

    # Verify cert was actually generated to disk.
    cert_paths = ensure_self_signed(rnsapi_home.certs_dir, cfg.tls.self_signed_cn)
    assert cert_paths.cert.exists()
    assert cert_paths.key.exists()

    app = build_app(cfg, rnsapi_home)

    from aiohttp import web

    runner = web.AppRunner(app)
    await runner.setup()
    port = await _find_free_port()
    site = web.TCPSite(runner, "127.0.0.1", port, ssl_context=ssl_ctx)
    await site.start()

    # Client-side: skip verification, self-signed
    client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    client_ctx.check_hostname = False
    client_ctx.verify_mode = ssl.CERT_NONE

    try:
        connector = aiohttp.TCPConnector(ssl=client_ctx)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(f"https://127.0.0.1:{port}/health") as r:
                assert r.status == 200
                body = await r.json()
                assert body == {"status": "ok"}

            async with session.ws_connect(f"wss://127.0.0.1:{port}/ws") as ws:
                attach = await _read_attach(ws)
                assert attach["is_anonymous"] is True
                await ws.close()
    finally:
        await site.stop()
        await runner.cleanup()


@pytest.mark.asyncio
async def test_dual_listener_when_allow_http(rnsapi_home):
    """When tls=true and allow_http=true, both ports must serve /health."""
    cfg = Config()
    cfg.network = NetworkConfig(
        bind_host="127.0.0.1", bind_port=0, tls=True, allow_http=True, http_port=0
    )
    cfg.tls = TlsConfig(mode="self_signed", self_signed_cn="localhost")

    ssl_ctx = build_ssl(cfg, rnsapi_home)
    app = build_app(cfg, rnsapi_home)

    from aiohttp import web

    runner = web.AppRunner(app)
    await runner.setup()
    tls_port = await _find_free_port()
    plain_port = await _find_free_port()
    tls_site = web.TCPSite(runner, "127.0.0.1", tls_port, ssl_context=ssl_ctx)
    plain_site = web.TCPSite(runner, "127.0.0.1", plain_port)
    await tls_site.start()
    await plain_site.start()

    client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    client_ctx.check_hostname = False
    client_ctx.verify_mode = ssl.CERT_NONE

    try:
        connector = aiohttp.TCPConnector(ssl=client_ctx)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(f"https://127.0.0.1:{tls_port}/health") as r:
                assert r.status == 200
            async with session.get(f"http://127.0.0.1:{plain_port}/health") as r:
                assert r.status == 200
    finally:
        await tls_site.stop()
        await plain_site.stop()
        await runner.cleanup()
