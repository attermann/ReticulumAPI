"""Phase 2 smoke test: bearer-token auth for REST + first-frame auth for WS."""
from __future__ import annotations

import asyncio
import time

import pytest
from aiohttp.test_utils import TestClient, TestServer

from rnsapi.auth.passwords import hash_password
from rnsapi.config import Config, AuthConfig, NetworkConfig, TlsConfig
from rnsapi.server import build_app


def _plain_config(auth_enabled: bool = False, password: str = "hunter2") -> Config:
    cfg = Config()
    cfg.network = NetworkConfig(bind_host="127.0.0.1", bind_port=0, tls=False)
    cfg.tls = TlsConfig(mode="disabled")
    cfg.auth = AuthConfig(
        enabled=auth_enabled,
        username="admin",
        password_hash=hash_password(password) if auth_enabled else "",
        ws_auth_frame_timeout=1,
    )
    return cfg


@pytest.mark.asyncio
async def test_anonymous_mode_permits_all_and_returns_shared_token(rnsapi_home):
    cfg = _plain_config(auth_enabled=False)
    app = build_app(cfg, rnsapi_home)
    server = TestServer(app)
    await server.start_server()
    try:
        async with TestClient(server) as c:
            # /health, /version are public
            assert (await c.get("/health")).status == 200
            assert (await c.get("/version")).status == 200
            # login returns shared anonymous token
            r = await c.post("/auth/login", json={})
            assert r.status == 200
            body1 = await r.json()
            assert body1["is_anonymous"] is True
            assert body1["auth_required"] is False
            # calling again returns the same token
            body2 = await (await c.post("/auth/login", json={})).json()
            assert body1["token"] == body2["token"]
            # GET /session works without a token
            r = await c.get("/session")
            assert r.status == 200
            body = await r.json()
            assert body["is_anonymous"] is True
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_login_logout_flow(rnsapi_home):
    cfg = _plain_config(auth_enabled=True, password="hunter2")
    app = build_app(cfg, rnsapi_home)
    server = TestServer(app)
    await server.start_server()
    try:
        async with TestClient(server) as c:
            # protected endpoint requires token
            r = await c.get("/session")
            assert r.status == 401

            # bad credentials rejected
            r = await c.post("/auth/login", json={"username": "admin", "password": "wrong"})
            assert r.status == 401

            r = await c.post("/auth/login", json={"username": "admin", "password": "hunter2"})
            assert r.status == 200
            body = await r.json()
            token = body["token"]
            assert body["auth_required"] is True
            assert body["is_anonymous"] is False

            headers = {"Authorization": f"Bearer {token}"}
            r = await c.get("/session", headers=headers)
            assert r.status == 200

            r = await c.post("/auth/logout", headers=headers)
            assert r.status == 200

            # token no longer valid
            r = await c.get("/session", headers=headers)
            assert r.status == 401
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_ws_first_frame_auth_required_when_enabled(rnsapi_home):
    cfg = _plain_config(auth_enabled=True, password="hunter2")
    app = build_app(cfg, rnsapi_home)
    server = TestServer(app)
    await server.start_server()
    try:
        async with TestClient(server) as c:
            # WS without an auth frame: server closes with 4001
            async with c.ws_connect("/ws") as ws:
                # server sends rejection then closes; wait_closed style
                async def wait_closed():
                    while True:
                        msg = await ws.receive()
                        if msg.type.name in ("CLOSE", "CLOSED", "CLOSING"):
                            return
                await asyncio.wait_for(wait_closed(), timeout=3)

            # WS with a bad token
            async with c.ws_connect("/ws") as ws:
                await ws.send_json({"type": "auth", "token": "not-a-real-token"})
                async def wait_closed2():
                    while True:
                        msg = await ws.receive()
                        if msg.type.name in ("CLOSE", "CLOSED", "CLOSING"):
                            return
                await asyncio.wait_for(wait_closed2(), timeout=3)

            # WS with a valid token succeeds
            body = await (
                await c.post("/auth/login", json={"username": "admin", "password": "hunter2"})
            ).json()
            token = body["token"]
            async with c.ws_connect("/ws") as ws:
                await ws.send_json({"type": "auth", "token": token})
                # First back: attach event, then session.connected
                attach = None
                while attach is None:
                    msg = await asyncio.wait_for(ws.receive(), timeout=3)
                    if msg.type.name != "TEXT":
                        continue
                    ev = msg.json()
                    if ev["type"] == "auth.session.attached":
                        attach = ev
                assert attach["is_anonymous"] is False
                # ping works
                await ws.send_json({"type": "ping", "id": "p1"})
                pong = None
                while pong is None:
                    msg = await asyncio.wait_for(ws.receive(), timeout=3)
                    if msg.type.name != "TEXT":
                        continue
                    ev = msg.json()
                    if ev["type"] == "pong":
                        pong = ev
                assert pong["id"] == "p1"
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_ws_events_are_session_scoped(rnsapi_home):
    """A session.connected event fires only for that session's WS, not others."""
    cfg = _plain_config(auth_enabled=True, password="hunter2")
    app = build_app(cfg, rnsapi_home)
    server = TestServer(app)
    await server.start_server()
    try:
        async with TestClient(server) as c:
            tok_a = (
                await (
                    await c.post("/auth/login", json={"username": "admin", "password": "hunter2"})
                ).json()
            )["token"]
            # Second login = second session
            tok_b = (
                await (
                    await c.post("/auth/login", json={"username": "admin", "password": "hunter2"})
                ).json()
            )["token"]

            ws_a = await c.ws_connect("/ws")
            ws_b = await c.ws_connect("/ws")
            try:
                await ws_a.send_json({"type": "auth", "token": tok_a})
                await ws_b.send_json({"type": "auth", "token": tok_b})

                # Drain until each sees its own attach
                async def wait_for(ws, want):
                    while True:
                        m = await asyncio.wait_for(ws.receive(), timeout=3)
                        if m.type.name != "TEXT":
                            continue
                        ev = m.json()
                        if ev.get("type") == want:
                            return ev

                await wait_for(ws_a, "auth.session.attached")
                await wait_for(ws_b, "auth.session.attached")

                # Open a third WS on session A; A should see connected for A's new conn,
                # B should not see that specific connection's connect event.
                ws_a2 = await c.ws_connect("/ws")
                try:
                    await ws_a2.send_json({"type": "auth", "token": tok_a})
                    attach_a2 = await wait_for(ws_a2, "auth.session.attached")
                    session_a_id = attach_a2["session_id"]

                    # ws_a should receive a session.connected event for session A
                    ev_a = await wait_for(ws_a, "auth.session.connected")
                    assert ev_a["session_id"] == session_a_id

                    # ws_b's own session should NOT be session A
                    b_events: list[dict] = []
                    async def collect_b():
                        while True:
                            m = await ws_b.receive()
                            if m.type.name != "TEXT":
                                continue
                            b_events.append(m.json())
                    try:
                        await asyncio.wait_for(collect_b(), timeout=0.4)
                    except asyncio.TimeoutError:
                        pass
                    for ev in b_events:
                        if ev.get("type") == "auth.session.connected":
                            assert ev["session_id"] != session_a_id, (
                                "session A's connected event leaked to session B"
                            )
                finally:
                    await ws_a2.close()
            finally:
                await ws_a.close()
                await ws_b.close()
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_reaper_expires_idle_sessions(rnsapi_home):
    """Wire the reaper with a short interval and inactivity=0 to exercise expiry."""
    cfg = _plain_config(auth_enabled=True, password="hunter2")
    cfg.auth.session_inactivity_timeout = 0  # expire everything immediately
    app = build_app(cfg, rnsapi_home)
    server = TestServer(app)
    await server.start_server()
    try:
        async with TestClient(server) as c:
            body = await (
                await c.post("/auth/login", json={"username": "admin", "password": "hunter2"})
            ).json()
            token = body["token"]
            # Push last_seen into the past
            reg = app["sessions"]
            s = reg.get_by_token(token)
            s.last_seen_at = time.time() - 100
            # Sweep manually (the reaper runs in the background too, but doing this
            # inline keeps the test deterministic).
            await reg.sweep_once()
            r = await c.get("/session", headers={"Authorization": f"Bearer {token}"})
            assert r.status == 401
    finally:
        await server.close()
