"""Phase 7 smoke tests for the Link endpoints.

RNS Link handshake requires transport-layer packet exchange between two
endpoints, which does not happen in an in-process test with no interfaces.
These tests therefore cover:

- input validation and error mapping (400/404/408)
- open with await_established=false: the link is created and cached even
  though it will never reach ACTIVE without a peer
- lifecycle events emitted via the wired callbacks
- REST and WS surface for open/close/status/list

Full end-to-end Link establishment is exercised by the RNS test suite;
here we validate ReticulumAPI's bridging layer.
"""
from __future__ import annotations

import asyncio
import base64
import secrets

import pytest

from rnsapi.config import Config, AuthConfig, NetworkConfig, TlsConfig
from rnsapi.server import build_app


def _cfg() -> Config:
    cfg = Config()
    cfg.network = NetworkConfig(bind_host="127.0.0.1", bind_port=0, tls=False)
    cfg.tls = TlsConfig(mode="disabled")
    cfg.auth = AuthConfig(enabled=False)
    cfg.limits.link_establish_timeout = 1
    return cfg


@pytest.fixture
async def client(rnsapi_home, rns_instance, aiohttp_client):
    app = build_app(_cfg(), rnsapi_home, start_rns=False)
    return await aiohttp_client(app)


async def _wait_event(ws, want_type, timeout=3):
    while True:
        m = await asyncio.wait_for(ws.receive(), timeout=timeout)
        if m.type.name != "TEXT":
            continue
        ev = m.json()
        if ev.get("type") == want_type:
            return ev


@pytest.mark.asyncio
async def test_open_rejects_unknown_identity(client):
    r = await client.post(
        "/links",
        json={
            "identity_hash": "ee" * 16,
            "app_name": "rnsapi_test",
            "aspects": ["x"],
            "await_established": False,
        },
    )
    assert r.status == 404


@pytest.mark.asyncio
async def test_open_validates_input(client):
    r = await client.post(
        "/links",
        json={"identity_hash": "nothex", "app_name": "a", "aspects": []},
    )
    assert r.status == 400


@pytest.mark.asyncio
async def test_open_without_awaiting_returns_link_id(client):
    ident = await (await client.post("/identities")).json()
    r = await client.post(
        "/links",
        json={
            "identity_hash": ident["hash"],
            "app_name": "rnsapi_test",
            "aspects": [secrets.token_hex(3)],
            "await_established": False,
            "path_lookup_timeout": 0.1,
        },
    )
    # 201 Created; link is registered in the session even though not ACTIVE.
    assert r.status == 201
    body = await r.json()
    assert body["link_id"] == body["destination_hash"]
    assert body["status"] in ("PENDING", "HANDSHAKE", "STALE", "CLOSED", "ACTIVE")
    link_id = body["link_id"]

    # list contains it
    r = await client.get("/links")
    body = await r.json()
    assert any(l["link_id"] == link_id for l in body["links"])

    # status endpoint returns it
    r = await client.get(f"/links/{link_id}")
    assert r.status == 200

    # delete works
    r = await client.delete(f"/links/{link_id}")
    assert r.status == 200


@pytest.mark.asyncio
async def test_open_await_established_times_out(client):
    ident = await (await client.post("/identities")).json()
    r = await client.post(
        "/links",
        json={
            "identity_hash": ident["hash"],
            "app_name": "rnsapi_test",
            "aspects": [secrets.token_hex(3)],
            "await_established": True,
            "establishment_timeout": 0.5,
            "path_lookup_timeout": 0.1,
        },
    )
    assert r.status == 408
    body = await r.json()
    assert "timed out" in body["error"]


@pytest.mark.asyncio
async def test_status_and_close_reject_unknown_link(client):
    r = await client.get("/links/" + "aa" * 16)
    assert r.status == 404
    r = await client.delete("/links/" + "aa" * 16)
    assert r.status == 404


@pytest.mark.asyncio
async def test_send_data_rejects_bad_input(client):
    r = await client.post("/links/" + "aa" * 16 + "/data", json={"data_b64": "!not-b64"})
    # unknown link -> 404
    assert r.status == 404


@pytest.mark.asyncio
async def test_ws_open_reuses_link_and_dispatches_status(client):
    ident = await (await client.post("/identities")).json()
    aspect = secrets.token_hex(3)
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")
        await ws.send_json(
            {
                "type": "link.open",
                "id": "op1",
                "identity_hash": ident["hash"],
                "app_name": "rnsapi_test",
                "aspects": [aspect],
                "await_established": False,
                "path_lookup_timeout": 0.1,
            }
        )
        ev = await _wait_event(ws, "link.open.result")
        assert ev["id"] == "op1"
        link_id = ev["link_id"]

        # link.list
        await ws.send_json({"type": "link.list", "id": "ls"})
        ev = await _wait_event(ws, "link.list.result")
        assert any(l["link_id"] == link_id for l in ev["links"])

        # link.status
        await ws.send_json({"type": "link.status", "id": "st", "link_id": link_id})
        ev = await _wait_event(ws, "link.status.result")
        assert ev["link_id"] == link_id

        # link.close
        await ws.send_json({"type": "link.close", "id": "cl", "link_id": link_id})
        ev = await _wait_event(ws, "link.close.result")
        assert ev["ok"] is True
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_session_cleanup_teardowns_open_links(client):
    """When the anonymous session is torn down, all its links are closed."""
    ident = await (await client.post("/identities")).json()
    r = await client.post(
        "/links",
        json={
            "identity_hash": ident["hash"],
            "app_name": "rnsapi_test",
            "aspects": [secrets.token_hex(3)],
            "await_established": False,
            "path_lookup_timeout": 0.1,
        },
    )
    body = await r.json()
    link_id = body["link_id"]

    sessions = client.app["sessions"]
    anon = sessions.anonymous()
    assert bytes.fromhex(link_id) in anon.open_links

    await sessions._teardown(anon, reason="test")
    assert anon.open_links == {}
