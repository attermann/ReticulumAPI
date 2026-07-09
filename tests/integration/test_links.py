"""Smoke tests for the Link endpoints.

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
            "path_lookup_timeout": 0.1,
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
async def test_open_accepts_destination_hash_alias(client):
    """destination_hash is an alias for identity_hash on link.open (WS + REST).

    RNS.Identity.recall() accepts both identity and destination hashes, so
    clients pasting a destination hash directly (e.g. from an announce
    they've observed) shouldn't have to translate it first.
    """
    ident = await (await client.post("/identities")).json()
    aspect = secrets.token_hex(3)

    # REST: destination_hash alone is accepted.
    r = await client.post(
        "/links",
        json={
            "destination_hash": ident["hash"],
            "app_name": "rnsapi_test",
            "aspects": [aspect],
            "await_established": False,
            "path_lookup_timeout": 0.1,
        },
    )
    assert r.status == 201, await r.text()
    link_id = (await r.json())["link_id"]
    await client.delete(f"/links/{link_id}")

    # REST: both spellings together -> 400.
    r = await client.post(
        "/links",
        json={
            "identity_hash": ident["hash"],
            "destination_hash": ident["hash"],
            "app_name": "rnsapi_test",
            "aspects": [aspect],
            "await_established": False,
        },
    )
    assert r.status == 400

    # REST: neither spelling -> 400.
    r = await client.post(
        "/links",
        json={"app_name": "rnsapi_test", "aspects": [aspect]},
    )
    assert r.status == 400

    # WS: destination_hash alone is accepted.
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")
        await ws.send_json(
            {
                "type": "link.open",
                "id": "op-dh",
                "destination_hash": ident["hash"],
                "app_name": "rnsapi_test",
                "aspects": [aspect],
                "establishment_timeout": 0.2,
                "path_lookup_timeout": 0.1,
            }
        )
        ev = await _wait_event(ws, "link.open.result")
        assert ev["id"] == "op-dh"
        assert ev["link_id"]
    finally:
        await ws.close()


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
                "establishment_timeout": 0.2,
                "path_lookup_timeout": 0.1,
            }
        )
        ev = await _wait_event(ws, "link.open.result")
        assert ev["id"] == "op1"
        assert ev["sent"] is True
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
async def test_ws_open_is_fully_async_with_phase_and_failure_events(client):
    """WS link.open never blocks. The immediate reply is an ack that carries
    the computed link_id; phase events fire during establishment; on
    failure a session-scoped `link.open.failed` event is emitted with the
    echoed client id and a categorized reason."""
    ident = await (await client.post("/identities")).json()
    aspect = secrets.token_hex(3)
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")
        await ws.send_json(
            {
                "type": "link.open",
                "id": "op-async",
                "identity_hash": ident["hash"],
                "app_name": "rnsapi_test",
                "aspects": [aspect],
                # tight timeouts so the background continuation completes
                # within the test window
                "establishment_timeout": 0.2,
                "path_lookup_timeout": 0.1,
            }
        )

        # Immediate ack — arrives before establishment finishes.
        ack = await _wait_event(ws, "link.open.result")
        assert ack["id"] == "op-async"
        assert ack["sent"] is True
        assert ack["reused"] is False
        assert ack["link_id"]
        assert ack["status"] in ("PENDING", "HANDSHAKE", "STALE", "CLOSED", "ACTIVE")
        link_id = ack["link_id"]

        # At least one phase event fires (finding_path when no path exists).
        ev = await _wait_event(ws, "link.open.phase", timeout=2)
        assert ev["id"] == "op-async"
        assert ev["phase"] in ("finding_path", "establishing_link")
        assert ev["link_id"] == link_id

        # Terminal failure event (no peer → establishment times out).
        failed = await _wait_event(ws, "link.open.failed", timeout=3)
        assert failed["id"] == "op-async"
        assert failed["link_id"] == link_id
        assert failed["reason"] == "link_establishment_timed_out"
        assert "timed out" in failed["detail"]
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_ws_open_returns_error_on_preflight_failure(client):
    """Pre-flight failures (bad hash, unknown identity, etc.) still return a
    synchronous `error` reply — they never enter the async pipeline, so
    `link.open.failed` is not appropriate."""
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")
        await ws.send_json(
            {
                "type": "link.open",
                "id": "op-bad",
                "identity_hash": "not-hex",
                "app_name": "rnsapi_test",
                "aspects": ["x"],
            }
        )
        ev = await _wait_event(ws, "error")
        assert ev["id"] == "op-bad"
        assert "invalid hash" in ev["error"]
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
