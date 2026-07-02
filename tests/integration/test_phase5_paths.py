"""Phase 5 smoke test: /paths query + /paths/request endpoints."""
from __future__ import annotations

import asyncio
import time

import pytest

from rnsapi.config import Config, AuthConfig, NetworkConfig, TlsConfig
from rnsapi.server import build_app


def _cfg() -> Config:
    cfg = Config()
    cfg.network = NetworkConfig(bind_host="127.0.0.1", bind_port=0, tls=False)
    cfg.tls = TlsConfig(mode="disabled")
    cfg.auth = AuthConfig(enabled=False)
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
async def test_get_paths_returns_json_list(client):
    r = await client.get("/paths")
    assert r.status == 200
    body = await r.json()
    assert "paths" in body
    assert isinstance(body["paths"], list)


@pytest.mark.asyncio
async def test_paths_filter_by_destination(client):
    import RNS

    dest_hash = bytes.fromhex("bb" * 16)
    RNS.Transport.path_table[dest_hash] = [
        time.time(), b"\xcc" * 16, 3, time.time() + 3600, None, "TestIface[0]",
    ]
    try:
        r = await client.get("/paths?destination=" + "bb" * 16)
        body = await r.json()
        assert len(body["paths"]) >= 1
        # every returned entry must match our filter
        assert all(p["hash"] == "bb" * 16 for p in body["paths"])
    finally:
        RNS.Transport.path_table.pop(dest_hash, None)


@pytest.mark.asyncio
async def test_paths_filter_by_interface(client):
    import RNS

    dest_hash = bytes.fromhex("ee" * 16)
    RNS.Transport.path_table[dest_hash] = [
        time.time(), b"\xdd" * 16, 4, time.time() + 3600, None, "InterfaceXYZ",
    ]
    try:
        r = await client.get("/paths?interface=InterfaceXYZ")
        body = await r.json()
        assert len(body["paths"]) >= 1
        assert all(p["interface"] == "InterfaceXYZ" for p in body["paths"])
    finally:
        RNS.Transport.path_table.pop(dest_hash, None)


@pytest.mark.asyncio
async def test_paths_rejects_invalid_destination(client):
    r = await client.get("/paths?destination=nothex")
    assert r.status == 400


@pytest.mark.asyncio
async def test_request_path_finds_existing_synthetic_path(client):
    import RNS

    dest_hash = bytes.fromhex("aa" * 16)
    RNS.Transport.path_table[dest_hash] = [
        time.time(), b"\xff" * 16, 2, time.time() + 3600, None, "TestIface[0]",
    ]
    try:
        r = await client.post("/paths/request", json={"destination_hash": "aa" * 16, "timeout": 1})
        assert r.status == 200
        body = await r.json()
        assert body["found"] is True
        assert body["destination_hash"] == "aa" * 16
        assert body["hops"] == 2
    finally:
        RNS.Transport.path_table.pop(dest_hash, None)


@pytest.mark.asyncio
async def test_request_path_408_on_timeout(client):
    r = await client.post(
        "/paths/request",
        json={"destination_hash": "12" * 16, "timeout": 0.1},
    )
    assert r.status == 408
    body = await r.json()
    assert body["found"] is False


@pytest.mark.asyncio
async def test_request_path_emits_session_only_sent_event(client):
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")
        # Fire the request in the background so we can observe the event
        req = asyncio.create_task(
            client.post("/paths/request", json={"destination_hash": "42" * 16, "timeout": 0.1})
        )
        ev = await _wait_event(ws, "path.request.sent")
        assert ev["destination_hash"] == "42" * 16
        await req
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_ws_paths_query_and_request(client):
    import RNS

    dest_hash = bytes.fromhex("77" * 16)
    RNS.Transport.path_table[dest_hash] = [
        time.time(), b"\x88" * 16, 1, time.time() + 3600, None, "IfaceWS",
    ]
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")

        # query
        await ws.send_json({"type": "paths.query", "id": "q1", "destination": "77" * 16})
        ev = await _wait_event(ws, "paths.query.result")
        assert ev["id"] == "q1"
        assert len(ev["paths"]) == 1
        assert ev["paths"][0]["hash"] == "77" * 16

        # request
        await ws.send_json({
            "type": "paths.request", "id": "r1",
            "destination_hash": "77" * 16, "timeout": 1,
        })
        ev = await _wait_event(ws, "paths.request.result")
        assert ev["id"] == "r1"
        assert ev["found"] is True
    finally:
        await ws.close()
        RNS.Transport.path_table.pop(dest_hash, None)
