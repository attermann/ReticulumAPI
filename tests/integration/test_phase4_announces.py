"""Phase 4 smoke test: /announce endpoint + global announce fanout."""
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
async def test_announce_rest_endpoint_broadcasts_sent(client):
    """POST /announce fires announce.sent globally."""
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")
        ident = await (await client.post("/identities")).json()
        await client.put("/session/active-identity", json={"hash": ident["hash"]})
        aspect = secrets.token_hex(3)
        dest = await (
            await client.post(
                "/destinations",
                json={
                    "direction": "in",
                    "type": "single",
                    "app_name": "rnsapi_test",
                    "aspects": [aspect],
                },
            )
        ).json()

        r = await client.post(
            "/announce",
            json={
                "destination_hash": dest["hash"],
                "app_data_b64": base64.b64encode(b"hello world").decode(),
            },
        )
        assert r.status == 200
        result = await r.json()
        assert result["ok"] is True
        assert result["destination_hash"] == dest["hash"]

        ev = await _wait_event(ws, "announce.sent")
        assert ev["destination_hash"] == dest["hash"]
        assert ev["app_data_b64"] == base64.b64encode(b"hello world").decode()

        await client.delete(f"/destinations/{dest['hash']}")
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_announce_ws_endpoint(client):
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")
        ident = await (await client.post("/identities")).json()
        await client.put("/session/active-identity", json={"hash": ident["hash"]})
        aspect = secrets.token_hex(3)
        dest = await (
            await client.post(
                "/destinations",
                json={
                    "direction": "in",
                    "type": "single",
                    "app_name": "rnsapi_test",
                    "aspects": [aspect],
                },
            )
        ).json()

        await ws.send_json(
            {
                "type": "announce.send",
                "id": "a1",
                "destination_hash": dest["hash"],
                "app_data_b64": base64.b64encode(b"ws-payload").decode(),
            }
        )
        # We expect both an announce.sent event (broadcast) and an
        # announce.send.result reply (targeted at this connection).
        received = []
        async def collect():
            while len(received) < 2:
                m = await ws.receive()
                if m.type.name != "TEXT":
                    continue
                ev = m.json()
                if ev.get("type") in ("announce.sent", "announce.send.result"):
                    received.append(ev)
        await asyncio.wait_for(collect(), timeout=3)
        types = {ev["type"] for ev in received}
        assert types == {"announce.sent", "announce.send.result"}
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_announce_rejects_unowned_destination(client):
    r = await client.post(
        "/announce",
        json={"destination_hash": "aa" * 16, "app_data_b64": None},
    )
    assert r.status == 404
    body = await r.json()
    assert "not owned" in body["error"]


@pytest.mark.asyncio
async def test_announce_received_event_fans_out_to_all_ws(client):
    """Two WS clients (same anonymous session) both receive an announce.received."""
    import base64 as b64

    ws_a = await client.ws_connect("/ws")
    ws_b = await client.ws_connect("/ws")
    try:
        await _wait_event(ws_a, "auth.session.attached")
        await _wait_event(ws_b, "auth.session.attached")

        # Directly invoke the announce service's handler to simulate an incoming
        # announce from the network. This exercises the fanout path without
        # requiring a second RNS node.
        svc = client.app["announces"]
        # Grab the handler and invoke it directly.
        svc._handler.received_announce(
            destination_hash=b"\xaa" * 16,
            announced_identity=None,
            app_data=b"incoming!",
            announce_packet_hash=b"\xbb" * 32,
            is_path_response=False,
        )
        ev_a = await _wait_event(ws_a, "announce.received")
        ev_b = await _wait_event(ws_b, "announce.received")
        assert ev_a["destination_hash"] == "aa" * 16
        assert ev_a["app_data_b64"] == b64.b64encode(b"incoming!").decode()
        assert ev_b == ev_a
    finally:
        await ws_a.close()
        await ws_b.close()
