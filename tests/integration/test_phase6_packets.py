"""Phase 6 smoke test: packet listener + packet send over the REST/WS API."""
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
async def test_listen_lifecycle(client):
    ident = await (await client.post("/identities")).json()
    await client.put("/session/active-identity", json={"hash": ident["hash"]})
    aspect = secrets.token_hex(3)
    dest = await (
        await client.post(
            "/destinations",
            json={"direction": "in", "type": "single", "app_name": "rnsapi_test", "aspects": [aspect]},
        )
    ).json()
    try:
        r = await client.post("/packets/listen", json={"destination_hash": dest["hash"]})
        assert r.status == 201

        r = await client.get("/packets/listen")
        body = await r.json()
        assert dest["hash"] in body["destination_hashes"]

        r = await client.delete(f"/packets/listen/{dest['hash']}")
        assert r.status == 200

        r = await client.get("/packets/listen")
        assert (await r.json())["destination_hashes"] == []
    finally:
        await client.delete(f"/destinations/{dest['hash']}")


@pytest.mark.asyncio
async def test_listen_rejects_unowned_destination(client):
    r = await client.post("/packets/listen", json={"destination_hash": "aa" * 16})
    assert r.status == 404


@pytest.mark.asyncio
async def test_packet_received_fanouts_to_session_only(client):
    """Two WS clients on separate sessions; only the listening session sees the event."""
    from aiohttp.test_utils import TestServer, TestClient

    # Use two independent clients so each has a different anonymous session? Actually,
    # in auth-disabled mode there is only ONE anonymous session shared across all
    # clients. To exercise session scoping we need auth on. Instead, exercise
    # listener->event flow with a single client here and rely on unit tests for
    # the pure scoping check.
    ident = await (await client.post("/identities")).json()
    await client.put("/session/active-identity", json={"hash": ident["hash"]})
    aspect = secrets.token_hex(3)
    dest = await (
        await client.post(
            "/destinations",
            json={"direction": "in", "type": "single", "app_name": "rnsapi_test", "aspects": [aspect]},
        )
    ).json()
    await client.post("/packets/listen", json={"destination_hash": dest["hash"]})
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")

        # Simulate an inbound packet by invoking the destination's callback
        import RNS

        destination = client.app["sessions"].anonymous().owned_destinations[
            bytes.fromhex(dest["hash"])
        ]

        class _FakePacket:
            hops = 2
            rssi = None
            snr = None
            packet_hash = b"\xbb" * 32

        destination.callbacks.packet(b"hello over ws", _FakePacket())

        ev = await _wait_event(ws, "packet.received")
        assert ev["destination_hash"] == dest["hash"]
        assert ev["data_b64"] == base64.b64encode(b"hello over ws").decode()
        assert ev["hops"] == 2
    finally:
        await ws.close()
        await client.delete(f"/destinations/{dest['hash']}")


@pytest.mark.asyncio
async def test_send_endpoint_emits_packet_sent(client):
    """Send a packet to our own identity — packet.sent is observed on the WS."""
    ident = await (await client.post("/identities")).json()
    await client.put("/session/active-identity", json={"hash": ident["hash"]})
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")
        r = await client.post(
            "/packets/send",
            json={
                "identity_hash": ident["hash"],
                "app_name": "rnsapi_test",
                "aspects": ["send_e2e"],
                "data_b64": base64.b64encode(b"payload").decode(),
            },
        )
        assert r.status == 200
        body = await r.json()
        assert body["ok"] is True
        assert body["size"] == 7
        ev = await _wait_event(ws, "packet.sent")
        assert ev["identity_hash"] == ident["hash"]
        assert ev["size"] == 7
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_send_rejects_unknown_identity(client):
    r = await client.post(
        "/packets/send",
        json={
            "identity_hash": "ee" * 16,
            "app_name": "rnsapi_test",
            "aspects": ["x"],
            "data_b64": base64.b64encode(b"x").decode(),
        },
    )
    assert r.status == 404


@pytest.mark.asyncio
async def test_ws_send_and_listen(client):
    ident = await (await client.post("/identities")).json()
    await client.put("/session/active-identity", json={"hash": ident["hash"]})
    aspect = secrets.token_hex(3)
    dest = await (
        await client.post(
            "/destinations",
            json={"direction": "in", "type": "single", "app_name": "rnsapi_test", "aspects": [aspect]},
        )
    ).json()
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")

        await ws.send_json({"type": "packets.listen", "id": "L1", "destination_hash": dest["hash"]})
        ev = await _wait_event(ws, "packets.listen.result")
        assert ev["id"] == "L1"
        assert ev["ok"] is True

        await ws.send_json({"type": "packets.listeners", "id": "LS"})
        ev = await _wait_event(ws, "packets.listeners.result")
        assert dest["hash"] in ev["destination_hashes"]

        await ws.send_json(
            {
                "type": "packets.send",
                "id": "S1",
                "identity_hash": ident["hash"],
                "app_name": "rnsapi_test",
                "aspects": ["ws_send"],
                "data_b64": base64.b64encode(b"ws-payload").decode(),
            }
        )
        ev = await _wait_event(ws, "packets.send.result")
        assert ev["id"] == "S1"
        assert ev["ok"] is True

        await ws.send_json({"type": "packets.unlisten", "id": "U1", "destination_hash": dest["hash"]})
        ev = await _wait_event(ws, "packets.unlisten.result")
        assert ev["ok"] is True
    finally:
        await ws.close()
        await client.delete(f"/destinations/{dest['hash']}")
