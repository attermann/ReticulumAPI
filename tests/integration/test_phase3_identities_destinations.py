"""Phase 3 smoke test: identities and destinations end-to-end."""
from __future__ import annotations

import asyncio
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
async def test_identity_create_list_and_get(client):
    r = await client.post("/identities")
    assert r.status == 201
    ident = await r.json()
    assert len(ident["hash"]) == 32
    assert len(ident["public_key"]) > 0

    r = await client.get("/identities")
    body = await r.json()
    assert any(i["hash"] == ident["hash"] for i in body["identities"])

    r = await client.get(f"/identities/{ident['hash']}")
    body = await r.json()
    assert body["hash"] == ident["hash"]


@pytest.mark.asyncio
async def test_set_active_identity_and_query(client):
    ident = await (await client.post("/identities")).json()

    r = await client.get("/session/active-identity")
    assert (await r.json())["active"] is False

    r = await client.put("/session/active-identity", json={"hash": ident["hash"]})
    assert r.status == 200
    body = await r.json()
    assert body["active"] is True
    assert body["hash"] == ident["hash"]

    r = await client.get("/session/active-identity")
    assert (await r.json())["hash"] == ident["hash"]

    # clear
    r = await client.delete("/session/active-identity")
    assert (await r.json())["active"] is False


@pytest.mark.asyncio
async def test_add_destination_requires_active_identity(client):
    r = await client.post(
        "/destinations",
        json={"direction": "in", "type": "single", "app_name": "rnsapi_test", "aspects": ["x"]},
    )
    assert r.status == 409
    assert (await r.json())["error"] == "no_active_identity"


@pytest.mark.asyncio
async def test_destination_lifecycle(client):
    ident = await (await client.post("/identities")).json()
    await client.put("/session/active-identity", json={"hash": ident["hash"]})

    aspect = secrets.token_hex(3)  # unique per test to avoid RNS collision
    r = await client.post(
        "/destinations",
        json={"direction": "in", "type": "single", "app_name": "rnsapi_test", "aspects": [aspect]},
    )
    assert r.status == 201
    dest = await r.json()

    r = await client.get("/destinations")
    assert any(d["hash"] == dest["hash"] for d in (await r.json())["destinations"])

    r = await client.delete(f"/destinations/{dest['hash']}")
    assert r.status == 200

    r = await client.get("/destinations")
    assert all(d["hash"] != dest["hash"] for d in (await r.json())["destinations"])


@pytest.mark.asyncio
async def test_set_active_identity_conflicts_when_session_dirty(client):
    a = await (await client.post("/identities")).json()
    b = await (await client.post("/identities")).json()
    await client.put("/session/active-identity", json={"hash": a["hash"]})

    aspect = secrets.token_hex(3)
    r = await client.post(
        "/destinations",
        json={"direction": "in", "type": "single", "app_name": "rnsapi_test", "aspects": [aspect]},
    )
    assert r.status == 201

    # Now attempt to swap identities — 409 expected
    r = await client.put("/session/active-identity", json={"hash": b["hash"]})
    assert r.status == 409
    body = await r.json()
    assert body["error"] == "session_dirty"
    assert body["owned_destinations"] >= 1

    r = await client.delete("/session/active-identity")
    assert r.status == 409

    # Cleanup
    r = await client.get("/destinations")
    for d in (await r.json())["destinations"]:
        await client.delete(f"/destinations/{d['hash']}")


@pytest.mark.asyncio
async def test_ws_receives_identity_and_destination_events(client):
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")

        # Create an identity via REST — WS should receive identity.created
        ident_resp = await client.post("/identities")
        ident = await ident_resp.json()
        ev = await _wait_event(ws, "identity.created")
        assert ev["hash"] == ident["hash"]

        # Set active
        await client.put("/session/active-identity", json={"hash": ident["hash"]})
        ev = await _wait_event(ws, "session.active_identity.changed")
        assert ev["active"] is True
        assert ev["hash"] == ident["hash"]

        # Add destination — WS should receive destination.added
        aspect = secrets.token_hex(3)
        dresp = await client.post(
            "/destinations",
            json={"direction": "in", "type": "single", "app_name": "rnsapi_test", "aspects": [aspect]},
        )
        dest = await dresp.json()
        ev = await _wait_event(ws, "destination.added")
        assert ev["destination"]["hash"] == dest["hash"]

        # Remove destination
        await client.delete(f"/destinations/{dest['hash']}")
        ev = await _wait_event(ws, "destination.removed")
        assert ev["destination"]["hash"] == dest["hash"]
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_ws_destination_add_via_ws(client):
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")

        ident = await (await client.post("/identities")).json()
        await _wait_event(ws, "identity.created")

        await ws.send_json({"type": "session.active_identity.set", "hash": ident["hash"]})
        await _wait_event(ws, "session.active_identity.changed")

        aspect = secrets.token_hex(3)
        await ws.send_json(
            {
                "type": "destination.add",
                "id": "d1",
                "direction": "in",
                "type_": "single",
                "app_name": "rnsapi_test",
                "aspects": [aspect],
            }
        )
        ev = await _wait_event(ws, "destination.added")
        assert ev["destination"]["app_name"] == "rnsapi_test"
        assert aspect in ev["destination"]["aspects"]

        # remove via ws
        await ws.send_json({"type": "destination.remove", "id": "d2", "hash": ev["destination"]["hash"]})
        removed = await _wait_event(ws, "destination.removed")
        assert removed["destination"]["hash"] == ev["destination"]["hash"]
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_session_cleanup_deregisters_destinations(client, rnsapi_home):
    """When a session ends, its owned destinations are deregistered from RNS."""
    import RNS

    # Login via anonymous mode returns the shared anonymous session.
    # Use the real registry / hub to force-expire the session.
    ident = await (await client.post("/identities")).json()
    await client.put("/session/active-identity", json={"hash": ident["hash"]})
    aspect = secrets.token_hex(3)
    dest = await (
        await client.post(
            "/destinations",
            json={"direction": "in", "type": "single", "app_name": "rnsapi_test", "aspects": [aspect]},
        )
    ).json()

    # Confirm RNS knows this destination
    dest_hash = bytes.fromhex(dest["hash"])
    with RNS.Transport.destinations_map_lock:
        assert dest_hash in RNS.Transport.destinations_map

    # Force session cleanup by revoking the anonymous session
    app = client.app
    sessions = app["sessions"]
    anon = sessions.anonymous()
    # Note: `revoke` on an anonymous session is normally rejected; here we
    # invoke the internal teardown to test cleanup coverage.
    await sessions._teardown(anon, reason="test")

    with RNS.Transport.destinations_map_lock:
        assert dest_hash not in RNS.Transport.destinations_map
