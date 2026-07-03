"""Integration tests for RNS Resource send/receive.

Direct-invokes the receive-side callbacks (started + concluded) to
simulate an inbound transfer end-to-end through REST + WS, since real
Link handshakes don't complete in the no-interface test environment.
The send-side is exercised via the REST upload endpoint but the actual
RNS transfer stays in PENDING (no peer) — the test focuses on the
plumbing (upload → temp file → RNS.Resource creation → transfer_id
returned).
"""
from __future__ import annotations

import asyncio
import base64
import secrets
import tempfile
import time
from pathlib import Path

import pytest

import RNS

from rnsapi.async_bridge import AsyncBridge
from rnsapi.config import Config, AuthConfig, NetworkConfig, TlsConfig, ResourcesConfig
from rnsapi.rns.resources import TransferState
from rnsapi.server import build_app


def _cfg() -> Config:
    cfg = Config()
    cfg.network = NetworkConfig(bind_host="127.0.0.1", bind_port=0, tls=False)
    cfg.tls = TlsConfig(mode="disabled")
    cfg.auth = AuthConfig(enabled=False)
    cfg.limits.link_establish_timeout = 1
    cfg.resources = ResourcesConfig(
        temp_dir="resources",
        retention_seconds=3600,
        sweep_interval_seconds=600,
        max_inline_bytes=64,
        progress_throttle_ms=50,
        progress_throttle_pct=1.0,
        default_auto_accept=True,
    )
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


async def _open_pending_link(client, aspect_suffix=None):
    """Open a session-owned link (that will stay PENDING) and return its id."""
    ident = await (await client.post("/identities")).json()
    await client.put("/session/active-identity", json={"hash": ident["hash"]})
    aspect = aspect_suffix or secrets.token_hex(3)
    r = await client.post(
        "/links",
        json={
            "identity_hash": ident["hash"],
            "app_name": "rnsapi_test",
            "aspects": [aspect],
            "await_established": False,
            "path_lookup_timeout": 0.1,
        },
    )
    body = await r.json()
    return body["link_id"]


# ---------- resources REST surface ----------


@pytest.mark.asyncio
async def test_list_empty(client):
    r = await client.get("/resources")
    assert r.status == 200
    body = await r.json()
    assert body["resources"] == []


@pytest.mark.asyncio
async def test_get_unknown(client):
    r = await client.get("/resources/notreal")
    assert r.status == 404


@pytest.mark.asyncio
async def test_download_unknown(client):
    r = await client.get("/resources/notreal/data")
    assert r.status == 404


@pytest.mark.asyncio
async def test_send_rejects_unknown_link(client):
    r = await client.post(
        "/links/" + "aa" * 16 + "/resources",
        data=b"hello",
        headers={"Content-Type": "application/octet-stream"},
    )
    assert r.status == 404


@pytest.mark.asyncio
async def test_send_rejects_empty_body(client):
    link_id = await _open_pending_link(client)
    r = await client.post(f"/links/{link_id}/resources", data=b"")
    assert r.status == 400


@pytest.mark.asyncio
async def test_send_pending_link_returns_409(client):
    """Sending to a link that hasn't reached ACTIVE returns 409."""
    link_id = await _open_pending_link(client)
    r = await client.post(
        f"/links/{link_id}/resources",
        data=b"a" * 5000,
        headers={"Content-Type": "application/octet-stream"},
    )
    body = await r.json()
    assert r.status == 409, body
    assert "not ACTIVE" in body["error"]


class _FakeActiveLink:
    """Enough of an RNS.Link to satisfy RNS.Resource + our attach hooks.

    RNS.Link.ACTIVE is 0x02. RNS.Resource reads .status, .mdu, .type, and a
    few other attributes; the constructor also calls link.register_outgoing_resource
    or similar. To avoid dragging in the whole Link internals, we monkey-patch
    RNS.Resource itself for send tests below.
    """

    def __init__(self):
        self.status = 0x02  # RNS.Link.ACTIVE
        self.mdu = 400
        self.mtu = 500
        # Attribute stubs used by resource strategy setters
        self.resource_strategy = 0
        self.callbacks = type("Cb", (), {})()

    def set_resource_strategy(self, s):
        self.resource_strategy = s

    def set_link_established_callback(self, cb): pass
    def set_link_closed_callback(self, cb): pass
    def set_packet_callback(self, cb): pass
    def set_remote_identified_callback(self, cb): pass
    def set_resource_callback(self, cb): pass
    def set_resource_started_callback(self, cb): pass
    def set_resource_concluded_callback(self, cb): pass


@pytest.mark.asyncio
async def test_send_active_link_creates_transfer_and_returns_201(client, monkeypatch):
    """With an ACTIVE link and a stubbed-out RNS.Resource, the full REST upload
    plumbing exercises: raw body → temp file → RNS.Resource construction →
    transfer_id returned + resource.started event on WS."""
    from rnsapi.rns.links import _LinkEntry
    session = client.app["sessions"].anonymous()

    # Set up a session identity so the identity-related bits are all wired.
    ident = await (await client.post("/identities")).json()
    await client.put("/session/active-identity", json={"hash": ident["hash"]})

    # Inject a fake ACTIVE link into the session
    fake_link = _FakeActiveLink()
    dest_hash = bytes(range(16))
    entry = _LinkEntry(fake_link, dest_hash, "rnsapi_test.active", "rnsapi_test", ["active"])
    session.open_links[dest_hash] = entry

    # Stub RNS.Resource so we don't need real transport
    captured = {}

    class _FakeResource:
        def __init__(self, data, link, metadata=None, auto_compress=True, callback=None,
                     progress_callback=None, **kw):
            captured["data"] = data
            captured["metadata"] = metadata
            captured["callback"] = callback
            captured["progress_callback"] = progress_callback
            self.status = 0x03  # TRANSFERRING
        def cancel(self): pass
        def get_progress(self): return 0.0

    monkeypatch.setattr("rnsapi.rns.resources.RNS.Resource", _FakeResource)

    link_id = dest_hash.hex()
    r = await client.post(
        f"/links/{link_id}/resources",
        data=b"a" * 500,
        headers={"Content-Type": "application/octet-stream"},
    )
    body = await r.json()
    assert r.status == 201, body
    assert "transfer_id" in body
    assert body["direction"] == "out"
    assert body["link_id"] == link_id
    assert body["awaited"] is False
    assert captured  # RNS.Resource was constructed

    # Listing should include the transfer
    r = await client.get(f"/links/{link_id}/resources")
    assert len(((await r.json())["resources"])) == 1


# ---------- receive flow via direct callback invocation ----------


class _FakeReceivedResource:
    def __init__(self, data: bytes, status_value):
        self._data = data
        self.status = status_value
        self._tmp = tempfile.NamedTemporaryFile(delete=False)
        self._tmp.write(data)
        self._tmp.flush()
        self._tmp.seek(0)
        self.data = self._tmp
        self.metadata = {"filename": "hello.txt"}

    def get_data_size(self): return len(self._data)
    def get_progress(self): return 1.0 if self.status == RNS.Resource.COMPLETE else 0.0
    def cancel(self): pass


@pytest.mark.asyncio
async def test_receive_small_inline_completes_and_download_works(client):
    """Small inbound resource → data_b64 inline + download URL returns same bytes."""
    session = client.app["sessions"].anonymous()
    svc = client.app["resources"]

    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")

        payload = b"hi from the mesh"  # <= max_inline_bytes (64)
        resource = _FakeReceivedResource(payload, RNS.Resource.COMPLETE)
        link_hash = bytes.fromhex("aa" * 16)

        await svc._on_receive_started(session, link_hash, "app.small", resource)
        await svc._on_receive_concluded(session, link_hash, "app.small", resource)

        ev = await _wait_event(ws, "resource.completed")
        assert ev["status"] == "COMPLETE"
        assert ev["direction"] == "in"
        assert ev["data_b64"] == base64.b64encode(payload).decode()
        transfer_id = ev["transfer_id"]

        # Download endpoint returns the same bytes
        r = await client.get(f"/resources/{transfer_id}/data")
        assert r.status == 200
        body = await r.read()
        assert body == payload
        assert r.headers.get("Content-Type") == "application/octet-stream"
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_receive_large_no_inline(client):
    session = client.app["sessions"].anonymous()
    svc = client.app["resources"]

    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")

        payload = b"x" * 10_000  # > max_inline_bytes
        resource = _FakeReceivedResource(payload, RNS.Resource.COMPLETE)
        link_hash = bytes.fromhex("bb" * 16)
        await svc._on_receive_started(session, link_hash, "app.large", resource)
        await svc._on_receive_concluded(session, link_hash, "app.large", resource)

        ev = await _wait_event(ws, "resource.completed")
        assert "data_b64" not in ev
        assert ev["download_url"] == f"/resources/{ev['transfer_id']}/data"
        assert ev["total_size"] == len(payload)
        r = await client.get(f"/resources/{ev['transfer_id']}/data")
        assert r.status == 200
        assert (await r.read()) == payload
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_delete_transfer_removes_temp_file(client):
    session = client.app["sessions"].anonymous()
    svc = client.app["resources"]
    payload = b"y" * 200
    resource = _FakeReceivedResource(payload, RNS.Resource.COMPLETE)
    link_hash = bytes.fromhex("cc" * 16)
    await svc._on_receive_started(session, link_hash, "app.del", resource)
    await svc._on_receive_concluded(session, link_hash, "app.del", resource)

    # Find our transfer
    transfers = svc.list_transfers(session)
    assert transfers
    transfer_id = transfers[-1]["transfer_id"]
    state = svc.get_state(session, transfer_id)
    temp_path = state.temp_path
    assert temp_path is not None and temp_path.exists()

    r = await client.delete(f"/resources/{transfer_id}")
    assert r.status == 200

    r = await client.get(f"/resources/{transfer_id}")
    assert r.status == 404
    assert not temp_path.exists()


# ---------- policy ----------


@pytest.mark.asyncio
async def test_policy_toggle_on_owned_link(client):
    link_id = await _open_pending_link(client)
    r = await client.post(f"/links/{link_id}/resources/policy", json={"accept": False})
    assert r.status == 200
    body = await r.json()
    assert body["accept"] is False

    r = await client.post(f"/links/{link_id}/resources/policy", json={"accept": True})
    assert r.status == 200
    assert (await r.json())["accept"] is True


@pytest.mark.asyncio
async def test_policy_rejects_unknown_link(client):
    r = await client.post(f"/links/{'aa'*16}/resources/policy", json={"accept": True})
    assert r.status == 404


# ---------- WS surface ----------


@pytest.mark.asyncio
async def test_ws_send_pending_link_returns_error(client):
    link_id = await _open_pending_link(client)
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")

        await ws.send_json(
            {
                "type": "resource.send",
                "id": "s1",
                "link_id": link_id,
                "data_b64": base64.b64encode(b"small-ws-send").decode(),
            }
        )
        ev = await _wait_event(ws, "error")
        assert ev["id"] == "s1"
        assert "not ACTIVE" in ev["error"]
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_ws_list_empty(client):
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")
        await ws.send_json({"type": "resource.list", "id": "l1"})
        ev = await _wait_event(ws, "resource.list.result")
        assert ev["resources"] == []
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_ws_policy(client):
    link_id = await _open_pending_link(client)
    ws = await client.ws_connect("/ws")
    try:
        await _wait_event(ws, "auth.session.attached")
        await ws.send_json({"type": "resource.policy", "id": "p1", "link_id": link_id, "accept": False})
        ev = await _wait_event(ws, "resource.policy.result")
        assert ev["accept"] is False
    finally:
        await ws.close()


# ---------- session cleanup ----------


@pytest.mark.asyncio
async def test_session_cleanup_removes_transfers_and_files(client):
    session = client.app["sessions"].anonymous()
    svc = client.app["resources"]

    # Seed one inbound completed transfer with a temp file
    payload = b"leftover"
    resource = _FakeReceivedResource(payload, RNS.Resource.COMPLETE)
    link_hash = bytes.fromhex("dd" * 16)
    await svc._on_receive_started(session, link_hash, "app.cleanup", resource)
    await svc._on_receive_concluded(session, link_hash, "app.cleanup", resource)

    transfers = svc.list_transfers(session)
    assert transfers
    temp_path = svc.get_state(session, transfers[-1]["transfer_id"]).temp_path
    assert temp_path.exists()

    # Force teardown of the anonymous session
    sessions = client.app["sessions"]
    await sessions._teardown(sessions.anonymous(), reason="test")

    assert session.active_transfers == {}
    assert not temp_path.exists()
