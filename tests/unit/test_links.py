"""Unit tests for LinksService.

The full establishment path involves RNS's Transport handshake which is
brittle in a bare in-process environment. These unit tests focus on:

- validation and error mapping
- callback wiring and event fanout via direct callback invocation
- session cleanup semantics
"""
from __future__ import annotations

import asyncio
import base64
import secrets
import time

import pytest

from rnsapi.async_bridge import AsyncBridge
from rnsapi.auth.session import Session
from rnsapi.rns.identities import IdentityService
from rnsapi.rns.links import LinkError, LinksService, _link_snapshot, _LinkEntry
from rnsapi.ws.hub import WSHub


class _FakeConn:
    def __init__(self, session):
        self.session = session
        self.sent: list[dict] = []

    async def send_json(self, data):
        self.sent.append(data)


def _new_session():
    now = time.time()
    return Session(id="sess-lnk", token="t", created_at=now, last_seen_at=now)


@pytest.mark.asyncio
async def test_open_validates_input(rns_instance):
    svc = LinksService(WSHub())
    session = _new_session()
    with pytest.raises(LinkError, match="invalid hash"):
        await svc.open_link(session, identity_hash="nothex", app_name="a", aspects=["b"])
    with pytest.raises(LinkError, match="app_name"):
        await svc.open_link(session, identity_hash="ee" * 16, app_name="bad name", aspects=["b"])
    with pytest.raises(LinkError, match="aspects"):
        await svc.open_link(session, identity_hash="ee" * 16, app_name="a", aspects=["bad aspect"])
    with pytest.raises(LinkError, match="not both"):
        await svc.open_link(
            session,
            identity_hash="ee" * 16,
            destination_hash="ee" * 16,
            app_name="a",
            aspects=["b"],
        )
    with pytest.raises(LinkError, match="required"):
        await svc.open_link(session, app_name="a", aspects=["b"])


@pytest.mark.asyncio
async def test_open_rejects_unknown_identity(rns_instance):
    svc = LinksService(WSHub())
    session = _new_session()
    with pytest.raises(LinkError, match="no known identity"):
        await svc.open_link(
            session, identity_hash="ee" * 16, app_name="rnsapi_test", aspects=["x"]
        )


@pytest.mark.asyncio
async def test_get_status_and_close_reject_unknown_link(rns_instance):
    svc = LinksService(WSHub())
    session = _new_session()
    with pytest.raises(LinkError, match="unknown link"):
        svc.get_status(session, "aa" * 16)
    with pytest.raises(LinkError, match="unknown link"):
        svc.close(session, "aa" * 16)


@pytest.mark.asyncio
async def test_callback_wiring_fires_events(rnsapi_home, rns_instance):
    """Directly wire an entry and invoke its callbacks; events should flow."""
    session = _new_session()
    hub = WSHub()
    conn = _FakeConn(session)
    hub.register(conn)
    svc = LinksService(hub, IdentityService(rnsapi_home))

    # Construct a fake link object exposing set_*_callback methods that just
    # record their argument. We then invoke the recorded callbacks to
    # simulate RNS events.
    class _FakeLink:
        status = 0x02  # RNS.Link.ACTIVE
        mtu = 500
        mdu = 400
        teardown_reason = None

        def __init__(self):
            self.callbacks: dict[str, object] = {}

        def set_link_established_callback(self, cb): self.callbacks["est"] = cb
        def set_link_closed_callback(self, cb):      self.callbacks["closed"] = cb
        def set_packet_callback(self, cb):           self.callbacks["packet"] = cb
        def set_remote_identified_callback(self, cb): self.callbacks["ident"] = cb
        def get_remote_identity(self): return None

    link = _FakeLink()
    dest_hash = bytes(secrets.token_bytes(16))
    entry = _LinkEntry(link, dest_hash, "rnsapi_test.cbwire", "rnsapi_test", ["cbwire"])
    session.open_links[dest_hash] = entry

    AsyncBridge.set_main_loop(asyncio.get_running_loop())
    try:
        svc._wire_callbacks(session, entry)

        # Simulate RNS firing each callback
        link.callbacks["est"](link)
        link.callbacks["packet"](b"payload", None)
        link.callbacks["ident"](link, None)
        link.callbacks["closed"](link)

        for _ in range(30):
            await asyncio.sleep(0.02)
            types = {e.get("type") for e in conn.sent}
            if {"link.established", "link.data.received", "link.remote_identified", "link.closed"}.issubset(types):
                break

        types = {e.get("type") for e in conn.sent}
        assert "link.established" in types
        assert "link.data.received" in types
        assert "link.remote_identified" in types
        assert "link.closed" in types

        received = next(e for e in conn.sent if e["type"] == "link.data.received")
        assert received["data_b64"] == base64.b64encode(b"payload").decode()
        assert received["destination_hash"] == dest_hash.hex()
    finally:
        AsyncBridge.clear_main_loop()


@pytest.mark.asyncio
async def test_close_never_emits_link_disconnected(rnsapi_home, rns_instance):
    """`link.closed` carries `teardown_reason`; the redundant
    `link.disconnected` event has been removed."""
    session = _new_session()
    hub = WSHub()
    conn = _FakeConn(session)
    hub.register(conn)
    svc = LinksService(hub, IdentityService(rnsapi_home))

    class _FakeLink:
        status = 0x04  # CLOSED
        mtu = 500
        mdu = 400
        # Simulate a remote-initiated close.
        teardown_reason = getattr(__import__("RNS").Link, "DESTINATION_CLOSED", None)
        def __init__(self):
            self.callbacks: dict[str, object] = {}
        def set_link_established_callback(self, cb): self.callbacks["est"] = cb
        def set_link_closed_callback(self, cb):      self.callbacks["closed"] = cb
        def set_packet_callback(self, cb):           self.callbacks["packet"] = cb
        def set_remote_identified_callback(self, cb): self.callbacks["ident"] = cb
        def get_remote_identity(self): return None

    link = _FakeLink()
    dest_hash = bytes(secrets.token_bytes(16))
    entry = _LinkEntry(link, dest_hash, "rnsapi_test.dc", "rnsapi_test", ["dc"])
    session.open_links[dest_hash] = entry

    AsyncBridge.set_main_loop(asyncio.get_running_loop())
    try:
        svc._wire_callbacks(session, entry)
        link.callbacks["closed"](link)

        for _ in range(30):
            await asyncio.sleep(0.02)
            if any(e.get("type") == "link.closed" for e in conn.sent):
                break

        types = [e.get("type") for e in conn.sent]
        assert "link.closed" in types
        assert "link.disconnected" not in types
        closed = next(e for e in conn.sent if e["type"] == "link.closed")
        # teardown_reason on link.closed conveys the info that
        # link.disconnected used to.
        assert closed["teardown_reason"] == "destination_closed"
    finally:
        AsyncBridge.clear_main_loop()


@pytest.mark.asyncio
async def test_link_established_echoes_open_client_id(rnsapi_home, rns_instance):
    """A `link.established` fired after `link.open` should carry the client
    id set on the entry — later re-establishment (STALE→ACTIVE) does not
    re-echo it."""
    session = _new_session()
    hub = WSHub()
    conn = _FakeConn(session)
    hub.register(conn)
    svc = LinksService(hub, IdentityService(rnsapi_home))

    class _FakeLink:
        status = 0x02  # ACTIVE
        mtu = 500
        mdu = 400
        teardown_reason = None
        def __init__(self):
            self.callbacks: dict[str, object] = {}
        def set_link_established_callback(self, cb): self.callbacks["est"] = cb
        def set_link_closed_callback(self, cb):      self.callbacks["closed"] = cb
        def set_packet_callback(self, cb):           self.callbacks["packet"] = cb
        def set_remote_identified_callback(self, cb): self.callbacks["ident"] = cb
        def get_remote_identity(self): return None

    link = _FakeLink()
    dest_hash = bytes(secrets.token_bytes(16))
    entry = _LinkEntry(
        link, dest_hash, "rnsapi_test.est", "rnsapi_test", ["est"],
        open_client_id="op-xyz",
    )
    session.open_links[dest_hash] = entry

    AsyncBridge.set_main_loop(asyncio.get_running_loop())
    try:
        svc._wire_callbacks(session, entry)

        # First establishment: echoes the open client id.
        link.callbacks["est"](link)
        for _ in range(30):
            await asyncio.sleep(0.02)
            if any(e.get("type") == "link.established" for e in conn.sent):
                break

        est = next(e for e in conn.sent if e["type"] == "link.established")
        assert est["id"] == "op-xyz"
        assert entry.open_client_id is None  # consumed

        # Subsequent lifecycle transition (e.g. STALE→ACTIVE) does not re-echo.
        conn.sent.clear()
        link.callbacks["est"](link)
        for _ in range(30):
            await asyncio.sleep(0.02)
            if any(e.get("type") == "link.established" for e in conn.sent):
                break

        est2 = next(e for e in conn.sent if e["type"] == "link.established")
        assert est2.get("id") is None
    finally:
        AsyncBridge.clear_main_loop()


@pytest.mark.asyncio
async def test_snapshot_shape(rns_instance):
    class _StubLink:
        status = 0x02
        mtu = 500
        mdu = 400
        teardown_reason = None
        def get_remote_identity(self): return None
    dest_hash = bytes(secrets.token_bytes(16))
    snap = _link_snapshot(_StubLink(), dest_hash, "app.aspect")
    assert snap["link_id"] == dest_hash.hex()
    assert snap["destination_hash"] == dest_hash.hex()
    assert snap["aspect"] == "app.aspect"
    assert snap["status"] == "ACTIVE"
    assert snap["mtu"] == 500
    assert snap["mdu"] == 400


@pytest.mark.asyncio
async def test_request_echoes_client_id_in_async_events(rnsapi_home, rns_instance):
    """The client-provided `id` on a WS link.request must appear on the
    async link.request.response / link.request.failed events so callers
    can correlate multiple in-flight requests."""
    session = _new_session()
    hub = WSHub()
    conn = _FakeConn(session)
    hub.register(conn)
    svc = LinksService(hub, IdentityService(rnsapi_home))

    captured: dict = {}

    class _StubLink:
        status = 0x02  # ACTIVE
        mtu = 500
        mdu = 400
        teardown_reason = None
        def get_remote_identity(self): return None
        def request(self, path, data=None, response_callback=None, failed_callback=None, timeout=None):
            captured["response_cb"] = response_callback
            captured["failed_cb"] = failed_callback

    link = _StubLink()
    dest_hash = bytes(secrets.token_bytes(16))
    entry = _LinkEntry(link, dest_hash, "rnsapi_test.echo", "rnsapi_test", ["echo"])
    session.open_links[dest_hash] = entry

    AsyncBridge.set_main_loop(asyncio.get_running_loop())
    try:
        # Fire off two concurrent requests with distinct client ids.
        await svc.request(session, dest_hash.hex(), "/a", None, None, await_response=False, client_id="req-alpha")
        cb_a = captured["response_cb"]
        failed_a = captured["failed_cb"]
        await svc.request(session, dest_hash.hex(), "/b", None, None, await_response=False, client_id="req-beta")
        cb_b = captured["response_cb"]
        failed_b = captured["failed_cb"]

        # Simulate: /a succeeds, /b fails.
        class _R:
            def __init__(self, payload): self.response = payload
        cb_a(_R(b"hello"))
        failed_b()

        for _ in range(30):
            await asyncio.sleep(0.02)
            types = [e.get("type") for e in conn.sent]
            if "link.request.response" in types and "link.request.failed" in types:
                break

        resp = next(e for e in conn.sent if e.get("type") == "link.request.response")
        fail = next(e for e in conn.sent if e.get("type") == "link.request.failed")
        assert resp["id"] == "req-alpha"
        assert resp["path"] == "/a"
        assert fail["id"] == "req-beta"
        assert fail["path"] == "/b"
    finally:
        AsyncBridge.clear_main_loop()


@pytest.mark.asyncio
async def test_cleanup_tears_down_all_links(rns_instance):
    session = _new_session()
    svc = LinksService(WSHub())

    calls = []

    class _StubLink:
        status = 0x02
        teardown_reason = None
        def teardown(self):
            calls.append("torn")

    for i in range(3):
        h = bytes(secrets.token_bytes(16))
        session.open_links[h] = _LinkEntry(_StubLink(), h, "a", "a", [])
    await svc.cleanup_session(session)
    assert calls == ["torn"] * 3
    assert session.open_links == {}
