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
