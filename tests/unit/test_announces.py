"""Unit tests for the AnnounceService and its RNS-side handler.

Uses a real RNS instance (via the session-scoped fixture) so that
`RNS.Transport.register_announce_handler` and `destination.announce()`
exercise the real code paths. We don't test the actual over-the-wire
receive of announces here (that's a two-node test in integration).
"""
import asyncio
import base64

import pytest

from rnsapi.async_bridge import AsyncBridge
from rnsapi.rns.announces import AnnounceError, AnnounceService, GlobalAnnounceHandler
from rnsapi.rns.destinations import DestinationService
from rnsapi.rns.identities import IdentityService
from rnsapi.ws.hub import WSHub


class _FakeConn:
    def __init__(self):
        from rnsapi.auth.session import Session
        import time
        self.session = Session(id="s1", token="t", created_at=time.time(), last_seen_at=time.time())
        self.sent: list[dict] = []

    async def send_json(self, data):
        self.sent.append(data)


@pytest.mark.asyncio
async def test_start_and_stop_register_and_deregister(rns_instance):
    import RNS

    hub = WSHub()
    svc = AnnounceService(hub)
    svc.start()
    assert svc._handler in RNS.Transport.announce_handlers
    svc.stop()
    assert svc._handler is None or svc._handler not in RNS.Transport.announce_handlers


@pytest.mark.asyncio
async def test_received_announce_broadcasts_to_hub(rns_instance):
    """Directly call received_announce; the hub should broadcast to registered WS."""
    hub = WSHub()
    conn = _FakeConn()
    hub.register(conn)
    svc = AnnounceService(hub)
    svc.start()

    # Set the async bridge to the current running loop so run_async can dispatch
    AsyncBridge.set_main_loop(asyncio.get_running_loop())

    try:
        # Simulate an inbound announce landing at the handler (on any thread).
        svc._handler.received_announce(
            destination_hash=b"\x11" * 16,
            announced_identity=None,
            app_data=b"hello",
            announce_packet_hash=b"\x22" * 32,
            is_path_response=False,
        )
        # Give the async bridge a chance to run
        for _ in range(20):
            await asyncio.sleep(0.02)
            if conn.sent:
                break
        assert conn.sent
        ev = conn.sent[0]
        assert ev["type"] == "announce.received"
        assert ev["destination_hash"] == ("11" * 16)
        assert ev["app_data_b64"] == base64.b64encode(b"hello").decode()
        assert ev["is_path_response"] is False
    finally:
        svc.stop()
        AsyncBridge.clear_main_loop()


@pytest.mark.asyncio
async def test_send_requires_owned_destination(rnsapi_home, rns_instance):
    hub = WSHub()
    svc = AnnounceService(hub)
    conn = _FakeConn()
    with pytest.raises(AnnounceError, match="not owned"):
        await svc.send(conn.session, "aa" * 16, None)


@pytest.mark.asyncio
async def test_send_invalid_hash_rejected(rnsapi_home, rns_instance):
    hub = WSHub()
    svc = AnnounceService(hub)
    conn = _FakeConn()
    with pytest.raises(AnnounceError, match="invalid destination hash"):
        await svc.send(conn.session, "nothex", None)


@pytest.mark.asyncio
async def test_send_broadcasts_announce_sent(rnsapi_home, rns_instance):
    identities = IdentityService(rnsapi_home)
    destinations = DestinationService()
    identity, _ = identities.create()

    hub = WSHub()
    conn = _FakeConn()
    hub.register(conn)
    svc = AnnounceService(hub)

    _, info = destinations.create(
        conn.session, identity, "in", "single", "rnsapi_test", ["send_bcast"]
    )
    try:
        result = await svc.send(conn.session, info.hash_hex, base64.b64encode(b"payload").decode())
        assert result["ok"] is True
        assert any(e.get("type") == "announce.sent" for e in conn.sent)
        sent = next(e for e in conn.sent if e["type"] == "announce.sent")
        assert sent["destination_hash"] == info.hash_hex
        assert sent["app_data_b64"] == base64.b64encode(b"payload").decode()
    finally:
        destinations.remove(conn.session, info.hash_hex)


@pytest.mark.asyncio
async def test_send_invalid_base64_rejected(rnsapi_home, rns_instance):
    identities = IdentityService(rnsapi_home)
    destinations = DestinationService()
    identity, _ = identities.create()

    hub = WSHub()
    conn = _FakeConn()
    svc = AnnounceService(hub)
    _, info = destinations.create(conn.session, identity, "in", "single", "rnsapi_test", ["b64bad"])
    try:
        with pytest.raises(AnnounceError, match="base64"):
            await svc.send(conn.session, info.hash_hex, "not-base64!!!")
    finally:
        destinations.remove(conn.session, info.hash_hex)
