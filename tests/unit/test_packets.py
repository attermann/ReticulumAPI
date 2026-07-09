"""Unit tests for PacketsService — listen, send, cleanup."""
import asyncio
import base64
import time

import pytest

from rnsapi.async_bridge import AsyncBridge
from rnsapi.auth.session import Session
from rnsapi.rns.destinations import DestinationService
from rnsapi.rns.identities import IdentityService
from rnsapi.rns.packets import PacketError, PacketsService
from rnsapi.ws.hub import WSHub


class _FakeConn:
    def __init__(self, session):
        self.session = session
        self.sent: list[dict] = []

    async def send_json(self, data):
        self.sent.append(data)


def _new_session():
    now = time.time()
    return Session(id="sess-pkt", token="t", created_at=now, last_seen_at=now)


@pytest.mark.asyncio
async def test_listen_requires_owned_destination(rnsapi_home, rns_instance):
    session = _new_session()
    svc = PacketsService(WSHub())
    with pytest.raises(PacketError, match="not owned"):
        svc.listen(session, "cd" * 16)


@pytest.mark.asyncio
async def test_listen_rejects_invalid_hash(rnsapi_home, rns_instance):
    session = _new_session()
    svc = PacketsService(WSHub())
    with pytest.raises(PacketError, match="invalid destination hash"):
        svc.listen(session, "not-hex")


@pytest.mark.asyncio
async def test_listen_attaches_callback_and_fanouts_packet_received(rnsapi_home, rns_instance):
    identity_svc = IdentityService(rnsapi_home)
    destinations = DestinationService()
    identity, _ = identity_svc.create()

    session = _new_session()
    hub = WSHub()
    conn = _FakeConn(session)
    hub.register(conn)
    svc = PacketsService(hub)

    _, info = destinations.create(session, identity, "in", "single", "rnsapi_test", ["listen"])
    try:
        result = svc.listen(session, info.hash_hex)
        assert result["ok"] is True
        assert bytes.fromhex(info.hash_hex) in session.packet_listeners

        AsyncBridge.set_main_loop(asyncio.get_running_loop())

        # Simulate a packet arriving at the destination by directly invoking
        # the packet callback that RNS would call.
        destination = session.owned_destinations[bytes.fromhex(info.hash_hex)]
        callback = destination.callbacks.packet

        class _FakePacket:
            hops = 1
            rssi = -80
            snr = 10
            packet_hash = b"\xaa" * 32

        callback(b"hello!", _FakePacket())

        for _ in range(30):
            await asyncio.sleep(0.02)
            if any(e.get("type") == "packet.received" for e in conn.sent):
                break

        received = [e for e in conn.sent if e["type"] == "packet.received"]
        assert received
        ev = received[0]
        assert ev["destination_hash"] == info.hash_hex
        assert ev["data_b64"] == base64.b64encode(b"hello!").decode()
        assert ev["hops"] == 1
        assert ev["rssi"] == -80
        assert ev["snr"] == 10
        assert ev["packet_hash"] == "aa" * 32
        assert ev["size"] == 6
    finally:
        AsyncBridge.clear_main_loop()
        destinations.remove(session, info.hash_hex)


@pytest.mark.asyncio
async def test_unlisten_clears_callback(rnsapi_home, rns_instance):
    identity_svc = IdentityService(rnsapi_home)
    destinations = DestinationService()
    identity, _ = identity_svc.create()

    session = _new_session()
    hub = WSHub()
    svc = PacketsService(hub)

    _, info = destinations.create(session, identity, "in", "single", "rnsapi_test", ["unlisten"])
    try:
        svc.listen(session, info.hash_hex)
        svc.unlisten(session, info.hash_hex)
        assert bytes.fromhex(info.hash_hex) not in session.packet_listeners
        destination = session.owned_destinations[bytes.fromhex(info.hash_hex)]
        assert destination.callbacks.packet is None
    finally:
        destinations.remove(session, info.hash_hex)


@pytest.mark.asyncio
async def test_send_requires_recallable_identity(rnsapi_home, rns_instance):
    session = _new_session()
    svc = PacketsService(WSHub())
    with pytest.raises(PacketError, match="no known identity"):
        await svc.send(
            session, "ee" * 16, "rnsapi_test", ["send"], base64.b64encode(b"x").decode(),
            path_lookup_timeout=0.05,
        )


@pytest.mark.asyncio
async def test_send_validates_input(rnsapi_home, rns_instance):
    session = _new_session()
    svc = PacketsService(WSHub())
    with pytest.raises(PacketError, match="invalid identity hash"):
        await svc.send(session, "nothex", "app", ["x"], base64.b64encode(b"x").decode())
    with pytest.raises(PacketError, match="app_name"):
        await svc.send(session, "ee" * 16, "bad name", ["x"], base64.b64encode(b"x").decode())
    with pytest.raises(PacketError, match="aspects"):
        await svc.send(session, "ee" * 16, "app", ["bad aspect"], base64.b64encode(b"x").decode())
    with pytest.raises(PacketError, match="base64"):
        await svc.send(session, "ee" * 16, "app", ["x"], "!!!not-base64!!!")


@pytest.mark.asyncio
async def test_send_to_self_emits_packet_sent(rnsapi_home, rns_instance):
    """Send a packet to ourselves; verify packet.sent event fires."""
    identity_svc = IdentityService(rnsapi_home)
    identity, _ = identity_svc.create()

    session = _new_session()
    hub = WSHub()
    conn = _FakeConn(session)
    hub.register(conn)
    svc = PacketsService(hub, identity_svc)

    result = await svc.send(
        session,
        identity.hexhash,
        "rnsapi_test",
        ["self_send"],
        base64.b64encode(b"ping").decode(),
        path_lookup_timeout=0.05,
    )
    assert result["ok"] is True
    assert result["size"] == 4
    assert any(e.get("type") == "packet.sent" for e in conn.sent)


@pytest.mark.asyncio
async def test_cleanup_clears_listeners_and_receipts(rnsapi_home, rns_instance):
    identity_svc = IdentityService(rnsapi_home)
    destinations = DestinationService()
    identity, _ = identity_svc.create()

    session = _new_session()
    hub = WSHub()
    svc = PacketsService(hub)

    _, info = destinations.create(session, identity, "in", "single", "rnsapi_test", ["cleanup"])
    svc = PacketsService(hub, identity_svc)
    svc.listen(session, info.hash_hex)
    assert bytes.fromhex(info.hash_hex) in session.packet_listeners

    # Send to self to populate pending_receipts
    await svc.send(
        session, identity.hexhash, "rnsapi_test", ["cleanup_send"],
        base64.b64encode(b"x").decode(), path_lookup_timeout=0.05,
    )
    assert len(session.pending_receipts) >= 1

    await svc.cleanup_session(session)
    assert session.packet_listeners == set()
    assert session.pending_receipts == {}
    destinations.remove(session, info.hash_hex)


# ---------- path resolution ordering ----------


@pytest.mark.asyncio
async def test_send_probes_path_before_recalling_identity(rns_instance, monkeypatch):
    """has_path first, then request_path (if no path), THEN Identity.recall.

    Previously send() skipped has_path/request_path entirely and rejected
    any destination we hadn't received an announce from.
    """
    import RNS

    calls: list[str] = []

    def _has_path(h):
        calls.append("has_path")
        return False

    def _request_path(h):
        calls.append("request_path")

    def _recall(h, from_identity_hash=False):
        calls.append("recall")
        return None

    monkeypatch.setattr(RNS.Transport, "has_path", _has_path)
    monkeypatch.setattr(RNS.Transport, "request_path", _request_path)
    monkeypatch.setattr(RNS.Identity, "recall", staticmethod(_recall))

    svc = PacketsService(WSHub())
    session = _new_session()
    with pytest.raises(PacketError, match="no known identity"):
        await svc.send(
            session, "ee" * 16, "rnsapi_test", ["x"], base64.b64encode(b"x").decode(),
            path_lookup_timeout=0.05,
        )

    assert "has_path" in calls
    assert "request_path" in calls
    assert "recall" in calls
    assert calls.index("has_path") < calls.index("request_path") < calls.index("recall")


@pytest.mark.asyncio
async def test_send_skips_request_path_when_path_already_known(rns_instance, monkeypatch):
    """If has_path returns True, request_path must not be issued."""
    import RNS

    request_calls: list[bytes] = []
    monkeypatch.setattr(RNS.Transport, "has_path", lambda h: True)
    monkeypatch.setattr(RNS.Transport, "request_path", lambda h: request_calls.append(h))

    recall_calls: list[bytes] = []

    def _recall(h, from_identity_hash=False):
        recall_calls.append(h)
        return None

    monkeypatch.setattr(RNS.Identity, "recall", staticmethod(_recall))

    svc = PacketsService(WSHub())
    session = _new_session()
    with pytest.raises(PacketError, match="no known identity"):
        await svc.send(
            session, "ee" * 16, "rnsapi_test", ["x"], base64.b64encode(b"x").decode(),
            path_lookup_timeout=1.0,
        )

    assert request_calls == []
    assert recall_calls, "Identity.recall should still be consulted after the path check"
