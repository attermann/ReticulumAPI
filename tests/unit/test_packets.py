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
        await svc.send(session, "ee" * 16, "rnsapi_test", ["send"], base64.b64encode(b"x").decode())


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
    await svc.send(session, identity.hexhash, "rnsapi_test", ["cleanup_send"], base64.b64encode(b"x").decode())
    assert len(session.pending_receipts) >= 1

    await svc.cleanup_session(session)
    assert session.packet_listeners == set()
    assert session.pending_receipts == {}
    destinations.remove(session, info.hash_hex)
