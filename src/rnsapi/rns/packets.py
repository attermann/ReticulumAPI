"""Packet listen/send bridge — session-scoped.

Everything a session does with packets is scoped to that session:
- Listening only fires on destinations the session owns.
- Sent packets fire `packet.sent` only to the session's own WS connections.
- Receipt callbacks (delivery, timeout) also fire only to the session.

RNS callbacks run on worker threads, so every event flows through
`AsyncBridge.run_async(hub.send_session(...))`.
"""
from __future__ import annotations

import base64
import logging
import re
from typing import TYPE_CHECKING, Optional

import RNS

from ..async_bridge import AsyncBridge


if TYPE_CHECKING:
    from ..auth.session import Session
    from ..ws.hub import WSHub
    from .identities import IdentityService


log = logging.getLogger(__name__)

_HEX_HASH = re.compile(r"^[0-9a-f]{32}$")
_ASPECT_RE = re.compile(r"^[a-zA-Z0-9_]+$")


class PacketError(Exception):
    """Packet-related errors that map to 4xx REST responses."""


def _b64_or_none(data: Optional[bytes]) -> Optional[str]:
    return base64.b64encode(data).decode("ascii") if data else None


_RECEIPT_STATUS_MAP = {
    RNS.PacketReceipt.FAILED: "FAILED",
    RNS.PacketReceipt.SENT: "SENT",
    RNS.PacketReceipt.DELIVERED: "DELIVERED",
    RNS.PacketReceipt.CULLED: "CULLED",
}


class PacketsService:
    def __init__(self, hub: "WSHub", identities: "IdentityService | None" = None):
        self._hub = hub
        self._identities = identities

    def _resolve_identity(self, identity_hash: bytes):
        """Return an RNS.Identity for *identity_hash* if we know it.

        Tries the local identity store first (identities we've generated
        ourselves) and falls back to RNS.Identity.recall, which succeeds when
        we've received an announce carrying this identity's public key.
        """
        if self._identities is not None:
            try:
                return self._identities.load(identity_hash.hex())
            except Exception:
                pass
        return RNS.Identity.recall(identity_hash)

    # ---------- listening ----------

    def listen(self, session: "Session", destination_hash_hex: str) -> dict:
        h = destination_hash_hex.lower()
        if not _HEX_HASH.match(h):
            raise PacketError(f"invalid destination hash: {destination_hash_hex!r}")
        hash_bytes = bytes.fromhex(h)
        destination = session.owned_destinations.get(hash_bytes)
        if destination is None:
            raise PacketError(f"destination not owned by this session: {destination_hash_hex}")
        if hash_bytes in session.packet_listeners:
            return {"ok": True, "destination_hash": h, "note": "already_listening"}

        session_id = session.id

        def _on_packet(data, packet):
            # Runs on an RNS thread.
            packet_hash = getattr(packet, "packet_hash", None) or getattr(packet, "hash", None)
            event = {
                "type": "packet.received",
                "session_id": session_id,
                "destination_hash": h,
                "data_b64": _b64_or_none(data),
                "size": len(data) if data else 0,
                "packet_hash": packet_hash.hex() if packet_hash else None,
                "hops": getattr(packet, "hops", None),
                "rssi": getattr(packet, "rssi", None),
                "snr": getattr(packet, "snr", None),
            }
            AsyncBridge.run_async(self._hub.send_session(session_id, event))

        destination.set_packet_callback(_on_packet)
        session.packet_listeners.add(hash_bytes)
        log.info("session %s listening on destination %s", session.id, h)
        return {"ok": True, "destination_hash": h}

    def unlisten(self, session: "Session", destination_hash_hex: str) -> dict:
        h = destination_hash_hex.lower()
        if not _HEX_HASH.match(h):
            raise PacketError(f"invalid destination hash: {destination_hash_hex!r}")
        hash_bytes = bytes.fromhex(h)
        if hash_bytes not in session.packet_listeners:
            raise PacketError(f"no listener on destination: {destination_hash_hex}")
        destination = session.owned_destinations.get(hash_bytes)
        if destination is not None:
            try:
                destination.set_packet_callback(None)
            except Exception:
                log.exception("clearing packet callback failed")
        session.packet_listeners.discard(hash_bytes)
        return {"ok": True, "destination_hash": h}

    def list_listeners(self, session: "Session") -> list[str]:
        return [h.hex() for h in session.packet_listeners]

    # ---------- sending ----------

    async def send(
        self,
        session: "Session",
        identity_hash_hex: str,
        app_name: str,
        aspects: list[str],
        data_b64: str,
        proof_timeout: Optional[float] = None,
    ) -> dict:
        ih = identity_hash_hex.lower()
        if not _HEX_HASH.match(ih):
            raise PacketError(f"invalid identity hash: {identity_hash_hex!r}")
        if not isinstance(app_name, str) or not _ASPECT_RE.match(app_name):
            raise PacketError("app_name must match [a-zA-Z0-9_]+")
        if not isinstance(aspects, list) or not all(_ASPECT_RE.match(a) for a in aspects):
            raise PacketError("aspects must be a list of [a-zA-Z0-9_]+ strings")
        try:
            data = base64.b64decode(data_b64, validate=True)
        except Exception as e:
            raise PacketError(f"data_b64 is not valid base64: {e}") from None

        identity_hash = bytes.fromhex(ih)
        target_identity = self._resolve_identity(identity_hash)
        if target_identity is None:
            raise PacketError(
                "no known identity for hash — send an announce or issue a path request first"
            )

        out_destination = RNS.Destination(
            target_identity,
            RNS.Destination.OUT,
            RNS.Destination.SINGLE,
            app_name,
            *aspects,
        )
        try:
            packet = RNS.Packet(out_destination, data)
            receipt = packet.send()
        except Exception as e:
            raise PacketError(f"RNS refused to send packet: {e}") from None

        packet_hash = (packet.packet_hash or b"").hex() if packet.packet_hash else None
        target_dest_hash = out_destination.hash.hex()

        session_id = session.id

        # Track the receipt on the session so cleanup can null its callbacks
        # and prevent GC cycles.
        if packet_hash:
            session.pending_receipts[packet_hash] = receipt

        def _on_delivered(r):
            event = {
                "type": "packet.receipt.delivered",
                "session_id": session_id,
                "destination_hash": target_dest_hash,
                "packet_hash": packet_hash,
                "rtt": r.get_rtt(),
                "status": _RECEIPT_STATUS_MAP.get(r.get_status(), "UNKNOWN"),
            }
            AsyncBridge.run_async(self._hub.send_session(session_id, event))
            if packet_hash:
                session.pending_receipts.pop(packet_hash, None)

        def _on_timeout(r):
            event = {
                "type": "packet.receipt.failed",
                "session_id": session_id,
                "destination_hash": target_dest_hash,
                "packet_hash": packet_hash,
                "status": _RECEIPT_STATUS_MAP.get(r.get_status(), "UNKNOWN"),
            }
            AsyncBridge.run_async(self._hub.send_session(session_id, event))
            if packet_hash:
                session.pending_receipts.pop(packet_hash, None)

        if receipt is not None:
            receipt.set_delivery_callback(_on_delivered)
            receipt.set_timeout_callback(_on_timeout)
            if proof_timeout is not None:
                try:
                    receipt.set_timeout(proof_timeout)
                except Exception:
                    log.debug("PacketReceipt.set_timeout not accepted, using default")

        await self._hub.send_session(
            session_id,
            {
                "type": "packet.sent",
                "session_id": session_id,
                "destination_hash": target_dest_hash,
                "identity_hash": target_identity.hexhash,
                "packet_hash": packet_hash,
                "size": len(data),
                "has_receipt": receipt is not None,
            },
        )
        return {
            "ok": True,
            "destination_hash": target_dest_hash,
            "identity_hash": target_identity.hexhash,
            "packet_hash": packet_hash,
            "size": len(data),
            "has_receipt": receipt is not None,
        }

    # ---------- cleanup ----------

    async def cleanup_session(self, session: "Session") -> None:
        # Clear packet callbacks on all still-owned destinations
        for hash_bytes in list(session.packet_listeners):
            destination = session.owned_destinations.get(hash_bytes)
            if destination is not None:
                try:
                    destination.set_packet_callback(None)
                except Exception:
                    log.exception("clearing packet callback during cleanup")
        session.packet_listeners.clear()

        # Null out receipt callbacks so the session and its closures can be GC'd
        for _, receipt in list(session.pending_receipts.items()):
            try:
                receipt.set_delivery_callback(None)
                receipt.set_timeout_callback(None)
            except Exception:
                log.exception("clearing receipt callback during cleanup")
        session.pending_receipts.clear()
