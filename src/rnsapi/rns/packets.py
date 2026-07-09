"""Packet listen/send bridge — session-scoped.

Everything a session does with packets is scoped to that session:
- Listening only fires on destinations the session owns.
- Sent packets fire `packet.sent` only to the session's own WS connections.
- Receipt callbacks (delivery, timeout) also fire only to the session.

RNS callbacks run on worker threads, so every event flows through
`AsyncBridge.run_async(hub.send_session(...))`.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import re
import time
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
_POLL_INTERVAL_S = 0.02  # matches LinksService / MeshChatX cadence


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

    def _try_local_identity(self, input_hash: bytes):
        """Fast-path: return an identity from the local .rid store, or None.

        Identities we've generated ourselves resolve without touching the
        network. Falls through silently otherwise so
        RNS.Identity.recall can be tried after the path lookup.
        """
        if self._identities is None:
            return None
        hex_hash = input_hash.hex()
        try:
            identity = self._identities.load(hex_hash)
            log.debug("resolved identity for %s from local IdentityService", hex_hash)
            return identity
        except Exception as e:
            log.debug(
                "local IdentityService could not load %s (%s); will consult RNS announce cache after path lookup",
                hex_hash, e,
            )
            return None

    async def _request_path(self, dest_hash: bytes, timeout: float = 15.0) -> None:
        log.debug("issuing RNS path request for %s", dest_hash.hex())
        try:
            RNS.Transport.request_path(dest_hash)
        except Exception as e:
            log.warning("RNS.Transport.request_path(%s) raised: %s", dest_hash.hex(), e)
        deadline = time.monotonic() + timeout
        while not RNS.Transport.has_path(dest_hash) and time.monotonic() < deadline:
            await asyncio.sleep(_POLL_INTERVAL_S)

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
            size = len(data) if data else 0
            log.debug(
                "session %s destination %s inbound packet (size=%d hops=%s rssi=%s snr=%s)",
                session_id, h, size,
                getattr(packet, "hops", None),
                getattr(packet, "rssi", None),
                getattr(packet, "snr", None),
            )
            event = {
                "type": "packet.received",
                "session_id": session_id,
                "destination_hash": h,
                "data_b64": _b64_or_none(data),
                "size": size,
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
        path_lookup_timeout: float = 15.0,
    ) -> dict:
        # `identity_hash_hex` accepts either an identity hash or a
        # destination hash — the local .rid store keys on identity hashes,
        # while `RNS.Identity.recall` looks up by destination hash. The
        # code below tries both, matching LinksService's dual-key handling.
        h = identity_hash_hex.lower()
        if not _HEX_HASH.match(h):
            log.warning("session %s packet send rejected — invalid identity hash %r", session.id, identity_hash_hex)
            raise PacketError(f"invalid identity hash: {identity_hash_hex!r}")
        if not isinstance(app_name, str) or not _ASPECT_RE.match(app_name):
            log.warning("session %s packet send rejected — invalid app_name %r", session.id, app_name)
            raise PacketError("app_name must match [a-zA-Z0-9_]+")
        if not isinstance(aspects, list) or not all(_ASPECT_RE.match(a) for a in aspects):
            log.warning("session %s packet send rejected — invalid aspects %r", session.id, aspects)
            raise PacketError("aspects must be a list of [a-zA-Z0-9_]+ strings")
        try:
            data = base64.b64decode(data_b64, validate=True)
        except Exception as e:
            log.warning("session %s packet send: invalid base64 data_b64: %s", session.id, e)
            raise PacketError(f"data_b64 is not valid base64: {e}") from None

        # Derive the on-wire destination hash so we can look up a path
        # without needing the full Identity object yet. RNS.Destination.hash
        # accepts identity-hash bytes directly.
        input_hash_bytes = bytes.fromhex(h)
        dest_hash_bytes = RNS.Destination.hash(input_hash_bytes, app_name, *aspects)

        # Fast path: identity we own locally.
        target_identity = self._try_local_identity(input_hash_bytes)

        # Path lookup runs regardless of local resolution — RNS.Packet
        # needs a route. The correct order is `has_path` first, then
        # `request_path` + wait, THEN `Identity.recall` — the path-request
        # response populates the announce cache that recall consults.
        if not RNS.Transport.has_path(dest_hash_bytes):
            await self._request_path(dest_hash_bytes, path_lookup_timeout)
            if not RNS.Transport.has_path(dest_hash_bytes):
                log.warning(
                    "no path to %s after %.1fs — proceeding anyway (RNS may still retry)",
                    dest_hash_bytes.hex(), path_lookup_timeout,
                )
            else:
                log.debug("path to %s resolved", dest_hash_bytes.hex())

        # If local didn't have it, try the announce cache now (possibly
        # populated by the path-request response).
        if target_identity is None:
            target_identity = RNS.Identity.recall(dest_hash_bytes)
            if target_identity is None:
                log.debug(
                    "RNS.Identity.recall(%s) returned None — no announce received for this hash",
                    dest_hash_bytes.hex(),
                )
            else:
                log.debug(
                    "resolved identity for %s via RNS.Identity.recall (announce cache)",
                    dest_hash_bytes.hex(),
                )

        if target_identity is None:
            log.warning(
                "session %s packet send rejected — identity of %s not known "
                "(no announce received and no local identity file)",
                session.id, dest_hash_bytes.hex(),
            )
            raise PacketError("no known identity for hash — send an announce or issue a path request first")

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
            log.warning(
                "session %s RNS refused to send packet to %s (%s.%s): %s",
                session.id, out_destination.hash.hex(), app_name, ".".join(aspects), e,
            )
            raise PacketError(f"RNS refused to send packet: {e}") from None

        packet_hash = (packet.packet_hash or b"").hex() if packet.packet_hash else None
        target_dest_hash = out_destination.hash.hex()
        log.debug(
            "session %s outbound packet to %s (size=%d packet_hash=%s has_receipt=%s)",
            session.id, target_dest_hash, len(data), packet_hash, receipt is not None,
        )

        session_id = session.id

        # Track the receipt on the session so cleanup can null its callbacks
        # and prevent GC cycles.
        if packet_hash:
            session.pending_receipts[packet_hash] = receipt

        def _on_delivered(r):
            rtt = r.get_rtt()
            log.debug(
                "session %s packet %s delivered to %s (rtt=%.3fs)",
                session_id, packet_hash, target_dest_hash, rtt if rtt is not None else -1,
            )
            event = {
                "type": "packet.receipt.delivered",
                "session_id": session_id,
                "destination_hash": target_dest_hash,
                "packet_hash": packet_hash,
                "rtt": rtt,
                "status": _RECEIPT_STATUS_MAP.get(r.get_status(), "UNKNOWN"),
            }
            AsyncBridge.run_async(self._hub.send_session(session_id, event))
            if packet_hash:
                session.pending_receipts.pop(packet_hash, None)

        def _on_timeout(r):
            log.info(
                "session %s packet %s to %s timed out awaiting proof",
                session_id, packet_hash, target_dest_hash,
            )
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
                except Exception as e:
                    log.warning(
                        "session %s PacketReceipt.set_timeout(%s) rejected, using RNS default: %s",
                        session_id, proof_timeout, e,
                    )

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
