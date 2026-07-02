"""Announce bridge: global receive listener + per-session send helper.

The single `GlobalAnnounceHandler` is registered with `RNS.Transport` at
service start with `aspect_filter=None`, so every announce that reaches this
node — regardless of app or aspect — becomes an `announce.received` event
broadcast to every WebSocket connection.

Sending an announce is scoped to a destination the session owns. The
resulting `announce.sent` event is broadcast globally so any connected
client can observe network activity originating from this daemon.
"""
from __future__ import annotations

import base64
import logging
import re
from typing import TYPE_CHECKING

import RNS

from ..async_bridge import AsyncBridge


if TYPE_CHECKING:
    from ..auth.session import Session
    from ..ws.hub import WSHub


log = logging.getLogger(__name__)

_HEX_HASH = re.compile(r"^[0-9a-f]{32}$")


class AnnounceError(Exception):
    """Announce-side errors that map to 4xx REST responses."""


def _b64_or_none(data: bytes | None) -> str | None:
    return base64.b64encode(data).decode("ascii") if data else None


class GlobalAnnounceHandler:
    """Match-all handler that fans out `announce.received` events."""

    aspect_filter = None
    receive_path_responses = True

    def __init__(self, hub: "WSHub"):
        self._hub = hub

    def received_announce(
        self,
        destination_hash,
        announced_identity,
        app_data,
        announce_packet_hash,
        is_path_response,
    ):
        # Runs on an RNS-owned thread.
        event = {
            "type": "announce.received",
            "destination_hash": destination_hash.hex() if destination_hash else None,
            "identity_hash": announced_identity.hexhash if announced_identity is not None else None,
            "app_data_b64": _b64_or_none(app_data),
            "packet_hash": announce_packet_hash.hex() if announce_packet_hash else None,
            "is_path_response": bool(is_path_response),
        }
        AsyncBridge.run_async(self._hub.broadcast(event))


class AnnounceService:
    def __init__(self, hub: "WSHub"):
        self._hub = hub
        self._handler: GlobalAnnounceHandler | None = None

    def start(self) -> None:
        """Register the global announce handler with RNS.Transport."""
        if self._handler is not None:
            return
        self._handler = GlobalAnnounceHandler(self._hub)
        RNS.Transport.register_announce_handler(self._handler)
        log.info("registered global announce handler")

    def stop(self) -> None:
        if self._handler is None:
            return
        try:
            RNS.Transport.deregister_announce_handler(self._handler)
        except Exception:
            log.exception("failed to deregister announce handler")
        self._handler = None

    async def send(
        self,
        session: "Session",
        destination_hash_hex: str,
        app_data_b64: str | None = None,
    ) -> dict:
        h = destination_hash_hex.lower()
        if not _HEX_HASH.match(h):
            raise AnnounceError(f"invalid destination hash: {destination_hash_hex!r}")
        destination = session.owned_destinations.get(bytes.fromhex(h))
        if destination is None:
            raise AnnounceError(f"destination not owned by this session: {destination_hash_hex}")

        app_data: bytes | None = None
        if app_data_b64 is not None:
            try:
                app_data = base64.b64decode(app_data_b64, validate=True)
            except Exception as e:
                raise AnnounceError(f"app_data_b64 is not valid base64: {e}") from None

        try:
            destination.announce(app_data=app_data)
        except Exception as e:
            raise AnnounceError(f"RNS refused to announce: {e}") from None

        event = {
            "type": "announce.sent",
            "destination_hash": destination.hash.hex(),
            "identity_hash": destination.identity.hexhash if destination.identity is not None else None,
            "session_id": session.id,
            "app_data_b64": app_data_b64,
        }
        await self._hub.broadcast(event)
        return {
            "ok": True,
            "destination_hash": destination.hash.hex(),
            "app_data_bytes": len(app_data) if app_data else 0,
        }
