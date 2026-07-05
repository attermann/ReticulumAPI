"""Path table queries and outgoing path-request awaiter.

RNS keeps a routing/path table on the Transport singleton; we surface it via
REST for inspection. `RNS.Transport.request_path` triggers a path discovery
for a specific destination; we can then poll `has_path` with a deadline to
implement a synchronous "await path" REST endpoint.

There is no *incoming* path-request event: RNS exposes no public hook for
observing path-request packets received on the network, and `rnsapid`
refuses to monkey-patch RNS. See docs/api-reference.md.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import TYPE_CHECKING, Optional

import RNS

from ..config import Config


if TYPE_CHECKING:
    from ..auth.session import Session
    from ..ws.hub import WSHub


log = logging.getLogger(__name__)

_HEX_HASH = re.compile(r"^[0-9a-f]{32}$")
_POLL_INTERVAL = 0.02  # 20 ms — same cadence as MeshChatX


class PathsError(Exception):
    """Path-related errors that map to 4xx REST responses."""


def _hex_or_none(b) -> Optional[str]:
    if b is None:
        return None
    if isinstance(b, bytes):
        return b.hex()
    return str(b)


def _serialise_entry(entry: dict) -> dict:
    return {
        "hash": _hex_or_none(entry.get("hash")),
        "via": _hex_or_none(entry.get("via")),
        "hops": entry.get("hops"),
        "interface": entry.get("interface"),
        "timestamp": entry.get("timestamp"),
        "expires": entry.get("expires"),
    }


class PathsService:
    def __init__(self, config: Config, hub: "WSHub"):
        self._config = config
        self._hub = hub
        self._reticulum: Optional[RNS.Reticulum] = None

    def attach(self, reticulum: RNS.Reticulum) -> None:
        self._reticulum = reticulum

    def _table(self, max_hops: Optional[int] = None) -> list[dict]:
        if self._reticulum is None:
            return []
        try:
            return list(self._reticulum.get_path_table(max_hops=max_hops))
        except Exception:
            log.exception("get_path_table failed")
            return []

    def list_paths(
        self,
        *,
        destination: Optional[str] = None,
        interface: Optional[str] = None,
        max_hops: Optional[int] = None,
    ) -> list[dict]:
        entries = self._table(max_hops=max_hops)
        if destination is not None:
            h = destination.lower()
            if not _HEX_HASH.match(h):
                raise PathsError(f"invalid destination hash: {destination!r}")
            hash_bytes = bytes.fromhex(h)
            entries = [e for e in entries if e.get("hash") == hash_bytes]
        if interface is not None:
            entries = [e for e in entries if e.get("interface") == interface]
        return [_serialise_entry(e) for e in entries]

    def _path_snapshot(self, destination_hash: bytes) -> dict:
        """Return the current known path info for a given destination hash."""
        dest_hex = destination_hash.hex()
        try:
            hops = RNS.Transport.hops_to(destination_hash)
        except Exception as e:
            log.debug("Transport.hops_to(%s) raised: %s", dest_hex, e)
            hops = None
        next_hop = None
        try:
            next_hop = RNS.Transport.next_hop(destination_hash)
        except Exception as e:
            log.debug("Transport.next_hop(%s) raised: %s", dest_hex, e)
        interface = None
        try:
            iface = RNS.Transport.next_hop_interface(destination_hash)
            interface = str(iface) if iface is not None else None
        except Exception as e:
            log.debug("Transport.next_hop_interface(%s) raised: %s", dest_hex, e)
        return {
            "destination_hash": dest_hex,
            "hops": hops if hops is not None and hops != RNS.Transport.PATHFINDER_M else None,
            "next_hop": _hex_or_none(next_hop),
            "interface": interface,
        }

    async def request_path(
        self,
        session: "Session",
        destination_hash_hex: str,
        timeout: Optional[float] = None,
    ) -> dict:
        h = destination_hash_hex.lower()
        if not _HEX_HASH.match(h):
            log.warning("session %s path request rejected — invalid hash %r", session.id, destination_hash_hex)
            raise PathsError(f"invalid destination hash: {destination_hash_hex!r}")
        hash_bytes = bytes.fromhex(h)

        await self._hub.send_session(
            session.id,
            {
                "type": "path.request.sent",
                "session_id": session.id,
                "destination_hash": h,
            },
        )

        effective_timeout = (
            timeout if timeout is not None else self._config.limits.path_request_timeout
        )
        log.debug(
            "session %s requesting path to %s (timeout=%.1fs)",
            session.id, h, effective_timeout,
        )
        try:
            RNS.Transport.request_path(hash_bytes)
        except Exception as e:
            log.warning("session %s RNS refused path request to %s: %s", session.id, h, e)
            raise PathsError(f"RNS refused path request: {e}") from None

        deadline = time.monotonic() + effective_timeout
        while True:
            has_path = False
            try:
                has_path = RNS.Transport.has_path(hash_bytes)
            except Exception as e:
                log.debug("Transport.has_path(%s) raised: %s", h, e)
            if has_path:
                log.info("session %s path resolved to %s", session.id, h)
                return {"found": True, **self._path_snapshot(hash_bytes)}
            if time.monotonic() >= deadline:
                log.info(
                    "session %s path request to %s timed out after %.1fs",
                    session.id, h, effective_timeout,
                )
                return {"found": False, "destination_hash": h}
            await asyncio.sleep(_POLL_INTERVAL)
