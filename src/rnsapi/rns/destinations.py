"""Session-scoped destination registry.

Each session owns the destinations it creates. A destination is:
- registered with `RNS.Transport` when constructed (RNS does this in
  `Destination.__init__`),
- held on the session so it isn't garbage-collected,
- deregistered explicitly via `RNS.Transport.deregister_destination()` when
  the session ends or the client calls DELETE /destinations/{hash}.

The destination's `hash` (16-byte truncated identifier) is our external
handle; clients pass it as a lowercase hex string.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Optional

import RNS


if TYPE_CHECKING:
    from ..auth.session import Session


log = logging.getLogger(__name__)

_ASPECT_RE = re.compile(r"^[a-zA-Z0-9_]+$")
_HEX_HASH = re.compile(r"^[0-9a-f]{32}$")


class DestinationError(Exception):
    """Raised for destination-related errors that map to 4xx responses."""


_DIRECTION_MAP = {"in": RNS.Destination.IN, "out": RNS.Destination.OUT}
_TYPE_MAP = {
    "single": RNS.Destination.SINGLE,
    "group": RNS.Destination.GROUP,
    "plain": RNS.Destination.PLAIN,
}


@dataclass
class DestinationInfo:
    hash_hex: str
    identity_hash_hex: str
    direction: str
    type: str
    app_name: str
    aspects: tuple[str, ...]

    def to_dict(self) -> dict:
        return {
            "hash": self.hash_hex,
            "identity_hash": self.identity_hash_hex,
            "direction": self.direction,
            "type": self.type,
            "app_name": self.app_name,
            "aspects": list(self.aspects),
        }


def _reverse_lookup(mapping: dict[str, int], value: int) -> str:
    for k, v in mapping.items():
        if v == value:
            return k
    return "unknown"


def _info_from_destination(destination) -> DestinationInfo:
    identity = destination.identity
    parts = destination.name.split(".")
    # Destination.name is "app.aspect1.aspect2[.identity_hexhash]"; drop the
    # trailing identity component if we have an identity attached.
    app_name = parts[0] if parts else ""
    aspects = parts[1:]
    if identity is not None and aspects and aspects[-1] == identity.hexhash:
        aspects = aspects[:-1]
    return DestinationInfo(
        hash_hex=destination.hash.hex(),
        identity_hash_hex=identity.hexhash if identity is not None else "",
        direction=_reverse_lookup(_DIRECTION_MAP, destination.direction),
        type=_reverse_lookup(_TYPE_MAP, destination.type),
        app_name=app_name,
        aspects=tuple(aspects),
    )


class DestinationService:
    def create(
        self,
        session: "Session",
        identity: RNS.Identity,
        direction: str,
        dtype: str,
        app_name: str,
        aspects: Iterable[str],
    ) -> tuple[object, DestinationInfo]:
        d = direction.lower()
        t = dtype.lower()
        if d not in _DIRECTION_MAP:
            raise DestinationError(f"invalid direction: {direction!r}")
        if t not in _TYPE_MAP:
            raise DestinationError(f"invalid type: {dtype!r}")
        if not app_name or not _ASPECT_RE.match(app_name):
            raise DestinationError("app_name must match [a-zA-Z0-9_]+")
        aspects_t = tuple(aspects)
        for a in aspects_t:
            if not _ASPECT_RE.match(a):
                raise DestinationError(f"aspect must match [a-zA-Z0-9_]+: {a!r}")

        try:
            destination = RNS.Destination(
                identity,
                _DIRECTION_MAP[d],
                _TYPE_MAP[t],
                app_name,
                *aspects_t,
            )
        except KeyError as e:
            # RNS raises KeyError when a destination with the same hash exists.
            raise DestinationError(f"destination already registered: {e}") from None
        session.owned_destinations[destination.hash] = destination
        info = _info_from_destination(destination)
        log.info(
            "session %s registered destination %s (%s.%s)",
            session.id,
            info.hash_hex,
            info.app_name,
            ".".join(info.aspects),
        )
        return destination, info

    def remove(self, session: "Session", hash_hex: str) -> DestinationInfo:
        h = hash_hex.lower()
        if not _HEX_HASH.match(h):
            raise DestinationError(f"invalid destination hash: {hash_hex!r}")
        hash_bytes = bytes.fromhex(h)
        destination = session.owned_destinations.pop(hash_bytes, None)
        if destination is None:
            raise DestinationError(f"destination not owned by this session: {hash_hex}")
        info = _info_from_destination(destination)
        try:
            RNS.Transport.deregister_destination(destination)
        except Exception:
            log.exception("deregister_destination raised for %s", info.hash_hex)
        log.info("session %s deregistered destination %s", session.id, info.hash_hex)
        return info

    def list(self, session: "Session") -> list[DestinationInfo]:
        return [_info_from_destination(d) for d in session.owned_destinations.values()]

    def get(self, session: "Session", hash_hex: str) -> Optional[object]:
        h = hash_hex.lower()
        if not _HEX_HASH.match(h):
            return None
        return session.owned_destinations.get(bytes.fromhex(h))

    async def cleanup_session(self, session: "Session") -> None:
        """Called by the session registry when a session ends."""
        for hash_bytes, destination in list(session.owned_destinations.items()):
            try:
                RNS.Transport.deregister_destination(destination)
            except Exception:
                log.exception("deregister_destination raised during cleanup")
        session.owned_destinations.clear()
