"""Session-scoped Link lifecycle, cache, and RPC.

Adapted from MeshChatX's rns_link_manager.py. Differences:

- Cache is per-session (session.open_links keyed on destination_hash bytes)
  rather than a global module-level dict. All lifecycle events fanout via
  hub.send_session so every connection in the session sees them.
- Lifecycle events are surfaced: link.established, link.closed,
  link.remote_identified, link.data.received, link.data.sent, link.proof.
  `link.closed` carries `teardown_reason` — clients that want to
  distinguish local from remote initiation key off that field rather
  than a separate `link.disconnected` event.
- Identity resolution tries the local IdentityService first, then falls
  back to RNS.Identity.recall.

Callbacks fire on RNS worker threads → AsyncBridge.run_async(...) is used
for every fanout back to asyncio.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import re
import time
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional

import RNS

from ..async_bridge import AsyncBridge


if TYPE_CHECKING:
    from ..auth.session import Session
    from ..ws.hub import WSHub
    from .identities import IdentityService


log = logging.getLogger(__name__)


_HEX_HASH = re.compile(r"^[0-9a-f]{32}$")
_ASPECT_RE = re.compile(r"^[a-zA-Z0-9_]+$")
_POLL_INTERVAL_S = 0.02  # same cadence as MeshChatX

_STATUS_MAP = {
    RNS.Link.PENDING: "PENDING",
    RNS.Link.HANDSHAKE: "HANDSHAKE",
    RNS.Link.ACTIVE: "ACTIVE",
    RNS.Link.STALE: "STALE",
    RNS.Link.CLOSED: "CLOSED",
}

_TEARDOWN_REASON_MAP = {
    getattr(RNS.Link, "TIMEOUT", None): "timeout",
    getattr(RNS.Link, "INITIATOR_CLOSED", None): "initiator_closed",
    getattr(RNS.Link, "DESTINATION_CLOSED", None): "destination_closed",
}


def link_error_reason(err: "LinkError | str") -> str:
    """Categorize a LinkError message into a stable reason code that clients
    can key off of. Used by `link.open.failed`."""
    msg = str(err).lower()
    if "no known identity" in msg:
        return "no_known_identity"
    if "link establishment timed out" in msg:
        return "link_establishment_timed_out"
    if "identify failed" in msg or "session has no active identity" in msg or "load session identity" in msg:
        return "identify_failed"
    if "invalid hash" in msg or "app_name" in msg or "aspects" in msg or "not both" in msg or "required" in msg:
        return "invalid_request"
    return "internal"


class LinkError(Exception):
    """Link-related errors that map to 4xx REST responses."""


def _b64(b: Optional[bytes]) -> Optional[str]:
    return base64.b64encode(b).decode("ascii") if b else None


def _status_str(link) -> str:
    return _STATUS_MAP.get(getattr(link, "status", None), "UNKNOWN")


def _teardown_reason(link) -> Optional[str]:
    reason = getattr(link, "teardown_reason", None)
    return _TEARDOWN_REASON_MAP.get(reason)


def _link_snapshot(link, destination_hash: bytes, aspect: str) -> dict:
    remote_identity = None
    try:
        rid = link.get_remote_identity()
        if rid is not None:
            remote_identity = rid.hexhash
    except Exception:
        pass
    return {
        "link_id": destination_hash.hex(),
        "destination_hash": destination_hash.hex(),
        "aspect": aspect,
        "status": _status_str(link),
        "mtu": getattr(link, "mtu", None),
        "mdu": getattr(link, "mdu", None),
        "remote_identity_hash": remote_identity,
        "teardown_reason": _teardown_reason(link),
    }


class _LinkEntry:
    """Bookkeeping for one Link owned by a session."""

    __slots__ = (
        "link",
        "destination_hash",
        "aspect",
        "app_name",
        "sub_aspects",
        "identified",
        "open_client_id",
    )

    def __init__(self, link, destination_hash, aspect, app_name, sub_aspects, identified=False, open_client_id=None):
        self.link = link
        self.destination_hash = destination_hash
        self.aspect = aspect
        self.app_name = app_name
        self.sub_aspects = tuple(sub_aspects)
        self.identified = identified
        # Echoed on `link.established` when this specific open_link call
        # succeeds, so a WS client that issued `link.open` with `id: X` can
        # correlate the terminal success event to its request.
        self.open_client_id = open_client_id


class LinksService:
    """Per-session link manager. All state lives on `session.open_links`."""

    def __init__(self, hub: "WSHub", identities: "IdentityService | None" = None):
        self._hub = hub
        self._identities = identities
        self._resources = None  # set via set_resources_service() after both services exist

    def set_resources_service(self, resources_svc) -> None:
        """Called by build_app after both services are constructed.

        Kept as a setter to avoid a circular import between links.py and
        resources.py.
        """
        self._resources = resources_svc

    # ---------- identity resolution ----------

    def _resolve_identity(self, identity_hash: bytes):
        if self._identities is not None:
            try:
                return self._identities.load(identity_hash.hex())
            except Exception:
                pass
        return RNS.Identity.recall(identity_hash)

    # ---------- session helpers ----------

    @staticmethod
    def _entry(session: "Session", link_id_hex: str) -> _LinkEntry:
        try:
            hash_bytes = bytes.fromhex(link_id_hex.lower())
        except ValueError:
            raise LinkError(f"invalid link id: {link_id_hex!r}") from None
        entry = session.open_links.get(hash_bytes)
        if entry is None:
            raise LinkError(f"unknown link: {link_id_hex}")
        return entry

    def list_links(self, session: "Session") -> list[dict]:
        return [
            _link_snapshot(e.link, e.destination_hash, e.aspect)
            for e in session.open_links.values()
        ]

    def get_status(self, session: "Session", link_id_hex: str) -> dict:
        e = self._entry(session, link_id_hex)
        return _link_snapshot(e.link, e.destination_hash, e.aspect)

    # ---------- open ----------

    def _prepare_open_link(
        self,
        session: "Session",
        *,
        identity_hash: Optional[str],
        destination_hash: Optional[str],
        app_name: str,
        aspects: list[str],
        client_id: Any = None,
    ) -> tuple[_LinkEntry, bool]:
        """Pre-flight: validate, resolve identity, construct destination, cache
        check. Returns (entry, is_reused). Never blocks on the network.

        Called both by the REST/full-pipeline `open_link()` and by the WS
        async path so the handler can send an immediate ack that carries the
        real `link_id`.
        """
        # Accept either identity_hash or destination_hash as the lookup key.
        # RNS.Identity.recall() resolves both to the target identity, and the
        # webconsole workflow pastes destination hashes rather than identity
        # hashes — so both spellings are first-class.
        source_hash = identity_hash if identity_hash is not None else destination_hash
        if identity_hash is not None and destination_hash is not None:
            raise LinkError("provide identity_hash or destination_hash, not both")
        if source_hash is None:
            raise LinkError("identity_hash or destination_hash is required")
        h = source_hash.lower()
        if not _HEX_HASH.match(h):
            raise LinkError(f"invalid hash: {source_hash!r}")
        if not isinstance(app_name, str) or not _ASPECT_RE.match(app_name):
            raise LinkError("app_name must match [a-zA-Z0-9_]+")
        if not isinstance(aspects, list) or not all(_ASPECT_RE.match(a) for a in aspects):
            raise LinkError("aspects must be a list of [a-zA-Z0-9_]+ strings")

        target_identity = self._resolve_identity(bytes.fromhex(h))
        if target_identity is None:
            raise LinkError("no known identity for hash — issue an announce or path request first")

        destination = RNS.Destination(
            target_identity, RNS.Destination.OUT, RNS.Destination.SINGLE, app_name, *aspects
        )
        dest_hash = destination.hash
        aspect_str = ".".join([app_name, *aspects])

        existing = session.open_links.get(dest_hash)
        if existing is not None and getattr(existing.link, "status", None) == RNS.Link.ACTIVE:
            existing.open_client_id = client_id
            return existing, True

        link = RNS.Link(destination)
        entry = _LinkEntry(
            link, dest_hash, aspect_str, app_name, aspects,
            identified=False, open_client_id=client_id,
        )
        session.open_links[dest_hash] = entry
        # Wire callbacks now so `link.established` fires the moment RNS
        # signals ACTIVE, even if the caller aborts the continuation.
        self._wire_callbacks(session, entry)
        return entry, False

    async def _continue_open_link(
        self,
        session: "Session",
        entry: _LinkEntry,
        *,
        is_reused: bool,
        auto_identify: bool,
        await_established: bool,
        establishment_timeout: float,
        path_lookup_timeout: float,
        on_phase: Optional[Callable[[str], Awaitable[None]]] = None,
    ) -> dict:
        """Async continuation of `_prepare_open_link`: path lookup, wait for
        ACTIVE, optional auto-identify. Emits phase events via `on_phase`.
        Raises LinkError on establishment failure.
        """
        dest_hash = entry.destination_hash
        aspect_str = entry.aspect
        link = entry.link

        if is_reused:
            if auto_identify and not entry.identified:
                if on_phase is not None:
                    await on_phase("identifying")
                await self._identify(session, entry)
            return {"reused": True, **_link_snapshot(link, dest_hash, aspect_str)}

        # Path lookup (skipped when we already have one cached).
        if not RNS.Transport.has_path(dest_hash):
            if on_phase is not None:
                await on_phase("finding_path")
            try:
                RNS.Transport.request_path(dest_hash)
            except Exception:
                pass
            deadline = time.monotonic() + path_lookup_timeout
            while not RNS.Transport.has_path(dest_hash) and time.monotonic() < deadline:
                await asyncio.sleep(_POLL_INTERVAL_S)

        if on_phase is not None:
            await on_phase("establishing_link")

        if not await_established:
            return {"reused": False, "awaited": False, **_link_snapshot(link, dest_hash, aspect_str)}

        deadline = time.monotonic() + establishment_timeout
        while (
            getattr(link, "status", None) not in (RNS.Link.ACTIVE, RNS.Link.CLOSED)
            and time.monotonic() < deadline
        ):
            await asyncio.sleep(_POLL_INTERVAL_S)

        if getattr(link, "status", None) != RNS.Link.ACTIVE:
            try:
                link.teardown()
            except Exception:
                pass
            session.open_links.pop(dest_hash, None)
            raise LinkError("link establishment timed out")

        if auto_identify:
            if on_phase is not None:
                await on_phase("identifying")
            await self._identify(session, entry)

        return {"reused": False, "awaited": True, **_link_snapshot(link, dest_hash, aspect_str)}

    async def open_link(
        self,
        session: "Session",
        *,
        identity_hash: Optional[str] = None,
        destination_hash: Optional[str] = None,
        app_name: str,
        aspects: list[str],
        auto_identify: bool = False,
        await_established: bool = True,
        establishment_timeout: float = 15.0,
        path_lookup_timeout: float = 15.0,
        on_phase: Optional[Callable[[str], Awaitable[None]]] = None,
        client_id: Any = None,
    ) -> dict:
        """Full pipeline: pre-flight + establishment. Used by REST callers
        and by unit tests. The WS handler splits these two stages so it can
        ack early — see `ws_open` in handlers/links.py."""
        entry, is_reused = self._prepare_open_link(
            session,
            identity_hash=identity_hash,
            destination_hash=destination_hash,
            app_name=app_name,
            aspects=aspects,
            client_id=client_id,
        )
        return await self._continue_open_link(
            session,
            entry,
            is_reused=is_reused,
            auto_identify=auto_identify,
            await_established=await_established,
            establishment_timeout=establishment_timeout,
            path_lookup_timeout=path_lookup_timeout,
            on_phase=on_phase,
        )

    def _wire_callbacks(self, session: "Session", entry: _LinkEntry) -> None:
        session_id = session.id
        aspect = entry.aspect
        dest_hash = entry.destination_hash

        def _fire(event: dict) -> None:
            AsyncBridge.run_async(self._hub.send_session(session_id, event))

        def _on_established(link):
            # Consume the client id of the open_link call that created this
            # entry so subsequent lifecycle events (STALE→ACTIVE transitions,
            # for example) don't re-echo it.
            client_id = entry.open_client_id
            entry.open_client_id = None
            _fire(
                {
                    "type": "link.established",
                    "session_id": session_id,
                    "id": client_id,
                    **_link_snapshot(link, dest_hash, aspect),
                }
            )

        def _on_closed(link):
            # `teardown_reason` on the payload lets clients distinguish
            # locally-initiated closes (`initiator_closed`) from remote
            # tear-downs (`timeout`, `destination_closed`) — no separate
            # `link.disconnected` event needed.
            _fire(
                {
                    "type": "link.closed",
                    "session_id": session_id,
                    **_link_snapshot(link, dest_hash, aspect),
                }
            )
            # Evict from the session cache (may already be popped by close()).
            entry_cur = session.open_links.get(dest_hash)
            if entry_cur is entry:
                session.open_links.pop(dest_hash, None)

        def _on_packet(data, packet):
            _fire(
                {
                    "type": "link.data.received",
                    "session_id": session_id,
                    "link_id": dest_hash.hex(),
                    "destination_hash": dest_hash.hex(),
                    "aspect": aspect,
                    "data_b64": _b64(bytes(data)),
                    "size": len(data) if data else 0,
                }
            )

        def _on_remote_identified(link, identity):
            _fire(
                {
                    "type": "link.remote_identified",
                    "session_id": session_id,
                    "link_id": dest_hash.hex(),
                    "destination_hash": dest_hash.hex(),
                    "aspect": aspect,
                    "remote_identity_hash": identity.hexhash if identity else None,
                }
            )

        try:
            entry.link.set_link_established_callback(_on_established)
        except Exception:
            log.debug("set_link_established_callback not supported")
        try:
            entry.link.set_link_closed_callback(_on_closed)
        except Exception:
            log.debug("set_link_closed_callback not supported")
        try:
            entry.link.set_packet_callback(_on_packet)
        except Exception:
            log.debug("set_packet_callback not supported")
        try:
            entry.link.set_remote_identified_callback(_on_remote_identified)
        except Exception:
            log.debug("set_remote_identified_callback not supported")

        # Wire Resource send/receive callbacks onto this Link.
        if self._resources is not None:
            try:
                self._resources.attach_link(session, entry.link, dest_hash, aspect)
            except Exception:
                log.exception("resources.attach_link failed for link %s", dest_hash.hex())

    # ---------- identify ----------

    async def identify(self, session: "Session", link_id: str) -> dict:
        entry = self._entry(session, link_id)
        return await self._identify(session, entry)

    async def _identify(self, session: "Session", entry: _LinkEntry) -> dict:
        if session.active_identity_hash is None:
            raise LinkError("session has no active identity")
        if self._identities is None:
            raise LinkError("identity service not configured")
        try:
            identity = self._identities.load(session.active_identity_hash.hex())
        except Exception as e:
            raise LinkError(f"could not load session identity: {e}") from None
        try:
            entry.link.identify(identity)
        except Exception as e:
            raise LinkError(f"identify failed: {e}") from None
        entry.identified = True
        return {"ok": True, "link_id": entry.destination_hash.hex(), "identified": True}

    # ---------- close ----------

    def close(self, session: "Session", link_id: str) -> dict:
        entry = self._entry(session, link_id)
        # Pre-emptive teardown; the callback will fire link.closed as a
        # side-effect and evict from open_links then.
        try:
            entry.link.teardown()
        except Exception:
            log.exception("link teardown raised")
        session.open_links.pop(entry.destination_hash, None)
        return {"ok": True, "link_id": link_id}

    # ---------- send raw data ----------

    async def send_data(self, session: "Session", link_id: str, data_b64: str) -> dict:
        entry = self._entry(session, link_id)
        try:
            data = base64.b64decode(data_b64, validate=True)
        except Exception as e:
            raise LinkError(f"data_b64 is not valid base64: {e}") from None
        try:
            RNS.Packet(entry.link, data).send()
        except Exception as e:
            raise LinkError(f"link send failed: {e}") from None

        await self._hub.send_session(
            session.id,
            {
                "type": "link.data.sent",
                "session_id": session.id,
                "link_id": entry.destination_hash.hex(),
                "destination_hash": entry.destination_hash.hex(),
                "aspect": entry.aspect,
                "size": len(data),
            },
        )
        return {"ok": True, "link_id": link_id, "size": len(data)}

    # ---------- request/response ----------

    async def request(
        self,
        session: "Session",
        link_id: str,
        path: str,
        data_b64: Optional[str] = None,
        timeout: Optional[float] = None,
        *,
        await_response: bool = True,
        client_id: Optional[object] = None,
    ) -> dict:
        entry = self._entry(session, link_id)
        if not isinstance(path, str) or not path:
            raise LinkError("path is required")
        data: Optional[bytes] = None
        if data_b64 is not None:
            try:
                data = base64.b64decode(data_b64, validate=True)
            except Exception as e:
                raise LinkError(f"data_b64 is not valid base64: {e}") from None

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        session_id = session.id
        link_id_hex = entry.destination_hash.hex()

        def _on_response(receipt, _fut=future, _loop=loop):
            payload = getattr(receipt, "response", None)
            AsyncBridge.run_async(
                self._hub.send_session(
                    session_id,
                    {
                        "type": "link.request.response",
                        "session_id": session_id,
                        "id": client_id,
                        "link_id": link_id_hex,
                        "path": path,
                        "response_b64": _b64(bytes(payload)) if payload else None,
                        "size": len(payload) if payload else 0,
                    },
                )
            )
            if not _fut.done():
                _loop.call_soon_threadsafe(
                    _fut.set_result, {"kind": "response", "response": payload}
                )

        def _on_failed(receipt=None, _fut=future, _loop=loop):
            AsyncBridge.run_async(
                self._hub.send_session(
                    session_id,
                    {
                        "type": "link.request.failed",
                        "session_id": session_id,
                        "id": client_id,
                        "link_id": link_id_hex,
                        "path": path,
                    },
                )
            )
            if not _fut.done():
                _loop.call_soon_threadsafe(_fut.set_result, {"kind": "failed"})

        try:
            entry.link.request(
                path,
                data=data,
                response_callback=_on_response,
                failed_callback=_on_failed,
                timeout=timeout,
            )
        except Exception as e:
            raise LinkError(f"link.request failed: {e}") from None

        if not await_response:
            return {"ok": True, "link_id": link_id, "awaited": False, "path": path}

        # Deadline slightly larger than the RNS-side timeout so we surface
        # `failed` rather than raising CancelledError ourselves.
        deadline = (timeout if timeout is not None else 30.0) + 5.0
        try:
            result = await asyncio.wait_for(future, timeout=deadline)
        except asyncio.TimeoutError:
            return {
                "ok": False,
                "link_id": link_id,
                "path": path,
                "kind": "timeout",
            }
        if result["kind"] == "response":
            return {
                "ok": True,
                "link_id": link_id,
                "path": path,
                "kind": "response",
                "response_b64": _b64(bytes(result["response"])) if result["response"] else None,
                "size": len(result["response"]) if result["response"] else 0,
            }
        return {"ok": False, "link_id": link_id, "path": path, "kind": "failed"}

    # ---------- cleanup ----------

    async def cleanup_session(self, session: "Session") -> None:
        for dest_hash, entry in list(session.open_links.items()):
            try:
                entry.link.teardown()
            except Exception:
                log.exception("cleanup teardown raised for link %s", dest_hash.hex())
        session.open_links.clear()
