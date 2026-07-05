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
import io
import logging
import re
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional

import RNS
from RNS.vendor import umsgpack

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
    # Callers may hand us a placeholder entry whose RNS.Link is not yet
    # constructed (ws_open reserves a session.open_links slot synchronously
    # so link.list is truthful right after the ack, then creates the real
    # Link inside the continuation).
    if link is None:
        return {
            "link_id": destination_hash.hex(),
            "destination_hash": destination_hash.hex(),
            "aspect": aspect,
            "status": "PENDING",
            "mtu": None,
            "mdu": None,
            "remote_identity_hash": None,
            "teardown_reason": None,
        }
    remote_identity = None
    try:
        rid = link.get_remote_identity()
        if rid is not None:
            remote_identity = rid.hexhash
    except Exception as e:
        log.debug("get_remote_identity() raised on link %s: %s", destination_hash.hex(), e)
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
        "suppress_initial_established",
    )

    def __init__(
        self,
        link,
        destination_hash,
        aspect,
        app_name,
        sub_aspects,
        identified=False,
        open_client_id=None,
        suppress_initial_established=False,
    ):
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
        # When True, the wired `_on_established` RNS callback logs the
        # ACTIVE transition but does NOT emit `link.established`. The
        # continuation coroutine is responsible for emitting it AFTER
        # link.identify() so the client can't race the LINKIDENTIFY
        # packet with a follow-up link.request (RNS's per-link packet
        # ordering then guarantees the peer sees identify before the
        # request). Set by the open pipeline when auto_identify is True.
        self.suppress_initial_established = suppress_initial_established


@dataclass
class _PreparedOpen:
    """Validated inputs for an open_link — everything needed to create the
    RNS.Link, but without having created it yet. Deferring RNS.Link()
    construction lets us emit `link.open.phase` events on the wire before
    RNS can fire `_on_established` from a worker thread and beat the phase
    event to the client."""

    destination: object  # RNS.Destination
    destination_hash: bytes
    aspect: str
    app_name: str
    sub_aspects: tuple


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
        hex_hash = identity_hash.hex()
        if self._identities is not None:
            try:
                identity = self._identities.load(hex_hash)
                log.debug("resolved identity %s from local IdentityService", hex_hash)
                return identity
            except Exception as e:
                # Not found locally is the common case (peer identity we only
                # know via announce), so this is expected — but log at debug
                # for troubleshooting.
                log.debug("local IdentityService could not load %s (%s); falling back to RNS.Identity.recall", hex_hash, e)
        identity = RNS.Identity.recall(identity_hash)
        if identity is None:
            log.debug("RNS.Identity.recall(%s) returned None — no announce received for this hash", hex_hash)
        else:
            log.debug("resolved identity %s via RNS.Identity.recall (announce cache)", hex_hash)
        return identity

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
    ) -> _PreparedOpen:
        """Pre-flight: validate, resolve identity, construct the RNS.Destination.
        Does NOT create the RNS.Link — see the `_PreparedOpen` docstring for
        why. Never blocks on the network.
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
        return _PreparedOpen(
            destination=destination,
            destination_hash=destination.hash,
            aspect=".".join([app_name, *aspects]),
            app_name=app_name,
            sub_aspects=tuple(aspects),
        )

    def _reuse_active_link(self, session: "Session", dest_hash: bytes) -> Optional[_LinkEntry]:
        """Return the cached ACTIVE _LinkEntry for *dest_hash*, or None if no
        ACTIVE link is cached. Callers use this to short-circuit re-open."""
        existing = session.open_links.get(dest_hash)
        if existing is not None and existing.link is not None and getattr(existing.link, "status", None) == RNS.Link.ACTIVE:
            return existing
        return None

    def reserve_link_slot(
        self,
        session: "Session",
        prepared: _PreparedOpen,
        *,
        client_id: Any = None,
    ) -> _LinkEntry:
        """Insert a placeholder _LinkEntry (link=None) into
        session.open_links. Callers use this to make `link.list` /
        `link.status` truthful the moment the `link.open` ack goes out,
        before the async continuation constructs the real RNS.Link."""
        entry = _LinkEntry(
            None,
            prepared.destination_hash,
            prepared.aspect,
            prepared.app_name,
            prepared.sub_aspects,
            identified=False,
            open_client_id=client_id,
        )
        session.open_links[prepared.destination_hash] = entry
        return entry

    def _create_link_entry(
        self,
        session: "Session",
        prepared: _PreparedOpen,
        *,
        client_id: Any = None,
        reserved_entry: Optional[_LinkEntry] = None,
        suppress_initial_established: bool = False,
    ) -> _LinkEntry:
        """Create the RNS.Link, wire lifecycle callbacks, cache the entry.

        Callers should have already emitted any progress phase events they
        want ordered *before* the link's `_on_established` callback can
        fire. Constructing `RNS.Link(destination)` kicks off the handshake
        immediately; wiring callbacks synchronously after construction
        means `_on_established` cannot fire until we return.

        If *reserved_entry* is provided (from `reserve_link_slot`), its
        `link` field is populated in place so any `session.open_links`
        consumer that was already holding the placeholder sees the real
        Link.

        When *suppress_initial_established* is True, the wired
        `_on_established` callback will NOT emit `link.established` for
        the initial ACTIVE transition; the caller must emit it itself via
        `_emit_established(...)` after any pre-request setup (typically
        auto_identify) has been done.
        """
        link = RNS.Link(prepared.destination)
        if reserved_entry is not None:
            entry = reserved_entry
            entry.link = link
            entry.suppress_initial_established = suppress_initial_established
        else:
            entry = _LinkEntry(
                link,
                prepared.destination_hash,
                prepared.aspect,
                prepared.app_name,
                prepared.sub_aspects,
                identified=False,
                open_client_id=client_id,
                suppress_initial_established=suppress_initial_established,
            )
        session.open_links[prepared.destination_hash] = entry
        self._wire_callbacks(session, entry)
        log.info(
            "session %s opening link to %s (aspect=%s)",
            session.id,
            prepared.destination_hash.hex(),
            prepared.aspect,
        )
        return entry

    async def _emit_established(self, session_id: str, entry: _LinkEntry) -> None:
        """Manually emit `link.established` for an entry whose wired
        `_on_established` callback was suppressed (auto_identify path).

        Consumes `entry.open_client_id` so subsequent STALE→ACTIVE
        transitions won't re-echo it — matches the wired callback's
        semantics. Runs on the asyncio loop, so we can await hub delivery
        directly instead of going through AsyncBridge.
        """
        if entry.link is None:
            log.warning(
                "session %s: _emit_established called with no RNS.Link on entry %s",
                session_id, entry.destination_hash.hex(),
            )
            return
        client_id = entry.open_client_id
        entry.open_client_id = None
        # Clear the flag so any *future* STALE→ACTIVE re-transition on
        # this Link fires normally through the wired callback.
        entry.suppress_initial_established = False
        await self._hub.send_session(
            session_id,
            {
                "type": "link.established",
                "session_id": session_id,
                "id": client_id,
                **_link_snapshot(entry.link, entry.destination_hash, entry.aspect),
            },
        )

    async def _wait_for_active(
        self,
        session: "Session",
        entry: _LinkEntry,
        *,
        establishment_timeout: float,
    ) -> None:
        """Poll until the entry's Link reaches ACTIVE. On timeout, tear it
        down, evict it from the session, and raise LinkError."""
        link = entry.link
        dest_hash = entry.destination_hash
        deadline = time.monotonic() + establishment_timeout
        while (
            getattr(link, "status", None) not in (RNS.Link.ACTIVE, RNS.Link.CLOSED)
            and time.monotonic() < deadline
        ):
            await asyncio.sleep(_POLL_INTERVAL_S)

        status = getattr(link, "status", None)
        if status != RNS.Link.ACTIVE:
            log.info(
                "session %s link %s failed to establish (status=%s)",
                session.id,
                dest_hash.hex(),
                _status_str(link),
            )
            try:
                link.teardown()
            except Exception as e:
                log.warning("teardown() raised while cleaning up unestablished link %s: %s", dest_hash.hex(), e)
            session.open_links.pop(dest_hash, None)
            raise LinkError("link establishment timed out")

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
        and by unit tests. The WS handler runs the pre-flight then drives
        the continuation itself so it can send an immediate ack — see
        `ws_open` in handlers/links.py.
        """
        prepared = self._prepare_open_link(
            session,
            identity_hash=identity_hash,
            destination_hash=destination_hash,
            app_name=app_name,
            aspects=aspects,
        )
        dest_hash = prepared.destination_hash
        log.info(
            "session %s open_link: dest=%s aspect=%s auto_identify=%s await_established=%s",
            session.id, dest_hash.hex(), prepared.aspect, auto_identify, await_established,
        )

        existing = self._reuse_active_link(session, dest_hash)
        if existing is not None:
            log.info(
                "session %s open_link: reusing cached ACTIVE link %s",
                session.id, dest_hash.hex(),
            )
            existing.open_client_id = client_id
            if auto_identify and not existing.identified:
                if on_phase is not None:
                    await on_phase("identifying")
                await self._identify(session, existing)
            return {"reused": True, **_link_snapshot(existing.link, dest_hash, prepared.aspect)}

        # Path lookup (skipped when we already have one cached).
        if not RNS.Transport.has_path(dest_hash):
            if on_phase is not None:
                await on_phase("finding_path")
            log.debug("issuing RNS path request for %s", dest_hash.hex())
            try:
                RNS.Transport.request_path(dest_hash)
            except Exception as e:
                log.warning("RNS.Transport.request_path(%s) raised: %s", dest_hash.hex(), e)
            deadline = time.monotonic() + path_lookup_timeout
            while not RNS.Transport.has_path(dest_hash) and time.monotonic() < deadline:
                await asyncio.sleep(_POLL_INTERVAL_S)
            if not RNS.Transport.has_path(dest_hash):
                log.warning(
                    "no path to %s after %.1fs — proceeding to Link creation anyway (RNS will retry)",
                    dest_hash.hex(), path_lookup_timeout,
                )
            else:
                log.debug("path to %s resolved", dest_hash.hex())

        # Emit `establishing_link` BEFORE creating the RNS.Link so it can't
        # race with `_on_established` (see _create_link_entry).
        if on_phase is not None:
            await on_phase("establishing_link")

        # `await_established=False` shortcut: we don't wait to see ACTIVE,
        # so we don't run identify here either — no client is waiting on
        # an established event to gate on. Suppression stays off.
        suppress = bool(auto_identify and await_established)
        entry = self._create_link_entry(
            session,
            prepared,
            client_id=client_id,
            suppress_initial_established=suppress,
        )

        if not await_established:
            return {"reused": False, "awaited": False, **_link_snapshot(entry.link, dest_hash, prepared.aspect)}

        await self._wait_for_active(session, entry, establishment_timeout=establishment_timeout)

        if auto_identify:
            if on_phase is not None:
                await on_phase("identifying")
            await self._identify(session, entry)
            # link.identify() has queued LINKIDENTIFY on the link — now
            # emit link.established. Any WS listener on this session will
            # see identify-then-established, matching the WS-open path.
            await self._emit_established(session.id, entry)

        return {"reused": False, "awaited": True, **_link_snapshot(entry.link, dest_hash, prepared.aspect)}

    async def continue_open_link_ws(
        self,
        session: "Session",
        prepared: _PreparedOpen,
        reserved_entry: _LinkEntry,
        *,
        client_id: Any,
        auto_identify: bool,
        establishment_timeout: float,
        path_lookup_timeout: float,
        on_phase: Callable[[str], Awaitable[None]],
    ) -> None:
        """WS variant of the open continuation: path lookup + phase events +
        create Link + wait for ACTIVE + optional identify. Never blocks on a
        cached ACTIVE link — the WS handler handles reuse synchronously
        before dispatching this.

        The RNS.Link is created *after* the `establishing_link` phase event
        is on the wire, which is what prevents the `_on_established`
        callback from beating `link.open.phase` to the client. The
        *reserved_entry* is a placeholder inserted synchronously by
        `ws_open` so `link.list`/`link.status` are correct immediately
        after the ack; we populate its `.link` field here.
        """
        dest_hash = prepared.destination_hash

        if not RNS.Transport.has_path(dest_hash):
            await on_phase("finding_path")
            log.debug("issuing RNS path request for %s", dest_hash.hex())
            try:
                RNS.Transport.request_path(dest_hash)
            except Exception as e:
                log.warning("RNS.Transport.request_path(%s) raised: %s", dest_hash.hex(), e)
            deadline = time.monotonic() + path_lookup_timeout
            while not RNS.Transport.has_path(dest_hash) and time.monotonic() < deadline:
                await asyncio.sleep(_POLL_INTERVAL_S)
            if not RNS.Transport.has_path(dest_hash):
                log.warning(
                    "no path to %s after %.1fs — proceeding to Link creation anyway (RNS will retry)",
                    dest_hash.hex(), path_lookup_timeout,
                )
            else:
                log.debug("path to %s resolved", dest_hash.hex())

        await on_phase("establishing_link")

        entry = self._create_link_entry(
            session,
            prepared,
            client_id=client_id,
            reserved_entry=reserved_entry,
            # When auto_identify is on, the wired _on_established
            # callback must NOT fire link.established — we'll do it
            # ourselves once identify has been sent, so the client can't
            # send a request that races the LINKIDENTIFY packet.
            suppress_initial_established=auto_identify,
        )
        await self._wait_for_active(session, entry, establishment_timeout=establishment_timeout)

        if auto_identify:
            await on_phase("identifying")
            await self._identify(session, entry)
            # Now that identify is queued on the link, tell the client
            # the link is established. RNS's per-link packet ordering
            # ensures the peer sees LINKIDENTIFY before any request the
            # client may fire in response to this event.
            await self._emit_established(session.id, entry)

    def _wire_callbacks(self, session: "Session", entry: _LinkEntry) -> None:
        session_id = session.id
        aspect = entry.aspect
        dest_hash = entry.destination_hash

        def _fire(event: dict) -> None:
            AsyncBridge.run_async(self._hub.send_session(session_id, event))

        def _on_established(link):
            log.info(
                "session %s link %s ACTIVE (mtu=%s mdu=%s)",
                session_id,
                dest_hash.hex(),
                getattr(link, "mtu", None),
                getattr(link, "mdu", None),
            )
            # If the continuation is going to run identify(), it will emit
            # `link.established` itself once the LINKIDENTIFY packet has
            # been queued to the link. Skipping the emission here prevents
            # the client from seeing "established" and firing off a
            # request before the peer has processed identify.
            if entry.suppress_initial_established:
                log.debug(
                    "session %s link %s: deferring link.established until identify completes",
                    session_id, dest_hash.hex(),
                )
                return
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
            log.info(
                "session %s link %s closed (reason=%s)",
                session_id,
                dest_hash.hex(),
                _teardown_reason(link),
            )
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
            size = len(data) if data else 0
            log.debug(
                "session %s link %s inbound raw packet (size=%d)",
                session_id, dest_hash.hex(), size,
            )
            _fire(
                {
                    "type": "link.data.received",
                    "session_id": session_id,
                    "link_id": dest_hash.hex(),
                    "destination_hash": dest_hash.hex(),
                    "aspect": aspect,
                    "data_b64": _b64(bytes(data)),
                    "size": size,
                }
            )

        def _on_remote_identified(link, identity):
            log.info(
                "session %s link %s remote identified as %s",
                session_id, dest_hash.hex(),
                identity.hexhash if identity else "<none>",
            )
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

        # Callback wiring — these attributes exist on every stock RNS.Link
        # build; a failure would indicate a shimmed test double or a
        # forked RNS. Log at warning so it's not silently swallowed while
        # still allowing the entry to exist (the caller may recover).
        try:
            entry.link.set_link_established_callback(_on_established)
        except Exception as e:
            log.warning("set_link_established_callback failed on link %s: %s", dest_hash.hex(), e)
        try:
            entry.link.set_link_closed_callback(_on_closed)
        except Exception as e:
            log.warning("set_link_closed_callback failed on link %s: %s", dest_hash.hex(), e)
        try:
            entry.link.set_packet_callback(_on_packet)
        except Exception as e:
            log.warning("set_packet_callback failed on link %s: %s", dest_hash.hex(), e)
        try:
            entry.link.set_remote_identified_callback(_on_remote_identified)
        except Exception as e:
            log.warning("set_remote_identified_callback failed on link %s: %s", dest_hash.hex(), e)

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
        if entry.link is None:
            raise LinkError("link is not yet established")
        if self._identities is None:
            raise LinkError("identity service not configured")
        # Identity resolution order:
        # 1. session.active_identity_hash if explicitly set (multi-user /
        #    auth-enabled setup, or a client that did PUT
        #    /session/active-identity first).
        # 2. The daemon's default identity — MeshChatX-parity fallback for
        #    the common case (webconsole with auth disabled, `auto_identify`
        #    checkbox on, no explicit identity selection). Peers with
        #    ALLOW_LIST request handlers rely on this being present.
        identity = None
        source = None
        if session.active_identity_hash is not None:
            try:
                identity = self._identities.load(session.active_identity_hash.hex())
                source = f"session active ({session.active_identity_hash.hex()})"
            except Exception as e:
                raise LinkError(f"could not load session identity: {e}") from None
        else:
            try:
                identity = self._identities.default_identity()
                source = f"default ({identity.hexhash})"
            except Exception as e:
                raise LinkError(f"could not load default identity: {e}") from None
        log.info(
            "session %s link %s identifying with %s identity",
            session.id, entry.destination_hash.hex(), source,
        )
        try:
            entry.link.identify(identity)
        except Exception as e:
            raise LinkError(f"identify failed: {e}") from None
        entry.identified = True
        return {
            "ok": True,
            "link_id": entry.destination_hash.hex(),
            "identified": True,
            "identity_hash": identity.hexhash,
        }

    # ---------- close ----------

    def close(self, session: "Session", link_id: str) -> dict:
        entry = self._entry(session, link_id)
        # Pre-emptive teardown; the callback will fire link.closed as a
        # side-effect and evict from open_links then. `entry.link` can be
        # None if close() races with the placeholder → real-Link swap in
        # continue_open_link_ws.
        log.info("session %s closing link %s (caller-initiated)", session.id, link_id)
        if entry.link is not None:
            try:
                entry.link.teardown()
            except Exception:
                log.exception("link teardown raised for %s", link_id)
        session.open_links.pop(entry.destination_hash, None)
        return {"ok": True, "link_id": link_id}

    # ---------- send raw data ----------

    async def send_data(self, session: "Session", link_id: str, data_b64: str) -> dict:
        entry = self._entry(session, link_id)
        if entry.link is None:
            log.warning("send_data called on link %s that is still preparing", link_id)
            raise LinkError("link is not yet established")
        try:
            data = base64.b64decode(data_b64, validate=True)
        except Exception as e:
            log.warning("send_data on link %s got invalid base64 data_b64: %s", link_id, e)
            raise LinkError(f"data_b64 is not valid base64: {e}") from None
        try:
            RNS.Packet(entry.link, data).send()
        except Exception as e:
            log.warning("RNS.Packet(...).send() failed on link %s: %s", link_id, e)
            raise LinkError(f"link send failed: {e}") from None

        log.debug(
            "session %s link %s outbound raw packet (size=%d)",
            session.id, link_id, len(data),
        )
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
        """Send a request over an ACTIVE link and route the outcome through
        the WS event stream.

        Timeout semantics: RNS.Link.request's `failed_callback` is only
        reliably invoked for resource-sized requests. For packet-sized
        requests (payload <= link.mdu, which is the common case), RNS's
        `RequestReceipt.request_timed_out` gates on `status == DELIVERED`
        and packet-based receipts never leave `SENT` — so the callback is
        silently swallowed on timeout (RNS 1.x bug, at least through the
        version pinned in .venv). To keep clients from hanging forever we
        run our own watchdog: if neither the response nor the RNS failed
        callback arrives within `timeout + grace`, we emit
        `link.request.failed` ourselves.
        """
        entry = self._entry(session, link_id)
        if entry.link is None:
            log.warning("request on link %s that is still preparing (path=%s)", link_id, path)
            raise LinkError("link is not yet established")
        if not isinstance(path, str) or not path:
            log.warning("request rejected — invalid/missing path (link=%s)", link_id)
            raise LinkError("path is required")
        # `data_b64` is the *msgpack encoding* of the caller's payload, not
        # opaque bytes. We decode to base64 → bytes → msgpack-unpack → native
        # Python value, then hand that native value to link.request(). RNS
        # then re-wraps the whole envelope as
        #     umsgpack.packb([timestamp, path_hash, data])
        # so the peer's registered request handler receives `data` as the
        # native structure the caller originally packed. Passing raw bytes
        # instead would surface at the peer as a msgpack `bin` value —
        # RNS-based firmware handlers (microReticulum, MeshChatX, etc.)
        # expect the structured form and won't parse the bin.
        # See MeshChatX's meshchat.py:16580-16591 for the same treatment.
        data: Any = None
        if data_b64 is not None:
            try:
                raw = base64.b64decode(data_b64, validate=True)
            except Exception as e:
                log.warning("request on link %s path %s: data_b64 is not valid base64: %s", link_id, path, e)
                raise LinkError(f"data_b64 is not valid base64: {e}") from None
            if len(raw) > 0:
                try:
                    data = umsgpack.unpackb(raw)
                except Exception as e:
                    log.warning("request on link %s path %s: data_b64 is not valid msgpack: %s", link_id, path, e)
                    raise LinkError(f"data_b64 is not valid msgpack: {e}") from None

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        session_id = session.id
        link_id_hex = entry.destination_hash.hex()

        def _on_response(receipt, _fut=future, _loop=loop):
            # The peer's response can be any msgpack-serializable native
            # Python value (dict, list, int, bytes, ...) or an
            # io.BufferedReader for a Resource-style large response. Read
            # the file first if present, then msgpack-pack whichever raw
            # payload we've got. `response_b64` is the *msgpack encoding*
            # of the response — symmetric with the request side, matching
            # the RNS wire convention. Callers msgpack-decode client-side.
            raw = getattr(receipt, "response", None)
            try:
                if hasattr(raw, "read") and not isinstance(raw, (bytes, bytearray)):
                    raw_to_pack = raw.read()
                else:
                    raw_to_pack = raw
            except Exception as e:
                log.warning(
                    "session %s link %s request %s: reading BufferedReader response failed: %s",
                    session_id, link_id_hex, path, e,
                )
                raw_to_pack = None
            try:
                packed = umsgpack.packb(raw_to_pack)
                response_b64 = base64.b64encode(packed).decode("ascii")
                response_size = len(packed)
            except Exception as e:
                log.warning(
                    "session %s link %s request %s: msgpack packing response failed: %s",
                    session_id, link_id_hex, path, e,
                )
                response_b64 = None
                response_size = 0
            log.info(
                "session %s link %s request %s response (packed_size=%d)",
                session_id, link_id_hex, path, response_size,
            )
            AsyncBridge.run_async(
                self._hub.send_session(
                    session_id,
                    {
                        "type": "link.request.response",
                        "session_id": session_id,
                        "id": client_id,
                        "link_id": link_id_hex,
                        "path": path,
                        "response_b64": response_b64,
                        "size": response_size,
                    },
                )
            )
            if not _fut.done():
                _loop.call_soon_threadsafe(
                    _fut.set_result,
                    {"kind": "response", "response_b64": response_b64, "size": response_size},
                )

        def _on_failed(receipt=None, _fut=future, _loop=loop):
            log.info(
                "session %s link %s request %s FAILED (no response before RNS timeout)",
                session_id,
                link_id_hex,
                path,
            )
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

        log.info(
            "session %s link %s request %s sending (data_type=%s, timeout=%s, link_status=%s)",
            session_id,
            link_id_hex,
            path,
            type(data).__name__ if data is not None else "None",
            timeout,
            _status_str(entry.link),
        )
        try:
            entry.link.request(
                path,
                data=data,
                response_callback=_on_response,
                failed_callback=_on_failed,
                timeout=timeout,
            )
        except Exception as e:
            log.warning(
                "session %s link %s: RNS Link.request(%s) raised at dispatch: %s",
                session_id, link_id_hex, path, e,
            )
            raise LinkError(f"link.request failed: {e}") from None

        # Effective deadline: RNS-side timeout + 5s grace so we surface
        # `timeout`/`failed` rather than raising CancelledError ourselves.
        # Also serves as the watchdog window that compensates for RNS's
        # packet-based-request failed_callback bug (see the docstring).
        deadline = (timeout if timeout is not None else 30.0) + 5.0

        if not await_response:
            async def _watchdog() -> None:
                try:
                    await asyncio.wait_for(asyncio.shield(future), timeout=deadline)
                except asyncio.TimeoutError:
                    if future.done():
                        return
                    log.info(
                        "session %s link %s request %s watchdog timeout — "
                        "RNS did not fire failed_callback (known packet-request bug)",
                        session_id,
                        link_id_hex,
                        path,
                    )
                    future.set_result({"kind": "timeout"})
                    await self._hub.send_session(
                        session_id,
                        {
                            "type": "link.request.failed",
                            "session_id": session_id,
                            "id": client_id,
                            "link_id": link_id_hex,
                            "path": path,
                        },
                    )
            asyncio.create_task(_watchdog())
            return {"ok": True, "link_id": link_id, "awaited": False, "path": path}

        try:
            result = await asyncio.wait_for(future, timeout=deadline)
        except asyncio.TimeoutError:
            log.info(
                "session %s link %s request %s watchdog timeout — "
                "RNS did not fire failed_callback (known packet-request bug)",
                session_id,
                link_id_hex,
                path,
            )
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
                "response_b64": result.get("response_b64"),
                "size": result.get("size", 0),
            }
        return {"ok": False, "link_id": link_id, "path": path, "kind": "failed"}

    # ---------- cleanup ----------

    async def cleanup_session(self, session: "Session") -> None:
        for dest_hash, entry in list(session.open_links.items()):
            if entry.link is None:
                continue  # placeholder — no RNS.Link to tear down
            try:
                entry.link.teardown()
            except Exception:
                log.exception("cleanup teardown raised for link %s", dest_hash.hex())
        session.open_links.clear()
