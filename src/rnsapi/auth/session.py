"""Session model and registry.

A session carries the state that survives across REST and WS calls from one
client: the active identity, session-owned destinations and links, and the
set of WebSocket connections currently attached. Long-lived resources like
destinations and links are cleaned up when a session ends so a client can't
leak them by disappearing.

Later phases hang their own state off the Session instance (see the
`owned_destinations`, `open_links`, `packet_listeners`, and `pending_receipts`
fields below).
"""
from __future__ import annotations

import asyncio
import logging
import secrets
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from ..config import Config
    from ..ws.hub import WSHub
    from ..ws.connection import WSConnection


log = logging.getLogger(__name__)


@dataclass
class Session:
    id: str
    token: str
    created_at: float
    last_seen_at: float
    is_anonymous: bool = False

    active_identity_hash: bytes | None = None
    owned_destinations: dict[bytes, Any] = field(default_factory=dict)
    open_links: dict[str, Any] = field(default_factory=dict)
    packet_listeners: set[bytes] = field(default_factory=set)
    pending_receipts: dict[str, Any] = field(default_factory=dict)

    ws_connections: set["WSConnection"] = field(default_factory=set)

    def touch(self) -> None:
        self.last_seen_at = time.time()


class SessionRegistry:
    def __init__(self, config: "Config", hub: "WSHub"):
        self._config = config
        self._hub = hub
        self._by_token: dict[str, Session] = {}
        self._by_id: dict[str, Session] = {}
        self._anon: Session | None = None
        self._reaper_task: asyncio.Task | None = None
        # Cleanup callbacks registered by later phases (identity/destination/link
        # services). Each takes a Session and is awaited during expiry/logout.
        self._cleanup_hooks: list[Any] = []

    def register_cleanup(self, coro_fn) -> None:
        """Later phases add resource-teardown callbacks here."""
        self._cleanup_hooks.append(coro_fn)

    def create(self, is_anonymous: bool = False) -> Session:
        token = secrets.token_urlsafe(32)
        sid = secrets.token_hex(8)
        now = time.time()
        s = Session(id=sid, token=token, created_at=now, last_seen_at=now, is_anonymous=is_anonymous)
        self._by_token[token] = s
        self._by_id[sid] = s
        return s

    def anonymous(self) -> Session:
        if self._anon is None:
            self._anon = self.create(is_anonymous=True)
        self._anon.touch()
        return self._anon

    def get_by_token(self, token: str) -> Session | None:
        s = self._by_token.get(token)
        if s is not None:
            s.touch()
        return s

    def get_by_id(self, sid: str) -> Session | None:
        return self._by_id.get(sid)

    def all(self) -> list[Session]:
        return list(self._by_token.values())

    async def revoke(self, token: str, *, reason: str = "logout") -> Session | None:
        s = self._by_token.pop(token, None)
        if s is None:
            return None
        self._by_id.pop(s.id, None)
        if self._anon is s:
            self._anon = None
        await self._teardown(s, reason=reason)
        return s

    async def _teardown(self, s: Session, *, reason: str) -> None:
        for hook in self._cleanup_hooks:
            try:
                await hook(s)
            except Exception:
                log.exception("session cleanup hook failed for %s", s.id)
        await self._hub.send_session(s.id, {"type": "auth.session.ended", "reason": reason})
        for conn in list(s.ws_connections):
            try:
                await conn.close(code=4001, message=f"session_{reason}")
            except Exception:
                pass

    def _is_expired(self, s: Session, now: float) -> bool:
        if s.is_anonymous:
            return False
        if now - s.last_seen_at > self._config.auth.session_inactivity_timeout:
            return True
        if now - s.created_at > self._config.auth.session_max_lifetime:
            return True
        return False

    async def sweep_once(self) -> list[Session]:
        """Expire any sessions past their limits. Returns the expired sessions."""
        now = time.time()
        expired = [s for s in self._by_token.values() if self._is_expired(s, now)]
        for s in expired:
            self._by_token.pop(s.token, None)
            self._by_id.pop(s.id, None)
            await self._teardown(s, reason="expired")
        return expired

    async def _reap_loop(self, interval: float = 5.0) -> None:
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    await self.sweep_once()
                except Exception:
                    log.exception("session sweep failed")
        except asyncio.CancelledError:
            pass

    def start_reaper(self, interval: float = 5.0) -> None:
        if self._reaper_task is None:
            self._reaper_task = asyncio.create_task(self._reap_loop(interval))

    async def stop_reaper(self) -> None:
        if self._reaper_task is not None:
            self._reaper_task.cancel()
            try:
                await self._reaper_task
            except asyncio.CancelledError:
                pass
            self._reaper_task = None
