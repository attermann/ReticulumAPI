"""Fanout of server-initiated events to WebSocket connections.

Provides three delivery scopes:
- broadcast(event)          — every connection currently open
- send_session(id, event)   — every connection attached to one session
- send_connection(c, event) — one connection

Every phase-specific service class pulls the hub off the app dict and calls
one of these to push events at clients.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Iterable


log = logging.getLogger(__name__)


class WSHub:
    def __init__(self) -> None:
        self._connections: set = set()

    def register(self, conn) -> None:
        self._connections.add(conn)

    def unregister(self, conn) -> None:
        self._connections.discard(conn)

    def connections(self) -> list:
        return list(self._connections)

    def session_connections(self, session_id: str) -> list:
        return [
            c
            for c in self._connections
            if c.session is not None and c.session.id == session_id
        ]

    async def _fanout(self, targets: Iterable, event: dict) -> None:
        results = await asyncio.gather(
            *(c.send_json(event) for c in targets), return_exceptions=True
        )
        for r in results:
            if isinstance(r, Exception):
                log.debug("ws fanout error: %r", r)

    async def broadcast(self, event: dict) -> None:
        await self._fanout(list(self._connections), event)

    async def send_session(self, session_id: str, event: dict) -> None:
        if not session_id:
            return
        await self._fanout(self.session_connections(session_id), event)

    async def send_connection(self, conn, event: dict) -> None:
        await conn.send_json(event)
