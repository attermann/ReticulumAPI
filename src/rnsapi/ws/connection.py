"""WebSocket connection wrapper.

Serializes concurrent sends via an asyncio.Lock. aiohttp's WebSocketResponse
is not safe for concurrent writes from multiple tasks, and broadcast fanouts
regularly race with reply-to-inbound sends. Every outbound frame in the
daemon goes through this wrapper.
"""
from __future__ import annotations

import asyncio
import logging
import secrets
from typing import TYPE_CHECKING

from aiohttp import web


if TYPE_CHECKING:
    from ..auth.session import Session


log = logging.getLogger(__name__)


class WSConnection:
    def __init__(self, ws: web.WebSocketResponse, app: web.Application | None = None):
        self.id = secrets.token_hex(6)
        self.ws = ws
        self.app = app
        self.session: "Session | None" = None
        self._send_lock = asyncio.Lock()

    async def send_json(self, data: dict) -> None:
        if self.ws.closed:
            return
        async with self._send_lock:
            if self.ws.closed:
                return
            try:
                await self.ws.send_json(data)
            except ConnectionResetError:
                log.debug("send_json on closed ws %s", self.id)
            except Exception:
                log.exception("send_json failed on ws %s", self.id)

    async def close(self, code: int = 1000, message: str = "") -> None:
        try:
            if not self.ws.closed:
                await self.ws.close(code=code, message=message.encode() if isinstance(message, str) else message)
        except Exception:
            pass

    def attach(self, session: "Session") -> None:
        self.session = session
        session.ws_connections.add(self)

    def detach(self) -> None:
        if self.session is not None:
            self.session.ws_connections.discard(self)
            self.session = None
