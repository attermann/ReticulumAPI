"""WebSocket message router.

Dispatches inbound WS frames by their `type` field to registered handlers.
Handlers are coroutines with the signature `(conn, msg) -> None`.
"""
from __future__ import annotations

import logging
from typing import Awaitable, Callable, Dict


log = logging.getLogger(__name__)


Handler = Callable[..., Awaitable[None]]


class WSRouter:
    def __init__(self) -> None:
        self._handlers: Dict[str, Handler] = {}

    def register(self, msg_type: str, handler: Handler) -> None:
        if msg_type in self._handlers:
            raise ValueError(f"handler for {msg_type!r} already registered")
        self._handlers[msg_type] = handler

    def has(self, msg_type: str) -> bool:
        return msg_type in self._handlers

    async def dispatch(self, conn, msg: dict) -> None:
        msg_type = msg.get("type")
        if not isinstance(msg_type, str):
            await conn.send_json(
                {"type": "error", "error": "missing_type", "id": msg.get("id")}
            )
            return
        handler = self._handlers.get(msg_type)
        if handler is None:
            await conn.send_json(
                {
                    "type": "error",
                    "error": "unknown_type",
                    "requested_type": msg_type,
                    "id": msg.get("id"),
                }
            )
            return
        try:
            await handler(conn, msg)
        except Exception:
            log.exception("ws handler for %s failed", msg_type)
            await conn.send_json(
                {
                    "type": "error",
                    "error": "internal",
                    "requested_type": msg_type,
                    "id": msg.get("id"),
                }
            )
