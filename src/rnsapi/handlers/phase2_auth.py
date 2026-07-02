"""REST and WS handlers for login/logout and session inspection.

REST:
- POST /auth/login       — issue a bearer token (or anonymous session when auth is off)
- POST /auth/logout      — revoke the current session
- GET  /session          — inspect the current session

WS:
- {"type": "auth.logout"} — same as REST /auth/logout but over WS
- {"type": "ping"}        — no-op, refreshes last_seen_at

The first-frame auth handshake for WS is implemented in server.py because it
is part of the connection lifecycle, not a routable message.
"""
from __future__ import annotations

import time

from aiohttp import web

from ..auth.passwords import verify_password


def _session_info(session) -> dict:
    return {
        "session_id": session.id,
        "created_at": session.created_at,
        "last_seen_at": session.last_seen_at,
        "is_anonymous": session.is_anonymous,
        "ws_connections": len(session.ws_connections),
    }


async def rest_login(request: web.Request) -> web.Response:
    config = request.app["config"]
    registry = request.app["sessions"]
    hub = request.app["hub"]

    if not config.auth.enabled:
        # Auth is disabled — return the (shared) anonymous session token so
        # clients have a uniform login flow regardless of server config.
        session = registry.anonymous()
        return web.json_response(
            {
                "token": session.token,
                "session_id": session.id,
                "auth_required": False,
                "is_anonymous": True,
            }
        )

    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid_json"}, status=400)

    username = body.get("username")
    password = body.get("password") or ""
    if username != config.auth.username or not verify_password(password, config.auth.password_hash):
        return web.json_response({"error": "invalid_credentials"}, status=401)

    session = registry.create()
    await hub.send_session(
        session.id,
        {"type": "auth.session.created", "session_id": session.id, "at": session.created_at},
    )
    return web.json_response(
        {
            "token": session.token,
            "session_id": session.id,
            "auth_required": True,
            "is_anonymous": False,
        }
    )


async def rest_logout(request: web.Request) -> web.Response:
    session = request["session"]
    registry = request.app["sessions"]
    if session.is_anonymous:
        return web.json_response({"ok": True, "note": "anonymous_session_not_revoked"})
    await registry.revoke(session.token, reason="logout")
    return web.json_response({"ok": True})


async def rest_session(request: web.Request) -> web.Response:
    return web.json_response(_session_info(request["session"]))


async def ws_logout(conn, msg: dict) -> None:
    if conn.session is None:
        await conn.send_json({"type": "error", "error": "not_authenticated", "id": msg.get("id")})
        return
    if conn.session.is_anonymous:
        await conn.send_json(
            {
                "type": "auth.logout.result",
                "ok": True,
                "note": "anonymous_session_not_revoked",
                "id": msg.get("id"),
            }
        )
        return
    registry = conn.app["sessions"]
    token = conn.session.token
    await conn.send_json({"type": "auth.logout.result", "ok": True, "id": msg.get("id")})
    await registry.revoke(token, reason="logout")


async def ws_ping(conn, msg: dict) -> None:
    if conn.session is not None:
        conn.session.touch()
    await conn.send_json({"type": "pong", "id": msg.get("id"), "t": time.time()})


async def ws_session(conn, msg: dict) -> None:
    if conn.session is None:
        await conn.send_json({"type": "error", "error": "not_authenticated", "id": msg.get("id")})
        return
    await conn.send_json({"type": "session.info", "id": msg.get("id"), **_session_info(conn.session)})


def register(app: web.Application) -> None:
    app.router.add_post("/auth/login", rest_login)
    app.router.add_post("/auth/logout", rest_logout)
    app.router.add_get("/session", rest_session)

    router = app["ws_router"]
    router.register("auth.logout", ws_logout)
    router.register("ping", ws_ping)
    router.register("session.info", ws_session)
