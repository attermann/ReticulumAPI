"""Identity and destination endpoints.

REST:
- POST   /identities                  — create a new identity
- GET    /identities                  — list persisted identities
- GET    /identities/{hash}           — inspect one identity
- GET    /session/active-identity     — active identity for this session
- PUT    /session/active-identity     — set the active identity (409 if session dirty)
- DELETE /session/active-identity     — clear the active identity (409 if session dirty)
- GET    /destinations                — list destinations owned by this session
- POST   /destinations                — register a destination
- DELETE /destinations/{hash}         — deregister a destination

WS message types are the exact same operations, prefixed `identity.*` /
`session.active_identity.*` / `destination.*`, dispatched via the router.
Server-emitted events (all session-scoped) are:
- `identity.created`
- `session.active_identity.changed`
- `destination.added`
- `destination.removed`
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from aiohttp import web

from ..rns.destinations import DestinationError, DestinationService
from ..rns.identities import IdentityError, IdentityService


if TYPE_CHECKING:
    from ..auth.session import Session


log = logging.getLogger(__name__)


def _session_is_dirty(session) -> tuple[bool, dict]:
    return (
        bool(session.owned_destinations) or bool(session.open_links),
        {
            "owned_destinations": len(session.owned_destinations),
            "open_links": len(session.open_links),
        },
    )


def _active_identity_body(session, identities: IdentityService) -> dict:
    if session.active_identity_hash is None:
        return {"active": False}
    try:
        identity = identities.load(session.active_identity_hash.hex())
    except IdentityError:
        return {"active": False}
    return {"active": True, **identities.info_for(identity).to_dict()}


async def _emit_session(app, session, event: dict) -> None:
    await app["hub"].send_session(session.id, event)


# ---------- REST ----------


async def rest_create_identity(request: web.Request) -> web.Response:
    session = request["session"]
    identities: IdentityService = request.app["identities"]
    _, info = identities.create()
    await _emit_session(
        request.app,
        session,
        {"type": "identity.created", "session_id": session.id, **info.to_dict()},
    )
    return web.json_response(info.to_dict(), status=201)


async def rest_list_identities(request: web.Request) -> web.Response:
    identities: IdentityService = request.app["identities"]
    return web.json_response({"identities": [i.to_dict() for i in identities.list()]})


async def rest_get_identity(request: web.Request) -> web.Response:
    identities: IdentityService = request.app["identities"]
    try:
        identity = identities.load(request.match_info["hash"])
    except IdentityError as e:
        return web.json_response({"error": str(e)}, status=404)
    return web.json_response(identities.info_for(identity).to_dict())


async def rest_get_active_identity(request: web.Request) -> web.Response:
    session = request["session"]
    identities: IdentityService = request.app["identities"]
    return web.json_response(_active_identity_body(session, identities))


async def rest_set_active_identity(request: web.Request) -> web.Response:
    session = request["session"]
    identities: IdentityService = request.app["identities"]

    dirty, counts = _session_is_dirty(session)
    if dirty:
        return web.json_response(
            {"error": "session_dirty", "message": "cannot switch identity with active destinations or links", **counts},
            status=409,
        )

    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid_json"}, status=400)
    hash_hex = body.get("hash") or body.get("identity_hash")
    if not isinstance(hash_hex, str):
        return web.json_response({"error": "missing_hash"}, status=400)
    try:
        identity = identities.load(hash_hex)
    except IdentityError as e:
        return web.json_response({"error": str(e)}, status=404)

    session.active_identity_hash = identity.hash
    info = identities.info_for(identity)
    await _emit_session(
        request.app,
        session,
        {
            "type": "session.active_identity.changed",
            "session_id": session.id,
            "active": True,
            **info.to_dict(),
        },
    )
    return web.json_response({"active": True, **info.to_dict()})


async def rest_clear_active_identity(request: web.Request) -> web.Response:
    session = request["session"]
    dirty, counts = _session_is_dirty(session)
    if dirty:
        return web.json_response(
            {"error": "session_dirty", **counts}, status=409
        )
    session.active_identity_hash = None
    await _emit_session(
        request.app,
        session,
        {"type": "session.active_identity.changed", "session_id": session.id, "active": False},
    )
    return web.json_response({"active": False})


async def rest_list_destinations(request: web.Request) -> web.Response:
    session = request["session"]
    destinations: DestinationService = request.app["destinations"]
    return web.json_response({"destinations": [d.to_dict() for d in destinations.list(session)]})


async def rest_add_destination(request: web.Request) -> web.Response:
    session = request["session"]
    if session.active_identity_hash is None:
        return web.json_response({"error": "no_active_identity"}, status=409)
    identities: IdentityService = request.app["identities"]
    destinations: DestinationService = request.app["destinations"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid_json"}, status=400)

    direction = body.get("direction", "in")
    dtype = body.get("type", "single")
    app_name = body.get("app_name")
    aspects = body.get("aspects", [])
    if not isinstance(aspects, list) or not all(isinstance(a, str) for a in aspects):
        return web.json_response({"error": "aspects must be a list of strings"}, status=400)
    if not isinstance(app_name, str) or not app_name:
        return web.json_response({"error": "app_name is required"}, status=400)

    try:
        identity = identities.load(session.active_identity_hash.hex())
    except IdentityError as e:
        return web.json_response({"error": str(e)}, status=500)

    try:
        _, info = destinations.create(session, identity, direction, dtype, app_name, aspects)
    except DestinationError as e:
        return web.json_response({"error": str(e)}, status=400)

    await _emit_session(
        request.app,
        session,
        {"type": "destination.added", "session_id": session.id, "destination": info.to_dict()},
    )
    return web.json_response(info.to_dict(), status=201)


async def rest_remove_destination(request: web.Request) -> web.Response:
    session = request["session"]
    destinations: DestinationService = request.app["destinations"]
    hash_hex = request.match_info["hash"]
    try:
        info = destinations.remove(session, hash_hex)
    except DestinationError as e:
        return web.json_response({"error": str(e)}, status=404)
    await _emit_session(
        request.app,
        session,
        {"type": "destination.removed", "session_id": session.id, "destination": info.to_dict()},
    )
    return web.json_response(info.to_dict())


# ---------- WS ----------


async def ws_create_identity(conn, msg: dict) -> None:
    if conn.session is None:
        return
    identities: IdentityService = conn.app["identities"]
    _, info = identities.create()
    reply = {"type": "identity.created", "id": msg.get("id"), "session_id": conn.session.id, **info.to_dict()}
    await conn.app["hub"].send_session(conn.session.id, reply)


async def ws_list_identities(conn, msg: dict) -> None:
    identities: IdentityService = conn.app["identities"]
    await conn.send_json(
        {"type": "identity.list.result", "id": msg.get("id"), "identities": [i.to_dict() for i in identities.list()]}
    )


async def ws_get_active_identity(conn, msg: dict) -> None:
    if conn.session is None:
        return
    identities: IdentityService = conn.app["identities"]
    await conn.send_json(
        {"type": "session.active_identity.info", "id": msg.get("id"), **_active_identity_body(conn.session, identities)}
    )


async def ws_set_active_identity(conn, msg: dict) -> None:
    if conn.session is None:
        return
    session = conn.session
    dirty, counts = _session_is_dirty(session)
    if dirty:
        await conn.send_json(
            {"type": "error", "error": "session_dirty", "id": msg.get("id"), **counts}
        )
        return
    identities: IdentityService = conn.app["identities"]
    hash_hex = msg.get("hash") or msg.get("identity_hash")
    if not isinstance(hash_hex, str):
        await conn.send_json({"type": "error", "error": "missing_hash", "id": msg.get("id")})
        return
    try:
        identity = identities.load(hash_hex)
    except IdentityError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    session.active_identity_hash = identity.hash
    info = identities.info_for(identity)
    await conn.app["hub"].send_session(
        session.id,
        {
            "type": "session.active_identity.changed",
            "session_id": session.id,
            "active": True,
            **info.to_dict(),
        },
    )


async def ws_clear_active_identity(conn, msg: dict) -> None:
    if conn.session is None:
        return
    session = conn.session
    dirty, counts = _session_is_dirty(session)
    if dirty:
        await conn.send_json({"type": "error", "error": "session_dirty", "id": msg.get("id"), **counts})
        return
    session.active_identity_hash = None
    await conn.app["hub"].send_session(
        session.id,
        {"type": "session.active_identity.changed", "session_id": session.id, "active": False},
    )


async def ws_list_destinations(conn, msg: dict) -> None:
    if conn.session is None:
        return
    destinations: DestinationService = conn.app["destinations"]
    await conn.send_json(
        {
            "type": "destination.list.result",
            "id": msg.get("id"),
            "destinations": [d.to_dict() for d in destinations.list(conn.session)],
        }
    )


async def ws_add_destination(conn, msg: dict) -> None:
    if conn.session is None:
        return
    session = conn.session
    if session.active_identity_hash is None:
        await conn.send_json({"type": "error", "error": "no_active_identity", "id": msg.get("id")})
        return
    identities: IdentityService = conn.app["identities"]
    destinations: DestinationService = conn.app["destinations"]
    direction = msg.get("direction", "in")
    dtype = msg.get("type_", msg.get("dtype", "single"))
    app_name = msg.get("app_name")
    aspects = msg.get("aspects", [])
    if not isinstance(aspects, list) or not all(isinstance(a, str) for a in aspects):
        await conn.send_json({"type": "error", "error": "invalid_aspects", "id": msg.get("id")})
        return
    if not isinstance(app_name, str) or not app_name:
        await conn.send_json({"type": "error", "error": "invalid_app_name", "id": msg.get("id")})
        return
    try:
        identity = identities.load(session.active_identity_hash.hex())
    except IdentityError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    try:
        _, info = destinations.create(session, identity, direction, dtype, app_name, aspects)
    except DestinationError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.app["hub"].send_session(
        session.id,
        {
            "type": "destination.added",
            "session_id": session.id,
            "id": msg.get("id"),
            "destination": info.to_dict(),
        },
    )


async def ws_remove_destination(conn, msg: dict) -> None:
    if conn.session is None:
        return
    session = conn.session
    destinations: DestinationService = conn.app["destinations"]
    hash_hex = msg.get("hash")
    if not isinstance(hash_hex, str):
        await conn.send_json({"type": "error", "error": "missing_hash", "id": msg.get("id")})
        return
    try:
        info = destinations.remove(session, hash_hex)
    except DestinationError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.app["hub"].send_session(
        session.id,
        {
            "type": "destination.removed",
            "session_id": session.id,
            "id": msg.get("id"),
            "destination": info.to_dict(),
        },
    )


# ---------- Registration ----------


def register(app: web.Application) -> None:
    # REST
    app.router.add_post("/identities", rest_create_identity)
    app.router.add_get("/identities", rest_list_identities)
    app.router.add_get("/identities/{hash}", rest_get_identity)
    app.router.add_get("/session/active-identity", rest_get_active_identity)
    app.router.add_put("/session/active-identity", rest_set_active_identity)
    app.router.add_delete("/session/active-identity", rest_clear_active_identity)
    app.router.add_get("/destinations", rest_list_destinations)
    app.router.add_post("/destinations", rest_add_destination)
    app.router.add_delete("/destinations/{hash}", rest_remove_destination)

    # WS
    router = app["ws_router"]
    router.register("identity.create", ws_create_identity)
    router.register("identity.list", ws_list_identities)
    router.register("session.active_identity.get", ws_get_active_identity)
    router.register("session.active_identity.set", ws_set_active_identity)
    router.register("session.active_identity.clear", ws_clear_active_identity)
    router.register("destination.list", ws_list_destinations)
    router.register("destination.add", ws_add_destination)
    router.register("destination.remove", ws_remove_destination)
