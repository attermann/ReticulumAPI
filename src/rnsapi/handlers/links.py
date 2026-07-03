"""REST and WS handlers for Link operations.

REST:
- POST   /links                            open (or reuse) a link
- GET    /links                            list open links
- GET    /links/{id}                       status snapshot
- DELETE /links/{id}                       teardown
- POST   /links/{id}/identify              identify to the remote side
- POST   /links/{id}/data                  send raw data
- POST   /links/{id}/request               send a request (awaits response)

WS:
- link.open, link.close, link.identify, link.send, link.request, link.status

All lifecycle events are session-scoped (see rns/links.py for the full set).
"""
from __future__ import annotations

from aiohttp import web

import asyncio
import logging

from ..rns.links import LinkError, LinksService, _link_snapshot, link_error_reason


log = logging.getLogger(__name__)


# ---------- REST ----------


async def rest_open_link(request: web.Request) -> web.Response:
    svc: LinksService = request.app["links"]
    session = request["session"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid_json"}, status=400)

    identity_hash = body.get("identity_hash")
    destination_hash = body.get("destination_hash")
    app_name = body.get("app_name")
    aspects = body.get("aspects", [])
    auto_identify = bool(body.get("auto_identify", False))
    await_established = bool(body.get("await_established", True))
    establishment_timeout = float(body.get("establishment_timeout", 15.0))
    path_lookup_timeout = float(body.get("path_lookup_timeout", 15.0))

    if identity_hash is not None and not isinstance(identity_hash, str):
        return web.json_response({"error": "identity_hash must be a string"}, status=400)
    if destination_hash is not None and not isinstance(destination_hash, str):
        return web.json_response({"error": "destination_hash must be a string"}, status=400)
    if identity_hash is None and destination_hash is None:
        return web.json_response({"error": "identity_hash or destination_hash required"}, status=400)
    if not isinstance(app_name, str):
        return web.json_response({"error": "app_name required"}, status=400)
    if not isinstance(aspects, list):
        return web.json_response({"error": "aspects must be a list"}, status=400)

    try:
        result = await svc.open_link(
            session,
            identity_hash=identity_hash,
            destination_hash=destination_hash,
            app_name=app_name,
            aspects=aspects,
            auto_identify=auto_identify,
            await_established=await_established,
            establishment_timeout=establishment_timeout,
            path_lookup_timeout=path_lookup_timeout,
        )
    except LinkError as e:
        status = 404 if "no known identity" in str(e) else 400
        if "timed out" in str(e):
            status = 408
        return web.json_response({"error": str(e)}, status=status)
    status_code = 200 if result.get("reused") else 201
    return web.json_response(result, status=status_code)


async def rest_list_links(request: web.Request) -> web.Response:
    svc: LinksService = request.app["links"]
    session = request["session"]
    return web.json_response({"links": svc.list_links(session)})


async def rest_link_status(request: web.Request) -> web.Response:
    svc: LinksService = request.app["links"]
    session = request["session"]
    try:
        info = svc.get_status(session, request.match_info["id"])
    except LinkError as e:
        return web.json_response({"error": str(e)}, status=404)
    return web.json_response(info)


async def rest_close_link(request: web.Request) -> web.Response:
    svc: LinksService = request.app["links"]
    session = request["session"]
    try:
        result = svc.close(session, request.match_info["id"])
    except LinkError as e:
        return web.json_response({"error": str(e)}, status=404)
    return web.json_response(result)


async def rest_identify_link(request: web.Request) -> web.Response:
    svc: LinksService = request.app["links"]
    session = request["session"]
    try:
        result = await svc.identify(session, request.match_info["id"])
    except LinkError as e:
        status = 404 if "unknown link" in str(e) else 400
        return web.json_response({"error": str(e)}, status=status)
    return web.json_response(result)


async def rest_send_data(request: web.Request) -> web.Response:
    svc: LinksService = request.app["links"]
    session = request["session"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid_json"}, status=400)
    data_b64 = body.get("data_b64")
    if not isinstance(data_b64, str):
        return web.json_response({"error": "data_b64 required"}, status=400)
    try:
        result = await svc.send_data(session, request.match_info["id"], data_b64)
    except LinkError as e:
        status = 404 if "unknown link" in str(e) else 400
        return web.json_response({"error": str(e)}, status=status)
    return web.json_response(result)


async def rest_link_request(request: web.Request) -> web.Response:
    svc: LinksService = request.app["links"]
    session = request["session"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid_json"}, status=400)
    path = body.get("path")
    data_b64 = body.get("data_b64")
    timeout = body.get("timeout")
    if not isinstance(path, str):
        return web.json_response({"error": "path required"}, status=400)
    if timeout is not None and not isinstance(timeout, (int, float)):
        return web.json_response({"error": "timeout must be a number"}, status=400)
    try:
        result = await svc.request(session, request.match_info["id"], path, data_b64, timeout, await_response=True)
    except LinkError as e:
        status = 404 if "unknown link" in str(e) else 400
        return web.json_response({"error": str(e)}, status=status)
    if result.get("kind") == "timeout":
        return web.json_response(result, status=408)
    if result.get("kind") == "failed":
        return web.json_response(result, status=502)
    return web.json_response(result)


# ---------- WS ----------


async def ws_open(conn, msg: dict) -> None:
    """WS `link.open` — always async.

    The WebSocket is inherently async, so blocking a reply until the link
    reaches ACTIVE gains nothing that the client can't already get by
    watching for `link.established`. Instead:

    1. Run the pre-flight (validation, identity resolve, cache check,
       Destination construction) synchronously. If it raises, reply with
       `{type: "error", ...}` on this handler.
    2. Send an immediate `link.open.result` ack carrying the computed
       `link_id`. This unblocks the handler loop so the client can queue
       further messages while establishment is in flight.
    3. Kick off the establishment continuation as a background task. It
       emits `link.open.phase` events at each checkpoint, then a terminal
       `link.established` (via the wired RNS callback) or `link.open.failed`
       event, all echoing the client-provided `id`.

    `await_established` is intentionally ignored on WS — the reply is
    always the ack, never the terminal snapshot.
    """
    if conn.session is None:
        return
    svc: LinksService = conn.app["links"]
    client_id = msg.get("id")
    session = conn.session
    hub = conn.app["hub"]

    try:
        entry, is_reused = svc._prepare_open_link(
            session,
            identity_hash=msg.get("identity_hash"),
            destination_hash=msg.get("destination_hash"),
            app_name=msg.get("app_name", ""),
            aspects=msg.get("aspects", []),
            client_id=client_id,
        )
    except LinkError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": client_id})
        return

    # Immediate ack — includes the computed link_id so the client can start
    # correlating phase / terminal events right away.
    prelim_snapshot = _link_snapshot(entry.link, entry.destination_hash, entry.aspect)
    await conn.send_json(
        {
            "type": "link.open.result",
            "id": client_id,
            "sent": True,
            "reused": is_reused,
            **prelim_snapshot,
        }
    )

    auto_identify = bool(msg.get("auto_identify", False))
    establishment_timeout = float(msg.get("establishment_timeout", 15.0))
    path_lookup_timeout = float(msg.get("path_lookup_timeout", 15.0))

    async def _on_phase(phase: str) -> None:
        await hub.send_session(
            session.id,
            {
                "type": "link.open.phase",
                "session_id": session.id,
                "id": client_id,
                "link_id": entry.destination_hash.hex(),
                "destination_hash": entry.destination_hash.hex(),
                "aspect": entry.aspect,
                "phase": phase,
            },
        )

    async def _run_continuation() -> None:
        try:
            await svc._continue_open_link(
                session,
                entry,
                is_reused=is_reused,
                auto_identify=auto_identify,
                await_established=True,
                establishment_timeout=establishment_timeout,
                path_lookup_timeout=path_lookup_timeout,
                on_phase=_on_phase,
            )
            # Success: `link.established` was already emitted by the wired
            # RNS callback (with the echoed client id).
        except LinkError as e:
            await hub.send_session(
                session.id,
                {
                    "type": "link.open.failed",
                    "session_id": session.id,
                    "id": client_id,
                    "link_id": entry.destination_hash.hex(),
                    "destination_hash": entry.destination_hash.hex(),
                    "aspect": entry.aspect,
                    "reason": link_error_reason(e),
                    "detail": str(e),
                },
            )
        except Exception:
            log.exception("link.open continuation failed for %s", entry.destination_hash.hex())
            await hub.send_session(
                session.id,
                {
                    "type": "link.open.failed",
                    "session_id": session.id,
                    "id": client_id,
                    "link_id": entry.destination_hash.hex(),
                    "destination_hash": entry.destination_hash.hex(),
                    "aspect": entry.aspect,
                    "reason": "internal",
                    "detail": "internal error",
                },
            )

    asyncio.create_task(_run_continuation())


async def ws_close(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: LinksService = conn.app["links"]
    link_id = msg.get("link_id")
    if not isinstance(link_id, str):
        await conn.send_json({"type": "error", "error": "link_id required", "id": msg.get("id")})
        return
    try:
        result = svc.close(conn.session, link_id)
    except LinkError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.send_json({"type": "link.close.result", "id": msg.get("id"), **result})


async def ws_identify(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: LinksService = conn.app["links"]
    link_id = msg.get("link_id")
    if not isinstance(link_id, str):
        await conn.send_json({"type": "error", "error": "link_id required", "id": msg.get("id")})
        return
    try:
        result = await svc.identify(conn.session, link_id)
    except LinkError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.send_json({"type": "link.identify.result", "id": msg.get("id"), **result})


async def ws_send(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: LinksService = conn.app["links"]
    link_id = msg.get("link_id")
    data_b64 = msg.get("data_b64")
    if not isinstance(link_id, str) or not isinstance(data_b64, str):
        await conn.send_json({"type": "error", "error": "link_id and data_b64 required", "id": msg.get("id")})
        return
    try:
        result = await svc.send_data(conn.session, link_id, data_b64)
    except LinkError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.send_json({"type": "link.send.result", "id": msg.get("id"), **result})


async def ws_request(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: LinksService = conn.app["links"]
    link_id = msg.get("link_id")
    path = msg.get("path")
    data_b64 = msg.get("data_b64")
    timeout = msg.get("timeout")
    if not isinstance(link_id, str) or not isinstance(path, str):
        await conn.send_json({"type": "error", "error": "link_id and path required", "id": msg.get("id")})
        return
    # WS variant does not wait synchronously — the response arrives on a
    # link.request.response / link.request.failed event, which echoes the
    # client-provided `id` so multiple in-flight requests can be correlated.
    try:
        await svc.request(
            conn.session,
            link_id,
            path,
            data_b64,
            timeout,
            await_response=False,
            client_id=msg.get("id"),
        )
    except LinkError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.send_json({"type": "link.request.result", "id": msg.get("id"), "sent": True, "path": path})


async def ws_status(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: LinksService = conn.app["links"]
    link_id = msg.get("link_id")
    if not isinstance(link_id, str):
        await conn.send_json({"type": "error", "error": "link_id required", "id": msg.get("id")})
        return
    try:
        info = svc.get_status(conn.session, link_id)
    except LinkError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.send_json({"type": "link.status.result", "id": msg.get("id"), **info})


async def ws_list(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: LinksService = conn.app["links"]
    await conn.send_json(
        {"type": "link.list.result", "id": msg.get("id"), "links": svc.list_links(conn.session)}
    )


def register(app: web.Application) -> None:
    app.router.add_post("/links", rest_open_link)
    app.router.add_get("/links", rest_list_links)
    app.router.add_get("/links/{id}", rest_link_status)
    app.router.add_delete("/links/{id}", rest_close_link)
    app.router.add_post("/links/{id}/identify", rest_identify_link)
    app.router.add_post("/links/{id}/data", rest_send_data)
    app.router.add_post("/links/{id}/request", rest_link_request)

    router = app["ws_router"]
    router.register("link.open", ws_open)
    router.register("link.close", ws_close)
    router.register("link.identify", ws_identify)
    router.register("link.send", ws_send)
    router.register("link.request", ws_request)
    router.register("link.status", ws_status)
    router.register("link.list", ws_list)
