"""REST and WS handlers for local announces.

- POST /announce                    body: {destination_hash, app_data_b64?}
- ws.announce.send                  params: {destination_hash, app_data_b64?}

The received-announce event (`announce.received`) is emitted globally by
`AnnounceService.GlobalAnnounceHandler`, not by any handler here.
"""
from __future__ import annotations

from aiohttp import web

from ..rns.announces import AnnounceError, AnnounceService


async def rest_announce(request: web.Request) -> web.Response:
    session = request["session"]
    svc: AnnounceService = request.app["announces"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid_json"}, status=400)

    dest_hash = body.get("destination_hash")
    app_data_b64 = body.get("app_data_b64")
    if not isinstance(dest_hash, str):
        return web.json_response({"error": "destination_hash required"}, status=400)
    if app_data_b64 is not None and not isinstance(app_data_b64, str):
        return web.json_response({"error": "app_data_b64 must be a string"}, status=400)

    try:
        result = await svc.send(session, dest_hash, app_data_b64)
    except AnnounceError as e:
        # Not-owned is a 404, everything else is a 400.
        status = 404 if "not owned" in str(e) else 400
        return web.json_response({"error": str(e)}, status=status)
    return web.json_response(result)


async def ws_announce_send(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: AnnounceService = conn.app["announces"]
    dest_hash = msg.get("destination_hash")
    app_data_b64 = msg.get("app_data_b64")
    if not isinstance(dest_hash, str):
        await conn.send_json(
            {"type": "error", "error": "destination_hash required", "id": msg.get("id")}
        )
        return
    if app_data_b64 is not None and not isinstance(app_data_b64, str):
        await conn.send_json(
            {"type": "error", "error": "app_data_b64 must be a string", "id": msg.get("id")}
        )
        return
    try:
        result = await svc.send(conn.session, dest_hash, app_data_b64)
    except AnnounceError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.send_json({"type": "announce.send.result", "id": msg.get("id"), **result})


def register(app: web.Application) -> None:
    app.router.add_post("/announce", rest_announce)
    app["ws_router"].register("announce.send", ws_announce_send)
