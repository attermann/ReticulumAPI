"""REST and WS handlers for path table queries and path requests.

- GET  /paths[?destination=...&interface=...&max_hops=N] — inspect the path table
- POST /paths/request                                    — synchronous path request
- ws.paths.query                                         — WS path table query
- ws.paths.request                                       — asynchronous WS path request

There is no server-emitted `path.request.received` event; RNS does not
expose a public listener for incoming path-request packets. See the
API reference for the documented limitation.
"""
from __future__ import annotations

from aiohttp import web

from ..rns.paths import PathsError, PathsService


async def rest_list_paths(request: web.Request) -> web.Response:
    svc: PathsService = request.app["paths"]
    destination = request.rel_url.query.get("destination") or None
    interface = request.rel_url.query.get("interface") or None
    max_hops_raw = request.rel_url.query.get("max_hops")
    max_hops = None
    if max_hops_raw:
        try:
            max_hops = int(max_hops_raw)
        except ValueError:
            return web.json_response({"error": "max_hops must be an integer"}, status=400)
    try:
        entries = svc.list_paths(destination=destination, interface=interface, max_hops=max_hops)
    except PathsError as e:
        return web.json_response({"error": str(e)}, status=400)
    return web.json_response({"paths": entries})


async def rest_request_path(request: web.Request) -> web.Response:
    svc: PathsService = request.app["paths"]
    session = request["session"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid_json"}, status=400)
    dest = body.get("destination_hash")
    if not isinstance(dest, str):
        return web.json_response({"error": "destination_hash required"}, status=400)
    timeout = body.get("timeout")
    if timeout is not None and not isinstance(timeout, (int, float)):
        return web.json_response({"error": "timeout must be a number"}, status=400)
    try:
        result = await svc.request_path(session, dest, timeout=timeout)
    except PathsError as e:
        return web.json_response({"error": str(e)}, status=400)
    return web.json_response(result, status=200 if result["found"] else 408)


async def ws_paths_query(conn, msg: dict) -> None:
    svc: PathsService = conn.app["paths"]
    try:
        entries = svc.list_paths(
            destination=msg.get("destination"),
            interface=msg.get("interface"),
            max_hops=msg.get("max_hops"),
        )
    except PathsError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.send_json({"type": "paths.query.result", "id": msg.get("id"), "paths": entries})


async def ws_paths_request(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: PathsService = conn.app["paths"]
    dest = msg.get("destination_hash")
    if not isinstance(dest, str):
        await conn.send_json(
            {"type": "error", "error": "destination_hash required", "id": msg.get("id")}
        )
        return
    timeout = msg.get("timeout")
    if timeout is not None and not isinstance(timeout, (int, float)):
        await conn.send_json(
            {"type": "error", "error": "timeout must be a number", "id": msg.get("id")}
        )
        return
    try:
        result = await svc.request_path(conn.session, dest, timeout=timeout)
    except PathsError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.send_json({"type": "paths.request.result", "id": msg.get("id"), **result})


def register(app: web.Application) -> None:
    app.router.add_get("/paths", rest_list_paths)
    app.router.add_post("/paths/request", rest_request_path)
    router = app["ws_router"]
    router.register("paths.query", ws_paths_query)
    router.register("paths.request", ws_paths_request)
